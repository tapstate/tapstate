package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.TransactionOptions;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.migration.MigrationRunner;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StorageBinding;
import com.mongodb.AuthenticationMechanism;
import java.io.ByteArrayInputStream;
import java.net.URLDecoder;
import java.util.Locale;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.bson.Document;

/**
 * The store connection substrate: opens a Mongo client from the externalized settings and verifies
 * the target is reachable and supports multi-document transactions through a replica set or
 * sharded router. It is the assembly root's driver-free handle on
 * the store — the public surface exposes only java types, so no driver type escapes this module
 * (rule R3). Driver failures are translated into {@code store.*} coded diagnostics.
 *
 * <p>This is the connection substrate only; the production compare-and-swap store implementation
 * lands later.
 */
public final class MongoConnection implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(MongoConnection.class);

    /** The database used when the connection URI names none. */
    private static final String DEFAULT_DATABASE = "tapstate";

    /** The longest application name the driver sends, in UTF-8 bytes. */
    private static final int MAX_APPLICATION_NAME_BYTES = 128;

    private final MongoConnectionSettings settings;
    private final String explicitDatabase;
    private final String caPem;
    private final boolean strictStorage;
    private final boolean userAtlas;
    private MongoClient client;
    private String databaseName;
    private SourceConfigKeyringHandle sourceConfigKeyring;

    public MongoConnection(MongoConnectionSettings settings) {
        this(settings, null, null, false, false);
    }

    /** A separate explicit maintenance owner uses the same verified CONFIG endpoint. */
    MongoConnectionSettings configurationSettings() { return settings; }

    /** A configuration domain preserves deployment authentication and verifies peers whenever TLS is enabled. */
    public static MongoConnection forConfiguration(MongoConnectionSettings settings) {
        return new MongoConnection(settings, null, null, true, false);
    }

    /** The stored database and CA contents are authoritative; no temporary trust file is required. */
    public static MongoConnection forCache(StorageBinding.Connection binding, Duration timeout, String processId) {
        Objects.requireNonNull(binding, "binding");
        if (!validDatabase(binding.database())) throw unsupportedStorage();
        return new MongoConnection(new MongoConnectionSettings(binding.uri(), null, timeout, processId),
                binding.database(), binding.caPem(), true, binding.kind() == StorageBinding.Connection.Kind.ATLAS);
    }

    private MongoConnection(MongoConnectionSettings settings, String explicitDatabase, String caPem,
            boolean strictStorage, boolean userAtlas) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.explicitDatabase = explicitDatabase;
        this.caPem = caPem;
        this.strictStorage = strictStorage;
        this.userAtlas = userAtlas;
        if (strictStorage) validateStorageOptions(settings.uri());
    }

    private static boolean validDatabase(String database) {
        return database != null && !database.isBlank() && database.getBytes(StandardCharsets.UTF_8).length <= 63
                && MongoDatabaseNames.isValid(database);
    }

    String selectedDatabase(ConnectionString connectionString) {
        String selected = explicitDatabase == null ? connectionString.getDatabase() : explicitDatabase;
        if (strictStorage && !validDatabase(selected)) throw unsupportedStorage();
        return selected == null ? DEFAULT_DATABASE : selected;
    }

    private static void validateStorageOptions(String uri) {
        int query = uri.indexOf('?');
        if (query < 0) return;
        try {
            for (String option : uri.substring(query + 1).split("[&;]")) {
                String[] pair = option.split("=", 2);
                String key = URLDecoder.decode(pair[0], StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
                String value = pair.length == 2 ? URLDecoder.decode(pair[1], StandardCharsets.UTF_8) : "";
                if ((java.util.Set.of("tlsinsecure", "tlsallowinvalidhostnames", "sslinvalidhostnameallowed",
                        "tlsallowinvalidcertificates").contains(key) && !value.equalsIgnoreCase("false"))
                        || java.util.Set.of("tlscertificatekeyfile", "tlscertificatekeyfilepassword", "tlscafile").contains(key)) {
                    throw unsupportedStorage();
                }
            }
        } catch (IllegalArgumentException invalid) {
            throw unsupportedStorage();
        }
    }

    private static TapstateException unsupportedStorage() {
        return new TapstateException(StoreError.STORAGE_CONNECTION_UNSUPPORTED, Map.of(), null);
    }

    /**
     * Opens the store and brings its system data to the shape this build expects, in that order, before
     * the connection is handed to anybody. Everything that touches the store gets its handle from here,
     * so putting the migration inside makes every one of those things come after it without any wiring
     * having to say so — and a migration that cannot finish stops the process rather than letting half
     * of it start.
     */
    public void verify() {
        verifyConnectivity();
        try {
            MigrationRunner.migrate(database());
            sourceConfigKeyring = new SourceConfigKeyringHandle(new SourceConfigKeyringStore(client, database()));
            verifySourceConfigStorage();
        } catch (RuntimeException e) {
            close();
            throw e;
        }
    }

    /**
     * Proves access to deployment-owned databases using this connection's existing identity.
     * Collection handles and ping do not check authorization. The probe exercises real CRUD inside
     * an aborted transaction, so no probe record or namespace is committed on success or failure.
     * User Pipeline databases are never supplied by the assembly to this method.
     */
    public void verifyDeploymentDatabases(List<String> databases) {
        Objects.requireNonNull(databases, "databases");
        for (String database : databases) {
            if (!MongoDatabaseNames.isValid(database)) {
                throw new IllegalArgumentException("a deployment database name is invalid");
            }
            UUID probeId = UUID.randomUUID();
            String id = probeId.toString();
            var collection = SystemCollections.accessProbeOn(client().getDatabase(database), probeId);
            try (var session = client().startSession()) {
                session.startTransaction(TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
                        .writeConcern(WriteConcern.MAJORITY.withJournal(true))
                        .timeout(Math.max(1L, settings.serverSelectionTimeout().toMillis()), TimeUnit.MILLISECONDS).build());
                try {
                    collection.insertOne(session, new Document("_id", id).append("value", "initial"));
                    Document read = collection.find(session, new Document("_id", id)).first();
                    if (read == null || !"initial".equals(read.getString("value"))
                            || collection.updateOne(session, new Document("_id", id),
                                    new Document("$set", new Document("value", "updated"))).getModifiedCount() != 1
                            || collection.deleteOne(session, new Document("_id", id)).getDeletedCount() != 1) {
                        throw new TapstateException(StoreError.DATABASE_ACCESS_FAILED, Map.of("database", database), null);
                    }
                } catch (RuntimeException | Error failure) {
                    // Preserve a programmer defect rather than allowing cleanup to replace it.
                    try { if (session.hasActiveTransaction()) session.abortTransaction(); }
                    catch (MongoException abortFailure) { /* An uncommitted failed probe must never be retried. */ }
                    throw failure;
                }
                session.abortTransaction();
            } catch (MongoException unavailable) {
                // Driver messages can contain credentials. Only the already validated database name travels out.
                throw new TapstateException(StoreError.DATABASE_ACCESS_FAILED, Map.of("database", database), null);
            }
        }
    }

    /**
     * Exercises the storage permission set on an isolated namespace, including index creation and
     * multi-document transactional CRUD. The transaction is always aborted; no probe namespace or
     * document becomes durable, including when authorization fails halfway through the check.
     */
    public void verifyStoragePermissions() {
        UUID probeId = UUID.randomUUID();
        var collection = SystemCollections.accessProbeOn(database(), probeId);
        String id = probeId.toString();
        try (var session = client().startSession()) {
            session.startTransaction(TransactionOptions.builder().readConcern(ReadConcern.LOCAL)
                    .writeConcern(WriteConcern.MAJORITY.withJournal(true))
                    .timeout(Math.max(1L, settings.serverSelectionTimeout().toMillis()), TimeUnit.MILLISECONDS).build());
            try {
                collection.createIndex(session, new Document("value", 1),
                        new com.mongodb.client.model.IndexOptions().name("storage_probe_value").unique(true));
                collection.insertOne(session, new Document("_id", id).append("value", "initial"));
                Document read = collection.find(session, new Document("_id", id)).first();
                if (read == null || !"initial".equals(read.get("value"))
                        || collection.updateOne(session, new Document("_id", id),
                        new Document("$set", new Document("value", "updated"))).getModifiedCount() != 1
                        || collection.deleteOne(session, new Document("_id", id)).getDeletedCount() != 1) {
                    throw new TapstateException(StoreError.DATABASE_ACCESS_FAILED,
                            Map.of("database", databaseName), null);
                }
            } finally {
                if (session.hasActiveTransaction()) session.abortTransaction();
            }
        } catch (MongoException refused) {
            throw new TapstateException(StoreError.DATABASE_ACCESS_FAILED, Map.of("database", databaseName), null);
        }
    }

    /** Verifies existing Source envelopes and logical identity before handing out the store connection. */
    private void verifySourceConfigStorage() {
        EncryptedArtifactCodec codec = new EncryptedArtifactCodec(sourceConfigKeyring);
        StoreIo.run(() -> {
            Document sources = new Document("$or", List.of(
                    new Document("kind", "source"), new Document("body.kind", "source")));
            try (var cursor = SystemCollections.ARTIFACTS.on(database()).find(sources).iterator()) {
                while (cursor.hasNext()) {
                    Document document = cursor.next();
                    if (!(codec.decode(document) instanceof SourceResource)) {
                        throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                                Map.of("id", String.valueOf(document.get("_id")), "field", "body"), null);
                    }
                }
            }
        });
    }

    /**
     * Opens the client and verifies the store is reachable and supports transactions, and nothing further.
     * Raises a {@code store.unreachable} coded diagnostic if the target cannot be reached within the
     * configured server-selection timeout, {@code io.store-unauthorized} for rejected database
     * credentials, or {@code store.not-replica-set} if it is reached but is a standalone server. On success the client is held open for the process lifetime.
     *
     * <p>Separate from {@link #verify()} for the read-only inspection commands, whose whole point is to
     * work on a store a normal start refuses on: they have to reach it without changing it.
     */
    public void verifyConnectivity() {
        // A repeated verify() must not orphan a previously opened client (its pool and monitor threads).
        close();

        ConnectionString connectionString = parseConnectionString(settings.uri(), ConnectionString::new);
        // The store database is the one named in the URI, falling back to the default when it names
        // none. Resolved here from the same URI the client uses, so database() reflects the target.
        this.databaseName = selectedDatabase(connectionString);
        String target = String.join(",", connectionString.getHosts());
        MongoClientSettings clientSettings = buildClientSettings(connectionString);

        MongoClient opened = null;
        Document hello;
        try {
            opened = MongoClients.create(clientSettings);
            hello = opened.getDatabase("admin").runCommand(new Document("hello", 1));
        } catch (MongoException e) {
            if (opened != null) {
                opened.close();
            }
            // Classify explicit server authentication results. The driver also wraps SASL socket failures
            // in MongoSecurityException; those remain connectivity failures.
            var authenticationFailure = e instanceof com.mongodb.MongoSecurityException
                    && e.getCause() instanceof com.mongodb.MongoCommandException command ? command : null;
            var failure = authenticationFailure != null && authenticationFailure.getErrorCode() == 334 ? unsupportedStorage()
                    : authenticationFailure != null && (authenticationFailure.getErrorCode() == 18 || authenticationFailure.getErrorCode() == 8000) ? StoreIo.coded(e)
                    : new TapstateException(StoreError.UNREACHABLE, Map.of("target", target), null);
            // Driver messages and causes can contain credentials. Log only bounded type and numeric metadata.
            Throwable cause = e.getCause();
            LOG.warn("Storage connection check failed [code={}, driver_type={}, driver_code={}, cause_type={}, cause_code={}]",
                    failure.code().code(), e.getClass().getSimpleName(), e.getCode(),
                    cause == null ? "none" : cause.getClass().getSimpleName(),
                    cause instanceof MongoException mongo ? mongo.getCode() : -1);
            throw failure;
        }
        // A replica-set member reports setName; a sharded router reports msg=isdbgrid.
        // A standalone server reports neither and cannot host checkpoint transactions.
        if (!isTransactionCapableTopology(hello)) {
            opened.close();
            throw new TapstateException(StoreError.NOT_REPLICA_SET, Map.of("target", target), null);
        }
        this.client = opened;
    }

    static boolean isTransactionCapableTopology(Document hello) {
        return hello.containsKey("setName") || "isdbgrid".equals(hello.get("msg"));
    }

    /** Classifies SRV/TXT DNS lookup failures before a client exists, without echoing URI userinfo. */
    static ConnectionString parseConnectionString(String uri, Function<String, ConnectionString> parser) {
        try {
            return parser.apply(uri);
        } catch (MongoException dnsOrConfigurationFailure) {
            throw new TapstateException(StoreError.UNREACHABLE,
                    Map.of("target", safeHost(uri)), null);
        } catch (IllegalArgumentException malformedUri) {
            throw new TapstateException(StoreError.INVALID_URI, Map.of(), null);
        }
    }

    /** The driver's effective authentication database, including SRV/TXT, or null without credentials. */
    public static String authenticationSource(String uri) {
        var credential = parseConnectionString(uri, ConnectionString::new).getCredential();
        return credential == null ? null : credential.getSource();
    }

    private static String safeHost(String uri) {
        try {
            String host = URI.create(uri).getHost();
            return host == null ? "<unresolved>" : host;
        } catch (IllegalArgumentException malformed) {
            return "<unresolved>";
        }
    }

    /**
     * What schema version the store says its system data is at, and what this build has that has not
     * run against it. Read live rather than reported from the build's own highest changeset: after a
     * successful start the two agree by construction, and if they ever do not, this shows it instead of
     * asserting it.
     */
    public MigrationRunner.Status systemDataStatus() {
        return MigrationRunner.inspect(database());
    }

    /** Every changeset this build carries, in the order they run. Named for the inspection commands. */
    public List<String> knownChangeSets() {
        return MigrationRunner.changeSets().stream().map(ChangeSet::changeSetName).toList();
    }

    /** What a changeset that has not run would do, without doing any of it. */
    public List<String> systemDataDryRun() {
        MongoDatabase database = database();
        int installed = systemDataStatus().installed();
        return MigrationRunner.changeSets().stream()
                .filter(changeSet -> changeSet.version() > installed)
                .map(changeSet -> changeSet.changeSetName() + ": " + changeSet.dryRunSummary(database))
                .toList();
    }

    /**
     * The verified store database — the one named in the connection URI (falling back to the default
     * when the URI names none). Package-private on purpose: the store adapter binds its collections on
     * it, so a driver type stays inside the module and never reaches the module's public surface.
     * Must be called after {@link #verify()} has opened the client.
     */
    MongoDatabase database() {
        if (client == null) {
            // Called before verify() opened the client: an assembly ordering error, i.e. a programmer
            // bug. It crashes bare with a stack rather than being laundered into a coded diagnostic.
            throw new IllegalStateException("store connection not verified");
        }
        return client.getDatabase(databaseName);
    }

    /**
     * The verified store client — the source of the sessions a store opens for its multi-document
     * transactions. Package-private on purpose, like {@link #database()}: a driver type stays inside the
     * module and never reaches the module's public surface. Must be called after {@link #verify()} has
     * opened the client.
     */
    MongoClient client() {
        if (client == null) {
            throw new IllegalStateException("store connection not verified");
        }
        return client;
    }

    /** The verified keyring view used by the artifact store after migration has completed. */
    public SourceConfigKeyringHandle sourceConfigKeyring() {
        if (sourceConfigKeyring == null) {
            throw new IllegalStateException("Source config keyring not verified");
        }
        return sourceConfigKeyring;
    }

    /** The database named in the connection string, or the default when it names none. */
    static String resolveDatabaseName(ConnectionString connectionString) {
        return connectionString.getDatabase() != null ? connectionString.getDatabase() : DEFAULT_DATABASE;
    }

    /**
     * Builds the driver client settings. TLS is opt-in: it is used only when the URI asks for it
     * ({@code ssl=true} / {@code tls=true}), and its settings are then taken from the URI as-is; the
     * default is a plaintext connection. An explicit CA file trusts a self-signed chain when TLS is on.
     */
    MongoClientSettings buildClientSettings(ConnectionString connectionString) {
        MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .applyToClusterSettings(b ->
                        b.serverSelectionTimeout(settings.serverSelectionTimeout().toMillis(), TimeUnit.MILLISECONDS));
        // TLS follows the URI (applyConnectionString already applied it). A CA file is meaningful only
        // when TLS is on, so it is applied then and ignored otherwise: it trusts the self-signed chain
        // without touching the JVM-wide trust store.
        boolean useTls = builder.build().getSslSettings().isEnabled();
        if (strictStorage && (Boolean.TRUE.equals(connectionString.getSslInvalidHostnameAllowed())
                || (!useTls && (userAtlas || caPem != null || settings.tlsCaFile() != null)))) throw unsupportedStorage();
        if (userAtlas) {
            var credential = connectionString.getCredential();
            if (credential == null || credential.getPassword() == null || !"admin".equals(credential.getSource())
                    || (credential.getAuthenticationMechanism() != null
                    && credential.getAuthenticationMechanism() != AuthenticationMechanism.SCRAM_SHA_256
                    && credential.getAuthenticationMechanism() != AuthenticationMechanism.SCRAM_SHA_1)) throw unsupportedStorage();
            // Preserve an explicit mechanism. Otherwise let the driver negotiate from server-advertised SCRAM support.
            builder.credential(credential);
        }
        if (useTls && caPem != null) {
            builder.applyToSslSettings(b -> b.context(buildPemSslContext(caPem)));
        } else if (useTls && settings.tlsCaFile() != null) {
            builder.applyToSslSettings(b -> b.context(strictStorage
                    ? buildPemSslContext(readTrustFile(settings.tlsCaFile())) : buildSslContext(settings.tlsCaFile())));
        }
        // Named for this start of the process, so that once it has gone - killed halfway through a write, say -
        // whoever is left can find the transactions it left open and end them; see endTransactionsLeftOpenBy.
        if (settings.processId() != null) {
            builder.applicationName(applicationName(connectionString.getApplicationName(), settings.processId()));
        }
        return builder.build();
    }

    /**
     * The part of a client's application name that says which process it belongs to. It ends the name, and
     * is what {@link #endTransactionsLeftOpenBy} looks for.
     */
    public static String processTag(String processId) {
        return "tapstate/" + Objects.requireNonNull(processId, "processId");
    }

    /**
     * The application name of a client of the process {@code processId}: the name the URI gives, when it gives
     * one, then the process's tag, within the length the driver sends. The tag goes last and whole, so it is the
     * name the URI gives that is cut short when there is not room for both.
     */
    static String applicationName(String given, String processId) {
        String tag = processTag(processId);
        if (given == null || given.isEmpty()) {
            return tag;
        }
        String kept = withinBytes(given, MAX_APPLICATION_NAME_BYTES - tag.getBytes(StandardCharsets.UTF_8).length - 1);
        return kept.isEmpty() ? tag : kept + " " + tag;
    }

    /** The longest start of {@code text} that is at most {@code bytes} long in UTF-8, cut between characters. */
    private static String withinBytes(String text, int bytes) {
        int used = 0;
        int end = 0;
        while (end < text.length()) {
            int codePoint = text.codePointAt(end);
            int size = new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8).length;
            if (used + size > bytes) {
                break;
            }
            used += size;
            end += Character.charCount(codePoint);
        }
        return text.substring(0, end);
    }

    /**
     * Ends every transaction a client of the process {@code processId} left open at the store, and answers how
     * many it ended.
     *
     * <p>What a transaction has written stays held until it ends, and one whose process stopped halfway through
     * it - killed, or cut off - ends only when the store gives up on it, a minute later by default. Until then
     * every other write to what it touched waits, or retries, behind it: the progress a sink reports, a
     * capture's claim being renewed or taken over, a change being appended to the durable log. A process that
     * has gone cannot end it, so whoever is left does, as soon as it knows the process has gone. The store lists
     * the transactions sitting open for each client, by the name the client connected under, and this ends
     * those of that process's clients. What they wrote is thrown away, which is all that could have become of
     * it: nobody is left to commit it.
     *
     * <p>Only this user's own sessions are listed and ended, which asks for no privilege beyond connecting, and
     * every process of a deployment connects to its store as the same user.
     */
    public int endTransactionsLeftOpenBy(String processId) {
        Objects.requireNonNull(processId, "processId");
        MongoDatabase admin = client().getDatabase("admin");
        Document leftOpen = new Document("appName",
                        new Document("$regex", "(^| )" + Pattern.quote(processTag(processId)) + "$"))
                .append("transaction", new Document("$exists", true));
        List<Document> sessions = new ArrayList<>();
        StoreIo.run(() -> admin.aggregate(List.of(
                        new Document("$currentOp", new Document("allUsers", false).append("idleSessions", true)),
                        new Document("$match", leftOpen)))
                .forEach(operation -> {
                    if (operation.get("lsid") instanceof Document session && session.get("id") != null) {
                        sessions.add(new Document("id", session.get("id")));
                    }
                }));
        if (sessions.isEmpty()) {
            return 0;
        }
        StoreIo.run(() -> admin.runCommand(new Document("killSessions", sessions)));
        return sessions.size();
    }

    private static String readTrustFile(String path) {
        try (InputStream input = Files.newInputStream(Path.of(path))) {
            byte[] content = input.readNBytes(65_537);
            if (content.length > 65_536) throw invalidTrust();
            return new String(content, StandardCharsets.UTF_8);
        } catch (IOException invalid) {
            throw new TapstateException(StoreError.TLS_CA_UNREADABLE, Map.of("path", path), null);
        }
    }

    static SSLContext buildPemSslContext(String pem) {
        if (pem == null || pem.isBlank() || pem.getBytes(StandardCharsets.UTF_8).length > 65_536
                || pem.toUpperCase(Locale.ROOT).contains("PRIVATE KEY")) throw invalidTrust();
        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
            trust.load(null, null);
            int count = 0;
            try (InputStream input = new ByteArrayInputStream(pem.getBytes(StandardCharsets.UTF_8))) {
                for (Certificate certificate : factory.generateCertificates(input)) {
                    trust.setCertificateEntry("ca-" + count++, certificate);
                }
            }
            if (count == 0) throw invalidTrust();
            TrustManagerFactory managers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            managers.init(trust);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, managers.getTrustManagers(), null);
            return context;
        } catch (GeneralSecurityException | IOException invalid) {
            throw invalidTrust();
        }
    }

    private static TapstateException invalidTrust() {
        return new TapstateException(StoreError.TLS_TRUST_INVALID, Map.of(), null);
    }

    /**
     * Builds a TLS context trusting only the CA certificate(s) in {@code caFile} (a PEM file). This is
     * how the local development self-signed chain is trusted without touching the JVM-wide trust store.
     */
    private static SSLContext buildSslContext(String caFile) {
        try {
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, buildTrustManagers(caFile), null);
            return context;
        } catch (GeneralSecurityException e) {
            throw new TapstateException(StoreError.TLS_CA_UNREADABLE, Map.of("path", caFile), e);
        }
    }

    /**
     * Builds trust managers trusting only the CA certificate(s) in {@code caFile} (a PEM file) — the
     * self-signed local development chain — kept out of the JVM-wide trust store.
     */
    static TrustManager[] buildTrustManagers(String caFile) {
        try {
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
            trust.load(null, null);
            int index = 0;
            try (InputStream in = Files.newInputStream(Path.of(caFile))) {
                for (Certificate certificate : factory.generateCertificates(in)) {
                    trust.setCertificateEntry("ca-" + index++, certificate);
                }
            }
            if (index == 0) {
                // A readable but cert-less PEM parses to zero anchors without throwing; a trust store
                // with nothing in it would silently distrust every server, so it is an unusable CA file.
                throw new TapstateException(StoreError.TLS_CA_UNREADABLE, Map.of("path", caFile), null);
            }
            TrustManagerFactory trustManagers =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagers.init(trust);
            return trustManagers.getTrustManagers();
        } catch (GeneralSecurityException | IOException e) {
            // The CA file is operator-supplied config; an unreadable or malformed one is a
            // diagnosable misconfiguration, not a programmer bug. The path is a filesystem path,
            // not a credential, so it is safe to echo back.
            throw new TapstateException(StoreError.TLS_CA_UNREADABLE, Map.of("path", caFile), e);
        }
    }

    @Override
    public void close() {
        sourceConfigKeyring = null;
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
