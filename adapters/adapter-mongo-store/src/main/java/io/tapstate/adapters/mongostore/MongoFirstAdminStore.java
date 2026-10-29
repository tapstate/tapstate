package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.FirstAdminStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.spi.store.User;
import io.tapstate.spi.store.UserStore;
import org.bson.Document;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** Permanent OP first-administrator facts and ordinary configuration User writes share the same client. */
final class MongoFirstAdminStore implements FirstAdminStore {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private final MongoClient client;
    private final MongoStorageBindingStore bindings;
    private final MongoCollection<Document> setup;
    private final MongoCollection<Document> bootstrap;
    private final MongoCollection<Document> users;
    private final MongoCollection<Document> versions;
    private final Runnable requireActive;

    MongoFirstAdminStore(MongoClient client, MongoDatabase database, MongoStorageBindingStore bindings) {
        this.client = Objects.requireNonNull(client, "client");
        this.bindings = Objects.requireNonNull(bindings, "bindings");
        this.setup = SystemCollections.STORAGE_SETUP.on(database);
        this.bootstrap = SystemCollections.OP_BOOTSTRAP.on(database);
        this.users = SystemCollections.CONFIG_USERS.on(database);
        this.versions = SystemCollections.CONFIGURATION_VERSION.on(database);
        this.requireActive = () -> {};
    }

    private MongoFirstAdminStore(MongoFirstAdminStore original, Runnable guard) {
        this.client = original.client;
        this.bindings = original.bindings;
        this.setup = original.setup;
        this.bootstrap = original.bootstrap;
        this.users = original.users;
        this.versions = original.versions;
        this.requireActive = () -> { original.requireActive.run(); guard.run(); };
    }

    @Override public FirstAdminStore guardedBy(Runnable guard) {
        return new MongoFirstAdminStore(this, Objects.requireNonNull(guard, "guard"));
    }

    @Override public Optional<Status> load() {
        return StoreIo.call(() -> transaction(this::load));
    }

    @Override public Status create(StorageSetupScope expected, User administrator) {
        Objects.requireNonNull(expected, "expected");
        Objects.requireNonNull(administrator, "administrator");
        if (!"admin".equals(administrator.role())) throw refused();
        return StoreIo.call(() -> transaction(session -> {
            Status pending = load(session).orElseThrow(MongoFirstAdminStore::refused);
            if (pending.phase() != Phase.PENDING || !expected.equals(pending.scope())) throw refused();
            ConfigurationVersion.advance(session, versions, client);
            users.insertOne(session, MongoUserStore.toDocument(administrator));
            Document filter = new Document("_id", "cluster").append("phase", Phase.PENDING.name())
                    .append("clusterId", expected.clusterId()).append("installationId", expected.installationId())
                    .append("operationId", expected.operationId()).append("generation", expected.generation())
                    .append("bindingVersion", pending.bindingVersion());
            if (bootstrap.updateOne(session, filter, new Document("$set", new Document("phase", Phase.COMPLETE.name())
                    .append("username", administrator.username()))).getMatchedCount() != 1) throw refused();
            return new Status(expected, pending.bindingVersion(), Phase.COMPLETE, administrator.username());
        }));
    }

    UserStore users() { return new Users(); }

    private final class Users implements UserStore {
        @Override public Optional<User> find(String username) {
            Objects.requireNonNull(username, "username");
            return StoreIo.call(() -> transaction(session -> {
                load(session).orElseThrow(MongoFirstAdminStore::refused);
                return Optional.ofNullable(users.find(session, new Document("_id", username)).first()).map(MongoFirstAdminStore::decodeUser);
            }));
        }

        @Override public boolean isEmpty() {
            return StoreIo.call(() -> transaction(session -> {
                load(session).orElseThrow(MongoFirstAdminStore::refused);
                return users.find(session).limit(1).first() == null;
            }));
        }

        @Override public void save(User user) {
            Objects.requireNonNull(user, "user");
            StoreIo.run(() -> transaction(session -> {
                Status state = load(session).orElseThrow(MongoFirstAdminStore::refused);
                // Only the dedicated insert command can create the first account, even for legacy callers.
                if (state.phase() != Phase.COMPLETE) throw refused();
                ConfigurationVersion.advance(session, versions, client);
                users.replaceOne(session, new Document("_id", user.username()), MongoUserStore.toDocument(user),
                        new ReplaceOptions().upsert(true));
                return null;
            }));
        }
    }

    Optional<Status> load(ClientSession session) {
        Optional<StorageBinding> bound = bindings.loadForTransaction(session);
        Document row = bootstrap.find(session, new Document("_id", "cluster")).first();
        if (bound.isEmpty() || bound.orElseThrow().mode() == StorageBinding.Mode.CLOUD) {
            requireNoLocalAccounts(session, bootstrap, users);
            return Optional.empty();
        }
        Document stage = setup.find(session, new Document("_id", "cluster")).first();
        if (!"COMPLETE".equals(stage.get("phase"))) {
            requireNoLocalAccounts(session, bootstrap, users);
            return Optional.empty();
        }
        if (row == null || !row.keySet().equals(Set.of("_id", "formatVersion", "clusterId", "installationId", "operationId",
                "generation", "bindingVersion", "phase", "username")) || !Integer.valueOf(1).equals(row.get("formatVersion"))
                || !stage.get("clusterId").equals(row.get("clusterId")) || !stage.get("installationId").equals(row.get("installationId"))
                || !stage.get("operationId").equals(row.get("operationId")) || !stage.get("generation").equals(row.get("generation"))
                || !Long.valueOf(bound.orElseThrow().bindingVersion()).equals(row.get("bindingVersion"))) throw unreadable();
        Status result;
        try {
            var scope = new StorageSetupScope(row.getString("clusterId"), row.getString("installationId"),
                    row.getString("operationId"), row.getLong("generation"));
            result = new Status(scope, row.getLong("bindingVersion"), Phase.valueOf(row.getString("phase")), row.getString("username"));
        } catch (NullPointerException | IllegalArgumentException | ClassCastException invalid) {
            throw unreadable();
        }
        if (result.phase() == Phase.PENDING) {
            if (users.find(session).limit(1).first() != null) throw unreadable();
        } else {
            Document administrator = users.find(session, new Document("_id", result.username())).first();
            if (administrator == null) throw unreadable();
            decodeUser(administrator);
        }
        return Optional.of(result);
    }

    /** Called only inside the original configuration setup-completion transaction. */
    static void preparePending(ClientSession session, MongoDatabase database, StorageSetupScope scope, long bindingVersion,
            StorageBinding.Mode mode) {
        var bootstrap = SystemCollections.OP_BOOTSTRAP.on(database);
        var users = SystemCollections.CONFIG_USERS.on(database);
        requireNoLocalAccounts(session, bootstrap, users);
        if (mode == StorageBinding.Mode.OP) {
            bootstrap.insertOne(session, new Document("_id", "cluster").append("formatVersion", 1)
                    .append("clusterId", scope.clusterId()).append("installationId", scope.installationId())
                    .append("operationId", scope.operationId()).append("generation", scope.generation())
                    .append("bindingVersion", bindingVersion).append("phase", Phase.PENDING.name()).append("username", null));
        }
    }

    private static void requireNoLocalAccounts(ClientSession session, MongoCollection<Document> bootstrap, MongoCollection<Document> users) {
        if (bootstrap.find(session).limit(1).first() != null || users.find(session).limit(1).first() != null) throw unreadable();
    }

    private static User decodeUser(Document row) {
        if (!row.keySet().equals(Set.of("_id", "passwordHash", "role")) || !(row.get("_id") instanceof String username)
                || !(row.get("passwordHash") instanceof String hash) || !(row.get("role") instanceof String role)) throw unreadable();
        try { return new User(username, hash, role); }
        catch (IllegalArgumentException invalid) { throw unreadable(); }
    }

    private <T> T transaction(Function<ClientSession, T> body) {
        try (var session = client.startSession()) {
            return session.withTransaction(() -> {
                requireActive.run();
                T result = body.apply(session);
                requireActive.run();
                return result;
            }, TRANSACTION);
        }
    }
    private static TapstateException refused() {
        return new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
    }
    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "op-bootstrap", "field", "administrator"), null);
    }
}
