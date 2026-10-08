package io.tapstate.app;

import io.tapstate.adapters.pdk.SeedOutcome;
import io.tapstate.adapters.pdk.SeedConnectorSweep;
import io.tapstate.adapters.pdk.ConnectorArtifactRegistrar;
import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.Provenance;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CapabilityDeriver;
import io.tapstate.spi.store.ConnectorCapabilities;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.json.JsonMapper;

import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Synthetic bytes distinguish release-shape checks; actual PDK loading is witnessed separately. */
class CloudConnectorSeedReadinessTest {

    private static final List<String> IDS = List.of(
            "mysql", "mongodb", "postgres", "oracle", "sqlserver", "mongodb-atlas", "aws-rds-mysql", "db2");
    private static final JsonMapper JSON = new JsonMapper();
    private static final String VERSION = "2.0.5-SNAPSHOT";
    private static final String REVISION = "a".repeat(40);
    private static final String SENTINEL = "untrusted-password-sentinel";

    @TempDir Path directory;
    private Path seeds;
    private Path release;
    private final List<Map<String, Object>> entries = new ArrayList<>();
    private final List<Map<String, Object>> licenses = new ArrayList<>();
    private final Stores stores = new Stores();
    private final List<SeedOutcome> outcomes = new ArrayList<>();
    private final List<String> loaded = new ArrayList<>();
    private RuntimeException loadFailure;
    private byte[] specOverride;
    private String eraseAfterLoad;
    private CloudConnectorSeedReadiness.DirectoryEntries directoryEntries = Files::list;
    private Set<String> functions = Set.of("batch_read_function", "stream_read_function");

    @BeforeEach
    void prepareIndependentRelease() throws Exception {
        seeds = Files.createDirectories(directory.resolve("connectors"));
        release = Files.createDirectories(directory.resolve("release"));
        Path licenseDirectory = Files.createDirectories(release.resolve("licenses"));
        for (String id : IDS) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("id", id);
            entry.put("upstreamRevision", REVISION);
            entry.put("pdkApiVersion", VERSION);
            entry.put("specPath", id + "-spec.json");
            entries.add(entry);
            writeJar(id, id, VERSION, id.equals("sqlserver") ? "mssql-connector" : id + "-connector");
            byte[] bytes = Files.readAllBytes(seeds.resolve(id + "-connector.jar"));
            RegistrationOutcome registered = stores.register(id, VERSION, RegistrationSource.SEED, bytes);
            outcomes.add(new SeedOutcome.Seeded(seeds.resolve(id + "-connector.jar"), registered));
            byte[] spec = spec(id);
            String specHash = ContentHash.of(spec);
            stores.specs.put(specHash, spec);
            ConnectorCatalogEntry base = TapstateCatalog.load().byId(id);
            stores.rows.put(id, withProvenance(base, new Provenance(
                    null, null, id + "-spec.json", specHash, null, null, base.provenance().modeSource())));
        }
        for (String name : List.of("MICROSOFT-MIT-LICENSE.txt", "ORACLE-FREE-USE-TERMS.txt")) {
            byte[] bytes = "synthetic license\n".getBytes(StandardCharsets.UTF_8);
            Files.write(licenseDirectory.resolve(name), bytes);
            licenses.add(new LinkedHashMap<>(Map.of("name", name, "bytes", bytes.length, "sha256", ContentHash.of(bytes))));
        }
        writeLock();
    }

    @Test
    void allLockedArtifactsMustBeRegisteredLoadableAndHaveReadableSpecsIncludingOnRestart() throws Exception {
        var gate = gate();
        gate.verifyRegistrations(outcomes);
        gate.verifyRegistrations(outcomes.stream().map(outcome -> {
            var seeded = (SeedOutcome.Seeded) outcome;
            return (SeedOutcome) new SeedOutcome.Seeded(seeded.artifact(),
                    new RegistrationOutcome(seeded.outcome().registration(), false));
        }).toList());
        assertThat(loaded).containsExactlyElementsOf(java.util.stream.Stream.concat(IDS.stream(), IDS.stream()).toList());
    }

    @Test
    void aMissingExternalLockCannotAuthorizeCloudStartup() throws Exception {
        byte[] trusted = lockBytes();
        Files.delete(release.resolve("connectors.lock.json"));
        refused(() -> gate(trusted), "lock-missing-or-mismatched");
    }

    @Test
    void changingTheExternalLockAndJarTogetherDoesNotChangeTheServersTrustedRelease() throws Exception {
        byte[] trusted = lockBytes();
        writeJar("mongodb-atlas", "mongodb-atlas", VERSION, "mongodb-atlas-connector");
        entry("mongodb-atlas").put("upstreamRevision", "b".repeat(40));
        writeLock();
        refused(() -> gate(trusted), "lock-missing-or-mismatched");
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "extra", "uppercase", "directory", "symlink"})
    void theSeedSetCannotOmitReplaceOrAddAnArtifact(String fault) throws Exception {
        Path atlas = seeds.resolve("mongodb-atlas-connector.jar");
        switch (fault) {
            case "missing" -> Files.delete(atlas);
            case "extra" -> Files.write(seeds.resolve("aliyun-db-mongodb-connector.jar"), new byte[] {1});
            case "uppercase" -> {
                // Force a spelling change even on a case-insensitive developer filesystem.
                Path moved = Files.move(atlas, directory.resolve("renaming.jar"));
                Files.move(moved, seeds.resolve("mongodb-atlas-connector.JAR"));
            }
            case "directory" -> { Files.delete(atlas); Files.createDirectory(atlas); }
            case "symlink" -> {
                Path moved = Files.move(atlas, directory.resolve("outside.jar"));
                Files.createSymbolicLink(atlas, moved);
            }
            default -> throw new AssertionError(fault);
        }
        assertThatThrownBy(this::gate).isInstanceOf(TapstateException.class)
                .extracting(error -> ((TapstateException) error).code()).isEqualTo(BootError.CLOUD_CONNECTORS_INVALID);
        assertThat(loaded).isEmpty();
    }

    @Test
    void aSameLengthByteReplacementFailsTheDigestCheckBeforeAnyPdkLoad() throws Exception {
        Path jar = seeds.resolve("mongodb-atlas-connector.jar");
        byte[] changed = Files.readAllBytes(jar);
        changed[changed.length - 1] ^= 1;
        Files.write(jar, changed);
        refused(this::gate, "artifact-missing-or-mismatched");
        assertThat(loaded).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "changed"})
    void theAddedDb2SeedCannotBeOmittedOrReplacedBeforeAnyPdkLoad(String fault) throws Exception {
        Path jar = seeds.resolve("db2-connector.jar");
        if (fault.equals("missing")) {
            Files.delete(jar);
        } else {
            byte[] changed = Files.readAllBytes(jar);
            changed[changed.length - 1] ^= 1;
            Files.write(jar, changed);
        }
        assertThatThrownBy(this::gate).isInstanceOf(TapstateException.class)
                .extracting(error -> ((TapstateException) error).code()).isEqualTo(BootError.CLOUD_CONNECTORS_INVALID);
        assertThat(loaded).isEmpty();
    }

    @Test
    void preexistingStoredArtifactBytesMustMatchTheirLockedContentAddress() throws Exception {
        String hash = (String) entry("mongodb-atlas").get("sha256");
        stores.artifacts.put(hash, stores.artifacts.get(entry("mysql").get("sha256")));
        refused(this::gate, "stored-artifact-mismatched");
        assertThat(loaded).isEmpty();
    }

    @Test
    void persistentArtifactLossDuringLoadingCannotBeHiddenByAValidCache() throws Exception {
        var gate = gate();
        eraseAfterLoad = "mongodb-atlas";
        refused(() -> gate.verifyRegistrations(outcomes), "stored-artifact-unavailable");
    }

    @Test
    void aLazyDirectoryIterationFailureCannotEchoItsOriginalCause() {
        directoryEntries = ignored -> java.util.stream.Stream.generate(() -> {
            throw new UncheckedIOException(new java.io.IOException(SENTINEL));
        });
        refused(this::gate, "seed-input-unreadable");
    }

    @Test
    void aCloudDirectoryIoFaultAfterPreflightIsCodedWithoutItsOriginalCause() throws Exception {
        var gate = gate();
        var sweep = new SeedConnectorSweep(new ConnectorArtifactRegistrar(stores, new ConnectorIntrospector(),
                id -> new ConnectorCapabilities(functions), stores.catalog(), stores.specStore()));
        Files.move(seeds, directory.resolve("saved-seeds"));
        Files.writeString(seeds, SENTINEL);
        refused(() -> new SeedSweepRunner(sweep, seeds, gate).run(
                new org.springframework.boot.DefaultApplicationArguments()), "seed-input-unreadable");
        assertThatThrownBy(() -> new SeedSweepRunner(sweep, seeds).run(
                new org.springframework.boot.DefaultApplicationArguments())).isInstanceOf(UncheckedIOException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"spec", "version", "title", "thin"})
    void trustedArtifactMetadataStillMustDescribeTheCorrectShadedConnector(String fault) throws Exception {
        writeJar("mongodb-atlas", fault.equals("spec") ? "mongodb" : "mongodb-atlas",
                fault.equals("version") ? "9.9.9" : VERSION,
                fault.equals("title") ? "mongodb-connector" : "mongodb-atlas-connector");
        if (fault.equals("thin")) {
            entry("mongodb-atlas").put("bytes", 3);
        }
        writeLock();
        refused(this::gate, fault.equals("thin") ? "lock-invalid" : "jar-identity-mismatched");
    }

    @Test
    void lockedUpstreamSpecKeepsTheExistingPdkDuplicateFieldSemantics() throws Exception {
        // Existing published specs repeat a data-type key; the established PDK parser accepts it.
        // The release lock remains duplicate-key strict and still pins the complete original bytes.
        specOverride = """
                {"properties":{"id":"aws-rds-mysql"},"dataTypes":{"year":{},"year":{}}}
                """.getBytes(StandardCharsets.UTF_8);
        writeJar("aws-rds-mysql", "aws-rds-mysql", VERSION, "aws-rds-mysql-connector");
        writeLock();
        assertThatCode(this::gate).doesNotThrowAnyException();
    }

    @Test
    void anUnknownLockVersionOrDuplicateConnectorCannotDefineANewReleaseSet() throws Exception {
        byte[] version = new String(lockBytes(), StandardCharsets.UTF_8).replace("\"schemaVersion\":2", "\"schemaVersion\":3")
                .getBytes(StandardCharsets.UTF_8);
        refused(() -> gate(version), "lock-invalid");
        entries.set(entries.size() - 1, entries.getFirst());
        writeLock();
        refused(this::gate, "lock-invalid");
    }

    @Test
    void duplicateJsonKeysMalformedContentAndTrailingValuesHaveSafeCodedDiagnostics() {
        for (String text : List.of("{\"schemaVersion\":2,\"schemaVersion\":2}",
                "{\"password\":\"" + SENTINEL + "\"", "{} {}")) {
            refused(() -> gate(text.getBytes(StandardCharsets.UTF_8)), "lock-invalid");
        }
    }

    @Test
    void bothCompanionLicensesMustMatchTheLock() throws Exception {
        Files.writeString(release.resolve("licenses/ORACLE-FREE-USE-TERMS.txt"), SENTINEL);
        refused(this::gate, "artifact-missing-or-mismatched");
    }

    @Test
    void aContainedSweepFailureCannotBecomeReadyOrLeakItsCause() throws Exception {
        var gate = gate();
        var changed = new ArrayList<>(outcomes);
        changed.set(5, new SeedOutcome.Failed(seeds.resolve("mongodb-atlas-connector.jar"), unsafeFailure()));
        refused(() -> gate.verifyRegistrations(changed), "registration-failed");
    }

    @Test
    void aMissingDuplicateOrWrongRegistrationOutcomeCannotPass() throws Exception {
        var gate = gate();
        refused(() -> gate.verifyRegistrations(outcomes.subList(0, 6)), "registration-incomplete");
        var changed = new ArrayList<>(outcomes);
        changed.set(6, changed.getFirst());
        refused(() -> gate.verifyRegistrations(changed), "registration-incomplete");
        changed.set(6, new SeedOutcome.Seeded(seeds.resolve("aws-rds-mysql-connector.jar"),
                new RegistrationOutcome(new ConnectorRegistration("aws-rds-mysql", "wrong-hash", VERSION,
                        RegistrationSource.SEED), true)));
        refused(() -> gate.verifyRegistrations(changed), "registration-failed");
    }

    @Test
    void missingStoredBytesCannotBeHiddenByASuccessfulSweepOutcome() throws Exception {
        var gate = gate();
        stores.artifacts.remove(entry("mongodb-atlas").get("sha256"));
        refused(() -> gate.verifyRegistrations(outcomes), "registered-artifact-unavailable");
    }

    @Test
    void aCachedCatalogRowDoesNotHideAReproducibleLoadRefusal() throws Exception {
        var gate = gate();
        loadFailure = unsafeFailure();
        refused(() -> gate.verifyRegistrations(outcomes), "registered-artifact-unavailable");
    }

    @Test
    void anUncodedPdkProgrammingDefectKeepsItsOriginalIdentity() throws Exception {
        var gate = gate();
        var defect = new IllegalStateException("synthetic programming defect");
        loadFailure = defect;
        assertThatThrownBy(() -> gate.verifyRegistrations(outcomes)).isSameAs(defect);
    }

    @Test
    void aMissingStreamFunctionCannotBePresentedAsACompleteCloudSource() throws Exception {
        var gate = gate();
        functions = Set.of("batch_read_function");
        refused(() -> gate.verifyRegistrations(outcomes), "read-functions-unavailable");
    }

    @Test
    void aRegistrationWithoutItsDerivedCatalogOrReadableSpecFailsClosed() throws Exception {
        var gate = gate();
        ConnectorCatalogEntry row = stores.rows.remove("mongodb-atlas");
        refused(() -> gate.verifyRegistrations(outcomes), "catalog-unavailable");
        stores.rows.put("mongodb-atlas", row);
        stores.specs.remove(row.provenance().specContentHash());
        refused(() -> gate.verifyRegistrations(outcomes), "spec-unavailable");
    }

    private CloudConnectorSeedReadiness gate() throws Exception {
        return gate(lockBytes());
    }

    private CloudConnectorSeedReadiness gate(byte[] trusted) {
        CapabilityDeriver deriver = id -> {
            loaded.add(id);
            if (loadFailure != null) { throw loadFailure; }
            try {
                Path plugins = Files.createDirectories(directory.resolve("plugins"));
                String hash = (String) entry(id).get("sha256");
                Files.write(plugins.resolve(hash + ".jar"), stores.artifacts.get(hash));
                if (id.equals(eraseAfterLoad)) { stores.artifacts.remove(hash); }
            } catch (java.io.IOException failure) {
                throw new UncheckedIOException(failure);
            }
            return new ConnectorCapabilities(functions);
        };
        return new CloudConnectorSeedReadiness(seeds, directory.resolve("plugins"), trusted,
                stores, stores.catalog(), stores.specStore(), deriver, directoryEntries);
    }

    private byte[] lockBytes() {
        return JSON.writeValueAsBytes(Map.of("schemaVersion", 2, "connectors", entries, "licenseFiles", licenses));
    }

    private void writeLock() throws Exception {
        Files.write(release.resolve("connectors.lock.json"), lockBytes());
    }

    private Map<String, Object> entry(String id) {
        return entries.stream().filter(entry -> id.equals(entry.get("id"))).findFirst().orElseThrow();
    }

    private void writeJar(String id, String specId, String version, String title) throws Exception {
        try (var output = new ZipOutputStream(Files.newOutputStream(seeds.resolve(id + "-connector.jar")))) {
            output.putNextEntry(new ZipEntry("META-INF/MANIFEST.MF"));
            output.write(("Manifest-Version: 1.0\r\nImplementation-Title: " + title + "\r\nGit-Commit-Id: "
                    + REVISION + "\r\nPDK-API-Version: " + version + "\r\n\r\n").getBytes(StandardCharsets.UTF_8));
            output.closeEntry();
            output.putNextEntry(new ZipEntry(id + "-spec.json"));
            output.write(specOverride == null ? spec(specId) : specOverride);
            output.closeEntry();
            byte[] padding = new byte[1_000_000];
            CRC32 crc = new CRC32();
            crc.update(padding);
            ZipEntry dependency = new ZipEntry("classes/SyntheticDependency.class");
            dependency.setMethod(ZipEntry.STORED);
            dependency.setSize(padding.length);
            dependency.setCrc(crc.getValue());
            output.putNextEntry(dependency);
            output.write(padding);
            output.closeEntry();
        }
        byte[] bytes = Files.readAllBytes(seeds.resolve(id + "-connector.jar"));
        entry(id).put("bytes", bytes.length);
        entry(id).put("sha256", ContentHash.of(bytes));
    }

    private static byte[] spec(String id) {
        return JSON.writeValueAsBytes(Map.of("properties", Map.of("id", id)));
    }

    private static ConnectorCatalogEntry withProvenance(ConnectorCatalogEntry row, Provenance provenance) {
        return new ConnectorCatalogEntry(row.id(), row.name(), row.displayName(), row.icon(), row.group(), row.modes(),
                row.discovery(), row.sink(), row.pushOut(), row.config(), provenance);
    }

    private static RuntimeException unsafeFailure() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", SENTINEL, "field", SENTINEL), new IllegalStateException(SENTINEL));
    }

    private static void refused(org.assertj.core.api.ThrowableAssert.ThrowingCallable call, String reason) {
        assertThatThrownBy(call).isInstanceOfSatisfying(TapstateException.class, failure -> {
            assertThat(failure.code()).isEqualTo(BootError.CLOUD_CONNECTORS_INVALID);
            assertThat(failure.args()).containsEntry("reason", reason);
            assertThat(failure.getCause()).isNull();
            StringWriter trace = new StringWriter();
            failure.printStackTrace(new PrintWriter(trace));
            assertThat(trace.toString()).doesNotContain(SENTINEL);
        });
    }

    private static final class Stores implements ConnectorRegistry {
        final Map<String, ConnectorRegistration> registrations = new HashMap<>();
        final Map<String, byte[]> artifacts = new HashMap<>();
        final Map<String, ConnectorCatalogEntry> rows = new HashMap<>();
        final Map<String, byte[]> specs = new HashMap<>();

        @Override public RegistrationOutcome register(String id, String version, RegistrationSource source, byte[] artifact) {
            String hash = ContentHash.of(artifact);
            var registration = new ConnectorRegistration(id, hash, version, source);
            registrations.put(id, registration);
            artifacts.put(hash, artifact);
            return new RegistrationOutcome(registration, true);
        }
        @Override public List<ConnectorRegistration> list() { return List.copyOf(registrations.values()); }
        @Override public Optional<byte[]> artifact(String hash) { return Optional.ofNullable(artifacts.get(hash)); }
        @Override public boolean hasArtifact(String hash) { return artifacts.containsKey(hash); }
        ConnectorCatalogStore catalog() {
            return new ConnectorCatalogStore() {
                @Override public void upsert(ConnectorCatalogEntry row) { rows.put(row.id(), row); }
                @Override public Optional<ConnectorCatalogEntry> get(String id) { return Optional.ofNullable(rows.get(id)); }
                @Override public List<ConnectorCatalogEntry> list() { return List.copyOf(rows.values()); }
            };
        }
        ConnectorSpecStore specStore() {
            return new ConnectorSpecStore() {
                @Override public void put(String hash, byte[] bytes) { specs.put(hash, bytes); }
                @Override public Optional<byte[]> get(String hash) { return Optional.ofNullable(specs.get(hash)); }
            };
        }
    }
}
