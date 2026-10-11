package io.tapstate.app;

import tools.jackson.core.JacksonException;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import io.tapstate.adapters.pdk.SeedOutcome;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.JsonReader;
import io.tapstate.spi.store.CapabilityDeriver;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorRegistry;
import io.tapstate.spi.store.ConnectorSpecStore;
import io.tapstate.spi.store.ContentHash;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/** Verifies the immutable Cloud seed release before registration and refuses incomplete startup. */
final class CloudConnectorSeedReadiness {

    static final String LOCK_RESOURCE = "/META-INF/tapstate/connectors.lock.json";
    private static final List<String> IDS = List.of(
            "mysql", "mongodb", "postgres", "oracle", "sqlserver", "mongodb-atlas", "aws-rds-mysql", "db2");
    private static final Set<String> LICENSES = Set.of("MICROSOFT-MIT-LICENSE.txt", "ORACLE-FREE-USE-TERMS.txt");
    private static final Set<String> ARTIFACT_KEYS = Set.of(
            "id", "bytes", "sha256", "upstreamRevision", "pdkApiVersion", "specPath");
    private static final int MAX_LOCK_BYTES = 64 * 1024;
    private static final int MAX_SPEC_BYTES = 1024 * 1024;
    private static final JsonMapper JSON = JsonMapper.builder(JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();

    private final Path seedDir;
    private final Path pluginsDir;
    private final ConnectorRegistry registry;
    private final ConnectorCatalogStore catalog;
    private final ConnectorSpecStore specs;
    private final CapabilityDeriver deriver;
    private final List<Artifact> artifacts;
    private final Map<String, String> specHashes = new HashMap<>();

    static CloudConnectorSeedReadiness checkedRelease(
            Path seedDir, Path pluginsDir, ConnectorRegistry registry, ConnectorCatalogStore catalog,
            ConnectorSpecStore specs, CapabilityDeriver deriver) {
        try (InputStream input = CloudConnectorSeedReadiness.class.getResourceAsStream(LOCK_RESOURCE)) {
            if (input == null) {
                throw invalid("release", "embedded-lock-missing");
            }
            return new CloudConnectorSeedReadiness(
                    seedDir, pluginsDir, boundedBytes(input, MAX_LOCK_BYTES), registry, catalog, specs, deriver);
        } catch (IOException failure) {
            throw invalid("release", "embedded-lock-unreadable");
        }
    }

    /** The explicit trusted bytes are a package-local seam for isolated release-shape tests. */
    CloudConnectorSeedReadiness(
            Path seedDir, Path pluginsDir, byte[] trustedLock, ConnectorRegistry registry, ConnectorCatalogStore catalog,
            ConnectorSpecStore specs, CapabilityDeriver deriver) {
        this(seedDir, pluginsDir, trustedLock, registry, catalog, specs, deriver, Files::list);
    }

    CloudConnectorSeedReadiness(
            Path seedDir, Path pluginsDir, byte[] trustedLock, ConnectorRegistry registry, ConnectorCatalogStore catalog,
            ConnectorSpecStore specs, CapabilityDeriver deriver, DirectoryEntries directoryEntries) {
        this.seedDir = seedDir.toAbsolutePath().normalize();
        this.pluginsDir = pluginsDir.toAbsolutePath().normalize();
        this.registry = registry;
        this.catalog = catalog;
        this.specs = specs;
        this.deriver = deriver;
        JsonNode lock = parse(trustedLock, "release", "lock-invalid");
        if (!keys(lock).equals(Set.of("schemaVersion", "connectors", "licenseFiles"))
                || !lock.path("schemaVersion").isInt() || lock.path("schemaVersion").intValue() != 2) {
            throw invalid("release", "lock-invalid");
        }
        artifacts = readArtifacts(lock.path("connectors"));
        // The runtime lock must be the same checked-in bytes embedded in this server, not a second
        // editable list that can authorize a replaced JAR. No fourth Cloud startup setting is needed.
        Path release = this.seedDir.resolveSibling("release");
        try {
            Path external = release.resolve("connectors.lock.json");
            if (!Files.isRegularFile(external, LinkOption.NOFOLLOW_LINKS)
                    || !Arrays.equals(readBounded(external, MAX_LOCK_BYTES), trustedLock)) {
                throw invalid("release", "lock-missing-or-mismatched");
            }
            verifyInventory(directoryEntries);
            for (Artifact artifact : artifacts) {
                Path jar = this.seedDir.resolve(artifact.id() + "-connector.jar");
                verifyBytes(jar, artifact.bytes(), artifact.sha256(), artifact.id());
                specHashes.put(artifact.id(), verifyJar(jar, artifact));
                verifyStoredArtifact(artifact, false);
                verifyCache(artifact, false);
            }
            verifyLicenses(lock.path("licenseFiles"), release.resolve("licenses"));
        } catch (IOException | UncheckedIOException failure) {
            throw invalid("release", "seed-input-unreadable");
        }
    }

    /** Runs before ApplicationReadyEvent, including on restart when registration is idempotent. */
    void verifyRegistrations(List<SeedOutcome> outcomes) {
        if (outcomes.size() != artifacts.size()) {
            throw invalid("release", "registration-incomplete");
        }
        Map<Path, SeedOutcome> byPath = new HashMap<>();
        for (SeedOutcome outcome : outcomes) {
            if (byPath.put(outcome.artifact().toAbsolutePath().normalize(), outcome) != null) {
                throw invalid("release", "registration-incomplete");
            }
        }
        for (Artifact artifact : artifacts) {
            String id = artifact.id();
            SeedOutcome outcome = byPath.get(seedDir.resolve(id + "-connector.jar"));
            if (!(outcome instanceof SeedOutcome.Seeded seeded)
                    || !seeded.outcome().registration().connectorId().equals(id)
                    || !seeded.outcome().registration().contentHash().equals(artifact.sha256())
                    || !artifact.pdkApiVersion().equals(seeded.outcome().registration().pdkApiVersion())) {
                // Do not log the failed outcome's cause: file paths and connector diagnostics may
                // carry arbitrary input. Only a release-owned id and fixed reason cross this gate.
                throw invalid(id, "registration-failed");
            }
            var registrations = readPort(id, () -> registry.findAll(id));
            if (registrations.size() != 1
                    || !registrations.getFirst().contentHash().equals(artifact.sha256())
                    || !artifact.pdkApiVersion().equals(registrations.getFirst().pdkApiVersion())
                    || !readPort(id, () -> registry.hasArtifact(artifact.sha256()))) {
                throw invalid(id, "registered-artifact-unavailable");
            }
            // A prior registration can have a catalog row while this server can no longer load its
            // PDK artifact. Re-probe rather than trusting a row cached by an earlier boot.
            var capabilities = readPort(id, () -> deriver.derive(id));
            try {
                verifyStoredArtifact(artifact, true);
                verifyCache(artifact, true);
            } catch (IOException failure) {
                throw invalid(id, "seed-input-unreadable");
            }
            if (!capabilities.capabilityIds().containsAll(Set.of("batch_read_function", "stream_read_function"))) {
                throw invalid(id, "read-functions-unavailable");
            }
            var row = readPort(id, () -> catalog.get(id));
            String specHash = specHashes.get(id);
            if (row.isEmpty() || !id.equals(row.get().id()) || !artifact.specPath().equals(row.get().provenance().specPath())
                    || !specHash.equals(row.get().provenance().specContentHash())) {
                throw invalid(id, "catalog-unavailable");
            }
            var spec = readPort(id, () -> specs.get(specHash));
            if (spec.isEmpty() || !ContentHash.of(spec.get()).equals(specHash)) {
                throw invalid(id, "spec-unavailable");
            }
        }
    }

    private void verifyInventory(DirectoryEntries directoryEntries) throws IOException {
        if (!Files.isDirectory(seedDir, LinkOption.NOFOLLOW_LINKS)) {
            throw invalid("release", "seed-directory-missing");
        }
        Set<String> expected = new HashSet<>();
        artifacts.forEach(artifact -> expected.add(artifact.id() + "-connector.jar"));
        Set<String> actual = new HashSet<>();
        try (var entries = directoryEntries.list(seedDir)) {
            entries.filter(path -> path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".jar"))
                    .forEach(path -> actual.add(path.getFileName().toString()));
        }
        if (!actual.equals(expected)) {
            throw invalid("release", "seed-set-mismatched");
        }
    }

    private void verifyStoredArtifact(Artifact artifact, boolean required) {
        var bytes = readPort(artifact.id(), () -> registry.artifact(artifact.sha256()));
        if (required && bytes.isEmpty()) {
            throw invalid(artifact.id(), "stored-artifact-unavailable");
        }
        if (bytes.isPresent() && (bytes.get().length != artifact.bytes()
                || !ContentHash.of(bytes.get()).equals(artifact.sha256()))) {
            throw invalid(artifact.id(), "stored-artifact-mismatched");
        }
    }

    private void verifyCache(Artifact artifact, boolean required) throws IOException {
        Path cached = pluginsDir.resolve(artifact.sha256() + ".jar");
        if (required || Files.exists(cached, LinkOption.NOFOLLOW_LINKS)) {
            verifyBytes(cached, artifact.bytes(), artifact.sha256(), artifact.id(), "cache-artifact-mismatched");
        }
    }

    static TapstateException sweepUnavailable() {
        return invalid("release", "seed-input-unreadable");
    }

    private static List<Artifact> readArtifacts(JsonNode entries) {
        if (!entries.isArray() || entries.size() != IDS.size()) {
            throw invalid("release", "lock-invalid");
        }
        List<Artifact> result = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        for (JsonNode entry : entries) {
            if (!keys(entry).equals(ARTIFACT_KEYS)) {
                throw invalid("release", "lock-invalid");
            }
            String id = entry.path("id").asString("");
            if (!IDS.contains(id) || !ids.add(id)
                    || !validLength(entry.path("bytes"), 1_000_000, 128_000_000)
                    || !matches(entry.path("sha256"), "[0-9a-f]{64}")
                    || !matches(entry.path("upstreamRevision"), "[0-9a-f]{40}")
                    || !matches(entry.path("pdkApiVersion"), "[A-Za-z0-9][A-Za-z0-9._-]*")
                    || !matches(entry.path("specPath"), "[A-Za-z0-9][A-Za-z0-9._-]*\\.json")) {
                throw invalid("release", "lock-invalid");
            }
            result.add(new Artifact(id, entry.path("bytes").longValue(), entry.path("sha256").stringValue(),
                    entry.path("upstreamRevision").stringValue(), entry.path("pdkApiVersion").stringValue(),
                    entry.path("specPath").stringValue()));
        }
        return List.copyOf(result);
    }

    private static void verifyLicenses(JsonNode entries, Path directory) throws IOException {
        if (!entries.isArray() || entries.size() != LICENSES.size()) {
            throw invalid("release", "license-lock-invalid");
        }
        Set<String> names = new HashSet<>();
        for (JsonNode entry : entries) {
            String name = entry.path("name").asString("");
            if (!keys(entry).equals(Set.of("name", "bytes", "sha256")) || !LICENSES.contains(name)
                    || !names.add(name) || !validLength(entry.path("bytes"), 1, 1_000_000)
                    || !matches(entry.path("sha256"), "[0-9a-f]{64}")) {
                throw invalid("release", "license-lock-invalid");
            }
            verifyBytes(directory.resolve(name), entry.path("bytes").longValue(), entry.path("sha256").stringValue(),
                    "release");
        }
    }

    private static void verifyBytes(Path path, long size, String hash, String id) throws IOException {
        verifyBytes(path, size, hash, id, "artifact-missing-or-mismatched");
    }

    private static void verifyBytes(Path path, long size, String hash, String id, String reason) throws IOException {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) != size) {
            throw invalid(id, reason);
        }
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException invariant) {
            throw new IllegalStateException("The JVM does not provide SHA-256", invariant);
        }
        try (InputStream input = Files.newInputStream(path)) {
            byte[] block = new byte[64 * 1024];
            long count = 0;
            int read;
            while ((read = input.read(block)) != -1) {
                count += read;
                if (count > size) {
                    throw invalid(id, reason);
                }
                digest.update(block, 0, read);
            }
            if (count != size || !HexFormat.of().formatHex(digest.digest()).equals(hash)) {
                throw invalid(id, reason);
            }
        }
    }

    private static String verifyJar(Path path, Artifact artifact) throws IOException {
        try (JarFile jar = new JarFile(path.toFile())) {
            for (String name : List.of("META-INF/MANIFEST.MF", artifact.specPath())) {
                if (jar.stream().filter(entry -> name.equals(entry.getName())).count() != 1) {
                    throw invalid(artifact.id(), "jar-identity-mismatched");
                }
            }
            byte[] manifestBytes;
            byte[] specBytes;
            try (InputStream manifest = jar.getInputStream(jar.getJarEntry("META-INF/MANIFEST.MF"));
                 InputStream spec = jar.getInputStream(jar.getJarEntry(artifact.specPath()))) {
                manifestBytes = boundedBytes(manifest, MAX_LOCK_BYTES);
                specBytes = boundedBytes(spec, MAX_SPEC_BYTES);
            }
            var headers = new Manifest(new ByteArrayInputStream(manifestBytes)).getMainAttributes();
            String title = artifact.id().equals("sqlserver") ? "mssql-connector" : artifact.id() + "-connector";
            Object spec;
            try {
                // These already hash-checked upstream bytes use the same established parser as
                // registration. Some published specs repeat a data-type key; a stricter parser here
                // would refuse an otherwise compatible PDK release. The release lock stays strict.
                spec = JsonReader.parse(new String(specBytes, StandardCharsets.UTF_8));
            } catch (IllegalArgumentException failure) {
                throw invalid(artifact.id(), "jar-identity-mismatched");
            }
            if (!title.equals(headers.getValue("Implementation-Title"))
                    || !artifact.upstreamRevision().equals(headers.getValue("Git-Commit-Id"))
                    || !artifact.pdkApiVersion().equals(headers.getValue("PDK-API-Version"))
                    || !(spec instanceof Map<?, ?> root
                    && root.get("properties") instanceof Map<?, ?> properties
                    && artifact.id().equals(properties.get("id")))) {
                throw invalid(artifact.id(), "jar-identity-mismatched");
            }
            return ContentHash.of(specBytes);
        }
    }

    private static <T> T readPort(String id, Supplier<T> reader) {
        try {
            return reader.get();
        } catch (TapstateException | UncheckedIOException failure) {
            throw invalid(id, "registered-artifact-unavailable");
        }
    }

    private static JsonNode parse(byte[] bytes, String id, String reason) {
        try {
            if (bytes.length > MAX_SPEC_BYTES) {
                throw invalid(id, reason);
            }
            JsonNode result = JSON.readTree(bytes);
            if (result == null || !result.isObject()) {
                throw invalid(id, reason);
            }
            return result;
        } catch (JacksonException failure) {
            throw invalid(id, reason);
        }
    }

    private static Set<String> keys(JsonNode node) {
        return new HashSet<>(node.propertyNames());
    }

    private static boolean matches(JsonNode value, String pattern) {
        return value.isString() && value.stringValue().matches(pattern);
    }

    private static boolean validLength(JsonNode value, long min, long max) {
        return value.isIntegralNumber() && value.canConvertToLong() && value.longValue() >= min && value.longValue() <= max;
    }

    private static byte[] readBounded(Path path, int maximum) throws IOException {
        try (InputStream input = Files.newInputStream(path)) {
            return boundedBytes(input, maximum);
        }
    }

    private static byte[] boundedBytes(InputStream input, int maximum) throws IOException {
        byte[] bytes = input.readNBytes(maximum + 1);
        if (bytes.length > maximum) {
            throw invalid("release", "seed-input-too-large");
        }
        return bytes;
    }

    private static TapstateException invalid(String id, String reason) {
        return new TapstateException(BootError.CLOUD_CONNECTORS_INVALID, Map.of("connector", id, "reason", reason), null);
    }

    private record Artifact(String id, long bytes, String sha256, String upstreamRevision,
                            String pdkApiVersion, String specPath) {
    }

    @FunctionalInterface
    interface DirectoryEntries {
        Stream<Path> list(Path directory) throws IOException;
    }
}
