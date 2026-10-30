package io.tapstate.archtests;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.core.domain.JavaConstructorCall;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import io.tapstate.adapters.mongostore.SystemCollections;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Arrays;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The registry of collections is only worth what its closure is worth. These two gates are what make
 * it exhaustive rather than merely current.
 *
 * <p>The first pins that a collection handle is taken in exactly one place. Without it the registry
 * is a list somebody maintains, and the failure mode is silent in both directions: a collection that
 * exists with no row, and a row nothing has taken a handle from in a year.
 *
 * <p>The second pins the rows themselves against a checked-in rendering. What it catches is not a new
 * row -- appending one is ordinary -- but an existing row changing meaning: a strategy flipped, an
 * introducedIn corrected after the fact, an index quietly dropped. Deployed stores were migrated on
 * the strength of those, so a changed row is a changed contract and has to be read by a person.
 *
 * <p>The live half of this -- a running database holding a collection no row declares -- cannot be
 * seen from here and is witnessed against a real server in the store adapter's own tests.
 */
class SystemCollectionsGatesTest {

    private static final Path GOLDEN = Path.of("src/test/resources/system-collections.golden");
    private static final Path CONTRACTS = Path.of("src/test/resources/storage-resources.json");

    @Test
    void everyCollectionHasTheSamePhysicalNamesAndLayerInTheStorageInventory() throws IOException {
        var manifest = JsonMapper.builder().build().readTree(
                Files.readString(Path.of("src/test/resources/storage-resources.json")));
        var rows = new java.util.LinkedHashMap<String, tools.jackson.databind.JsonNode>();
        for (var row : manifest.get("resources")) {
            assertThat(rows.put(row.get("id").asString(), row)).isNull();
        }
        Set<String> expectedResources = new HashSet<>(
                Arrays.stream(SystemCollections.values()).map(SystemCollections::resourceId).toList());
        expectedResources.addAll(Set.of("local.installation", "local.connector-cache",
                "temporary.connector-registration", "temporary.connector-trust", "temporary.connector-download",
                "user.cli-context", "user.cli-auth", "user.workspace", "user.local-stack", "user.alias-links", "build.catalog"));
        assertThat(rows.keySet()).containsExactlyInAnyOrderElementsOf(expectedResources);
        // Every declaration, including file resources, retains its contract and bytecode-owner checks.
        ObjectNode completeManifest = (ObjectNode) new ObjectMapper().readTree(Files.readString(CONTRACTS));
        validateContracts(completeManifest);
        assertFileOwners(completeManifest);
        for (SystemCollections declaration : SystemCollections.values()) {
            var row = rows.get(declaration.resourceId());
            assertThat(row.get("database").asString()).isEqualTo(declaration.database().name());
            String expectedLayer = switch (declaration.database()) {
                case CONFIG -> "CONFIG";
                case CACHE -> "CACHE";
                default -> "LEGACY";
            };
            assertThat(row.get("layer").asString()).isEqualTo(expectedLayer);
            assertThat(row.get("kind").asString()).isEqualTo(declaration.kind().name());
            var names = new java.util.ArrayList<String>();
            row.get("physicalNames").forEach(name -> names.add(name.asString()));
            assertThat(names).containsExactlyElementsOf(declaration.physicalNames());
        }
    }

    private static JavaClasses tapstateClasses;

    /** A call that turns a database into a plain collection handle. */
    private static final DescribedPredicate<JavaMethodCall> TAKES_A_COLLECTION =
            new DescribedPredicate<>("a call that takes a Mongo collection handle") {
                @Override
                public boolean test(JavaMethodCall call) {
                    return call.getTargetOwner().isAssignableTo(MongoDatabase.class)
                            && call.getName().equals("getCollection");
                }
            };

    /** A call that opens a GridFS bucket, which is the other way a collection comes into existence. */
    private static final DescribedPredicate<JavaMethodCall> OPENS_A_BUCKET =
            new DescribedPredicate<>("a call that opens a GridFS bucket") {
                @Override
                public boolean test(JavaMethodCall call) {
                    return call.getTargetOwner().isAssignableTo(GridFSBuckets.class)
                            && call.getName().equals("create");
                }
            };

    @BeforeAll
    static void importTapstateClasses() {
        tapstateClasses = ProductionClassImports.packages("io.tapstate");
    }

    @Test
    @DisplayName("a plain collection handle is taken in exactly one place")
    void collectionHandlesAreTakenOnlyInTheRegistry() {
        assertTakenOnlyInTheRegistry(TAKES_A_COLLECTION, "getCollection");
    }

    @Test
    @DisplayName("a GridFS bucket is opened in exactly one place")
    void bucketHandlesAreTakenOnlyInTheRegistry() {
        assertTakenOnlyInTheRegistry(OPENS_A_BUCKET, "GridFSBuckets.create");
    }

    /**
     * Asserts the call is matched at all and then that every one of them comes from the registry.
     *
     * <p>The two forms are checked separately, and that is the whole reason this is a method rather
     * than one predicate over both: matched as a disjunction, one half could stop matching -- a driver
     * rename, a wrapper introduced -- and the other half would keep the positive control satisfied on
     * its own. Measured while building this gate: breaking the plain-collection half left both
     * assertions green with only the bucket form still being checked, and the plain form is how every
     * collection but one is reached.
     */
    private static void assertTakenOnlyInTheRegistry(
            DescribedPredicate<JavaMethodCall> takesAHandle, String form) {
        List<JavaMethodCall> handleCalls = tapstateClasses.stream()
                .flatMap(type -> type.getMethodCallsFromSelf().stream())
                .filter(takesAHandle::test)
                .toList();
        assertThat(handleCalls)
                .as("positive control: %s must be called somewhere, or this gate checks nothing", form)
                .isNotEmpty();
        assertThat(handleCalls).allSatisfy(call -> assertThat(call.getOriginOwner().getName())
                .as("a collection handle is taken only in the registry; a second place is a collection "
                        + "that can exist with no row describing it")
                .isEqualTo(SystemCollections.class.getName()));
    }

    @Test
    @DisplayName("the registry matches the checked-in rendering (a changed row is a changed contract)")
    void theRegistryMatchesTheCheckedInRendering() throws IOException {
        assertThat(SystemCollections.render())
                .as("rows are appended, not edited: an existing row's meaning is what deployed stores "
                        + "were migrated on. If this diff changes a line rather than adding one, that is "
                        + "the thing to read, not the thing to regenerate")
                .isEqualTo(Files.readString(GOLDEN, StandardCharsets.UTF_8));
    }

    @Test
    void everyResourceHasACompleteVersionedRecoveryContract() throws IOException {
        validateContracts(contracts());
    }

    @Test
    void omittedResourcesWrongLayersAndIncompleteRecoveryAreRejected() throws IOException {
        ObjectNode missing = contracts();
        ((ArrayNode) missing.get("resources")).remove(0);
        assertThatThrownBy(() -> validateContracts(missing)).isInstanceOf(AssertionError.class);
        ObjectNode wrongLayer = contracts();
        resource(wrongLayer, "mongo.config.sources").put("layer", "CACHE");
        assertThatThrownBy(() -> validateContracts(wrongLayer)).isInstanceOf(AssertionError.class);
        ObjectNode noPrerequisites = contracts();
        ((ObjectNode) noPrerequisites.path("recoveryProfiles").path("runtime-state"))
                .putArray("prerequisites");
        assertThatThrownBy(() -> validateContracts(noPrerequisites)).isInstanceOf(AssertionError.class);
        ObjectNode noEvidence = contracts();
        resource(noEvidence, "mongo.cache.srs_models").putArray("evidence");
        assertThatThrownBy(() -> validateContracts(noEvidence)).isInstanceOf(AssertionError.class);
        ObjectNode noVersion = contracts();
        resource(noVersion, "mongo.config.sources").put("proposalVersion", 0);
        assertThatThrownBy(() -> validateContracts(noVersion)).isInstanceOf(AssertionError.class);
    }

    @Test
    void gridFsRequiresBothPhysicalCollectionsAndIndependentResourcesCannotShareOne() throws IOException {
        ObjectNode missingChunks = contracts();
        resource(missingChunks, "mongo.cache.connector_artifacts").putArray("physicalNames")
                .add("connector_artifacts.files");
        assertThatThrownBy(() -> validateContracts(missingChunks)).isInstanceOf(AssertionError.class);
        ObjectNode merged = contracts();
        resource(merged, "mongo.config.pipelines").putArray("physicalNames").add("sources");
        assertThatThrownBy(() -> validateContracts(merged)).isInstanceOf(AssertionError.class);
    }

    @Test
    void directFileMutationsAndChannelOpenersHaveExplicitResourceOwners() throws IOException {
        ObjectNode declarations = contracts();
        assertFileOwners(declarations);
        ((ArrayNode) declarations.get("resources")).removeAll();
        assertThatThrownBy(() -> assertFileOwners(declarations)).isInstanceOf(AssertionError.class);
    }

    @Test
    void directFileConstructorsCannotIntroduceAnUnregisteredPersistenceOwner() {
        var fixture = new ClassFileImporter().importClasses(UnregisteredFileWriter.class);
        var calls = fileConstructors(fixture);
        assertThat(calls).as("positive control for direct file constructors").hasSize(1);
        assertThatThrownBy(() -> assertConstructorOwners(Set.of("unrelated.Owner"), calls))
                .isInstanceOf(AssertionError.class);
    }

    static final class UnregisteredFileWriter {
        void write(Path path) throws IOException {
            // Imported as bytecode only; this method is never executed by the test.
            try (var output = new java.io.FileOutputStream(path.toFile())) { output.write(1); }
        }
    }

    private static ObjectNode contracts() throws IOException {
        return (ObjectNode) new ObjectMapper().readTree(Files.readString(CONTRACTS));
    }

    private static ObjectNode resource(ObjectNode document, String id) {
        for (JsonNode row : document.path("resources"))
            if (id.equals(row.path("id").asText())) return (ObjectNode) row;
        throw new AssertionError("missing resource " + id);
    }

    /** Definitions and test references describe a contract; they do not certify a human decision. */
    private static void validateContracts(ObjectNode document) {
        assertThat(document.path("schemaVersion").isIntegralNumber()).isTrue();
        assertThat(document.path("schemaVersion").asInt()).isEqualTo(1);
        assertThat(document.path("resources").isArray()).isTrue();
        Map<String, JsonNode> rows = new LinkedHashMap<>();
        Set<String> physical = new HashSet<>();
        for (JsonNode row : document.path("resources")) {
            String id = requiredText(row, "id");
            assertThat(rows.put(id, row)).as("duplicate resource %s", id).isNull();
            for (String version : List.of("resourceVersion", "proposalVersion")) {
                assertThat(row.path(version).isIntegralNumber()).isTrue();
                assertThat(row.path(version).canConvertToInt()).isTrue();
                assertThat(row.path(version).asInt()).isPositive();
            }
            assertThat(requiredText(row, "kind")).isIn("COLLECTION", "GRIDFS_BUCKET", "FILE_SET", "TEMPORARY_FILES", "USER_OUTPUT");
            assertThat(requiredText(row, "layer")).isIn("CONFIG", "CACHE", "LEGACY", "USER", "TEMPORARY", "BUILD");
            String database = requiredText(row, "database");
            requiredText(row, "responsibility"); requiredText(row, "growth");
            for (String location : requiredTexts(row, "physicalNames"))
                assertThat(physical.add(database + ":" + location)).as("separate resources cannot share %s", location).isTrue();
            requiredTexts(row, "writers"); requiredTexts(row, "readers");
            for (String evidence : requiredTexts(row, "evidence")) {
                Path path = Path.of(evidence);
                assertThat(path.isAbsolute()).isFalse();
                assertThat(path.normalize().startsWith("..")).isFalse();
                assertThat(Path.of("..").resolve(path)).as("test/evidence reference for %s", id).isRegularFile();
            }
            JsonNode recovery = document.path("recoveryProfiles").path(requiredText(row, "recovery"));
            requiredText(recovery, "goal"); requiredText(recovery, "entry"); requiredTexts(recovery, "prerequisites");
            JsonNode consistency = document.path("consistencyProfiles").path(requiredText(row, "consistency"));
            requiredText(consistency, "atomicDomain"); requiredText(consistency, "fencing");
            if (database.equals("CONFIG") || database.equals("CACHE"))
                assertThat(consistency.path("atomicDomain").asText()).isEqualTo(database);
        }
        Set<String> mongo = new HashSet<>();
        for (SystemCollections declaration : SystemCollections.values()) {
            mongo.add(declaration.resourceId());
            JsonNode row = rows.get(declaration.resourceId());
            assertThat(row).as("closed resource declaration %s", declaration.resourceId()).isNotNull();
            assertThat(row.path("database").asText()).isEqualTo(declaration.database().name());
            assertThat(row.path("kind").asText()).isEqualTo(declaration.kind().name());
            String layer = switch (declaration.database()) {
                case CONFIG -> "CONFIG";
                case CACHE -> "CACHE";
                case STORE, NEST -> "LEGACY";
            };
            assertThat(row.path("layer").asText()).isEqualTo(layer);
            assertThat(requiredTexts(row, "physicalNames")).containsExactlyElementsOf(declaration.physicalNames());
            assertThat(requiredTexts(row, "writers")).contains(declaration.owner().getName());
        }
        assertThat(rows.keySet().stream().filter(id -> id.startsWith("mongo.")).toList())
                .containsExactlyInAnyOrderElementsOf(mongo);
    }

    private static String requiredText(JsonNode object, String name) {
        assertThat(object.path(name).isTextual()).as(name).isTrue();
        String result = object.path(name).asText();
        assertThat(result).as(name).isNotBlank();
        return result;
    }

    private static List<String> requiredTexts(JsonNode object, String name) {
        JsonNode values = object.path(name);
        assertThat(values.isArray()).as(name).isTrue();
        List<String> result = new java.util.ArrayList<>();
        for (JsonNode value : values) {
            assertThat(value.isTextual()).as(name).isTrue();
            assertThat(value.asText()).as(name).isNotBlank();
            result.add(value.asText());
        }
        assertThat(result).as(name).isNotEmpty().doesNotHaveDuplicates();
        return result;
    }

    private static void assertFileOwners(ObjectNode document) {
        Set<String> owners = new HashSet<>();
        for (JsonNode row : document.path("resources"))
            if (row.path("database").asText().equals("LOCAL")) owners.addAll(requiredTexts(row, "writers"));
        Set<String> fileMethods = Set.of("createDirectory", "createFile", "createDirectories", "createTempFile",
                "createTempDirectory", "write", "writeString", "newOutputStream", "newByteChannel", "move", "copy",
                "delete", "deleteIfExists", "createSymbolicLink", "createLink", "setAttribute", "setPosixFilePermissions");
        // The separately versioned Cloud SDK is a dependency, not a persistence implementation owned by this repository.
        List<JavaMethodCall> files = tapstateClasses.stream().filter(type -> !type.getPackageName().startsWith("io.tapstate.cloud.sdk"))
                .flatMap(type -> type.getMethodCallsFromSelf().stream())
                .filter(call -> call.getTargetOwner().getName().equals("java.nio.file.Files") && fileMethods.contains(call.getName())).toList();
        List<JavaMethodCall> channels = tapstateClasses.stream().filter(type -> !type.getPackageName().startsWith("io.tapstate.cloud.sdk"))
                .flatMap(type -> type.getMethodCallsFromSelf().stream())
                .filter(call -> Set.of("java.nio.channels.FileChannel", "java.nio.channels.AsynchronousFileChannel")
                        .contains(call.getTargetOwner().getName()) && call.getName().equals("open")).toList();
        assertThat(files).as("positive control: direct Files mutations are present").isNotEmpty();
        assertThat(channels).as("positive control: channel opens, including lock/read access, are present").isNotEmpty();
        java.util.stream.Stream.concat(files.stream(), channels.stream()).forEach(call -> {
            String origin = call.getOriginOwner().getName();
            assertThat(owners.stream().anyMatch(owner -> origin.equals(owner) || origin.startsWith(owner + "$")))
                    .as("unclassified persistence entry point: %s", call.getDescription()).isTrue();
        });
        assertConstructorOwners(owners, fileConstructors(tapstateClasses));
    }

    private static List<JavaConstructorCall> fileConstructors(JavaClasses classes) {
        return classes.stream().filter(type -> !type.getPackageName().startsWith("io.tapstate.cloud.sdk"))
                .flatMap(type -> type.getConstructorCallsFromSelf().stream())
                .filter(call -> Set.of("java.io.FileOutputStream", "java.io.FileWriter", "java.io.RandomAccessFile")
                        .contains(call.getTargetOwner().getName()))
                .toList();
    }

    private static void assertConstructorOwners(Set<String> owners, List<JavaConstructorCall> calls) {
        for (var call : calls) {
            String origin = call.getOriginOwner().getName();
            assertThat(owners.stream().anyMatch(owner -> origin.equals(owner) || origin.startsWith(owner + "$")))
                    .as("unclassified file constructor: %s", call.getDescription()).isTrue();
        }
    }
}
