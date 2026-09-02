package io.tapstate.archtests;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import io.tapstate.adapters.mongostore.SystemCollections;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
        tapstateClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.tapstate");
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
     * assertions green while eighteen of the nineteen handle-takings went unchecked.
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
}
