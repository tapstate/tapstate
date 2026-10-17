package io.tapstate.archtests;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaCodeUnitAccess;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import io.tapstate.spi.store.WorkloadClaim;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whether a workload claim is still somebody's is decided on the coordination store's clock and nowhere
 * else. The classes that acquire, renew and act on a claim measure their own patience on the monotonic
 * clock, which cannot be moved; none of them may ask what time it is, and none of them may take the
 * deadline the store wrote and subtract its own idea of now from it.
 *
 * <p>Hosts drift and operators move clocks. An expiry decided here would hand one member a claim another
 * still holds when this one runs fast, and let a member that has lost its claim keep calling out when it
 * runs slow -- in both cases silently, and in both cases only on the machines whose clocks are off, which
 * is exactly the population no test run on an agreeing pair of clocks can reach. That is why this is a
 * structural rule and not a behavioural one: a case can witness a member acting on the wrong answer only
 * where the two clocks disagree, and the ban is what covers every path no such case runs through.
 *
 * <p>Both rules carry a positive control, because a ban that matches nothing looks identical whether it
 * is holding or whether it has stopped reading anything at all: the first asserts the wall-clock matcher
 * still finds calls elsewhere in the codebase, the second that every guarded class and the accessor being
 * banned still exist under the names used here.
 */
class ClaimExpiryIsNeverDecidedByANodeClockTest {

    /**
     * Everything that decides, maintains or acts on a workload claim. A member's side of it and the
     * store's side, because each half can answer "has this run out" on its own and each half is wrong to.
     */
    private static final List<String> DECIDES_ABOUT_A_CLAIM = List.of(
            "io.tapstate.app.ExecutionAuthorization",
            "io.tapstate.app.PipelineActuationOwnership",
            "io.tapstate.app.CaptureOwnership",
            "io.tapstate.app.CaptureClaimLease",
            "io.tapstate.app.NodeSessionLease",
            "io.tapstate.app.ClusterWorkloadClaims",
            "io.tapstate.adapters.mongostore.MongoWorkloadClaimStore");

    private static JavaClasses tapstateClasses;

    @BeforeAll
    static void importTapstateClasses() {
        tapstateClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.tapstate");
    }

    @Test
    @DisplayName("nothing that decides about a workload claim asks the node what time it is")
    void noClaimDecisionReadsAWallClock() {
        assertThat(wallClockReadsIn(tapstateClasses.stream().toList()))
                .as("positive control: some class somewhere reads a wall clock, so this matcher is live; "
                        + "an empty answer here means the ban below is checking nothing")
                .isNotEmpty();

        assertThat(wallClockReadsIn(guardedClasses()))
                .as("a claim's lease is the store's arithmetic on the store's clock; a member that asks "
                        + "its own clock instead is right only while the two agree")
                .isEmpty();
    }

    @Test
    @DisplayName("nothing that decides about a workload claim reads the deadline the store wrote")
    void noClaimDecisionReadsTheStoredDeadline() {
        assertThat(WorkloadClaim.class.getRecordComponents())
                .as("positive control: the accessor this rule bans still exists under this name")
                .anySatisfy(component -> assertThat(component.getName()).isEqualTo("leaseUntil"));

        List<String> readers = guardedClasses().stream()
                .flatMap(type -> type.getCodeUnitAccessesFromSelf().stream())
                .filter(call -> call.getTargetOwner().isAssignableTo(WorkloadClaim.class)
                        && call.getName().equals("leaseUntil"))
                .map(call -> call.getOriginOwner().getName() + " -> " + call.getName())
                .toList();

        assertThat(readers)
                .as("the deadline in the record was written by the store's clock; the only honest thing "
                        + "to do with it here is nothing -- how much is left is asked for, not worked out")
                .isEmpty();
    }

    /** The guarded classes, asserting each one still exists rather than silently covering fewer of them. */
    private static List<JavaClass> guardedClasses() {
        return DECIDES_ABOUT_A_CLAIM.stream()
                .map(name -> {
                    JavaClass found = tapstateClasses.stream()
                            .filter(type -> type.getName().equals(name))
                            .findFirst()
                            .orElse(null);
                    assertThat(found)
                            .as("positive control: %s is what this rule guards, and it was not imported "
                                    + "-- renamed or moved, so the rule now covers less than it names", name)
                            .isNotNull();
                    return found;
                })
                .toList();
    }

    private static List<String> wallClockReadsIn(List<JavaClass> classes) {
        return classes.stream()
                .flatMap(type -> type.getCodeUnitAccessesFromSelf().stream())
                .filter(ClaimExpiryIsNeverDecidedByANodeClockTest::readsAWallClock)
                .map(call -> call.getOriginOwner().getName() + " -> "
                        + call.getTargetOwner().getName() + "." + call.getName())
                .toList();
    }

    /** A call that answers "what time is it" from the machine this code happens to be running on. */
    private static boolean readsAWallClock(JavaCodeUnitAccess<?> call) {
        String owner = call.getTargetOwner().getName();
        String name = call.getName();
        if (owner.equals("java.time.Clock")) {
            return true;
        }
        if (owner.equals("java.lang.System")) {
            return name.equals("currentTimeMillis");
        }
        if (owner.equals("java.util.Date")) {
            return name.equals("<init>") && call.getTarget().getRawParameterTypes().isEmpty();
        }
        if (owner.equals("java.util.Calendar")) {
            return name.equals("getInstance");
        }
        return name.equals("now") && WALL_CLOCK_TYPES.contains(owner);
    }

    /** The java.time types whose {@code now()} reads the machine's clock rather than a given one. */
    private static final Set<String> WALL_CLOCK_TYPES = Set.of(
            "java.time.Instant",
            "java.time.LocalDate",
            "java.time.LocalDateTime",
            "java.time.LocalTime",
            "java.time.OffsetDateTime",
            "java.time.OffsetTime",
            "java.time.Year",
            "java.time.YearMonth",
            "java.time.ZonedDateTime");
}
