package io.tapstate.archtests;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.StateStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The pipeline lifecycle has a single write path on each side of the desired/actual split, and this
 * gate pins it: desired intent is written only through the control layer's one lifecycle service, and
 * actual state is written only through the runtime's one converge loop. No second control layer and no
 * second converger may write the store — a bypassing writer turns this red rather than quietly forking
 * the write path.
 *
 * <p>Each rule first asserts the write is actually called somewhere (a positive control) and only then
 * asserts every caller sits in the one allowed ring. Written this way rather than as a plain
 * {@code noClasses()} ban so the rule cannot pass vacuously: if the write call ever stops being matched (a
 * rename, a moved method), the positive control fails on the empty set instead of a silent green.
 */
class SingleWritePathRulesTest {

    private static JavaClasses tapstateClasses;

    /** A call that writes actual pipeline state: {@code StateStore.compareAndSwap} or {@code create}. */
    private static final DescribedPredicate<JavaMethodCall> WRITES_ACTUAL_STATE =
            new DescribedPredicate<>("a call that writes actual pipeline state") {
                @Override
                public boolean test(JavaMethodCall call) {
                    return call.getTargetOwner().isAssignableTo(StateStore.class)
                            && (call.getName().equals("compareAndSwap") || call.getName().equals("create"));
                }
            };

    /** A call that runs a system-data changeset: {@code ChangeSet.up}. */
    private static final DescribedPredicate<JavaMethodCall> RUNS_A_CHANGESET =
            new DescribedPredicate<>("a call that runs a system-data changeset") {
                @Override
                public boolean test(JavaMethodCall call) {
                    return call.getTargetOwner().isAssignableTo(ChangeSet.class)
                            && call.getName().equals("up");
                }
            };

    /**
     * The migrator's package. Changesets run before anything else touches the store, so a step that
     * reshapes what one of the writers below owns is the second legitimate writer of it -- and the only
     * one, which is what this names rather than leaves to be argued at the time.
     */
    private static final String MIGRATION_PACKAGE = "io.tapstate.adapters.mongostore.migration.";

    /** A call that writes desired pipeline intent: {@code DesiredStore.save}. */
    private static final DescribedPredicate<JavaMethodCall> WRITES_DESIRED_INTENT =
            new DescribedPredicate<>("a call that writes desired pipeline intent") {
                @Override
                public boolean test(JavaMethodCall call) {
                    return call.getTargetOwner().isAssignableTo(DesiredStore.class)
                            && call.getName().equals("save");
                }
            };

    @BeforeAll
    static void importTapstateClasses() {
        tapstateClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("io.tapstate");
    }

    @Test
    @DisplayName("actual pipeline state is written only by the runtime converge loop, or a changeset")
    void actualStateIsWrittenOnlyByTheConvergeLoop() {
        assertSingleWriter(WRITES_ACTUAL_STATE,
                List.of("io.tapstate.runtime.scheduler.", MIGRATION_PACKAGE),
                "actual state lands only through the single converge loop's fencing write and its seed");
    }

    @Test
    @DisplayName("desired pipeline intent is written only by the control lifecycle service, or a changeset")
    void desiredIntentIsWrittenOnlyByTheControlLayer() {
        assertSingleWriter(WRITES_DESIRED_INTENT,
                List.of("io.tapstate.control.core.", MIGRATION_PACKAGE),
                "desired intent is written only through the control layer's one lifecycle service");
    }

    @Test
    @DisplayName("a system-data changeset is run only by the migrator")
    void changesetsAreRunOnlyByTheMigrator() {
        assertSingleWriter(RUNS_A_CHANGESET, List.of("io.tapstate.adapters.mongostore.migration.MigrationRunner"),
                "a changeset runs only from the migrator, which is what holds the lock while it does; "
                        + "a second caller is a step reshaping the store with nothing keeping other "
                        + "members out of it");
    }

    /**
     * Asserts the write is called at all (positive control against a silently non-matching predicate) and
     * that every calling class sits under one of {@code allowedPrefixes}.
     */
    private static void assertSingleWriter(
            DescribedPredicate<JavaMethodCall> writes, List<String> allowedPrefixes, String because) {
        List<JavaMethodCall> writeCalls = tapstateClasses.stream()
                .flatMap(type -> type.getMethodCallsFromSelf().stream())
                .filter(writes::test)
                .toList();
        assertThat(writeCalls)
                .as("positive control: %s — the write must be called somewhere, or this gate is checking nothing", because)
                .isNotEmpty();
        assertThat(writeCalls).allSatisfy(call -> assertThat(allowedPrefixes)
                .as("%s; no second writer may call it (called from %s)", because, call.getOriginOwner().getName())
                .anySatisfy(prefix -> assertThat(call.getOriginOwner().getName()).startsWith(prefix)));
    }
}
