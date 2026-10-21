package io.tapstate.archtests;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.PendingPipelineResume;
import io.tapstate.spi.store.StateStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

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
                            && (call.getName().equals("compareAndSwap") || call.getName().equals("create"))
                            && !isCanonicalStateCasForwarding(call);
                }
            };

    /** Only the two canonical CAS overloads may forward within the same store owner. */
    private static boolean isCanonicalStateCasForwarding(JavaMethodCall call) {
        if (!call.getOriginOwner().isAssignableTo(StateStore.class)
                || !call.getOriginOwner().equals(call.getTargetOwner())
                || !call.getOrigin().getName().equals("compareAndSwap")
                || !call.getName().equals("compareAndSwap")) {
            return false;
        }
        List<String> origin = call.getOrigin().getRawParameterTypes().stream()
                .map(type -> type.getName()).toList();
        List<String> target = call.getTarget().getRawParameterTypes().stream()
                .map(type -> type.getName()).toList();
        List<String> plain = List.of(String.class.getName(), long.class.getName(), String.class.getName(),
                Instant.class.getName());
        List<String> withResume = List.of(String.class.getName(), long.class.getName(), String.class.getName(),
                Instant.class.getName(), PendingPipelineResume.class.getName());
        return (origin.equals(plain) && target.equals(withResume))
                || (origin.equals(withResume) && target.equals(plain));
    }

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
    void canonicalCasOverloadForwardingIsNotAnIndependentStateWriter() {
        JavaClasses fixtures = new ClassFileImporter().importClasses(StateStore.class, CanonicalCasForwarder.class);
        List<JavaMethodCall> calls = fixtures.stream()
                .flatMap(type -> type.getMethodCallsFromSelf().stream())
                .filter(call -> call.getName().equals("compareAndSwap"))
                .toList();

        assertThat(calls).hasSize(2);
        assertThat(calls).extracting(call -> call.getOrigin().getRawParameterTypes().size())
                .containsExactlyInAnyOrder(4, 5);
        assertThat(calls).allSatisfy(call -> {
            assertThat(isCanonicalStateCasForwarding(call)).isTrue();
            assertThat(WRITES_ACTUAL_STATE.test(call)).isFalse();
        });
    }

    @Test
    void anExternalCallerAndANonCasStoreMethodStillFailTheWriterRule() {
        JavaClasses fixtures = new ClassFileImporter().importClasses(
                RogueStateWriter.class, NonCasStoreWriter.class, StateStore.class);
        List<JavaMethodCall> writes = stateWrites(fixtures);

        assertThat(writes).hasSize(3);
        assertThat(writes).extracting(call -> call.getName())
                .containsExactlyInAnyOrder("compareAndSwap", "create", "compareAndSwap");
        assertThat(writes).allSatisfy(call -> assertThat(isCanonicalStateCasForwarding(call)).isFalse());
        Throwable refused = catchThrowable(() -> assertSingleWriter(fixtures, WRITES_ACTUAL_STATE,
                List.of("io.tapstate.runtime.scheduler.", MIGRATION_PACKAGE), "fixture writers must remain forbidden"));
        assertThat(refused).isInstanceOf(AssertionError.class)
                .hasMessageContaining(RogueStateWriter.class.getName())
                .hasMessageContaining(NonCasStoreWriter.class.getName());
    }

    @Test
    void malformedAndCrossOwnerCasForwardsStillFailTheWriterRule() {
        JavaClasses fixtures = new ClassFileImporter().importClasses(
                MalformedCasForwarder.class, CrossOwnerCasForwarder.class, StateStore.class);
        List<JavaMethodCall> writes = stateWrites(fixtures);

        assertThat(writes).hasSize(3);
        assertThat(writes).allSatisfy(call -> assertThat(isCanonicalStateCasForwarding(call)).isFalse());
        Throwable refused = catchThrowable(() -> assertSingleWriter(fixtures, WRITES_ACTUAL_STATE,
                List.of("io.tapstate.runtime.scheduler.", MIGRATION_PACKAGE), "noncanonical forwards must remain forbidden"));
        assertThat(refused).isInstanceOf(AssertionError.class)
                .hasMessageContaining(MalformedCasForwarder.class.getName())
                .hasMessageContaining(CrossOwnerCasForwarder.class.getName());
    }

    private static List<JavaMethodCall> stateWrites(JavaClasses imported) {
        return imported.stream().flatMap(type -> type.getMethodCallsFromSelf().stream())
                .filter(WRITES_ACTUAL_STATE::test).toList();
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
        assertSingleWriter(tapstateClasses, writes, allowedPrefixes, because);
    }

    private static void assertSingleWriter(JavaClasses imported,
            DescribedPredicate<JavaMethodCall> writes, List<String> allowedPrefixes, String because) {
        List<JavaMethodCall> writeCalls = imported.stream()
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

    private abstract static class CanonicalCasForwarder implements StateStore {
        @Override
        public CasOutcome compareAndSwap(String pipelineId, long expectedEpoch, String stateJson, Instant touchTime) {
            return compareAndSwap(pipelineId, expectedEpoch, stateJson, touchTime, null);
        }

        @Override
        public CasOutcome compareAndSwap(String pipelineId, long expectedEpoch, String stateJson, Instant touchTime,
                PendingPipelineResume pendingResume) {
            return new CasOutcome.Fenced(expectedEpoch);
        }
    }

    private static final class RogueStateWriter {
        CasOutcome change(StateStore store, String pipelineId, long expectedEpoch, String stateJson, Instant touchTime) {
            return store.compareAndSwap(pipelineId, expectedEpoch, stateJson, touchTime);
        }

        void seed(StateStore store, String pipelineId, String stateJson, Instant touchTime) {
            store.create(pipelineId, stateJson, touchTime);
        }
    }

    private abstract static class NonCasStoreWriter implements StateStore {
        CasOutcome outsideCas(String pipelineId, long expectedEpoch, String stateJson, Instant touchTime) {
            return compareAndSwap(pipelineId, expectedEpoch, stateJson, touchTime, null);
        }
    }

    private abstract static class MalformedCasForwarder implements StateStore {
        @Override
        public CasOutcome compareAndSwap(String pipelineId, long expectedEpoch, String stateJson, Instant touchTime) {
            return compareAndSwap(pipelineId, expectedEpoch, stateJson, touchTime, (Object) null);
        }

        public CasOutcome compareAndSwap(String pipelineId, long expectedEpoch, String stateJson, Instant touchTime,
                Object pendingResume) {
            return compareAndSwap(pipelineId, expectedEpoch, stateJson, touchTime);
        }
    }

    private abstract static class CrossOwnerCasForwarder implements StateStore {
        private final StateStore delegate;

        CrossOwnerCasForwarder(StateStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CasOutcome compareAndSwap(String pipelineId, long expectedEpoch, String stateJson, Instant touchTime) {
            return delegate.compareAndSwap(pipelineId, expectedEpoch, stateJson, touchTime, null);
        }
    }
}
