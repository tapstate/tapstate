package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The executor turns a specification into calls on a tier binding. What it must get right is order
 * and honesty: provisioning strictly before data, data strictly before steps, and a wait that fails
 * loudly on timeout reporting what it expected against what it actually read.
 */
class E2eExecutorTest {

    private static final String PIPELINE_ID = "mongo2mongo";
    private static final TableAlias SOURCE = new TableAlias("src_mongo", "orders");
    private static final TableAlias TARGET = new TableAlias("tgt_mongo", "orders");

    private final RecordingBinding binding = new RecordingBinding();

    /**
     * The dependency order, and the seed is part of it. A connector must be registered before a resource
     * naming it can be applied, and a resource must exist before its model can be discovered - but a model
     * is discovered from what the source holds, and what the source holds is what the seed put there. The
     * harness's own seed is what materializes the table: it drops and rewrites it, so a discovery running
     * first reads a table that is not there yet, comes back empty, and leaves the sink with no target model
     * and no key to upsert on. Discovering last is what makes the discovery real.
     */
    @Test
    void discoversBetweenTheSeedThatFeedsItAndTheApplyThatRequiresIt() {
        execute(
                """
                name: n
                setup:
                  connectors: [mongodb]
                  apply: [src_mongo.tap.yml, tgt_mongo.tap.yml]
                  discover: [src_mongo]
                pipeline: p.tap.yml
                seed:
                  src_mongo.orders: { rows: 3 }
                steps:
                  - start
                """);

        // Discovery is pinned from both sides, and this order is the whole assertion. It cannot precede
        // the seed, which is what materializes the table it reads. It cannot follow the apply, because a
        // pipeline whose expression reads a row field is refused unless its sources were discovered
        // first. Reading the resources is therefore its own step: the seed needs the source's address
        // and the discovery needs its connector and settings, both before the product is told anything.
        assertThat(binding.calls)
                .containsExactly(
                        "register:mongodb",
                        "read:[src_mongo.tap.yml, tgt_mongo.tap.yml]",
                        "seed:src_mongo.orders=3",
                        "discover:src_mongo",
                        // One apply, not one per file: the product resolves references within the set
                        // submitted together, so a pipeline and its source must arrive in the same batch.
                        // The pipeline is in it without having been listed - it is applied because the
                        // envelope names it, and the steps below drive exactly it.
                        "apply:[src_mongo.tap.yml, tgt_mongo.tap.yml, p.tap.yml]",
                        "drive:START");
    }

    /**
     * The pipeline the envelope names is applied, whether or not the author also listed it. It is the one
     * resource every specification declares by a field of its own, and a step that drives it is the most
     * ordinary thing a specification does - so a surface where naming it is not enough to have it exist is
     * a surface that reads correct and fails at the verb.
     *
     * <p>It rides in the same batch rather than one of its own: the pipeline names its source and target
     * by id, and the product resolves references within the set submitted together.
     */
    @Test
    void appliesThePipelineTheEnvelopeNamesEvenWhenTheAuthorListedOnlyItsEndpoints() {
        execute(
                """
                name: n
                setup:
                  apply: [src_mongo.tap.yml, tgt_mongo.tap.yml]
                pipeline: p.tap.yml
                steps:
                  - start
                """);

        assertThat(binding.calls)
                .contains("apply:[src_mongo.tap.yml, tgt_mongo.tap.yml, p.tap.yml]");
    }

    /**
     * And listing it is still allowed, because every checked-in example does. Applying it twice would
     * submit one id twice in a batch the product reads as a closure, so the spelling an author chooses
     * cannot change what is submitted.
     */
    @Test
    void appliesThePipelineOnceWhenTheAuthorListedItToo() {
        execute(
                """
                name: n
                setup:
                  apply: [src_mongo.tap.yml, p.tap.yml]
                pipeline: p.tap.yml
                steps:
                  - start
                """);

        assertThat(binding.calls).contains("apply:[src_mongo.tap.yml, p.tap.yml]");
    }

    @Test
    void drivesEveryLifecycleVerbInDeclarationOrder() {
        execute(minimal("steps:\n  - start\n  - pause\n  - resume\n  - stop\n"));

        // The apply leads because the pipeline these verbs drive is what it submits.
        assertThat(binding.calls)
                .containsExactly(
                        "apply:[p.tap.yml]", "drive:START", "drive:PAUSE", "drive:RESUME", "drive:STOP");
    }

    @Test
    void awaitPollsUntilTheMatcherHolds() {
        binding.countsOverTime(TARGET, 0L, 0L, 100L);

        execute(minimal("steps:\n  - start\n  - await: { count: { tgt_mongo.orders: 100 } }\n"));

        assertThat(binding.countReads).isEqualTo(3);
    }

    @Test
    void awaitFailsLoudlyReportingExpectedAgainstActual() {
        binding.countsOverTime(TARGET, 7L);

        assertThatThrownBy(() -> execute(minimal("steps:\n  - await: { count: { tgt_mongo.orders: 100 } }\n")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("timed out after")
                // Pinned as one phrase: asserting the numbers separately cannot tell this message
                // apart from one that reports them the wrong way round.
                .hasMessageContaining("tgt_mongo.orders expected 100, found 7");
    }

    @Test
    void assertChecksOnceAndDoesNotWait() {
        binding.countsOverTime(TARGET, 0L, 100L);

        assertThatThrownBy(() -> execute(minimal("steps:\n  - assert: { count: { tgt_mongo.orders: 100 } }\n")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("tgt_mongo.orders expected 100, found 0");
        assertThat(binding.countReads).isEqualTo(1);
    }

    @Test
    void awaitAndAssertReadTheSameMatcherVocabulary() {
        binding.countsOverTime(TARGET, 100L);

        execute(minimal("steps:\n  - await: { count: { tgt_mongo.orders: 100 } }\n"));
        execute(minimal("steps:\n  - assert: { count: { tgt_mongo.orders: 100 } }\n"));
    }

    @Test
    void awaitsTheStateOfThePipelineResolvedFromTheEnvelope() {
        binding.states(PipelineState.NEW, PipelineState.RUNNING);

        execute(minimal("steps:\n  - await: { state: RUNNING }\n"));

        assertThat(binding.stateReads).isEqualTo(2);
        // The state read must address the pipeline the envelope names, not a hand-copied id.
        assertThat(binding.statedPipelineIds).containsOnly(PIPELINE_ID);
    }

    @Test
    void awaitPollsThroughAPipelineThatHasPublishedNoObservationYet() {
        // The window every real run opens: a start intent is recorded, and until the first convergence pass
        // lands there is nothing published to read at all. Sitting through it is what the bound is for, so
        // an unpublished reading is "not yet" - a wait that failed here could never wait for a start.
        binding.unobservedThen(PipelineState.RUNNING);

        execute(minimal("steps:\n  - start\n  - await: { state: RUNNING }\n"));

        assertThat(binding.stateReads).isEqualTo(2);
    }

    @Test
    void reportsAnUnpublishedObservationAsTheReadingRatherThanAsAState() {
        binding.neverObserved();

        assertThatThrownBy(() -> execute(minimal("steps:\n  - assert: { state: RUNNING }\n")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("mongo2mongo expected RUNNING, found no published observation");
    }

    @Test
    void awaitsTheFailureCodeOfThePipelineResolvedFromTheEnvelope() {
        // A dead pipeline is already assertable as a state and a count; neither says what killed it, and a
        // regression that swaps the reason for another would pass both. The code is the assertable identity.
        binding.failureCodesUnobservedThen("engine.job-failed");

        execute(minimal("steps:\n  - await: { failure_code: engine.job-failed }\n"));

        assertThat(binding.failureCodedPipelineIds).containsOnly(PIPELINE_ID);
    }

    @Test
    void reportsTheFailureCodeMismatchAgainstTheResolvedPipeline() {
        binding.failureCodes("io.store-unauthorized");

        assertThatThrownBy(() -> execute(minimal("steps:\n  - assert: { failure_code: engine.job-failed }\n")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("mongo2mongo expected failure code engine.job-failed")
                .hasMessageContaining("io.store-unauthorized");
    }

    @Test
    void reportsAHealthyPipelineApartFromTheWrongFailureCode() {
        // "the pipeline never failed" and "it failed for another reason" send an author looking in different
        // places, so they must not read the same way.
        binding.failureCodeNeverPublished();

        assertThatThrownBy(() -> execute(minimal("steps:\n  - assert: { failure_code: engine.job-failed }\n")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("no published failure");
    }

    @Test
    void reportsTheStateMismatchAgainstTheResolvedPipeline() {
        binding.states(PipelineState.PAUSED);

        assertThatThrownBy(() -> execute(minimal("steps:\n  - assert: { state: RUNNING }\n")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("mongo2mongo expected RUNNING, found PAUSED");
    }

    @Test
    void awaitsTheErrorCountOfThePipelineResolvedFromTheEnvelope() {
        binding.errorCounts(0L, 0L, 1L);

        execute(minimal("steps:\n  - await: { error_count: 1 }\n"));

        assertThat(binding.errorCountReads).isEqualTo(3);
        // The metrics read must address the pipeline the envelope names, not a hand-copied id.
        assertThat(binding.errorCountedPipelineIds).containsOnly(PIPELINE_ID);
    }

    @Test
    void awaitsThroughAPipelineThatHasPublishedNoObservationYetForTheErrorCount() {
        // The same window the state matcher sits through: a start intent is recorded and there is nothing
        // published to read until the first convergence pass lands. An unpublished reading is "not yet", so a
        // wait for an error count can still wait for a start.
        binding.errorCountsUnobservedThen(1L);

        execute(minimal("steps:\n  - start\n  - await: { error_count: 1 }\n"));

        assertThat(binding.errorCountReads).isEqualTo(2);
    }

    @Test
    void reportsAnUnpublishedObservationAsTheErrorCountReadingRatherThanACount() {
        binding.errorCountNeverObserved();

        assertThatThrownBy(() -> execute(minimal("steps:\n  - assert: { error_count: 1 }\n")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("mongo2mongo expected error count 1, found no published observation");
    }

    @Test
    void reportsTheErrorCountMismatchAgainstTheResolvedPipeline() {
        binding.errorCounts(0L);

        assertThatThrownBy(() -> execute(minimal("steps:\n  - assert: { error_count: 1 }\n")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("mongo2mongo expected error count 1, found 0");
    }

    @Test
    void awaitsTheDiscardedChangesOfThePipelineResolvedFromTheEnvelope() {
        binding.deadLettered(0L, 0L, 3L);

        execute(minimal("steps:\n  - await: { dead_lettered: 3 }\n"));

        assertThat(binding.deadLetteredReads).isEqualTo(3);
        // The metrics read must address the pipeline the envelope names, not a hand-copied id.
        assertThat(binding.deadLetteredPipelineIds).containsOnly(PIPELINE_ID);
    }

    @Test
    void awaitsThroughAPipelineThatHasPublishedNoObservationYetForTheDiscardedChanges() {
        binding.deadLetteredUnobservedThen(2L);

        execute(minimal("steps:\n  - await: { dead_lettered: 2 }\n"));

        assertThat(binding.deadLetteredReads).isEqualTo(2);
    }

    /**
     * Asserting zero is the case worth having, and the reason this word exists. Nothing else a specification
     * can say distinguishes a nest that placed every row from one that quietly threw rows away: the row
     * counts, the state and the failure code all read identically either way.
     */
    @Test
    void reportsChangesThatWereDiscardedWhereTheSpecificationExpectedNone() {
        binding.deadLettered(4L);

        assertThatThrownBy(() -> execute(minimal("steps:\n  - assert: { dead_lettered: 0 }\n")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(
                        "mongo2mongo expected 0 changes that could not be placed in a document, found 4");
    }

    @Test
    void producesCdcChangesAgainstTheNamedTable() {
        execute(minimal("steps:\n  - cdc: { src_mongo.orders: insert 10 }\n"));

        assertThat(binding.calls).containsExactly("apply:[p.tap.yml]", "cdc:src_mongo.orders=INSERT x10");
    }

    /**
     * A valued change reaches the driver as the row and the value the specification wrote, not as a
     * count. Without this the two shapes could parse apart and still execute the same way, and the
     * whole point of the valued form is that the driver is told which row to move.
     */
    @Test
    void drivesAnUpdateThatNamesTheRowAndTheValue() {
        execute(minimal(
                "steps:\n  - cdc: { src_mongo.orders: { update: { where: { id: 2 }, set: { seq: 99 } } } }\n"));

        assertThat(binding.calls)
                .containsExactly("apply:[p.tap.yml]", "update:src_mongo.orders where={id=2} set={seq=99}");
    }

    /**
     * The scoped form has to reach a different seam from the bare one. Both spell {@code pause}, so an
     * executor that read the word and ignored the source would drive the whole pipeline and every
     * assertion about the other stream still moving would go on holding - there is nothing left running
     * to contradict it.
     */
    @Test
    void holdsOneStreamWithoutDrivingThePipeline() {
        execute(minimal("steps:\n  - pause: src_shipments\n  - resume: src_shipments\n"));

        assertThat(binding.calls)
                .containsExactly(
                        "apply:[p.tap.yml]", "stream:pause:src_shipments", "stream:resume:src_shipments");
        assertThat(binding.drivenPipelineIds)
                .as("a scoped hold is not a lifecycle intent and must not reach the pipeline")
                .isEmpty();
    }

    /**
     * A hold outlives the step that put it on, so something has to take it off. Leaving that to the
     * specification would make every case about a held stream end in a line that proves nothing, and a
     * case that forgot it would leave the next run waiting on a gate nobody remembers closing.
     */
    @Test
    void releasesAStreamTheSpecificationLeftHeld() {
        execute(minimal("steps:\n  - pause: src_shipments\n"));

        assertThat(binding.calls)
                .containsExactly("apply:[p.tap.yml]", "stream:pause:src_shipments", "stream:resume:src_shipments");
    }

    @Test
    void releasesAHeldStreamEvenWhenAStepFails() {
        assertThatThrownBy(() -> execute(minimal(
                        "steps:\n  - pause: src_shipments\n  - assert: { count: { src_mongo.orders: 99 } }\n")))
                .isInstanceOf(AssertionError.class);

        assertThat(binding.calls)
                .as("a run that fails while holding must still let go, or it poisons whatever runs next")
                .endsWith("stream:resume:src_shipments");
    }

    @Test
    void doesNotReleaseAStreamTheSpecificationAlreadyReleased() {
        execute(minimal("steps:\n  - pause: src_shipments\n  - resume: src_shipments\n"));

        assertThat(binding.calls)
                .containsExactly(
                        "apply:[p.tap.yml]", "stream:pause:src_shipments", "stream:resume:src_shipments");
    }

    @Test
    void keepsTheBareLifecycleWordDrivingTheWholePipeline() {
        execute(minimal("steps:\n  - pause\n"));

        assertThat(binding.calls).containsExactly("apply:[p.tap.yml]", "drive:PAUSE");
        assertThat(binding.drivenPipelineIds).containsExactly(PIPELINE_ID);
    }

    @Test
    void drivesADeleteThatNamesTheRow() {
        execute(minimal("steps:\n  - cdc: { src_mongo.orders: { delete: { where: { id: 2 } } } }\n"));

        assertThat(binding.calls)
                .containsExactly("apply:[p.tap.yml]", "delete:src_mongo.orders where={id=2}");
    }

    @Test
    void countMatcherOverSeveralTablesHoldsOnlyWhenEveryTableMatches() {
        binding.countsOverTime(TARGET, 100L);
        binding.countsOverTime(SOURCE, 1L);

        assertThatThrownBy(
                        () ->
                                execute(
                                        minimal(
                                                "steps:\n  - assert: { count: { tgt_mongo.orders: 100,"
                                                        + " src_mongo.orders: 2 } }\n")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("src_mongo.orders expected 2, found 1");
    }

    @Test
    void readsTheCountEndpointsInTheOrderTheAuthorWroteThem() {
        binding.countsOverTime(new TableAlias("c", "t"), 9L);
        binding.countsOverTime(new TableAlias("a", "t"), 9L);
        binding.countsOverTime(new TableAlias("b", "t"), 9L);

        execute(minimal("steps:\n  - assert: { count: { c.t: 9, a.t: 9, b.t: 9 } }\n"));

        assertThat(binding.readTables)
                .containsExactly(new TableAlias("c", "t"), new TableAlias("a", "t"), new TableAlias("b", "t"));
    }

    @Test
    void drivesTheVerbAgainstThePipelineResolvedFromTheEnvelope() {
        execute(minimal("steps:\n  - start\n"));

        assertThat(binding.drivenPipelineIds).containsExactly(PIPELINE_ID);
    }

    @Test
    void pollsAtLeastOnceEvenWithAZeroBound() {
        binding.countsOverTime(TARGET, 7L);

        assertThatThrownBy(
                        () ->
                                new E2eExecutor(binding, path -> PIPELINE_ID, Duration.ZERO, Duration.ofMillis(1))
                                        .execute(
                                                EnvelopeParser.parse(
                                                        minimal("steps:\n  - await: { count: { tgt_mongo.orders: 100 } }\n"))))
                .isInstanceOf(AssertionError.class);
        assertThat(binding.countReads).isEqualTo(1);
    }

    /**
     * A change written right after a real change stream comes up can land in the source before the
     * stream is positioned, and is then never delivered: the await that follows stalls at a reading
     * that never moves. The executor closes that window without a fixed sleep: an await that reads the
     * same mismatch enough consecutive polls, when the last step was a change, asks the binding to
     * redeliver the changed table's current rows. Redelivery re-asserts state row-wise, so a batch that
     * was merely slow is absorbed by the same keys rather than doubled.
     */
    @Test
    void aStalledAwaitAfterAChangeRedeliversTheChangedTable() {
        binding.countsOverTime(TARGET, 5L);
        binding.onRedeliverCountBecomes(TARGET, 8L);

        execute(minimal("steps:\n  - cdc: { src_mongo.orders: insert 3 }\n"
                + "  - await: { count: { tgt_mongo.orders: 8 } }\n"));

        assertThat(binding.calls).contains("redeliver:src_mongo.orders");
    }

    /** Redelivery exists for lost changes; an await with no change before it has nothing to redeliver. */
    @Test
    void aStalledAwaitWithNoPrecedingChangeNeverRedelivers() {
        binding.countsOverTime(TARGET, 5L);

        assertThatThrownBy(() -> execute(minimal("steps:\n  - await: { count: { tgt_mongo.orders: 8 } }\n")))
                .isInstanceOf(AssertionError.class);

        assertThat(binding.calls).noneMatch(call -> call.startsWith("redeliver:"));
    }

    /** A reading that keeps moving is a delivery in progress, not a loss; redelivering it would double it. */
    @Test
    void anAwaitStillMakingProgressIsNotRedelivered() {
        binding.countsOverTime(TARGET, 5L, 5L, 6L, 6L, 7L, 7L, 8L);

        execute(minimal("steps:\n  - cdc: { src_mongo.orders: insert 3 }\n"
                + "  - await: { count: { tgt_mongo.orders: 8 } }\n"));

        assertThat(binding.calls).noneMatch(call -> call.startsWith("redeliver:"));
    }

    /** Once an await has held, its change was delivered; a later stall is not that change's fault. */
    @Test
    void aDeliveredChangeIsNotRedeliveredByALaterAwait() {
        binding.countsOverTime(TARGET, 8L);
        binding.countsOverTime(SOURCE, 0L);

        assertThatThrownBy(() -> execute(minimal("steps:\n  - cdc: { src_mongo.orders: insert 3 }\n"
                + "  - await: { count: { tgt_mongo.orders: 8 } }\n"
                + "  - await: { count: { src_mongo.orders: 9 } }\n")))
                .isInstanceOf(AssertionError.class);

        assertThat(binding.calls).noneMatch(call -> call.startsWith("redeliver:"));
    }

    /**
     * A wait on the product's own observation is not a wait on delivered data. Its reading holds still
     * for the ordinary reason that the product has not converged yet - so on a long enough wait the
     * stall is the normal case, not the exceptional one - and no rewrite of the source would move it.
     * A specification that waits for a failure is usually asserting about the very rows a redelivery
     * would rewrite, so the harness would be mutating the fixture whose failure is the subject.
     */
    @Test
    void aStalledAwaitOnAPublishedStateNeverRedeliversTheSource() {
        binding.states(PipelineState.RUNNING);

        assertThatThrownBy(() -> execute(minimal("steps:\n  - cdc: { src_mongo.orders: insert 3 }\n"
                + "  - await: { state: FAILED }\n")))
                .isInstanceOf(AssertionError.class);

        assertThat(binding.calls).noneMatch(call -> call.startsWith("redeliver:"));
    }

    /** An error count is read the same way, and stalls the same way, so it redelivers no more than a state. */
    @Test
    void aStalledAwaitOnAnErrorCountNeverRedeliversTheSource() {
        binding.errorCounts(0L);

        assertThatThrownBy(() -> execute(minimal("steps:\n  - cdc: { src_mongo.orders: insert 3 }\n"
                + "  - await: { error_count: 1 }\n")))
                .isInstanceOf(AssertionError.class);

        assertThat(binding.calls).noneMatch(call -> call.startsWith("redeliver:"));
    }

    @Test
    void holdsADocumentToValuesByPathAndListSizesByPath() {
        binding.holdsDocument(TARGET, Map.of(
                "id", 1L,
                "name", "widget",
                "items", List.of(Map.of("sku", "a"), Map.of("sku", "b"))));

        execute(minimal("steps:\n  - assert: { doc: { tgt_mongo.orders: { where: { id: 1 }, "
                + "expect: { name: widget, \"items[1].sku\": b }, size: { items: 2 } } } }\n"));
    }

    /** Absence and disagreement send an author to different places, so they read differently. */
    @Test
    void reportsAnAbsentDocumentAsItsOwnMismatch() {
        assertThatThrownBy(() -> execute(minimal(
                "steps:\n  - assert: { doc: { tgt_mongo.orders: { where: { id: 1 }, expect: { name: widget } } } }\n")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("holds no document where {id=1}");
    }

    /** All the disagreeing paths, not the first: one read, one full account. */
    @Test
    void reportsEveryDisagreeingPathInOneReading() {
        binding.holdsDocument(TARGET, Map.of("id", 1L, "name", "gadget", "items", List.of()));

        assertThatThrownBy(() -> execute(minimal(
                "steps:\n  - assert: { doc: { tgt_mongo.orders: { where: { id: 1 }, "
                        + "expect: { name: widget, missing: x }, size: { items: 2 } } } }\n")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("at name expected widget, found gadget")
                .hasMessageContaining("has nothing at missing")
                .hasMessageContaining("at items expected 2 elements, found 0");
    }

    /** Whole numbers agree across representations: a store answering Int32 for a long is the same value. */
    @Test
    void holdsWholeNumbersAcrossTheirRepresentations() {
        binding.holdsDocument(TARGET, Map.of("id", 1, "seq", 7));

        execute(minimal("steps:\n  - assert: { doc: { tgt_mongo.orders: { where: { id: 1 }, "
                + "expect: { seq: 7 } } } }\n"));
    }

    private void execute(String yaml) {
        binding.calls.clear();
        new E2eExecutor(binding, path -> PIPELINE_ID, Duration.ofMillis(200), Duration.ofMillis(1))
                .execute(EnvelopeParser.parse(yaml));
    }

    private static String minimal(String body) {
        return "name: n\npipeline: p.tap.yml\n" + body;
    }

    /** Records what the executor asked for, and can vary a reading over successive polls. */
    private static final class RecordingBinding implements TierBinding {

        private final List<String> calls = new ArrayList<>();
        private final List<String> drivenPipelineIds = new ArrayList<>();
        private final List<String> statedPipelineIds = new ArrayList<>();
        private final List<TableAlias> readTables = new ArrayList<>();
        private final Map<TableAlias, List<Long>> countSeries = new HashMap<>();
        private final Map<TableAlias, AtomicInteger> countCursor = new HashMap<>();
        private List<Optional<PipelineState>> stateSeries = List.of(Optional.of(PipelineState.RUNNING));
        private List<Optional<Long>> errorCountSeries = List.of(Optional.of(0L));
        private final List<String> errorCountedPipelineIds = new ArrayList<>();
        private List<Optional<String>> failureCodeSeries = List.of(Optional.empty());
        private final List<String> failureCodedPipelineIds = new ArrayList<>();
        private List<Optional<Long>> deadLetteredSeries = List.of(Optional.of(0L));
        private final List<String> deadLetteredPipelineIds = new ArrayList<>();
        private int stateReads;
        private int errorCountReads;
        private int failureCodeReads;
        private int deadLetteredReads;
        private int countReads;

        void countsOverTime(TableAlias table, Long... readings) {
            countSeries.put(table, List.of(readings));
            countCursor.put(table, new AtomicInteger());
        }

        void states(PipelineState... readings) {
            stateSeries = Stream.of(readings).map(Optional::of).toList();
        }

        void errorCounts(Long... readings) {
            errorCountSeries = Stream.of(readings).map(Optional::of).toList();
        }

        /** Publishes no observation on the first read, then these counts: the window a real start opens. */
        void errorCountsUnobservedThen(Long... readings) {
            errorCountSeries =
                    Stream.concat(
                                    Stream.of(Optional.<Long>empty()),
                                    Stream.of(readings).map(Optional::of))
                            .toList();
        }

        /** Publishes nothing, ever: a pipeline no convergence pass has reached. */
        void errorCountNeverObserved() {
            errorCountSeries = List.of(Optional.empty());
        }

        void failureCodes(String... readings) {
            failureCodeSeries = Stream.of(readings).map(Optional::of).toList();
        }

        /** Publishes no failure on the first read, then these codes: the window before a job dies. */
        void failureCodesUnobservedThen(String... readings) {
            failureCodeSeries =
                    Stream.concat(
                                    Stream.of(Optional.<String>empty()),
                                    Stream.of(readings).map(Optional::of))
                            .toList();
        }

        /** Never publishes a failure: a pipeline that is running fine, or one never observed at all. */
        void failureCodeNeverPublished() {
            failureCodeSeries = List.of(Optional.empty());
        }

        void deadLettered(Long... readings) {
            deadLetteredSeries = Stream.of(readings).map(Optional::of).toList();
        }

        /** Publishes no observation on the first read, then these counts: the window a real start opens. */
        void deadLetteredUnobservedThen(Long... readings) {
            deadLetteredSeries =
                    Stream.concat(
                                    Stream.of(Optional.<Long>empty()),
                                    Stream.of(readings).map(Optional::of))
                            .toList();
        }

        /** Publishes nothing on the first read, then these states: the window a real start opens. */
        void unobservedThen(PipelineState... readings) {
            stateSeries =
                    Stream.concat(
                                    Stream.of(Optional.<PipelineState>empty()),
                                    Stream.of(readings).map(Optional::of))
                            .toList();
        }

        /** Publishes nothing, ever: a pipeline no convergence pass has reached. */
        void neverObserved() {
            stateSeries = List.of(Optional.empty());
        }

        @Override
        public void registerConnector(String connectorId) {
            calls.add("register:" + connectorId);
        }

        @Override
        public void applyResources(List<String> resourceFiles) {
            calls.add("apply:" + resourceFiles);
        }

        @Override
        public void readResources(List<String> resourceFiles) {
            calls.add("read:" + resourceFiles);
        }

        @Override
        public void discoverSchema(String resourceId) {
            calls.add("discover:" + resourceId);
        }

        @Override
        public void seed(TableAlias table, List<Map<String, Object>> rows) {
            calls.add("seed:" + table + "=" + rows.size());
        }

        private final Map<TableAlias, Map<String, Object>> fetchable = new HashMap<>();

        void holdsDocument(TableAlias table, Map<String, Object> document) {
            fetchable.put(table, document);
        }

        @Override
        public Optional<Map<String, Object>> fetch(TableAlias table, Map<String, Object> where) {
            calls.add("fetch:" + table + "=" + where);
            return Optional.ofNullable(fetchable.get(table));
        }

        @Override
        public void drive(String pipelineId, LifecycleVerb verb) {
            drivenPipelineIds.add(pipelineId);
            calls.add("drive:" + verb);
        }

        @Override
        public void driveStream(String sourceId, StreamVerb verb) {
            calls.add("stream:" + verb.word() + ":" + sourceId);
        }

        @Override
        public void restart(String pipelineId, boolean rereadEverything) {
            drivenPipelineIds.add(pipelineId);
            calls.add("restart:" + (rereadEverything ? "rerun" : "carry-on"));
        }

        @Override
        public void cdc(TableAlias table, CdcOp op, long rows) {
            calls.add("cdc:" + table + "=" + op + " x" + rows);
        }

        @Override
        public void update(TableAlias table, Map<String, Object> where, Map<String, Object> set) {
            calls.add("update:" + table + " where=" + where + " set=" + set);
        }

        @Override
        public void delete(TableAlias table, Map<String, Object> where) {
            calls.add("delete:" + table + " where=" + where);
        }

        @Override
        public void insert(TableAlias table, List<Map<String, Object>> rows) {
            calls.add("insert:" + table + " rows=" + rows);
        }

        private TableAlias redeliverMovesTable;
        private long redeliverMovesTo;

        /** Arranges for a redelivery to unblock a count, the way a re-emitted batch reaches a target. */
        void onRedeliverCountBecomes(TableAlias table, long value) {
            redeliverMovesTable = table;
            redeliverMovesTo = value;
        }

        @Override
        public void redeliver(TableAlias table) {
            calls.add("redeliver:" + table);
            if (redeliverMovesTable != null) {
                countsOverTime(redeliverMovesTable, redeliverMovesTo);
            }
        }

        @Override
        public long count(TableAlias table) {
            countReads++;
            readTables.add(table);
            List<Long> series = countSeries.getOrDefault(table, List.of(0L));
            int index = countCursor.computeIfAbsent(table, t -> new AtomicInteger()).getAndIncrement();
            return series.get(Math.min(index, series.size() - 1));
        }

        @Override
        public Optional<PipelineState> state(String pipelineId) {
            statedPipelineIds.add(pipelineId);
            int index = stateReads++;
            return stateSeries.get(Math.min(index, stateSeries.size() - 1));
        }

        @Override
        public Optional<Long> errorCount(String pipelineId) {
            errorCountedPipelineIds.add(pipelineId);
            int index = errorCountReads++;
            return errorCountSeries.get(Math.min(index, errorCountSeries.size() - 1));
        }

        @Override
        public Optional<String> failureCode(String pipelineId) {
            failureCodedPipelineIds.add(pipelineId);
            int index = failureCodeReads++;
            return failureCodeSeries.get(Math.min(index, failureCodeSeries.size() - 1));
        }

        @Override
        public Optional<Long> deadLettered(String pipelineId) {
            deadLetteredPipelineIds.add(pipelineId);
            int index = deadLetteredReads++;
            return deadLetteredSeries.get(Math.min(index, deadLetteredSeries.size() - 1));
        }
    }
}
