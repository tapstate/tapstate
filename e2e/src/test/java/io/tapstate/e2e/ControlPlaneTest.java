package io.tapstate.e2e;

import io.tapstate.control.core.MonitorError;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.PipelineState;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What the harness is allowed to read into a status answer.
 *
 * <p>"This pipeline has published no observation yet" is the one refusal a wait must sit through; every
 * other refusal has to stay loud. The whole of the wait model rests on telling those apart, so the rule is
 * pinned here directly rather than only through a live server - the answers that must not be confused are
 * the ones a running product is least willing to produce on demand.
 */
class ControlPlaneTest {

    private static final String PIPELINE = "mongo2mongo";

    @Test
    void readsThePublishedState() {
        assertThat(ControlPlane.interpretState(200, status("RUNNING"), PIPELINE)).contains(PipelineState.RUNNING);
    }

    @Test
    void readsTheProductsOwnNoObservationCodeAsNothingPublishedYet() {
        assertThat(ControlPlane.interpretState(404, coded(MonitorError.NO_OBSERVATION.code()), PIPELINE))
                .isEmpty();
    }

    /**
     * The rule is written on the code and not on the status, so a 404 that means something else stays loud.
     * The status read serves only this one code today, so no live server can produce the answer below - the
     * rule is pinned here against the day another one is added, which is exactly when a status-only reading
     * would start passing a real failure off as a pipeline that is merely slow.
     */
    @Test
    void keepsAnotherCodesNotFoundLoud() {
        // Asserted on the status this reports, not on the code: the message quotes the body back verbatim, so
        // a code the test itself planted there would be echoed by an implementation that never read it.
        assertThatThrownBy(
                        () ->
                                ControlPlane.interpretState(
                                        404, coded(LifecycleError.UNKNOWN_PIPELINE.code()), PIPELINE))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("got 404");
    }

    @Test
    void keepsAServerFailureLoud() {
        assertThatThrownBy(() -> ControlPlane.interpretState(500, "boom", PIPELINE))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("got 500");
    }

    /**
     * A refusal that is not the product's structured body at all - an empty answer, or a proxy's HTML. The
     * caller has to hear the status and the body it actually got; reading the body for a code must not throw
     * a parse error over the top of the diagnostic, which is the one thing this answer is good for.
     */
    @Test
    void keepsARefusalThatCarriesNoStructuredBodyLoud() {
        for (String body : List.of("", "<html><body>Not Found</body></html>", "boom")) {
            assertThatThrownBy(() -> ControlPlane.interpretState(404, body, PIPELINE))
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("got 404")
                    .hasMessageContaining(PIPELINE);
        }
    }

    @Test
    void refusesAnAnswerThatCarriesNoState() {
        assertThatThrownBy(
                        () ->
                                ControlPlane.interpretState(
                                        200, JsonWriter.write(Map.of("pipelineId", PIPELINE)), PIPELINE))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("carried no state");
    }

    // The metrics face is read the same way, so the same distinctions are pinned here: only no-observation
    // reads as "nothing yet", and a published observation is required to carry the errorCount the runtime
    // derives from the state, so an answer missing it is a contract regression, surfaced rather than waited out.

    @Test
    void readsThePublishedErrorCount() {
        assertThat(ControlPlane.interpretErrorCount(200, metrics(1), PIPELINE)).contains(1L);
    }

    @Test
    void readsTheProductsNoObservationCodeAsNothingPublishedYetForMetrics() {
        assertThat(ControlPlane.interpretErrorCount(404, coded(MonitorError.NO_OBSERVATION.code()), PIPELINE))
                .isEmpty();
    }

    @Test
    void keepsAnotherCodesNotFoundLoudForMetrics() {
        assertThatThrownBy(
                        () ->
                                ControlPlane.interpretErrorCount(
                                        404, coded(LifecycleError.UNKNOWN_PIPELINE.code()), PIPELINE))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("got 404");
    }

    @Test
    void keepsAServerFailureLoudForMetrics() {
        assertThatThrownBy(() -> ControlPlane.interpretErrorCount(500, "boom", PIPELINE))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("got 500");
    }

    @Test
    void refusesAMetricsAnswerThatCarriesNoMetrics() {
        assertThatThrownBy(
                        () ->
                                ControlPlane.interpretErrorCount(
                                        200, JsonWriter.write(Map.of("pipelineId", PIPELINE)), PIPELINE))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("carried no metrics");
    }

    @Test
    void refusesAPublishedObservationThatCarriesNoErrorCount() {
        // The runtime derives errorCount from the actual state, so a published observation always carries it;
        // an answer that does not is the metric wiring having regressed, and the harness says so loudly rather
        // than sitting out its whole bound as though the pipeline were slow to converge.
        assertThatThrownBy(
                        () ->
                                ControlPlane.interpretErrorCount(
                                        200,
                                        JsonWriter.write(Map.of("pipelineId", PIPELINE, "metrics", Map.of())),
                                        PIPELINE))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("carried no errorCount");
    }

    /**
     * The count a specification asserts on is one number, and the product publishes one per namespace. This
     * is where the two meet, so it is where a nest that discarded rows in a namespace nobody thought to look
     * at would go unreported.
     */
    @Test
    void addsUpTheDiscardedChangesOfEveryNamespaceThatReportedAny() {
        String body = JsonWriter.write(Map.of("pipelineId", PIPELINE, "metrics", Map.of(
                "errorCount", 0,
                "nestDeadLettered.nest.p.doc.items", 3,
                "nestDeadLettered.nest.p.doc.items.tags", 4)));

        assertThat(ControlPlane.interpretDeadLettered(200, body, PIPELINE)).contains(7L);
    }

    /**
     * A pipeline that discarded nothing publishes no such metric at all, and that reads as zero rather than
     * as nothing measured - which is what makes {@code dead_lettered: 0} an assertion a specification can
     * hold a healthy pipeline to, rather than a wait that never resolves.
     */
    @Test
    void readsAMetricsAnswerWithNoDiscardedChangesAsNoneRatherThanAsUnmeasured() {
        assertThat(ControlPlane.interpretDeadLettered(200, metrics(0L), PIPELINE)).contains(0L);
    }

    /** And an unpublished observation stays "not yet", the same window every other reading sits through. */
    @Test
    void readsAnUnpublishedObservationAsNothingYetForDiscardedChanges() {
        assertThat(ControlPlane.interpretDeadLettered(
                404, coded(MonitorError.NO_OBSERVATION.code()), PIPELINE)).isEmpty();
    }

    /** A metric whose name merely starts the same way is not one of these, and must not be added in. */
    @Test
    void countsOnlyTheMetricsThatAreDiscardedChanges() {
        String body = JsonWriter.write(Map.of("pipelineId", PIPELINE, "metrics", Map.of(
                "errorCount", 0,
                "nestStateEntries.nest.p.doc.items", 4_000,
                "nestDeadLettered.nest.p.doc.items", 3)));

        assertThat(ControlPlane.interpretDeadLettered(200, body, PIPELINE)).contains(3L);
    }

    // The durable position rides beside the metrics rather than inside them, because a position is a
    // string and every metric is a number. It is the one reading a witness may compare only for
    // difference: its shape belongs to the connector that issued it.

    /** The position for one table, taken from the map that carries them all. */
    @Test
    void readsTheDurablePositionOfOneTable() {
        String body = JsonWriter.write(Map.of("pipelineId", PIPELINE,
                "metrics", Map.of("errorCount", 0),
                "targetAckedPosition", Map.of("orders", "bin.000003:1544", "order_items", "bin.000003:2210")));

        assertThat(ControlPlane.interpretDurablePosition(200, body, PIPELINE, "order_items"))
                .contains("bin.000003:2210");
    }

    /**
     * A table with nothing acked yet reads as absent rather than as a crash, and so does a whole answer
     * carrying no positions at all. Both are real readings: positions appear only once something is acked,
     * so a witness watching for one has to be able to sit through the window where there is none.
     */
    @Test
    void readsATableWithNothingAckedYetAsAbsent() {
        String body = JsonWriter.write(Map.of("pipelineId", PIPELINE,
                "metrics", Map.of("errorCount", 0),
                "targetAckedPosition", Map.of("orders", "bin.000003:1544")));

        assertThat(ControlPlane.interpretDurablePosition(200, body, PIPELINE, "order_items")).isEmpty();
    }

    @Test
    void readsAnAnswerWithNoPositionsAtAllAsAbsent() {
        assertThat(ControlPlane.interpretDurablePosition(200, metrics(0L), PIPELINE, "orders")).isEmpty();
    }

    /** And an unpublished observation stays "not yet", the same window every other reading sits through. */
    @Test
    void readsAnUnpublishedObservationAsNothingYetForTheDurablePosition() {
        assertThat(ControlPlane.interpretDurablePosition(
                404, coded(MonitorError.NO_OBSERVATION.code()), PIPELINE, "orders")).isEmpty();
    }

    /** A server failure stays loud here too - it says nothing about where the frontier is. */
    @Test
    void keepsAServerFailureLoudForTheDurablePosition() {
        assertThatThrownBy(() -> ControlPlane.interpretDurablePosition(500, "boom", PIPELINE, "orders"))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("500");
    }

    // A verb the harness expects to be refused reads its answer the same way: the product declining a
    // request is the outcome under test, and everything else - it succeeding, or the server failing - is
    // the specification's own failure and has to stay loud. Only the client-error range is a refusal; a
    // 5xx is the server unable to answer at all, which says nothing about whether the request was
    // acceptable, and reading it as the expected refusal is how a real regression goes green.

    @Test
    void readsAClientErrorAsTheRefusalItIs() {
        assertThat(ControlPlane.interpretRefusal(400, coded("dsl.row-expression-type-unsupported"), "the batch"))
                .isEqualTo(new ControlPlane.Refusal(
                        400, "dsl.row-expression-type-unsupported", Map.of("pipeline", PIPELINE)));
    }

    /**
     * The named arguments travel with the refusal, because several refusals are only actionable through
     * them - which resources still reference the one being removed, what state a pipeline is actually in.
     * A reader that stopped at the code would leave a caller able to assert that a refusal happened but
     * never that it named the right thing.
     */
    @Test
    void carriesTheNamedArgumentsTheRefusalWasSentWith() {
        assertThat(ControlPlane.interpretRefusal(409, coded("artifact.in-use"), "the removal").params())
                .containsEntry("pipeline", PIPELINE);
    }

    /** A refusal that carried no arguments reads as none rather than as a missing field. */
    @Test
    void readsARefusalWithNoArgumentsAsCarryingNone() {
        assertThat(ControlPlane.interpretRefusal(
                        404, JsonWriter.write(Map.of("code", "artifact.not-found")), "the removal").params())
                .isEmpty();
    }

    /**
     * One null argument must not cost the whole refusal. The obvious defensive copy - {@code Map.copyOf} -
     * rejects a null value, so reading a body the server sent one in would throw here and lose the code and
     * message along with it, turning a precise refusal into an unexplained harness crash. The product side
     * has already been bitten by exactly this; the reader must not reintroduce it.
     */
    @Test
    void keepsARefusalWhoseArgumentTheServerSentAsNull() {
        ControlPlane.Refusal refusal = ControlPlane.interpretRefusal(
                409, "{\"code\":\"artifact.pipeline-not-stopped\",\"params\":{\"actual\":null}}", "the removal");

        assertThat(refusal.code()).isEqualTo("artifact.pipeline-not-stopped");
        // The value is pinned alongside the key: a reader that keeps the key but substitutes a
        // placeholder for the null would satisfy a key-only assertion while reporting an argument the
        // server never sent.
        assertThat(refusal.params()).containsEntry("actual", null);
    }

    /** Every 4xx, not only the one the product happens to use today. */
    @Test
    void readsAnyClientErrorAsARefusal() {
        assertThat(ControlPlane.interpretRefusal(409, coded("lifecycle.forbidden"), "the batch").status())
                .isEqualTo(409);
    }

    @Test
    void keepsAVerbThatWasNotRefusedAtAllLoud() {
        assertThatThrownBy(() -> ControlPlane.interpretRefusal(200, "{}", "the batch"))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("the batch");
    }

    @Test
    void keepsAServerFailureLoudRatherThanReadingItAsARefusal() {
        assertThatThrownBy(() -> ControlPlane.interpretRefusal(500, "boom", "the batch"))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("500");
    }

    /** A redirect is not the product declining anything either. */
    @Test
    void keepsARedirectLoud() {
        assertThatThrownBy(() -> ControlPlane.interpretRefusal(302, "", "the batch"))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("302");
    }

    private static String status(String state) {
        return JsonWriter.write(Map.of("pipelineId", PIPELINE, "state", state));
    }

    private static String metrics(long errorCount) {
        return JsonWriter.write(Map.of("pipelineId", PIPELINE, "metrics", Map.of("errorCount", errorCount)));
    }

    /** A structured coded error body, as the product's shared advice renders one. */
    private static String coded(String code) {
        return JsonWriter.write(Map.of("code", code, "params", Map.of("pipeline", PIPELINE), "message", "rendered"));
    }
}
