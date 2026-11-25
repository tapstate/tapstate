package io.tapstate.app;

import static org.assertj.core.api.Assertions.*;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.tapstate.adapters.mongostore.MongoHttpDiagnosticsScope;
import io.tapstate.adapters.mongostore.MongoHttpDiagnosticsScope.Phase;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class PipelineDiagnosticsTest {
    private final Logger logger = (Logger) LoggerFactory.getLogger("io.tapstate.diagnostics.pipeline");
    private final ListAppender<ILoggingEvent> written = new ListAppender<>();
    private Level previous;

    @BeforeEach void capture() {
        previous = logger.getLevel(); logger.setLevel(Level.DEBUG);
        written.start(); logger.addAppender(written);
    }
    @AfterEach void restore() {
        logger.detachAppender(written); written.stop(); logger.setLevel(previous);
    }
    private List<Map<?, ?>> events() {
        synchronized (written) {
            return new ArrayList<>(written.list).stream()
                    .<Map<?, ?>>map(event -> (Map<?, ?>) JsonReader.parse(event.getFormattedMessage())).toList();
        }
    }

    @Test void aBlockedWorkerHasABeginRecordAndRejectedAdmissionsNameTheOccupiedPipelines() throws Exception {
        var entered = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        try (var executor = new PipelinePassExecutor(2)) {
            Runnable blocked = () -> {
                try (var phase = PipelineDiagnostics.phase(Phase.PIPELINE_PUBLISH)) {
                    entered.countDown();
                    try { release.await(); }
                    catch (InterruptedException interrupted) { throw new AssertionError(interrupted); }
                }
            };
            try {
                assertThat(executor.submit("a", blocked)).isTrue();
                assertThat(executor.submit("b", blocked)).isTrue();
                assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
                assertThat(executor.submit("a", () -> fail("duplicate pass"))).isFalse();
                assertThat(executor.submit("c", () -> fail("saturated pass"))).isFalse();
                assertThat(events()).anySatisfy(event -> {
                    assertThat(event.get("phase")).isEqualTo("PIPELINE_PUBLISH");
                    assertThat(event.get("edge")).isEqualTo("begin");
                    assertThat(event.get("thread_id")).isInstanceOf(Number.class);
                });
                assertThat(events()).noneSatisfy(event -> {
                    assertThat(event.get("phase")).isEqualTo("PIPELINE_PUBLISH");
                    assertThat(event.get("edge")).isEqualTo("end");
                });
                assertThat(events()).anySatisfy(event -> {
                    assertThat(event.get("outcome")).isEqualTo("saturated");
                    assertThat(event.get("active_pipelines")).isEqualTo(List.of("a", "b"));
                });
                assertThat(events()).anySatisfy(event -> assertThat(event.get("outcome")).isEqualTo("in_flight"));
            } finally { release.countDown(); }
        }
        var ends = events().stream().filter(event -> "PIPELINE_PASS".equals(event.get("phase"))
                && "end".equals(event.get("edge"))).toList();
        assertThat(ends).hasSize(2);
        assertThat(ends).extracting(event -> event.get("pass_id")).doesNotHaveDuplicates();
        assertThat(ends).allSatisfy(event -> assertThat(event.get("complete")).isEqualTo(true));
    }

    @Test void failureIdentityIsPreservedAndTheNextPassDoesNotInheritItsContextOrSecrets() {
        var failure = new TapstateException(IoError.STORE_UNAVAILABLE,
                Map.of("detail", "credential-and-position-sentinel"), new IllegalStateException("private-body-sentinel"));
        assertThatThrownBy(() -> PipelineDiagnostics.measure("a", Phase.PIPELINE_START, () -> { throw failure; }))
                .isSameAs(failure);
        PipelineDiagnostics.measure("b", Phase.PIPELINE_START, () -> "done");
        var ends = events().stream().filter(event -> "end".equals(event.get("edge"))).toList();
        assertThat(ends).hasSize(2);
        assertThat(ends.getFirst().get("error_code")).isEqualTo(IoError.STORE_UNAVAILABLE.code());
        assertThat(ends.getLast().get("pipeline_id")).isEqualTo("b");
        assertThat(ends.getLast().containsKey("error_code")).isFalse();
        assertThat(ends).extracting(event -> event.get("pass_id")).doesNotHaveDuplicates();
        assertThat(written.list.toString()).doesNotContain("credential-and-position-sentinel", "private-body-sentinel");
    }

    @Test void nestingPreservesTheExistingRequestMeasurementAndLabelsInclusivePhaseTiming() {
        try (var request = MongoHttpDiagnosticsScope.open()) {
            PipelineDiagnostics.measure("a", Phase.PIPELINE_START, () -> {
                try (var child = PipelineDiagnostics.phase(Phase.PIPELINE_DAG_BUILD)) { return "done"; }
            });
            assertThat((Map<?, ?>) request.snapshot().get("phases")).isEmpty();
        }
        var root = events().stream().filter(event -> "PIPELINE_START".equals(event.get("phase"))
                && "end".equals(event.get("edge"))).findFirst().orElseThrow();
        assertThat(((Map<?, ?>) root.get("phases")).keySet())
                .isEqualTo(java.util.Set.of("pipeline_start", "pipeline_dag_build"));
        assertThat(root.get("measurement_scope")).isEqualTo("synchronous_thread");
        assertThat(root.get("timing_semantics").toString()).contains("inclusive_not_additive");
        assertThat(root.get("complete")).isEqualTo(true);
    }

    @Test void diagnosticsRemainOffWithoutExplicitLoggerOptIn() {
        logger.setLevel(Level.INFO);
        assertThat(PipelineDiagnostics.measure("a", Phase.PIPELINE_START, () -> "unchanged")).isEqualTo("unchanged");
        assertThat(events()).isEmpty();
    }

    @Test void oversizedIdentifiersHaveBoundedConsistentCorrelations() {
        String pipeline = "p".repeat(8192);
        String source = "s".repeat(8192);
        var attempt = PipelineDiagnostics.attempt(pipeline);
        attempt.admission("accepted", java.util.Set.of(pipeline));
        attempt.run(() -> PipelineDiagnostics.source(source));
        var values = events();
        assertThat(values.stream().map(event -> String.valueOf(event.get("pipeline_id"))).toList()).containsOnly(
                "sha256:" + io.tapstate.core.model.canonical.CanonicalHash.ofText(pipeline));
        assertThat(values).anySatisfy(event -> assertThat(event.get("source_id")).isEqualTo(
                "sha256:" + io.tapstate.core.model.canonical.CanonicalHash.ofText(source)));
        assertThat(written.list).allSatisfy(event -> assertThat(event.getFormattedMessage().length()).isLessThan(4096));
    }
}
