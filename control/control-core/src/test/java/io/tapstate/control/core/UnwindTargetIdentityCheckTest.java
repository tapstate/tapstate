package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class UnwindTargetIdentityCheckTest {
    private final StartChecksBench bench = new StartChecksBench();

    @AfterEach
    void close() { bench.executor.close(); }

    @Test
    void automaticProjectionIsInformationalAndNeedsNoExtraConfirmation() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        bench.checks.add(new UnwindTargetIdentityCheck((definition, fullLoad) -> List.of(assessment("automatic"))));
        StartOutcome outcome = bench.service().start("alice", "pl", List.of(), null);
        assertThat(outcome.report().outcome()).isEqualTo(StartCheckReport.Outcome.READY);
        assertThat(outcome.report().findings()).anySatisfy(finding -> {
            assertThat(finding.code()).isEqualTo("start-check.target-identity-automatic");
            assertThat(finding.params()).containsEntry("identityStatus", "automatic")
                    .containsEntry("preserved", "_unwind_source_id");
        });
        assertThat(bench.desiredById).containsKey("pl");
    }

    @Test
    void anUnsafeTargetStopsBeforeDesiredStateOrAuditIsWritten() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        bench.checks.add(new UnwindTargetIdentityCheck((definition, fullLoad) -> List.of(assessment("action-required"))));
        StartRefusal refusal = catchThrowableOfType(StartRefusal.class,
                () -> bench.service().start("alice", "pl", List.of(), null));
        assertThat(refusal.report().outcome()).isEqualTo(StartCheckReport.Outcome.BLOCKED);
        assertThat(bench.desiredById).isEmpty();
        assertThat(bench.audit).isEmpty();
    }

    @Test
    void missingTargetMetadataNeverBecomesANoChangeResult() {
        bench.artifacts.put(StartChecksBench.pipeline(null));
        bench.checks.add(new UnwindTargetIdentityCheck((definition, fullLoad) -> {
            throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "metadata unavailable"), null);
        }));
        StartRefusal refusal = catchThrowableOfType(StartRefusal.class,
                () -> bench.service().start("alice", "pl", List.of(), null));
        assertThat(refusal.report().outcome()).isEqualTo(StartCheckReport.Outcome.BLOCKED);
        assertThat(refusal.report().findings()).anySatisfy(finding -> {
            assertThat(finding.check()).isEqualTo(UnwindTargetIdentityCheck.ID);
            assertThat(finding.evaluation()).isEqualTo(StartFinding.Evaluation.UNAVAILABLE);
        });
        assertThat(bench.desiredById).isEmpty();
    }

    private static PipelineTargetIdentities.Assessment assessment(String status) {
        return new PipelineTargetIdentities.Assessment("out", "warehouse", "orders", status,
                status.equals("automatic") ? "generated-target-identity" : "field-collision",
                "_id", "_unwind_source_id", List.of("_unwind_source_id", "TAG_NO"));
    }
}
