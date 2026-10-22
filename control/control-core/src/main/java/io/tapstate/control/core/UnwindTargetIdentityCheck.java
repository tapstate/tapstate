package io.tapstate.control.core;

import io.tapstate.spi.store.StartLoad;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Refuses unsafe target identity mappings before changing desired pipeline state. */
public final class UnwindTargetIdentityCheck implements StartCheck {
    public static final String ID = "unwind-target-identity";
    private final PipelineTargetIdentities identities;

    public UnwindTargetIdentityCheck(PipelineTargetIdentities identities) {
        this.identities = Objects.requireNonNull(identities, "identities");
    }

    @Override public String id() { return ID; }
    @Override public StartFinding.Behavior whenUnavailable() { return StartFinding.Behavior.BLOCK; }

    @Override
    public List<StartFinding> evaluate(StartCheckContext context) {
        return identities.assess(context.definition(), context.plan().load() == StartLoad.FULL_LOAD)
                .stream().map(this::finding).toList();
    }

    private StartFinding finding(PipelineTargetIdentities.Assessment assessment) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("target", assessment.table());
        params.put("connection", assessment.connection());
        params.put("field", assessment.sourceField() == null ? "" : assessment.sourceField());
        params.put("preserved", assessment.preservedField() == null ? "" : assessment.preservedField());
        params.put("key", String.join(", ", assessment.writeKey()));
        params.put("reason", assessment.reason());
        params.put("identityStatus", assessment.status());
        boolean automatic = "automatic".equals(assessment.status());
        boolean compatible = "not-required".equals(assessment.status());
        return new StartFinding(ID, new StartFinding.Subject(StartFinding.Subject.TARGET,
                assessment.connection() + "/" + assessment.table() + "/" + assessment.element(),
                assessment.element(), assessment.table() + " on " + assessment.connection()),
                automatic || compatible ? StartFinding.Behavior.PASS : StartFinding.Behavior.BLOCK,
                StartFinding.Evaluation.COMPLETE,
                automatic ? StartCheckCode.TARGET_IDENTITY_AUTOMATIC
                        : compatible ? StartCheckCode.TARGET_IDENTITY_COMPATIBLE : StartCheckCode.TARGET_IDENTITY_REFUSED,
                params, List.of());
    }
}
