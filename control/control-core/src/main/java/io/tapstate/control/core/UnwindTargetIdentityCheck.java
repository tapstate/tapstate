package io.tapstate.control.core;

import io.tapstate.spi.store.StartLoad;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Refuses unsafe target identity mappings before changing desired pipeline state. */
public final class UnwindTargetIdentityCheck implements StartCheck {
    public static final String CHECK_ID = "unwind-target-identity";
    private final PipelineTargetIdentities identities;

    public UnwindTargetIdentityCheck(PipelineTargetIdentities identities) {
        this.identities = Objects.requireNonNull(identities, "identities");
    }

    @Override public String id() { return CHECK_ID; }
    @Override public StartFinding.Behavior whenUnavailable() { return StartFinding.Behavior.BLOCK; }

    @Override
    public List<StartFinding> evaluate(StartCheckContext context) {
        if (!identities.relevant(context.definition())) return List.of();
        try {
            return identities.assess(context.definition(), context.plan().load() == StartLoad.FULL_LOAD)
                    .stream().map(this::finding).toList();
        } catch (io.tapstate.core.common.TapstateException diagnosed) {
            if (diagnosed.code() == io.tapstate.core.dsl.DslError.UNWIND_COLUMN_ALREADY_EXISTS) {
                // Assembly already rejects this source-column collision before any sink opens.
                // Preserve that lifecycle diagnosis rather than replace it with a target-identity error.
                return List.of();
            }
            throw diagnosed;
        }
    }

    private static StartCheckCode identityCode(String status) {
        return switch (status) {
            case "automatic" -> StartCheckCode.TARGET_IDENTITY_AUTOMATIC;
            case "not-required" -> StartCheckCode.TARGET_IDENTITY_COMPATIBLE;
            default -> StartCheckCode.TARGET_IDENTITY_REFUSED;
        };
    }

    private static String subjectId(PipelineTargetIdentities.Assessment assessment) {
        // Identifiers may contain the separator; encode each coordinate without changing its label.
        return List.of(assessment.connection(), assessment.table(), assessment.element(), assessment.stream())
                .stream().map(part -> URLEncoder.encode(part, StandardCharsets.UTF_8))
                .collect(java.util.stream.Collectors.joining("/"));
    }

    private StartFinding finding(PipelineTargetIdentities.Assessment assessment) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("target", assessment.table());
        params.put("stream", assessment.stream());
        params.put("connection", assessment.connection());
        params.put("field", assessment.sourceField() == null ? "" : assessment.sourceField());
        params.put("preserved", assessment.preservedField() == null ? "" : assessment.preservedField());
        params.put("key", String.join(", ", assessment.writeKey()));
        params.put("reason", assessment.reason());
        params.put("identityStatus", assessment.status());
        boolean automatic = "automatic".equals(assessment.status());
        boolean compatible = "not-required".equals(assessment.status());
        return new StartFinding(CHECK_ID, new StartFinding.Subject(StartFinding.Subject.TARGET,
                subjectId(assessment),
                assessment.element(), assessment.table() + " on " + assessment.connection() + " from " + assessment.stream()),
                automatic || compatible ? StartFinding.Behavior.PASS : StartFinding.Behavior.BLOCK,
                StartFinding.Evaluation.COMPLETE,
                identityCode(assessment.status()),
                params, List.of());
    }
}
