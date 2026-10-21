package io.tapstate.control.core;

import java.util.List;
import java.util.Map;

/**
 * The result of validating and canonicalizing a batch of drafts: the artifacts an apply would
 * upsert, in submission order, and the advisory findings over them. Producing a plan performs no
 * writes — comparing each hash against the store and upserting the changed artifacts is the caller's
 * next step.
 *
 * <p>The warnings are notes about a batch that validated, never reasons it did not: a plan exists only
 * for a batch that passed, and a plan carrying warnings is applied exactly like one that carries none.
 *
 * <p>{@code preconditions} carries a caller-declared version or the stored version from which an
 * existing partial Source copied omitted fields, keyed by resource id. It travels with the plan
 * because the declaration and field presence belong to the submitted draft, not to the artifact.
 */
public record ApplyPlan(List<PreparedArtifact> artifacts, List<ValidationDiagnostic> warnings,
        Map<String, String> preconditions, Map<String, String> workspacePreconditions) {

    public ApplyPlan {
        artifacts = List.copyOf(artifacts);
        warnings = List.copyOf(warnings);
        preconditions = Map.copyOf(preconditions);
        workspacePreconditions = Map.copyOf(workspacePreconditions);
    }

    /** A plan with no write preconditions. */
    public ApplyPlan(List<PreparedArtifact> artifacts, List<ValidationDiagnostic> warnings) {
        this(artifacts, warnings, Map.of(), Map.of());
    }

    /** A plan with nothing advisory to say and no write preconditions. */
    public ApplyPlan(List<PreparedArtifact> artifacts) {
        this(artifacts, List.of(), Map.of(), Map.of());
    }

    /** The version the write for {@code id} must still match, or null when it needs no condition. */
    public String precondition(String id) {
        return preconditions.get(id);
    }
}
