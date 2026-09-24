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
 * <p>{@code preconditions} carries the version each draft declared it was editing, keyed by the id
 * that draft parsed to, and holds only the ids whose draft declared one. It travels with the plan
 * because the declaration belongs to the submitted draft, not to the artifact: by the time the plan
 * is written, the drafts are behind us and nothing else can say which write was version-checked.
 */
public record ApplyPlan(List<PreparedArtifact> artifacts, List<ValidationDiagnostic> warnings,
        Map<String, String> preconditions, Map<String, String> workspacePreconditions) {

    public ApplyPlan {
        artifacts = List.copyOf(artifacts);
        warnings = List.copyOf(warnings);
        preconditions = Map.copyOf(preconditions);
        workspacePreconditions = Map.copyOf(workspacePreconditions);
    }

    /** A plan whose drafts declared no version preconditions. */
    public ApplyPlan(List<PreparedArtifact> artifacts, List<ValidationDiagnostic> warnings) {
        this(artifacts, warnings, Map.of(), Map.of());
    }

    /** A plan with nothing advisory to say and no version preconditions declared. */
    public ApplyPlan(List<PreparedArtifact> artifacts) {
        this(artifacts, List.of(), Map.of(), Map.of());
    }

    /** The version the draft for {@code id} declared it was editing, or null when it declared none. */
    public String precondition(String id) {
        return preconditions.get(id);
    }
}
