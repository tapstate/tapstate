package io.tapstate.control.core;

import io.tapstate.core.model.Resource;

import java.util.List;
import java.util.Objects;

/** A no-write candidate validation result and the complete workspace it was compiled against. */
public record CandidateWorkspacePlan(ApplyPlan plan, List<Resource> resources) {

    public CandidateWorkspacePlan {
        Objects.requireNonNull(plan, "plan");
        resources = List.copyOf(resources);
    }
}
