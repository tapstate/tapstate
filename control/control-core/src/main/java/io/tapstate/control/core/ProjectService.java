package io.tapstate.control.core;

import io.tapstate.core.dsl.ProjectLabel;
import io.tapstate.core.model.ProjectManifest;
import io.tapstate.core.model.Resource;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ArtifactStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The projects a server holds, read off the {@code project} label its resources carry. There is no
 * project document on the server: a project exists while something is labelled with it, except the
 * Default project, which always exists and holds every resource that carries no label.
 */
public final class ProjectService {

    /** One resource as a project lists it. */
    public record Member(String id, String kind) {
    }

    /**
     * One project: its id, the name it is shown under, whether it can be removed as a whole, and what it
     * holds, in id order.
     */
    public record ProjectSummary(String id, String title, boolean removable, List<Member> resources) {
        public ProjectSummary {
            resources = List.copyOf(resources);
        }
    }

    private final ArtifactStore store;
    private final ArtifactMutationService mutations;

    public ProjectService(ArtifactStore store, ArtifactMutationService mutations) {
        this.store = Objects.requireNonNull(store, "store");
        this.mutations = Objects.requireNonNull(mutations, "mutations");
    }

    /** Every project, the Default project first and present even when it holds nothing, then by id. */
    public List<ProjectSummary> list() {
        TreeMap<String, List<Member>> byProject = new TreeMap<>();
        List<Member> unlabelled = new ArrayList<>();
        for (Resource resource : ReadableArtifactInventory.list(store)) {
            String project = ProjectLabel.of(resource);
            Member member = new Member(resource.id(), resource.kind());
            if (project == null) {
                unlabelled.add(member);
            } else {
                byProject.computeIfAbsent(project, ignored -> new ArrayList<>()).add(member);
            }
        }
        List<ProjectSummary> projects = new ArrayList<>();
        projects.add(summary(ProjectManifest.DEFAULT, unlabelled));
        byProject.forEach((id, members) -> projects.add(summary(id, members)));
        return projects;
    }

    /**
     * Removes a project: every resource labelled with it, together (see
     * {@link ArtifactMutationService#deleteAll}). The Default project is refused - it can be emptied, not
     * removed - and a project nothing is labelled with is not found.
     */
    public void remove(String principal, String project) {
        Objects.requireNonNull(project, "project");
        if (ProjectManifest.DEFAULT.equals(project)) {
            throw new TapstateException(ArtifactError.DEFAULT_PROJECT_NOT_REMOVABLE, Map.of("project", project), null);
        }
        List<String> ids = list().stream()
                .filter(summary -> summary.id().equals(project))
                .flatMap(summary -> summary.resources().stream().map(Member::id))
                .toList();
        if (ids.isEmpty()) {
            throw new TapstateException(ArtifactError.NOT_FOUND, Map.of("id", project), null);
        }
        mutations.deleteAll(principal, ids);
    }

    private static ProjectSummary summary(String id, List<Member> members) {
        List<Member> sorted = members.stream().sorted(java.util.Comparator.comparing(Member::id)).toList();
        boolean isDefault = ProjectManifest.DEFAULT.equals(id);
        return new ProjectSummary(id, isDefault ? ProjectManifest.DEFAULT_TITLE : id, !isDefault, sorted);
    }
}
