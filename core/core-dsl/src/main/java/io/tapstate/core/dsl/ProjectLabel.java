package io.tapstate.core.dsl;

import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ProjectManifest;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TransformResource;
import io.tapstate.core.model.ViewResource;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The reserved {@code project} label: which project a resource belongs to on a server. Apply writes
 * it onto every resource a project declares, so an author never has to. Writing it by hand is allowed
 * only when it agrees with the project it is applied from; a different value is refused rather than
 * silently replaced, because a file that claims another project's name is a mistake worth seeing.
 */
public final class ProjectLabel {

    /** The label key apply manages. */
    public static final String KEY = "project";

    private ProjectLabel() {
    }

    /**
     * Refuses {@code project} when it is the reserved id of the Default project. Nothing is applied as
     * the Default project: a resource belongs to it by carrying no label, so a label naming it would be a
     * second, disagreeing way to say the same thing.
     */
    public static void requireNotReserved(String project, String path) {
        if (ProjectManifest.DEFAULT.equals(project)) {
            throw new DslException(DslError.ILLEGAL_VALUE, path, 0, 0, null, Map.of("value", project,
                    "expected", "a project id other than `" + ProjectManifest.DEFAULT
                            + "`, which is reserved for the Default project"));
        }
    }

    /** The project {@code resource} is labelled with, or null when it carries no project label. */
    public static String of(Resource resource) {
        Metadata metadata = resource.metadata();
        return metadata == null ? null : metadata.labels().get(KEY);
    }

    /**
     * Refuses a resource whose hand-written project label names a project other than {@code project}.
     * The refusal names both values, so the author can see which one the file got wrong.
     */
    public static void requireConsistent(Resource resource, String project) {
        String written = of(resource);
        if (project == null) {
            // The Default project: a resource belongs to it by carrying no label, so any label is a claim
            // on a project this directory does not name.
            if (written != null) {
                throw new DslException(DslError.RESERVED_LABEL, "metadata.labels." + KEY, 0, 0, null,
                        Map.of("id", resource.id(), "key", KEY, "value", written,
                                "expected", ProjectManifest.DEFAULT));
            }
            return;
        }
        if (written != null && !written.equals(project)) {
            throw new DslException(DslError.RESERVED_LABEL, "metadata.labels." + KEY, 0, 0, null,
                    Map.of("id", resource.id(), "key", KEY, "value", written, "expected", project));
        }
    }

    /** {@code resource} carrying {@code project} as its project label, every other field unchanged. */
    public static Resource stamped(Resource resource, String project) {
        Objects.requireNonNull(project, "project");
        if (project.equals(of(resource))) {
            return resource;
        }
        Metadata metadata = resource.metadata();
        Map<String, String> labels = new LinkedHashMap<>(metadata == null ? Map.of() : metadata.labels());
        labels.put(KEY, project);
        return withMetadata(resource, new Metadata(labels, metadata == null ? null : metadata.description()));
    }

    private static Resource withMetadata(Resource resource, Metadata metadata) {
        return switch (resource) {
            case SourceResource s -> new SourceResource(s.id(), metadata, s.connector(), s.config(), s.mode(),
                    s.tables(), s.srs(), s.experimental());
            case PipelineResource p -> new PipelineResource(p.id(), metadata, p.sources(), p.transforms(),
                    p.view(), p.serve(), p.settings(), p.experimental());
            case TransformResource t -> new TransformResource(t.id(), metadata, t.body(), t.experimental());
            case ViewResource v -> new ViewResource(v.id(), metadata, v.primaryKey(), v.storage(),
                    v.experimental());
            case ServeResource s -> new ServeResource(s.id(), metadata, s.sync(), s.query(), s.push(),
                    s.experimental());
        };
    }
}
