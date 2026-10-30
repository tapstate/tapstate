package io.tapstate.core.model;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/** Pure connector identities resolved from one authoring snapshot, shared by preparation and cleanup. */
public final class PipelineConnectorNodes {
    private PipelineConnectorNodes() { }

    public record Identity(PipelineNode node, PipelineNode.Purpose purpose) {
        public Identity {
            Objects.requireNonNull(node, "node");
            Objects.requireNonNull(purpose, "purpose");
        }
    }

    public static Set<Identity> of(PipelineResource pipeline, Function<String, Resource> definitions) {
        Objects.requireNonNull(pipeline, "pipeline");
        Objects.requireNonNull(definitions, "definitions");
        Set<Identity> result = new LinkedHashSet<>();
        pipeline.sources().forEach(source -> result.add(new Identity(
                new PipelineNode(pipeline.id(), source.id()), PipelineNode.Purpose.CAPTURE)));
        String viewId = switch (pipeline.view()) {
            case ViewBlock.Inline view -> view.id();
            case ViewBlock.Use view -> view.id();
            case null -> null;
        };
        if (viewId != null) result.add(new Identity(new PipelineNode(pipeline.id(), viewId), PipelineNode.Purpose.VIEW));
        List<SyncElement> sync = switch (pipeline.serve()) {
            case ServeBlock.Inline serve -> serve.sync();
            case ServeBlock.Use use -> {
                Resource definition = definitions.apply(use.use());
                if (!(definition instanceof ServeResource serve)) {
                    throw new IllegalStateException("a referenced serve definition is missing or has another kind");
                }
                yield serve.sync();
            }
            case null -> null;
        };
        if (sync != null) sync.forEach(element -> result.add(new Identity(
                new PipelineNode(pipeline.id(), syncNodeId(element)), PipelineNode.Purpose.SYNC)));
        return Collections.unmodifiableSet(result);
    }

    public static String syncNodeId(SyncElement element) {
        return element.id() != null && !element.id().isBlank() ? element.id() : element.source();
    }
}
