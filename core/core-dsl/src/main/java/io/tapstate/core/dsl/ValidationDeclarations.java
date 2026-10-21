package io.tapstate.core.dsl;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.TransformResource;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.ViewResource;
import io.tapstate.core.validation.ValidationError;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Resolves one selected output's declarations without running production operators or reading data.
 * Reusable bodies keep their call-site wiring. Dynamic references remain explicit obligations.
 *
 * <p>The caller supplies an owned, stable, already resource-admitted model snapshot. Its materialized
 * bodies, literal values and connector configuration are not copied or admitted by this traversal.
 * The limits below bound traversal and additional metadata. Required resource ids identify the
 * declarations whose versions a subsequent preflight must pin and recheck; they are not a store
 * transaction, version receipt, operator-support decision or native-read admission.
 */
public final class ValidationDeclarations {
    private ValidationDeclarations() {}

    public enum OutputKind { VIEW, SYNC }

    public record Output(OutputKind kind, String id) {
        public Output {
            Objects.requireNonNull(kind, "kind");
            if (id == null || id.isBlank() || id.length() > 512) throw invalid("validation.output.id");
        }
    }

    public record Limits(int nodes, int references, int routes, int depth, long metadataBytes) {
        public Limits { new Wiring.InspectionLimits(nodes, references, routes, depth, metadataBytes); }
        public static Limits defaults() { return new Limits(1024, 16384, 1024, 128, 4L * 1024 * 1024); }
        private Wiring.InspectionLimits wiring(long remaining) {
            return new Wiring.InspectionLimits(nodes, references, routes, depth, remaining);
        }
    }

    public record Route(String source, String table, FromRef selector, List<Step.Inline> steps) {
        public Route { steps = List.copyOf(steps); }
    }

    public record Pending(FromRef reference, List<Step.Inline> downstreamSteps) {
        public Pending { downstreamSteps = List.copyOf(downstreamSteps); }
    }

    /** A declaration result only; even an empty pending list does not authorize a validation run. */
    public static final class Inspection {
        private final Output output;
        private final FromClause from;
        private final ViewBlock.Inline view;
        private final SyncElement sync;
        private final Map<String, Step.Inline> steps;
        private final List<Route> routes;
        private final List<Pending> pending;
        private final Set<String> requiredResourceIds;
        private final long accountedMetadataBytes;

        private Inspection(Inspector inspector, FromClause from, List<Route> routes, List<Pending> pending) {
            output = inspector.output;
            this.from = from;
            view = inspector.view;
            sync = inspector.sync;
            steps = Map.copyOf(inspector.steps);
            this.routes = List.copyOf(routes);
            this.pending = List.copyOf(pending);
            requiredResourceIds = Set.copyOf(inspector.required);
            accountedMetadataBytes = inspector.bytes;
        }
        public Output output() { return output; }
        public FromClause from() { return from; }
        /** The selected or traversed view, or null when the selected path does not visit a view. */
        public ViewBlock.Inline view() { return view; }
        /** The selected sync, or null for a view selection. */
        public SyncElement sync() { return sync; }
        public Map<String, Step.Inline> steps() { return steps; }
        public List<Route> routes() { return routes; }
        public List<Pending> pending() { return pending; }
        public Set<String> requiredResourceIds() { return requiredResourceIds; }
        public long accountedMetadataBytes() { return accountedMetadataBytes; }
    }

    public static Inspection inspect(PipelineResource pipeline, Map<String, Resource> snapshot,
            Output output, Limits limits) {
        return new Inspector(Objects.requireNonNull(pipeline, "pipeline"),
                Objects.requireNonNull(snapshot, "snapshot"), Objects.requireNonNull(output, "output"),
                Objects.requireNonNull(limits, "limits")).inspect();
    }

    private static final class Inspector {
        private final PipelineResource pipeline;
        private final Map<String, Resource> snapshot;
        private final Output output;
        private final Limits limits;
        private final Set<String> required = new LinkedHashSet<>();
        private final Map<String, Step.Inline> steps = new LinkedHashMap<>();
        private ViewBlock.Inline view;
        private SyncElement sync;
        private long bytes;

        private Inspector(PipelineResource pipeline, Map<String, Resource> snapshot, Output output, Limits limits) {
            this.pipeline = pipeline; this.snapshot = snapshot; this.output = output; this.limits = limits;
            charge(512);
            requireId(pipeline.id());
        }

        private Inspection inspect() {
            FromClause from = selectOutput();
            if (bytes == limits.metadataBytes()) throw limit("bytes");
            Wiring.Inspection wiring = Wiring.inspect(pipeline, snapshot, from,
                    limits.wiring(limits.metadataBytes() - bytes));
            charge(wiring.accountedBytes());
            for (String source : wiring.sourceDeclarations()) read(source, SourceResource.class, "source");
            if (pipeline.transforms() != null) {
                for (Step step : pipeline.transforms()) {
                    if (wiring.nodes().contains(step.id())) {
                        charge(128L + 2L * step.id().length());
                        steps.put(step.id(), expandStep(step));
                    }
                }
            }
            if (pipeline.view() != null && wiring.nodes().contains(viewId(pipeline.view()))) {
                view = expandView(pipeline.view());
            }
            List<Route> routes = new ArrayList<>();
            for (Wiring.InspectedRoute route : wiring.routes()) {
                charge(256L + 32L * route.steps().size());
                routes.add(new Route(route.upstream().source(), route.upstream().table(), route.selector(), expanded(route.steps())));
            }
            List<Pending> pending = new ArrayList<>();
            for (Wiring.PendingReference reference : wiring.pending()) {
                charge(256L + 32L * reference.downstreamSteps().size());
                pending.add(new Pending(reference.reference(), expanded(reference.downstreamSteps())));
            }
            return new Inspection(this, from, routes, pending);
        }

        private FromClause selectOutput() {
            if (output.kind() == OutputKind.VIEW) {
                if (pipeline.view() == null || !output.id().equals(viewId(pipeline.view()))) throw missing("validation.output", output.id());
                return FromClause.list(FromRef.literal(output.id()));
            }
            FromClause from;
            List<SyncElement> candidates;
            switch (pipeline.serve()) {
                case ServeBlock.Inline inline -> { from = inline.from(); candidates = inline.sync(); }
                case ServeBlock.Use use -> {
                    ServeResource definition = read(use.use(), ServeResource.class, "serve.use");
                    from = use.from(); candidates = definition.sync();
                }
                case null -> throw missing("validation.output", output.id());
            }
            if (candidates == null) throw missing("validation.output", output.id());
            if (candidates.size() > limits.nodes()) throw limit("outputs");
            for (SyncElement candidate : candidates) {
                if (output.id().equals(candidate.id())) {
                    if (sync != null) throw new DslException(DslError.DUPLICATE_ID, "serve.sync", 0, 0, null, Map.of("id", output.id()));
                    sync = candidate;
                }
            }
            if (sync == null) throw missing("validation.output", output.id());
            read(sync.source(), SourceResource.class, "serve.sync.source");
            charge(128L + 2L * output.id().length());
            return from;
        }

        private Step.Inline expandStep(Step step) {
            if (step instanceof Step.Inline inline) return inline;
            Step.Use use = (Step.Use) step;
            TransformResource definition = read(use.use(), TransformResource.class, "transforms." + use.id() + ".use");
            boolean aliases = definition.body() instanceof TransformBody.Nest || definition.body() instanceof TransformBody.Join;
            if (aliases != (use.from() instanceof FromClause.Aliases)) {
                throw new DslException(DslError.COMPOSITION, "transforms." + use.id() + ".from", 0, 0, null,
                        Map.of("detail", "reusable transform input form does not match its body"));
            }
            // The model constructor copies this map, while its values and the body remain borrowed.
            if (definition.experimental() != null) charge(128L + 96L * definition.experimental().size());
            return new Step.Inline(use.id(), use.from(), definition.body(), use.execution(), definition.experimental());
        }

        private ViewBlock.Inline expandView(ViewBlock block) {
            if (block instanceof ViewBlock.Inline inline) return inline;
            ViewBlock.Use use = (ViewBlock.Use) block;
            ViewResource definition = read(use.use(), ViewResource.class, "view.use");
            charge(128L + 2L * use.id().length());
            return new ViewBlock.Inline(use.id(), use.from(), definition.primaryKey(), definition.storage(), definition.execution());
        }

        private List<Step.Inline> expanded(List<Step> route) {
            List<Step.Inline> result = new ArrayList<>(route.size());
            for (Step step : route) result.add(Objects.requireNonNull(steps.get(step.id()), "resolved step"));
            return result;
        }

        private <T extends Resource> T read(String id, Class<T> type, String path) {
            Resource resource = snapshot.get(id);
            if (!type.isInstance(resource) || !id.equals(resource.id())) throw missing(path, id);
            requireId(id);
            return type.cast(resource);
        }

        private void requireId(String id) {
            if (required.contains(id)) return;
            if (required.size() == limits.nodes()) throw limit("resources");
            charge(128L + 2L * id.length());
            required.add(id);
        }
        private void charge(long amount) {
            if (amount > limits.metadataBytes() - bytes) throw limit("bytes");
            bytes += amount;
        }
    }

    private static String viewId(ViewBlock view) {
        return view instanceof ViewBlock.Inline inline ? inline.id() : ((ViewBlock.Use) view).id();
    }
    private static DslException missing(String path, String id) {
        return new DslException(DslError.MISSING_REFERENCE, path, 0, 0, null, Map.of("ref", id));
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
    private static TapstateException limit(String dimension) {
        return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                Map.of("limit", "validation.declarations." + dimension), null);
    }
}
