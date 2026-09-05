package io.tapstate.core.dsl;

import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TransformResource;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.ViewResource;
import io.tapstate.core.model.ServeResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Batch reference closure (plan poc1 B3-6). Offline the closure is the batch (ADR-0021 §3):
 * every reference a pipeline makes must resolve within the loaded resources. Per pipeline, in
 * order: minimal composition (X17), pipeline-internal id uniqueness, {@code source:} id
 * references, step-id no-shadowing (ADR-0016 §5), {@code from:} addressing (step id / view id /
 * table universe / {@code /…/} regex — ADR-0016 §4/§5/§8), {@code use:} definition references
 * (X19), and {@code sync}/{@code push} connection-source references (X18). The first violation
 * throws a {@link DslException} whose {@code rule} is a corpus-vocabulary key (corpus/README.md).
 *
 * <p>Scope note: the alias-map of a {@code nest}/{@code join} step is wiring and its values are
 * resolved here; the step body's internal alias references ({@code root.from} / {@code embed.from})
 * resolve against that alias map, not the batch, and are out of closure scope.
 */
final class ReferenceClosure {

    private final Map<String, Resource> byId = new LinkedHashMap<>();
    private final Map<String, SourceResource> sources = new LinkedHashMap<>();

    private ReferenceClosure(Collection<Resource> batch) {
        for (Resource r : batch) {
            byId.put(r.id(), r);
            if (r instanceof SourceResource s) {
                sources.put(s.id(), s);
            }
        }
    }

    /** Validates every pipeline's references against the batch; throws on the first violation. */
    static void validate(Collection<Resource> batch) {
        ReferenceClosure closure = new ReferenceClosure(batch);
        for (Resource r : batch) {
            if (r instanceof PipelineResource p) {
                closure.validatePipeline(p);
            }
        }
    }

    private void validatePipeline(PipelineResource p) {
        // The visual editor creates a persisted blank draft before any Source has been chosen.
        // It is intentionally not runnable yet; the next typed save must supply the full
        // source + view/serve composition before the normal closure checks apply.
        if (p.sources().isEmpty() && (p.transforms() == null || p.transforms().isEmpty())
                && p.view() == null && p.serve() == null) {
            return;
        }
        // X17 minimal composition: source (model guarantees non-empty) + an output surface.
        if (p.view() == null && p.serve() == null) {
            throw new DslException(DslError.COMPOSITION, "", 0, 0, null,
                    Map.of("detail", "pipeline '" + p.id() + "' has neither view nor serve; "
                            + "minimal composition = source + view/serve (X17)"));
        }
        checkInternalIds(p);
        // source: id references must name kind: source resources in the batch.
        for (String sid : p.sources()) {
            if (!sources.containsKey(sid)) {
                throw missing("source", sid);
            }
        }

        Set<String> stepIds = stepIds(p);
        List<SourceTables> universe = universeOf(p);
        checkNoShadowing(p, universe);

        List<Step> transforms = p.transforms();
        if (transforms != null) {
            for (int i = 0; i < transforms.size(); i++) {
                Step step = transforms.get(i);
                resolveFrom(step.from(), stepIds, universe, "transforms[" + i + "].from");
                if (step instanceof Step.Use u) {
                    resolveUse(u.use(), Kind.TRANSFORM, "transforms[" + i + "].use");
                }
            }
        }

        ViewBlock view = p.view();
        if (view != null) {
            resolveRef(viewFrom(view), stepIds, universe, "view.from");
            if (view instanceof ViewBlock.Use u) {
                resolveUse(u.use(), Kind.VIEW, "view.use");
            }
        }

        ServeBlock serve = p.serve();
        if (serve != null) {
            resolveFrom(serveFrom(serve), serveScope(stepIds, view), universe, "serve.from");
            validateServeSinks(serve);
            if (serve instanceof ServeBlock.Use u) {
                resolveUse(u.use(), Kind.SERVE, "serve.use");
            }
        }
    }

    // ---- internal id uniqueness ---------------------------------------------------------

    /**
     * Pipeline-internal ids share one flat namespace — the runtime {@code <pipeline_id>.<id>}
     * address — across transform steps, the inline view, the inline serve block, and its
     * sync / push elements (2026-06-15 decision; F8 uniqueness extended inward). Ids are
     * already post-generation here, so two anonymous syncs ({@code sync_1} / {@code sync_2})
     * never collide, while a generated id clashing with a declared one is caught. The pipeline's
     * own top-level id and its source ids are separate namespaces and are excluded.
     */
    private void checkInternalIds(PipelineResource p) {
        Set<String> seen = new HashSet<>();
        if (p.transforms() != null) {
            for (int i = 0; i < p.transforms().size(); i++) {
                reserve(seen, p.transforms().get(i).id(), "transforms[" + i + "].id");
            }
        }
        if (p.view() != null) {
            reserve(seen, viewId(p.view()), "view.id");
        }
        if (p.serve() instanceof ServeBlock.Inline serve) {
            reserve(seen, serve.id(), "serve.id");
            if (serve.sync() != null) {
                for (int i = 0; i < serve.sync().size(); i++) {
                    reserve(seen, serve.sync().get(i).id(), "serve.sync[" + i + "].id");
                }
            }
            if (serve.push() != null) {
                for (int i = 0; i < serve.push().size(); i++) {
                    reserve(seen, serve.push().get(i).id(), "serve.push[" + i + "].id");
                }
            }
        }
    }

    private static void reserve(Set<String> seen, String id, String path) {
        if (id != null && !seen.add(id)) {
            throw new DslException(DslError.DUPLICATE_ID, path, 0, 0, null, Map.of("id", id));
        }
    }

    /**
     * No-shadowing (ADR-0016 §5): a transform step id — declared or generated — must not equal a
     * referenced source id or a table name in the universe, or {@code from:} resolution could not
     * tell the step's output apart from the source / table it shadows. This is a distinct check from
     * internal uniqueness above: it crosses namespaces (step id vs the source-id and table-name
     * spaces). Offline only the closeable half is enforced — source ids in full, but only the
     * <em>literal</em> table names; open-universe table names (a source that omits {@code tables} or
     * uses a regex) cannot be enumerated without a connection, so that half is deferred to
     * connected-mode validation (the same §4 offline projection the closure itself accepts).
     */
    private void checkNoShadowing(PipelineResource p, List<SourceTables> universe) {
        if (p.transforms() == null) {
            return;
        }
        Set<String> sourceIds = new LinkedHashSet<>(p.sources());
        Set<String> literalTables = new HashSet<>();
        for (SourceTables st : universe) {
            literalTables.addAll(st.literals);
        }
        List<Step> steps = p.transforms();
        for (int i = 0; i < steps.size(); i++) {
            String id = steps.get(i).id();
            String path = "transforms[" + i + "].id";
            if (sourceIds.contains(id) || literalTables.contains(id)) {
                // step id shadows a source id or a literal table name — either way from: addressing
                // could not tell the step output apart from what it shadows (ADR-0016 §5)
                throw new DslException(DslError.DUPLICATE_ID, path, 0, 0, null, Map.of("id", id));
            }
        }
    }

    // ---- from: resolution ---------------------------------------------------------------

    private void resolveFrom(FromClause from, Set<String> scope, List<SourceTables> universe, String path) {
        switch (from) {
            // streaming list: each ref is a step id / table / regex.
            case FromClause.Flow f -> {
                for (FromRef ref : f.refs()) {
                    resolveRef(ref, scope, universe, path);
                }
            }
            // nest/join wiring: only the alias-map values are batch references.
            case FromClause.Aliases a -> {
                for (FromRef ref : a.aliases().values()) {
                    resolveRef(ref, scope, universe, path);
                }
            }
        }
    }

    private void resolveRef(FromRef ref, Set<String> scope, List<SourceTables> universe, String path) {
        if (ref instanceof FromRef.Regex) {
            return;     // dynamic link: zero match is a warning, never a closure error (§4)
        }
        String token = ((FromRef.Literal) ref).ref();
        int dot = token.indexOf('.');
        if (dot >= 0) {
            resolveQualified(token, dot, universe, path);
            return;
        }
        if (scope.contains(token)) {
            return;     // a step id, or (for serve.from) the view id
        }
        int matches = 0;
        boolean anyOpen = false;
        for (SourceTables st : universe) {
            if (st.literals.contains(token)) {
                matches++;
            }
            if (st.open) {
                anyOpen = true;
            }
        }
        if (matches >= 2) {
            throw new DslException(DslError.AMBIGUOUS_REFERENCE, path, 0, 0, null, Map.of("ref", token));
        }
        if (matches == 0 && !anyOpen) {
            throw missing(path, token);
        }
        // matches == 1 (resolved as a table), or matches == 0 with an open source (cannot prove missing).
    }

    /** {@code <source_id>.<table>} disambiguation (§4): the prefix must be a pipeline source. */
    private void resolveQualified(String token, int dot, List<SourceTables> universe, String path) {
        String prefix = token.substring(0, dot);
        String table = token.substring(dot + 1);
        for (SourceTables st : universe) {
            if (st.sourceId.equals(prefix)) {
                if (!st.open && !st.literals.contains(table)) {
                    throw missing(path, token);
                }
                return;
            }
        }
        throw missing(path, token);
    }

    // ---- use: and sink references -------------------------------------------------------

    private void resolveUse(String useId, Kind kind, String path) {
        Resource target = byId.get(useId);
        boolean resolved = switch (kind) {
            case TRANSFORM -> target instanceof TransformResource;
            case VIEW -> target instanceof ViewResource;
            case SERVE -> target instanceof ServeResource;
        };
        if (!resolved) {
            throw missing(path, useId);
        }
    }

    private void validateServeSinks(ServeBlock serve) {
        // A use-reference serve carries its sinks in the referenced kind: serve definition;
        // inline serve owns them directly (the corpus form).
        if (!(serve instanceof ServeBlock.Inline inline)) {
            return;
        }
        if (inline.sync() != null) {
            for (int i = 0; i < inline.sync().size(); i++) {
                String src = inline.sync().get(i).source();
                if (!sources.containsKey(src)) {
                    throw missing("serve.sync[" + i + "].source", src);
                }
            }
        }
        if (inline.push() != null) {
            for (int i = 0; i < inline.push().size(); i++) {
                String src = inline.push().get(i).source();
                if (!sources.containsKey(src)) {
                    throw missing("serve.push[" + i + "].source", src);
                }
            }
        }
    }

    // ---- pipeline addressing scope ------------------------------------------------------

    private static Set<String> stepIds(PipelineResource p) {
        Set<String> ids = new HashSet<>();
        if (p.transforms() != null) {
            for (Step s : p.transforms()) {
                ids.add(s.id());
            }
        }
        return ids;
    }

    /** serve.from may additionally name the pipeline's view (ADR-0016 §8). */
    private static Set<String> serveScope(Set<String> stepIds, ViewBlock view) {
        if (view == null) {
            return stepIds;
        }
        Set<String> scope = new HashSet<>(stepIds);
        scope.add(viewId(view));
        return scope;
    }

    private List<SourceTables> universeOf(PipelineResource p) {
        // The universe is a SET of sources: a source listed twice contributes one table set, so a
        // duplicate id never inflates the cross-source match count into a false ambiguity. (Rejecting
        // a duplicate id in the source list itself is a separate concern, out of B3-6 closure scope.)
        List<SourceTables> out = new ArrayList<>();
        for (String sid : new LinkedHashSet<>(p.sources())) {
            out.add(tableSetOf(sources.get(sid)));     // sources resolved above
        }
        return out;
    }

    private static SourceTables tableSetOf(SourceResource s) {
        List<TableRef> tables = s.tables();
        if (tables == null) {
            return new SourceTables(s.id(), Set.of(), true);    // omitted = whole source (open, §4)
        }
        Set<String> literals = new HashSet<>();
        boolean open = false;
        for (TableRef t : tables) {
            switch (t) {
                case TableRef.Literal l -> literals.add(l.name());
                case TableRef.Spec sp -> literals.add(sp.name());
                case TableRef.Regex r -> open = true;           // dynamic: new tables may join
            }
        }
        return new SourceTables(s.id(), literals, open);
    }

    private static FromRef viewFrom(ViewBlock v) {
        return switch (v) {
            case ViewBlock.Inline i -> i.from();
            case ViewBlock.Use u -> u.from();
        };
    }

    private static String viewId(ViewBlock v) {
        return switch (v) {
            case ViewBlock.Inline i -> i.id();
            case ViewBlock.Use u -> u.id();
        };
    }

    private static FromClause serveFrom(ServeBlock s) {
        return switch (s) {
            case ServeBlock.Inline i -> i.from();
            case ServeBlock.Use u -> u.from();
        };
    }

    private static DslException missing(String path, String ref) {
        return new DslException(DslError.MISSING_REFERENCE, path, 0, 0, null, Map.of("ref", ref));
    }

    /** The literal table universe of one source plus whether it is open (omitted / regex). */
    private record SourceTables(String sourceId, Set<String> literals, boolean open) {
    }

    private enum Kind {
        TRANSFORM("transform"), VIEW("view"), SERVE("serve");

        private final String label;

        Kind(String label) {
            this.label = label;
        }
    }
}
