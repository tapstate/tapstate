package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.validation.*;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static io.tapstate.core.validation.ValidationRootCheck.*;

/**
 * Independent, bounded current-source projection of an admitted object-only Nest tree. No target,
 * production renderer, historical winner or operator state participates. Array/flat contributions
 * require their separate identity/membership composition and are refused before any child query.
 * Native query contracts and cancellation of blocked I/O remain the caller's admitted obligations.
 */
public final class ValidationNestObjectProjection {
    public record Scope(ValidationRunIdentity run, String mapping, String output) {
        public Scope {
            Objects.requireNonNull(run, "run");
            if (mapping == null || mapping.isBlank() || output == null || output.isBlank()) throw invalid("objectProjection.scope");
            ValidationError.fieldName(mapping, "objectProjection.mapping"); ValidationError.fieldName(output, "objectProjection.output");
        }
    }
    public record Limits(ValidationValues.Limits row, long bytes, long operations, int queries, int rows, int depth) {
        public Limits {
            Objects.requireNonNull(row, "row");
            if (bytes < 1 || bytes > 32L * 1024 * 1024 || operations < 1 || operations > 64L * 1024 * 1024
                    || queries < 1 || queries > 256 || rows < 1 || rows > 65536 || depth < 1 || depth > 32)
                throw invalid("objectProjection.limits");
        }
    }
    /** Physical identity and the two comparison endpoints are explicit; they need not use the same fields. */
    public record RootKeys(ValidationKeyBinding physical, ValidationKeyBinding original, ValidationKeyBinding projected) {
        public RootKeys(ValidationKeyBinding original, ValidationKeyBinding projected) { this(original, original, projected); }
        public RootKeys {
            Objects.requireNonNull(physical, "physical"); Objects.requireNonNull(original, "original"); Objects.requireNonNull(projected, "projected");
            if (original.components().size() != projected.components().size()) throw invalid("objectProjection.rootKeyArity");
            for (int i = 0; i < original.components().size(); i++) if (original.components().get(i).domain() != projected.components().get(i).domain())
                throw invalid("objectProjection.rootKeyDomain");
        }
    }
    public record Context(ValidationObservation root, List<String> outputPath, String readId) {
        public Context { outputPath = List.copyOf(outputPath); }
    }
    @FunctionalInterface
    public interface ChildRule {
        /**
         * Independently classifies one complete raw child row. Included rows require their actual physical
         * identity. Evidence/uncertainty uses this root and absolute output paths; no target input is allowed.
         * Partial fields are represented by evidence's affected paths, never by invented NULL values.
         * Rule execution uses separately admitted bounded scratch; returned working/physical facts are
         * charged to the shared root reservation before the relation reader may retain them.
         */
        ValidationRootReader.Source classify(Context context, ValidationValues.Row row);
    }
    public record Binding(ValidationNestLookupBinding lookup, ValidationNestRelationRead.Queries queries, ChildRule rule) {
        public Binding { Objects.requireNonNull(lookup, "lookup"); Objects.requireNonNull(queries, "queries"); Objects.requireNonNull(rule, "rule"); }
    }
    public record Result(ValidationStaticMap.Projection view, ValidationRootReader.SourceEvidence evidence) {}
    private final Scope scope;
    private final ValidationNestRelations.Plan plan;
    private final RootKeys keys;
    private final Map<List<String>, Binding> bindings;
    private final Limits limits;
    private final ValidationStaticMap identity;

    public ValidationNestObjectProjection(Scope scope, ValidationNestRelations.Plan plan, RootKeys keys,
            Map<List<String>, Binding> bindings, Limits limits) {
        this.scope = Objects.requireNonNull(scope, "scope"); this.plan = Objects.requireNonNull(plan, "plan");
        this.keys = Objects.requireNonNull(keys, "keys"); this.limits = Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(bindings, "bindings");
        if (bindings.size() > 256) throw invalid("objectProjection.bindings");
        var copied = new LinkedHashMap<List<String>, Binding>();
        bindings.forEach((path, binding) -> copied.put(List.copyOf(path), Objects.requireNonNull(binding, "binding")));
        this.bindings = Map.copyOf(copied);
        var used = new LinkedHashSet<List<String>>();
        validate(plan.children(), 2, used);
        if (!used.equals(this.bindings.keySet())) throw invalid("objectProjection.extraBinding");
        identity = ValidationStaticMap.compile(plan.node(), new TransformBody.MapProjection(Map.of()), limits.row());
    }

    /** Caller reserves compilation inputs, native workers/decoders and this complete root working budget. */
    public Result read(ValidationObservation root, SampledSide original, ValidationStaticMap.Projection working,
            ValidationRootReader.SourceEvidence incoming, Clock clock, Runnable requireAuthorized) {
        return read(root, original, working, incoming, clock, requireAuthorized, null);
    }

    /** A composite root driver can share one ledger across successive independent source components. */
    Result read(ValidationObservation root, SampledSide original, ValidationStaticMap.Projection working,
            ValidationRootReader.SourceEvidence incoming, Clock clock, Runnable requireAuthorized, ValidationNestBudget shared) {
        try { return readShared(root, original, working, incoming, clock, requireAuthorized, shared); }
        catch (RuntimeException | Error error) { if (shared != null) shared.abort(error); throw error; }
    }

    private Result readShared(ValidationObservation root, SampledSide original, ValidationStaticMap.Projection working,
            ValidationRootReader.SourceEvidence incoming, Clock clock, Runnable requireAuthorized, ValidationNestBudget shared) {
        if (shared != null) shared.requireScope(root);
        Objects.requireNonNull(root, "root"); Objects.requireNonNull(original, "original"); Objects.requireNonNull(working, "working");
        Objects.requireNonNull(incoming, "incoming"); Objects.requireNonNull(clock, "clock"); Objects.requireNonNull(requireAuthorized, "requireAuthorized");
        if (!root.run().equals(scope.run()) || !root.root().mappingId().equals(scope.mapping()) || !root.root().outputId().equals(scope.output())
                || !root.root().ancestors().isEmpty()) throw invalid("objectProjection.originalScope");
        requireAuthorized.run();
        if (working.unavailablePaths().contains(List.of())) throw unsupported("root", "nest.wholeUnknownObjectProjection");
        for (var component : keys.projected().components()) for (var mask : working.unavailablePaths())
            if (starts(component.path(), mask) || starts(mask, component.path())) throw unsupported("root", "nest.rootKeyUnavailable");
        if (!keys.physical().components().stream().map(ValidationKeyBinding.Component::path).toList().equals(original.identity().keyFields()))
            throw invalid("objectProjection.originalKeyFields");
        var budget = new Work(root, original.readId(), clock, requireAuthorized, shared);
        budget.bytes(plan.retainedBytes() + identity.retainedBytes() + original.accountedBytes() + working.accountedBytes()
                + 6L * ValidationKey.MAX_KEY_BYTES);
        if (!bound(keys.physical().read(original.row())).equals(original.identity().actualKey())
                || !bound(keys.original().read(original.row())).equals(root.root().rootKey())
                || !bound(keys.projected().read(working.row())).equals(root.root().rootKey()))
            throw unsupported("root", "nest.rootIdentityChanged");
        budget.merge(incoming);
        for (var path : working.unavailablePaths()) budget.requireExactCause(path);
        for (var problem : incoming.issues()) for (var affected : problem.affected())
            if (!working.unavailablePaths().contains(affected.fieldPath())) throw invalid("objectProjection.inputKnowledge");
        var view = budget.node(original, working, plan.children(), List.of(), 1);
        requireAuthorized.run();
        return new Result(view, budget.evidence());
    }

    private void validate(List<ValidationNestRelations.Relation> children, int depth, Set<List<String>> used) {
        if (children.isEmpty()) return;
        if (depth > limits.depth()) throw limit("depth");
        for (var relation : children) {
            if (relation.shape() != EmbedAs.OBJECT) throw unsupported(String.join(".", relation.treePath()), "nest.objectProjectionShape");
            if (used.size() == 256 || !used.add(relation.treePath())) throw invalid("objectProjection.tree");
            var binding = bindings.get(relation.treePath());
            if (binding == null || !binding.lookup().relation().equals(relation)) throw invalid("objectProjection.relationBinding");
            path(relation);
            if (!relation.children().isEmpty()) validate(relation.children(), depth + 1, used);
        }
    }

    private final class Work {
        private final ValidationObservation root;
        private final Clock clock;
        private final Runnable authorized;
        private final Map<Finding, ValidationRootReader.SourceIssue> issues = new LinkedHashMap<>();
        private final ValidationNestBudget quota;
        private final ValidationNestBudget shared;
        private Work(ValidationObservation root, String originalReadId, Clock clock, Runnable authorized, ValidationNestBudget shared) {
            this.root = root; this.clock = clock; this.authorized = authorized;
            this.shared = shared;
            quota = new ValidationNestBudget(root, originalReadId, new ValidationNestBudget.Limits(limits.bytes(), limits.operations(),
                    limits.queries(), limits.rows(), limits.depth()), "objectProjection");
            if (shared != null) { shared.requireScope(root); shared.requireParentRead(originalReadId); }
        }

        private ValidationStaticMap.Projection node(SampledSide original, ValidationStaticMap.Projection input,
                List<ValidationNestRelations.Relation> children, List<String> prefix, int depth) {
            authorized.run();
            quota.depth(depth); if (shared != null) shared.depth(depth);
            operations(1L + 1024L * (input.row().fields().size() + children.size()) * input.unavailablePaths().size());
            var fields = new ArrayList<ValidationNestFieldCheck.Field>();
            for (String name : input.row().fields().keySet()) if (knownPresence(input, name))
                fields.add(new ValidationNestFieldCheck.Field("parent", ValidationNestFieldCheck.FieldKind.PARENT, List.of(name)));
            for (int i = 0; i < children.size(); i++) fields.add(new ValidationNestFieldCheck.Field("embed-" + i,
                    ValidationNestFieldCheck.FieldKind.EMBED_PATH, path(children.get(i))));
            long fieldWork = 1L + (long) fields.size() * fields.size() * 1025;
            operations(fieldWork);
            var inspected = ValidationNestFieldCheck.inspect(fields, new ValidationNestFieldCheck.Limits(limits.bytes(),
                    Math.min(64L * 1024 * 1024, fieldWork), 1024, 1024));
            bytes(256L + 2048L * fields.size());
            merge(prefixed(ValidationRootReader.SourceEvidence.fromFieldConflicts(root, root, inspected, Optional.of(original)), prefix));
            var blocked = new LinkedHashSet<String>();
            for (var conflict : inspected.conflicts()) {
                if (conflict.contributor().kind() == ValidationNestFieldCheck.FieldKind.EMBED_PATH) blocked.add(conflict.contributor().owner());
                if (conflict.occupied().kind() == ValidationNestFieldCheck.FieldKind.EMBED_PATH) blocked.add(conflict.occupied().owner());
            }
            bytes(128L + 64L * input.row().fields().size());
            var output = new LinkedHashMap<String, Object>(input.row().fields());
            long projectedBytes = input.row().accountedBytes();
            for (int i = 0; i < children.size(); i++) {
                authorized.run();
                var relation = children.get(i); var local = path(relation); var absolute = concat(prefix, local);
                if (blocked.contains("embed-" + i)) continue;
                // Unknown presence at a declared output cannot be used to assert no parent conflict.
                if (!knownPresence(input, local.getFirst())) {
                    requireCause(absolute);
                    continue;
                }
                quota.depth(depth + 1); if (shared != null) shared.depth(depth + 1);
                var binding = bindings.get(relation.treePath());
                operations(1L + 1024L * relation.join().size() * input.unavailablePaths().size());
                var uncertain = new LinkedHashSet<Finding>();
                final String[] readId = { null };
                var result = ValidationNestRelationRead.read(root, binding.lookup(), input, request -> {
                    var query = quota.prepare(root, () -> shared == null ? binding.queries().prepare(request)
                            : shared.prepare(root, () -> binding.queries().prepare(request)));
                    readId[0] = query.readId();
                    return query;
                }, new ValidationRelationRows.Limits(limits.bytes(), limits.row()), clock, authorized, raw -> {
                    quota.row(root); if (shared != null) shared.row(root);
                    bytes(raw.accountedBytes());
                    var classified = Objects.requireNonNull(binding.rule().classify(new Context(root, absolute, readId[0]), raw), "child classification");
                    bytes(128L + (classified.row().working() == raw ? 0 : classified.row().working().accountedBytes())
                            + classified.row().physical().map(ValidationPhysicalIdentity::retainedBytes).orElse(0L));
                    if (classified.row().disposition() == ValidationRelationRows.Disposition.INCLUDED && classified.row().physical().isEmpty())
                        throw invalid("objectProjection.childPhysicalIdentity");
                    for (var problem : classified.evidence().issues()) for (var affected : problem.affected())
                        if (!starts(affected.fieldPath(), absolute)) throw invalid("objectProjection.childEvidencePath");
                    merge(classified.evidence());
                    for (Finding cause : classified.uncertainty()) { uncertain.add(cause); issue(new ValidationRootReader.SourceIssue(cause, Optional.empty(),
                            List.of(location(absolute)))); }
                    return classified.row();
                });
                var at = location(absolute);
                if (result.preparation() instanceof ValidationNestLookupBinding.Unresolved unknown) {
                    for (String field : unknown.fields()) propagate(concat(prefix, List.of(field)), absolute);
                    problems(unknown.knownProblems(), prefix, absolute, original);
                    continue;
                }
                if (result.preparation() instanceof ValidationNestLookupBinding.Invalid invalid) {
                    problems(invalid.problems(), prefix, absolute, original); continue;
                }
                if (result.preparation() instanceof ValidationNestLookupBinding.NoReference) continue;
                var rows = result.rows().orElseThrow();
                Optional<ValidationNestSingleSource.Reference> reference = Optional.empty();
                if (relation.direction() == ValidationNestRelations.Direction.REFERENCE) {
                    var references = new ValidationReferenceFields(relation.join().stream().map(ValidationNestRelations.Pair::parent).distinct().toList(), limits.row());
                    reference = Optional.of(new ValidationNestSingleSource.Reference((ValidationReferenceFields.Complete) references.read(input.row()), original));
                }
                var facts = ValidationNestSingleSource.classify(rows, relation.direction() == ValidationNestRelations.Direction.REFERENCE
                        ? ValidationNestSingleSource.Relation.REFERENCE : ValidationNestSingleSource.Relation.GATHER,
                        reference, at, List.of(at), new ArrayList<>(uncertain), limits.bytes());
                bytes(facts.accountedBytes());
                merge(ValidationRootReader.SourceEvidence.fromSingleSource(root, facts));
                if (facts.state() != ValidationNestSingleSource.State.UNIQUE) continue;
                var child = facts.unique().orElseThrow();
                var childOriginal = new SampledSide(child.physical().orElseThrow(), child.original(), rows.read().readId());
                var childInput = projection(child.working(), absolute);
                var childView = childInput.unavailablePaths().contains(List.of()) ? childInput
                        : node(childOriginal, childInput, relation.children(), absolute, depth + 1);
                projectedBytes += childView.row().accountedBytes() + 2048L * local.size();
                put(output, local, childView.row().fields());
            }
            // Charge copies before creating the final owned view and its identity projection.
            bytes(3L * projectedBytes + 1024);
            return projection(ValidationValues.ownRow(output, limits.row()), prefix);
        }

        private ValidationStaticMap.Projection projection(ValidationValues.Row row, List<String> prefix) {
            var masks = new LinkedHashSet<List<Object>>();
            for (var issue : issues.values()) for (var affected : issue.affected()) {
                var absolute = affected.fieldPath();
                operations(1L + 1024L * (absolute.size() + prefix.size()));
                if (starts(absolute, prefix)) masks.add(List.copyOf(absolute.subList(prefix.size(), absolute.size())));
                else if (starts(prefix, absolute)) masks.add(List.of());
                if (masks.size() > 256) throw limit("masks");
            }
            if (masks.size() > 256) throw limit("masks");
            bytes(row.accountedBytes() + 4096L * masks.size());
            return identity.apply(row, new ArrayList<>(masks));
        }
        private void propagate(List<String> from, List<String> to) {
            var known = new ArrayList<>(issues.values()); boolean found = false;
            for (var problem : known) for (var affected : problem.affected()) if (starts(from, affected.fieldPath()) || starts(affected.fieldPath(), from)) {
                issue(new ValidationRootReader.SourceIssue(problem.finding(), problem.sample(), List.of(location(to)))); found = true; break;
            }
            if (!found) throw invalid("objectProjection.unknownWithoutCause");
        }
        private void requireExactCause(List<?> path) {
            if (issues.values().stream().flatMap(issue -> issue.affected().stream()).noneMatch(affected -> affected.fieldPath().equals(path)))
                throw invalid("objectProjection.unknownWithoutCause");
        }
        private void requireCause(List<?> path) {
            if (issues.values().stream().flatMap(issue -> issue.affected().stream())
                    .noneMatch(affected -> starts(path, affected.fieldPath()) || starts(affected.fieldPath(), path)))
                throw invalid("objectProjection.unknownWithoutCause");
        }
        private void problems(List<ValidationKeyBinding.Problem> problems, List<String> prefix, List<String> output, SampledSide original) {
            for (var problem : problems) issue(new ValidationRootReader.SourceIssue(new Finding(
                    problem.kind() == ValidationKeyBinding.ProblemKind.MISSING_FIELD ? Kind.SOURCE_FIELD_MISSING : Kind.SOURCE_FIELD_INVALID,
                    location(concat(prefix, problem.path()))), Optional.of(original), List.of(location(output))));
        }
        private void merge(ValidationRootReader.SourceEvidence evidence) {
            if (evidence.observation() != null && !evidence.observation().equals(root)) throw invalid("objectProjection.evidenceScope");
            for (var read : evidence.reads()) { quota.read(read); if (shared != null) shared.read(read); }
            for (var issue : evidence.issues()) issue(issue);
        }
        private void issue(ValidationRootReader.SourceIssue value) {
            check(value.finding().location()); for (Location path : value.affected()) check(path);
            var previous = issues.get(value.finding());
            bytes(128L + (previous == null ? 0 : 64L * previous.affected().size()));
            var affected = new LinkedHashSet<Location>(); if (previous != null) affected.addAll(previous.affected());
            for (Location path : value.affected()) if (affected.add(path)) bytes(256L + 1088L * path.fieldPath().size());
            if (affected.size() > 256) throw limit("affected");
            if (previous == null) {
                if (issues.size() == 256) throw limit("issues");
                bytes(512L + 1088L * value.finding().location().fieldPath().size() + value.sample().map(SampledSide::accountedBytes).orElse(0L));
            }
            issues.put(value.finding(), new ValidationRootReader.SourceIssue(value.finding(), previous == null ? value.sample() : previous.sample(), new ArrayList<>(affected)));
        }
        private void check(Location at) {
            operations(1L + ValidationKey.MAX_KEY_BYTES + 1024L * at.fieldPath().size());
            if (!at.scope().equals(root.root())) throw invalid("objectProjection.issueScope");
        }
        private Location location(List<?> path) { return new Location(root.root(), new ArrayList<>(path)); }
        private ValidationRootReader.SourceEvidence evidence() { return new ValidationRootReader.SourceEvidence(root, quota.reads(), new ArrayList<>(issues.values())); }
        private ValidationRootReader.SourceEvidence prefixed(ValidationRootReader.SourceEvidence evidence, List<String> prefix) {
            return new ValidationRootReader.SourceEvidence(root, evidence.reads(), evidence.issues().stream().map(issue ->
                    new ValidationRootReader.SourceIssue(new Finding(issue.finding().kind(), location(concatObjects(prefix, issue.finding().location().fieldPath())), issue.finding().multiplicity()),
                            issue.sample(), issue.affected().stream().map(at -> location(concatObjects(prefix, at.fieldPath()))).toList())).toList());
        }
        private void bytes(long amount) { quota.bytes(amount); if (shared != null) shared.bytes(amount); }
        private void operations(long amount) { quota.operations(amount); if (shared != null) shared.operations(amount); }
    }

    private static boolean knownPresence(ValidationStaticMap.Projection view, String field) {
        for (var path : view.unavailablePaths()) if (path.isEmpty() ? !view.definedFields().contains(field)
                : path.size() == 1 && path.getFirst().equals(field)) return false;
        return true;
    }
    private static List<String> path(ValidationNestRelations.Relation relation) {
        var path = List.of(relation.outputPath().orElseThrow().split("\\.", -1));
        if (path.size() > 32 || path.stream().anyMatch(String::isBlank)) throw invalid("objectProjection.path");
        return path;
    }
    private static List<String> concat(List<String> first, List<String> second) { var all = new ArrayList<>(first); all.addAll(second); return List.copyOf(all); }
    private static List<Object> concatObjects(List<String> first, List<Object> second) { var all = new ArrayList<Object>(first); all.addAll(second); return List.copyOf(all); }
    private static boolean starts(List<?> value, List<?> prefix) { return value.size() >= prefix.size() && value.subList(0, prefix.size()).equals(prefix); }
    private static ValidationKey bound(ValidationKeyBinding.Result value) {
        if (value instanceof ValidationKeyBinding.Bound bound) return bound.key();
        throw invalid("objectProjection.rootKey");
    }
    @SuppressWarnings("unchecked")
    private static void put(Map<String, Object> target, List<String> path, Map<String, Object> value) {
        Map<String, Object> current = target;
        for (int i = 0; i < path.size() - 1; i++) current = (Map<String, Object>) current.computeIfAbsent(path.get(i), ignored -> new LinkedHashMap<>());
        if (current.putIfAbsent(path.getLast(), value) != null) throw new IllegalStateException("conflicting object contribution escaped inspection");
    }
    private TapstateException unsupported(String field, String rule) { return new TapstateException(ValidationError.RULE_UNSUPPORTED,
            Map.of("node", plan.node(), "field", field, "rule", rule), null); }
    private static TapstateException invalid(String field) { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null); }
    private static TapstateException limit(String dimension) { return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "objectProjection." + dimension), null); }
}
