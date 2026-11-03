package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.validation.*;
import java.time.Clock;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;

import static io.tapstate.core.validation.ValidationRootCheck.*;

/**
 * One independently read object/array tree, followed by actual target traversal. No production
 * transformation, target-derived source value or restored state is used. Caller admits native
 * readers, codecs and rule scratch separately before this bounded logical root starts. Full
 * configuration preflight, timing confirmation, result publication and run control are separate.
 */
public final class ValidationNestTree {
    public record Context(ValidationObservation root, ValidationNestScope scope, String readId) {}
    public record Row(ValidationRootReader.Source source, ValidationStaticMap.Projection projection) {
        public Row {
            Objects.requireNonNull(source, "source"); Objects.requireNonNull(projection, "projection");
            if (source.row().working() != projection.row()) throw invalid("rowProjection");
        }
    }
    @FunctionalInterface public interface Rule {
        /**
         * Evidence uses the original root and this exact context scope. Array rules first run at
         * their unkeyed array path to decide participation; included rows are then rebound to their
         * validated element identity. Rules do not perform nested relationship reads.
         */
        Row apply(Context context, ValidationValues.Row original);
    }
    /** Source bindings read original physical row fields, before static projection can rename/drop them. */
    public record ArrayKeys(ValidationKeyBinding sourceRow, ValidationKeyBinding sourceElement,
                            ValidationKeyBinding targetElement, Optional<ValidationKeyBinding> targetRow) {
        public ArrayKeys {
            Objects.requireNonNull(sourceRow, "sourceRow"); Objects.requireNonNull(sourceElement, "sourceElement");
            Objects.requireNonNull(targetElement, "targetElement"); Objects.requireNonNull(targetRow, "targetRow");
            keyDomains(sourceElement, targetElement); targetRow.ifPresent(target -> keyDomains(sourceRow, target));
        }
    }
    public record Binding(ValidationNestLookupBinding lookup, ValidationNestRelationRead.Queries queries,
                          Rule rule, Optional<ArrayKeys> arrayKeys) {
        public Binding {
            Objects.requireNonNull(lookup, "lookup"); Objects.requireNonNull(queries, "queries");
            Objects.requireNonNull(rule, "rule"); Objects.requireNonNull(arrayKeys, "arrayKeys");
            if ((lookup.relation().shape() == EmbedAs.ARRAY) != arrayKeys.isPresent()) throw invalid("arrayKeys");
        }
    }
    public record Limits(ValidationValues.Limits row, long bytes, long operations, long checkOperations,
                         int queries, int rows, int depth, int elements, ValidationRootCheck.Limits result) {
        public Limits {
            Objects.requireNonNull(row, "row"); Objects.requireNonNull(result, "result");
            new ValidationNestBudget.Limits(bytes, operations, queries, rows, depth);
            if (checkOperations < 1 || checkOperations > operations || elements < 1 || elements > 65536) throw invalid("limits");
        }
    }
    private final ValidationNestObjectProjection.Scope scope;
    private final ValidationNestRelations.Plan plan;
    private final ValidationNestObjectProjection.RootKeys rootKeys;
    private final Map<List<String>, Binding> bindings;
    private final Limits limits;
    private final Set<String> containers;
    private final BiPredicate<String, String> compatible;
    private final ValidationStaticMap identity;
    private final Optional<ValidationFieldCheck.GeneratedId> generatedId;

    public ValidationNestTree(ValidationNestObjectProjection.Scope scope, ValidationNestRelations.Plan plan,
            ValidationNestObjectProjection.RootKeys rootKeys, Map<List<String>, Binding> bindings, Limits limits,
            Set<String> containers, BiPredicate<String, String> compatible) {
        this(scope, plan, rootKeys, bindings, limits, containers, compatible, Optional.empty());
    }

    public ValidationNestTree(ValidationNestObjectProjection.Scope scope, ValidationNestRelations.Plan plan,
            ValidationNestObjectProjection.RootKeys rootKeys, Map<List<String>, Binding> bindings, Limits limits,
            Set<String> containers, BiPredicate<String, String> compatible, Optional<ValidationFieldCheck.GeneratedId> generatedId) {
        this.generatedId = Objects.requireNonNull(generatedId, "generatedId");
        this.scope = Objects.requireNonNull(scope, "scope"); this.plan = Objects.requireNonNull(plan, "plan");
        this.rootKeys = Objects.requireNonNull(rootKeys, "rootKeys"); this.limits = Objects.requireNonNull(limits, "limits");
        this.compatible = Objects.requireNonNull(compatible, "compatible");
        if (bindings == null || bindings.size() > 256 || containers == null || containers.size() > 32) throw invalid("bindings");
        var copy = new LinkedHashMap<List<String>, Binding>();
        bindings.forEach((path, binding) -> copy.put(List.copyOf(path), Objects.requireNonNull(binding, "binding")));
        this.bindings = Map.copyOf(copy); this.containers = Set.copyOf(containers);
        this.containers.forEach(type -> ValidationError.fieldName(type, "tree.containerType"));
        var used = new LinkedHashSet<List<String>>(); validate(plan.children(), used, 2);
        if (!used.equals(this.bindings.keySet())) throw invalid("extraBinding");
        identity = ValidationStaticMap.compile(plan.node(), new TransformBody.MapProjection(Map.of()), limits.row());
    }

    public ValidationRootCheck read(ValidationObservation root, ValidationRootReader.Query sourceQuery,
            ValidationRootReader.Query targetQuery, Clock clock, Runnable requireAuthorized, Rule rootRule,
            Function<ValidationValues.Row, ValidationRootReader.Target> targetRule) {
        if (!root.run().equals(scope.run()) || !root.root().mappingId().equals(scope.mapping())
                || !root.root().outputId().equals(scope.output()) || !root.root().ancestors().isEmpty()) throw invalid("root");
        Objects.requireNonNull(rootRule, "rootRule"); Objects.requireNonNull(targetRule, "targetRule");
        if (sourceQuery.readId().equals(targetQuery.readId())) throw invalid("rootReadIds");
        var session = new Session(root, sourceQuery.readId(), clock, requireAuthorized);
        try {
            var nodes = new IdentityHashMap<ValidationValues.Row, Node>();
            return ValidationRootReader.read(root, ValidationMethod.FULL_COLUMN, session.query(sourceQuery, true), session.query(targetQuery, false),
                    new ValidationRelationRows.Limits(limits.bytes(), limits.row()), limits.result(), clock, requireAuthorized, raw -> {
                        session.budget.row(root); session.budget.reserve(raw.accountedBytes());
                        var classified = session.classify(rootRule, new ValidationNestScope(root), sourceQuery.readId(), raw);
                        if (classified.source().row().disposition() != ValidationRelationRows.Disposition.INCLUDED) return classified.source();
                        var physical = classified.source().row().physical().orElseThrow(() -> invalid("rootPhysicalIdentity"));
                        session.budget.reserve(3L * ValidationKey.MAX_KEY_BYTES);
                        if (!physical.keyFields().equals(rootKeys.physical().components().stream().map(ValidationKeyBinding.Component::path).toList())
                                || !bound(rootKeys.physical().read(raw)).equals(physical.actualKey())
                                || !bound(rootKeys.original().read(raw)).equals(root.root().rootKey())
                                || !bound(rootKeys.projected().read(classified.projection().row())).equals(root.root().rootKey())) throw invalid("rootIdentity");
                        var node = session.node(new ValidationNestScope(root), new SampledSide(physical, raw, sourceQuery.readId()),
                                classified.projection(), plan.children(), 1);
                        session.budget.reserve(128); nodes.put(raw, node);
                        return new ValidationRootReader.Source(new ValidationRelationReader.Evaluation(ValidationRelationRows.Disposition.INCLUDED,
                                node.view.row(), Optional.of(physical)), List.of(), session.evidence());
                    }, raw -> {
                        session.budget.row(root); session.budget.reserve(raw.accountedBytes());
                        var target = Objects.requireNonNull(targetRule.apply(raw), "target classification");
                        session.budget.reserve(128L + (target.working() == raw ? 0 : target.working().accountedBytes())
                                + target.physical().retainedBytes());
                        return target;
                    }, (pair, result) -> {
                        session.budget.read(pair.sourceRead()); session.budget.read(pair.targetRead());
                        var node = nodes.get(pair.source().original());
                        if (node == null || node.view.row() != pair.source().working()) throw invalid("rootNode");
                        var physical = new ValidationCheckEvent.Side(pair.target().physical().orElseThrow(() -> invalid("targetIdentity")),
                                pair.target().original(), 1, ValidationCheckEvent.GroupStatus.COMPLETE, pair.targetRead());
                        session.check(node, pair.target().working(), physical, result);
                    });
        } catch (RuntimeException | Error error) { session.budget.abort(error); throw error; }
    }

    private final class Session {
        private final ValidationObservation root;
        private final Clock clock;
        private final Runnable authorized;
        private final ValidationNestBudget budget;
        private final Map<Finding, ValidationRootReader.SourceIssue> issues = new LinkedHashMap<>();
        Session(ValidationObservation root, String read, Clock clock, Runnable authorized) {
            this.root = root; this.clock = Objects.requireNonNull(clock, "clock"); this.authorized = Objects.requireNonNull(authorized, "authorized");
            budget = new ValidationNestBudget(root, read, new ValidationNestBudget.Limits(limits.bytes(), limits.operations(), limits.queries(), limits.rows(), limits.depth()), "nestTree");
            budget.reserve(plan.retainedBytes() + identity.retainedBytes());
        }
        ValidationRootReader.Query query(ValidationRootReader.Query query, boolean original) {
            return new ValidationRootReader.Query(query.readId(), query.contractVersion(), () -> {
                if (original) budget.originalQuery(query); else budget.prepare(root, () -> query);
                return query.open().get();
            });
        }
        Row classify(Rule rule, ValidationNestScope scope, String readId, ValidationValues.Row raw) {
            return retain(evaluate(rule, scope, readId, raw), scope);
        }
        Row evaluate(Rule rule, ValidationNestScope scope, String readId, ValidationValues.Row raw) {
            authorized.run();
            var row = Objects.requireNonNull(rule.apply(new Context(root, scope, readId), raw), "tree row");
            budget.reserve(row.projection().accountedBytes() + row.source().row().physical().map(ValidationPhysicalIdentity::retainedBytes).orElse(0L));
            if (row.source().evidence().observation() != null && !root.equals(row.source().evidence().observation())) throw invalid("ruleEvidenceScope");
            if (!row.source().evidence().reads().isEmpty()) throw invalid("ruleReadBoundary");
            for (var issue : row.source().evidence().issues()) {
                ruleLocation(scope, issue.finding().location());
                issue.sample().ifPresent(sample -> {
                    if (!sample.readId().equals(readId) || sample.row() != raw
                            || row.source().row().physical().isPresent() && !row.source().row().physical().orElseThrow().equals(sample.identity())) {
                        throw invalid("ruleEvidenceSample");
                    }
                });
                for (var affected : issue.affected()) ruleLocation(scope, affected);
            }
            for (var cause : row.source().uncertainty()) ruleLocation(scope, cause.location());
            return row;
        }
        Row retain(Row row, ValidationNestScope scope) {
            merge(row.source().evidence());
            if (row.source().row().disposition() == ValidationRelationRows.Disposition.INCLUDED)
                for (var path : row.projection().unavailablePaths()) requireCause(scope, path);
            return row;
        }
        void ruleLocation(ValidationNestScope scope, Location at) {
            budget.operations(1L + ValidationKey.MAX_KEY_BYTES + 1024L * (at.fieldPath().size() + scope.objectPath().size()));
            if (!at.scope().equals(scope.observation().root()) || !starts(at.fieldPath(), scope.objectPath())) throw invalid("ruleEvidenceScope");
        }
        Row memberRow(Row row, ValidationNestScope from, ValidationNestScope to) {
            var old = row.source();
            budget.reserve(512L + 4096L * (old.evidence().issues().size() + old.uncertainty().size()));
            var rebound = old.evidence().issues().stream().map(issue -> new ValidationRootReader.SourceIssue(
                    rebase(issue.finding(), from, to), issue.sample(), issue.affected().stream().map(at -> rebase(at, from, to)).toList())).toList();
            var evidence = new ValidationRootReader.SourceEvidence(root, old.evidence().reads(), rebound);
            return new Row(new ValidationRootReader.Source(old.row(), old.uncertainty().stream().map(cause -> rebase(cause, from, to)).toList(), evidence), row.projection());
        }
        Finding rebase(Finding finding, ValidationNestScope from, ValidationNestScope to) {
            return new Finding(finding.kind(), rebase(finding.location(), from, to), finding.multiplicity());
        }
        Location rebase(Location at, ValidationNestScope from, ValidationNestScope to) {
            ruleLocation(from, at);
            var path = new ArrayList<Object>(to.objectPath()); path.addAll(at.fieldPath().subList(from.objectPath().size(), at.fieldPath().size()));
            budget.reserve(128L + 1088L * path.size());
            return new Location(to.observation().root(), path);
        }
        Node node(ValidationNestScope scope, SampledSide original, ValidationStaticMap.Projection input,
                  List<ValidationNestRelations.Relation> relations, int depth) {
            authorized.run(); budget.depth(depth); budget.reserve(256L + input.accountedBytes());
            budget.reserve(256L + 2048L * (input.row().fields().size() + relations.size()));
            var fields = new ArrayList<ValidationNestFieldCheck.Field>();
            for (String name : input.row().fields().keySet()) if (known(input, name))
                fields.add(new ValidationNestFieldCheck.Field("parent", ValidationNestFieldCheck.FieldKind.PARENT, List.of(name)));
            for (int i = 0; i < relations.size(); i++) fields.add(new ValidationNestFieldCheck.Field("embed-" + i,
                    ValidationNestFieldCheck.FieldKind.EMBED_PATH, path(relations.get(i))));
            long work = 1L + (long) fields.size() * fields.size() * 1025;
            budget.operations(work);
            var conflicts = ValidationNestFieldCheck.inspect(fields, new ValidationNestFieldCheck.Limits(limits.bytes(), Math.min(64L * 1024 * 1024, work), 1024, 1024));
            merge(prefixed(scope, ValidationRootReader.SourceEvidence.fromFieldConflicts(root, scope.observation(), conflicts, Optional.of(original))));
            var blocked = new LinkedHashSet<String>();
            for (var conflict : conflicts.conflicts()) {
                if (conflict.contributor().kind() == ValidationNestFieldCheck.FieldKind.EMBED_PATH) blocked.add(conflict.contributor().owner());
                if (conflict.occupied().kind() == ValidationNestFieldCheck.FieldKind.EMBED_PATH) blocked.add(conflict.occupied().owner());
            }
            var branches = new ArrayList<Branch>(); var output = new LinkedHashMap<String, Object>(input.row().fields());
            for (int i = 0; i < relations.size(); i++) {
                var relation = relations.get(i); var path = path(relation);
                if (blocked.contains("embed-" + i)) continue;
                if (!known(input, path.getFirst())) { propagate(scope, List.of(path.getFirst()), path); continue; }
                budget.depth(depth + 1); budget.reserve(2L * ValidationKey.MAX_KEY_BYTES);
                var binding = bindings.get(relation.treePath());
                Branch branch = relation.shape() == EmbedAs.ARRAY ? array(scope, original, input, relation, binding, depth)
                        : object(scope, original, input, relation, binding, depth);
                if (branch == null) continue;
                if (branch.preparation instanceof ValidationNestLookupBinding.NoReference) {
                    if (relation.shape() == EmbedAs.ARRAY) put(output, path, List.of());
                    continue;
                }
                if (branch.object != null && branch.object.state() == ValidationNestSingleSource.State.EMPTY) continue;
                if (branch.object != null && branch.object.state() == ValidationNestSingleSource.State.BLOCKED || branch.array == null && branch.object == null) continue;
                prefixes(output, path); budget.reserve(128L + 128L * path.size()); branches.add(branch);
            }
            var masks = new LinkedHashSet<List<Object>>(input.unavailablePaths());
            for (var issue : issues.values()) for (var affected : issue.affected()) {
                budget.operations(1L + ValidationKey.MAX_KEY_BYTES + 1024L * (affected.fieldPath().size() + scope.objectPath().size() + branches.size()));
                if (!affected.scope().equals(scope.observation().root()) || !starts(affected.fieldPath(), scope.objectPath())) continue;
                var local = List.copyOf(affected.fieldPath().subList(scope.objectPath().size(), affected.fieldPath().size()));
                if (!masks.contains(local) && branches.stream().noneMatch(branch -> starts(local, path(branch.relation)))) {
                    if (masks.size() == 256) throw limit("masks");
                    budget.reserve(128L + 1088L * local.size()); masks.add(local);
                }
            }
            budget.reserve(3L * input.row().accountedBytes() + 4096L * (masks.size() + relations.size()) + 1024);
            var view = identity.applyPartial(ValidationValues.ownRow(output, limits.row()), new ArrayList<>(masks), input.definedFields());
            return new Node(scope, original, view, List.copyOf(branches));
        }
        Branch array(ValidationNestScope scope, SampledSide parent, ValidationStaticMap.Projection input,
                     ValidationNestRelations.Relation relation, Binding binding, int depth) {
            var nodes = new IdentityHashMap<ValidationNestArrayMembers.Member, Node>(); var keys = binding.arrayKeys().orElseThrow();
            var result = ValidationNestArraySourceRead.read(scope, binding.lookup(), input, parent, binding.queries(),
                    new ValidationNestArraySourceRead.Limits(new ValidationRelationRows.Limits(limits.bytes(), limits.row()),
                            new ValidationNestArrayMembers.Limits(limits.bytes(), limits.elements(), limits.rows(), limits.row()), limits.rows(), 256),
                    clock, authorized, (context, raw) -> {
                        var unkeyed = scope.object(path(relation));
                        var evaluated = evaluate(binding.rule(), unkeyed, context.readId(), raw);
                        if (evaluated.source().row().disposition() != ValidationRelationRows.Disposition.INCLUDED) {
                            retain(evaluated, unkeyed);
                            return new ValidationNestArraySourceRead.Classified(evaluated.source(), Optional.empty());
                        }
                        budget.reserve(2L * ValidationKey.MAX_KEY_BYTES);
                        var rowKey = keys.sourceRow().read(raw); var element = keys.sourceElement().read(raw);
                        if (!(rowKey instanceof ValidationKeyBinding.Bound) || !(element instanceof ValidationKeyBinding.Bound)) {
                            retain(evaluated, unkeyed);
                            var causes = new LinkedHashSet<Finding>();
                            for (var key : List.of(rowKey, element)) if (key instanceof ValidationKeyBinding.Invalid invalid) {
                                for (var problem : invalid.problems()) {
                                    var at = new ArrayList<String>(path(relation)); at.addAll(problem.path());
                                    var cause = new Finding(problem.kind() == ValidationKeyBinding.ProblemKind.MISSING_FIELD
                                            ? Kind.SOURCE_FIELD_MISSING : Kind.SOURCE_FIELD_INVALID, scope.location(at));
                                    causes.add(cause); issue(new ValidationRootReader.SourceIssue(cause, Optional.empty(), List.of(scope.location(path(relation)))));
                                }
                            }
                            return new ValidationNestArraySourceRead.Classified(new ValidationRootReader.Source(
                                    new ValidationRelationReader.Evaluation(ValidationRelationRows.Disposition.INDETERMINATE, raw, Optional.empty()), new ArrayList<>(causes)), Optional.empty());
                        }
                        var memberScope = scope.member(path(relation), bound(element));
                        var classified = retain(memberRow(evaluated, unkeyed, memberScope), memberScope);
                        var physical = classified.source().row().physical().orElseThrow(() -> invalid("memberPhysicalIdentity"));
                        var child = node(memberScope, new SampledSide(physical, raw, context.readId()), classified.projection(), relation.children(), depth + 1);
                        var source = new ValidationRootReader.Source(new ValidationRelationReader.Evaluation(ValidationRelationRows.Disposition.INCLUDED,
                                child.view.row(), Optional.of(physical)), List.of());
                        var member = new ValidationNestArrayMembers.Member(Optional.of(bound(rowKey)), bound(element), physical, raw, child.view.row());
                        budget.reserve(128); nodes.put(member, child);
                        return new ValidationNestArraySourceRead.Classified(source, Optional.of(member));
                    }, (context, source) -> { /* The rule retained all evidence before returning. */ }, budget);
            if (!prepared(scope, parent, relation, result.preparation())) return null;
            result.source().ifPresent(source -> merge(ValidationRootReader.SourceEvidence.fromArraySource(root, source, limits.bytes())));
            result.uncertain().ifPresent(source -> merge(ValidationRootReader.SourceEvidence.fromUncertainArraySource(root, source, limits.bytes())));
            result.referenceProblem().ifPresent(source -> merge(ValidationRootReader.SourceEvidence.fromSingleSource(root, source)));
            return new Branch(relation, binding, result.preparation(), result.source().orElse(null), null, nodes, null);
        }
        Branch object(ValidationNestScope scope, SampledSide parent, ValidationStaticMap.Projection input,
                      ValidationNestRelations.Relation relation, Binding binding, int depth) {
            var nodes = new IdentityHashMap<ValidationValues.Row, Node>(); var uncertain = new LinkedHashSet<Finding>();
            final String[] readId = { null };
            var result = ValidationNestRelationRead.read(scope.observation(), binding.lookup(), input, request -> {
                var query = budget.prepare(scope.observation(), () -> binding.queries().prepare(request)); readId[0] = query.readId(); return query;
            }, new ValidationRelationRows.Limits(limits.bytes(), limits.row()), clock, authorized, raw -> {
                budget.row(scope.observation()); budget.reserve(raw.accountedBytes());
                var childScope = scope.object(path(relation)); var classified = classify(binding.rule(), childScope, readId[0], raw);
                for (var cause : classified.source().uncertainty()) if (!uncertain.contains(cause)) {
                    if (uncertain.size() == 256) throw limit("uncertainty");
                    budget.reserve(512L + 1024L * cause.location().fieldPath().size()); uncertain.add(cause);
                }
                if (classified.source().row().disposition() != ValidationRelationRows.Disposition.INCLUDED) return classified.source().row();
                var physical = classified.source().row().physical().orElseThrow(() -> invalid("objectPhysicalIdentity"));
                var child = node(childScope, new SampledSide(physical, raw, readId[0]), classified.projection(), relation.children(), depth + 1);
                budget.reserve(128); nodes.put(raw, child);
                return new ValidationRelationReader.Evaluation(ValidationRelationRows.Disposition.INCLUDED, child.view.row(), Optional.of(physical));
            });
            if (!prepared(scope, parent, relation, result.preparation())) return null;
            if (result.rows().isEmpty()) return new Branch(relation, binding, result.preparation(), null, null, Map.of(), null);
            var rows = result.rows().orElseThrow(); budget.read(rows.read());
            var reference = relation.direction() == ValidationNestRelations.Direction.REFERENCE
                    ? Optional.of(new ValidationNestSingleSource.Reference(binding.lookup().requireReferenceFields(input.row()), parent))
                    : Optional.<ValidationNestSingleSource.Reference>empty();
            var location = scope.location(path(relation));
            var single = ValidationNestSingleSource.classify(rows, relation.direction() == ValidationNestRelations.Direction.REFERENCE
                    ? ValidationNestSingleSource.Relation.REFERENCE : ValidationNestSingleSource.Relation.GATHER,
                    reference, location, List.of(location), new ArrayList<>(uncertain), limits.bytes());
            budget.reserve(single.accountedBytes()); merge(ValidationRootReader.SourceEvidence.fromSingleSource(root, single));
            var child = single.unique().map(sample -> nodes.get(sample.original())).orElse(null);
            return new Branch(relation, binding, result.preparation(), null, single, Map.of(), child);
        }
        boolean prepared(ValidationNestScope scope, SampledSide parent, ValidationNestRelations.Relation relation, ValidationNestLookupBinding.Result preparation) {
            if (preparation instanceof ValidationNestLookupBinding.Invalid invalid) {
                problems(scope, parent, path(relation), invalid.problems()); return false;
            }
            if (preparation instanceof ValidationNestLookupBinding.Unresolved unresolved) {
                for (String field : unresolved.fields()) propagate(scope, List.of(field), path(relation));
                problems(scope, parent, path(relation), unresolved.knownProblems()); return false;
            }
            return true;
        }
        void check(Node node, ValidationValues.Row target, ValidationCheckEvent.Side physical, Builder result) {
            authorized.run(); budget.requireScope(node.scope.observation());
            budget.reserve(target.accountedBytes() + 256L * node.children.size());
            var checked = new ArrayList<ValidationNestCheckedSubtree>();
            for (var branch : node.children) {
                budget.operations(limits.checkOperations());
                if (branch.array != null) {
                    var keys = branch.binding.arrayKeys().orElseThrow();
                    var proof = ValidationNestArrayFieldCheck.check(node.scope, path(branch.relation), node.view.row(), physical, target,
                            keys.targetElement(), keys.targetRow(), containers, new ValidationNestTargetArray.Limits(limits.row(), limits.elements(), limits.checkOperations()),
                            budget.targetMembers(branch.array), result, group -> {
                                var source = group.sourceSamples().getFirst(); var child = branch.members.get(source);
                                if (child == null || child.view.row() != source.values()) throw invalid("memberNode");
                                check(child, group.targetSamples().getFirst().values(), physical, result);
                            });
                    checked.add(proof);
                } else {
                    // Reserve a conservative parent-sized copy before ObjectCheck owns the child view.
                    budget.row(node.scope.observation()); budget.reserve(target.accountedBytes());
                    checked.add(ValidationNestObjectFieldCheck.check(node.scope, path(branch.relation), node.view.row(), branch.object,
                            physical, target, containers, new ValidationValueComparison.TraversalLimits(limits.row(), limits.checkOperations()), result,
                            (source, childTarget) -> {
                                if (branch.child == null || branch.child.view.row() != source.working()) throw invalid("objectNode");
                                check(branch.child, childTarget, physical, result);
                            }));
                }
            }
            budget.operations(limits.checkOperations());
            ValidationFieldCheck.checkComposed(node.scope, node.view.row(), target, Optional.of(node.original), Optional.empty(),
                    node.scope.observation().root().ancestors().isEmpty() && node.scope.objectPath().isEmpty() ? generatedId : Optional.empty(), compatible,
                    new ValidationValueComparison.TraversalLimits(limits.row(), limits.checkOperations()), node.view.unavailablePaths(), node.view.definedFields(), checked, result);
            authorized.run();
        }
        void merge(ValidationRootReader.SourceEvidence evidence) {
            if (evidence.observation() != null && !root.equals(evidence.observation())) throw invalid("evidenceRoot");
            for (var read : evidence.reads()) budget.read(read);
            for (var issue : evidence.issues()) issue(issue);
        }
        void issue(ValidationRootReader.SourceIssue value) {
            var previous = issues.get(value.finding());
            if (previous == null && issues.size() == 256) throw limit("issues");
            budget.reserve(512L + 2048L * (value.affected().size() + (previous == null ? 0 : previous.affected().size()))
                    + value.sample().map(SampledSide::accountedBytes).orElse(0L));
            var affected = new LinkedHashSet<Location>(); if (previous != null) affected.addAll(previous.affected());
            for (var at : value.affected()) if (!affected.contains(at)) {
                if (affected.size() == 256) throw limit("affected"); affected.add(at);
            }
            issues.put(value.finding(), new ValidationRootReader.SourceIssue(value.finding(), previous == null ? value.sample() : previous.sample(), new ArrayList<>(affected)));
        }
        void requireCause(ValidationNestScope scope, List<Object> local) {
            var full = new ArrayList<Object>(scope.objectPath()); full.addAll(local);
            for (var issue : issues.values()) for (var at : issue.affected()) {
                budget.operations(1L + ValidationKey.MAX_KEY_BYTES + 1024L * (at.fieldPath().size() + full.size()));
                if (at.scope().equals(scope.observation().root()) && at.fieldPath().equals(full)) return;
            }
            throw invalid("unknownWithoutCause");
        }
        void propagate(ValidationNestScope scope, List<String> from, List<String> to) {
            var full = scope.location(from); boolean found = false;
            budget.reserve(128L + 64L * issues.size());
            for (var value : new ArrayList<>(issues.values())) for (var at : value.affected()) {
                budget.operations(1L + ValidationKey.MAX_KEY_BYTES + 1024L * (at.fieldPath().size() + full.fieldPath().size()));
                if (at.scope().equals(full.scope()) && (starts(at.fieldPath(), full.fieldPath()) || starts(full.fieldPath(), at.fieldPath()))) {
                    issue(new ValidationRootReader.SourceIssue(value.finding(), value.sample(), List.of(scope.location(to)))); found = true; break;
                }
            }
            if (!found) throw invalid("unknownWithoutCause");
        }
        void problems(ValidationNestScope scope, SampledSide parent, List<String> output, List<ValidationKeyBinding.Problem> problems) {
            for (var problem : problems) issue(new ValidationRootReader.SourceIssue(new Finding(problem.kind() == ValidationKeyBinding.ProblemKind.MISSING_FIELD
                    ? Kind.SOURCE_FIELD_MISSING : Kind.SOURCE_FIELD_INVALID, scope.location(problem.path())), Optional.of(parent), List.of(scope.location(output))));
        }
        ValidationRootReader.SourceEvidence evidence() { return new ValidationRootReader.SourceEvidence(root, budget.reads(), new ArrayList<>(issues.values())); }
        ValidationRootReader.SourceEvidence prefixed(ValidationNestScope scope, ValidationRootReader.SourceEvidence evidence) {
            return new ValidationRootReader.SourceEvidence(root, evidence.reads(), evidence.issues().stream().map(issue ->
                    new ValidationRootReader.SourceIssue(new Finding(issue.finding().kind(), prefix(scope, issue.finding().location()), issue.finding().multiplicity()),
                            issue.sample(), issue.affected().stream().map(at -> prefix(scope, at)).toList())).toList());
        }
    }
    private record Node(ValidationNestScope scope, SampledSide original, ValidationStaticMap.Projection view, List<Branch> children) {}
    private record Branch(ValidationNestRelations.Relation relation, Binding binding, ValidationNestLookupBinding.Result preparation,
                          ValidationNestArrayMembers.Source array, ValidationNestSingleSource.Result object,
                          Map<ValidationNestArrayMembers.Member, Node> members, Node child) {}
    private void validate(List<ValidationNestRelations.Relation> relations, Set<List<String>> used, int depth) {
        if (relations.isEmpty()) return;
        if (depth > limits.depth()) throw limit("depth");
        for (var relation : relations) {
            if (relation.shape() == EmbedAs.FLAT) throw new TapstateException(ValidationError.RULE_UNSUPPORTED,
                    Map.of("node", plan.node(), "field", relation.treePath().toString(), "rule", "nest.mixedTreeFlat"), null);
            var binding = bindings.get(relation.treePath());
            if (!used.add(relation.treePath()) || binding == null || !binding.lookup().relation().equals(relation)) throw invalid("relationBinding");
            path(relation); validate(relation.children(), used, depth + 1);
        }
    }
    private static Location prefix(ValidationNestScope scope, Location at) {
        var path = new ArrayList<Object>(scope.objectPath()); path.addAll(at.fieldPath()); return new Location(at.scope(), path);
    }
    private static boolean known(ValidationStaticMap.Projection view, String name) {
        return view.unavailablePaths().stream().noneMatch(path -> path.isEmpty() ? !view.definedFields().contains(name)
                : path.size() == 1 && path.getFirst().equals(name));
    }
    private static List<String> path(ValidationNestRelations.Relation relation) {
        var path = List.of(relation.outputPath().orElseThrow().split("\\.", -1));
        if (path.size() > 32 || path.stream().anyMatch(String::isBlank)) throw invalid("path"); return path;
    }
    private static void keyDomains(ValidationKeyBinding source, ValidationKeyBinding target) {
        var left = source.components(); var right = target.components();
        if (left.size() != right.size()) throw invalid("arrayKeyDomain");
        for (int i = 0; i < left.size(); i++) if (left.get(i).domain() != right.get(i).domain()
                || left.get(i).numericPolicy() != right.get(i).numericPolicy()) throw invalid("arrayKeyDomain");
    }
    private static ValidationKey bound(ValidationKeyBinding.Result value) {
        if (value instanceof ValidationKeyBinding.Bound bound) return bound.key(); throw invalid("key");
    }
    private static boolean starts(List<?> path, List<?> prefix) { return path.size() >= prefix.size() && path.subList(0, prefix.size()).equals(prefix); }
    @SuppressWarnings("unchecked") private static Map<String, Object> prefixes(Map<String, Object> row, List<String> path) {
        Map<String, Object> current = row;
        for (int i = 0; i < path.size() - 1; i++) current = (Map<String, Object>) current.computeIfAbsent(path.get(i), ignored -> new LinkedHashMap<>());
        return current;
    }
    private static void put(Map<String, Object> row, List<String> path, Object value) { prefixes(row, path).put(path.getLast(), value); }
    private static TapstateException invalid(String field) { return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "nestTree." + field), null); }
    private static TapstateException limit(String field) { return new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED, Map.of("limit", "nestTree." + field), null); }
}
