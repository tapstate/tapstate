package io.tapstate.app;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.JoinTree;
import io.tapstate.spi.capture.BoundedQueryCancellation;
import io.tapstate.spi.capture.BoundedSnapshotQueryPort;
import io.tapstate.spi.capture.BoundedSnapshotQueryRequest;
import io.tapstate.spi.capture.BoundedSnapshotQueryResult;
import io.tapstate.spi.store.SourceTable;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Resolves a small root sample and only the exact relation rows those roots can reach. */
final class PreviewSelectionPlanner {

    static final int MAX_INPUT_ROWS = 20_000;
    static final long MAX_SAMPLE_BYTES = 32L * 1024L * 1024L;

    private final BoundedSnapshotQueryPort queries;
    private final PreviewSampleCache sampleCache;
    private final Clock clock;
    private final SourceView sourceView;

    @FunctionalInterface
    interface SourceView {
        List<PreviewSourceTable> sourceTables(PipelineResource pipeline);

        default Set<String> inputSourceKeys(PipelineResource pipeline) {
            throw new UnsupportedOperationException("input source keys are not available");
        }

        default Map<String, StoreBackedDagSource.CompiledJoin> compiledJoins(PipelineResource pipeline) {
            return Map.of();
        }
    }

    PreviewSelectionPlanner(BoundedSnapshotQueryPort queries, PreviewSampleCache sampleCache, Clock clock) {
        this.queries = Objects.requireNonNull(queries, "queries");
        this.sampleCache = Objects.requireNonNull(sampleCache, "sampleCache");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.sourceView = null;
    }

    PreviewSelectionPlanner(BoundedSnapshotQueryPort queries, PreviewSampleCache sampleCache, Clock clock,
            SourceView sourceView) {
        this.queries = Objects.requireNonNull(queries, "queries");
        this.sampleCache = Objects.requireNonNull(sampleCache, "sampleCache");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.sourceView = Objects.requireNonNull(sourceView, "sourceView");
    }

    Sample load(String principal, String pipelineId, String sampleId,
            PipelineResource pipeline, StoreBackedDagSource dagSource, int rootLimit,
            Instant deadline, BoundedQueryCancellation cancellation) {
        SourceView sources = sourceView == null ? new SourceView() {
            @Override
            public List<PreviewSourceTable> sourceTables(PipelineResource candidate) {
                return dagSource.previewSourceTables(candidate);
            }

            @Override
            public Set<String> inputSourceKeys(PipelineResource candidate) {
                return dagSource.previewInputSourceKeys(candidate);
            }

            @Override
            public Map<String, StoreBackedDagSource.CompiledJoin> compiledJoins(PipelineResource candidate) {
                return dagSource.previewCompiledJoins(candidate);
            }
        } : sourceView;
        Context context = new Context(principal, pipelineId, sampleId, pipeline, sources, deadline, cancellation);
        Set<String> required = sources.inputSourceKeys(pipeline);
        Map<String, StoreBackedDagSource.CompiledJoin> joins = sources.compiledJoins(pipeline);
        LinkedHashSet<String> dimensions = new LinkedHashSet<>();
        LinkedHashSet<String> roots = new LinkedHashSet<>();

        for (Map.Entry<String, StoreBackedDagSource.CompiledJoin> entry : joins.entrySet()) {
            Step step = context.steps.get(entry.getKey());
            if (step == null || !(step.from() instanceof FromClause.Aliases)) {
                refuse("a Join preview could not resolve its declared input aliases");
            }
            FromClause.Aliases aliases = (FromClause.Aliases) step.from();
            String factAlias = entry.getValue().plan().factSource().name();
            roots.add(context.lineage(aliases.aliases().get(factAlias)).sourceKey());
            for (String alias : entry.getValue().plan().from().sources().stream()
                    .map(JoinTree.Source::declared).toList()) {
                if (!alias.equals(factAlias)) {
                    dimensions.add(context.lineage(aliases.aliases().get(alias)).sourceKey());
                }
            }
        }

        List<NestSelection> nests = new ArrayList<>();
        for (Step step : context.steps.values()) {
            if (step instanceof Step.Inline inline && inline.body() instanceof TransformBody.Nest nest) {
                if (!(step.from() instanceof FromClause.Aliases)) {
                    refuse("a Nest preview could not resolve its declared input aliases");
                }
                FromClause.Aliases aliases = (FromClause.Aliases) step.from();
                Lineage root = context.lineage(aliases.aliases().get(nest.root().from()));
                roots.add(root.sourceKey());
                nests.add(new NestSelection(root,
                        context.resolveRelations(aliases, nest.root().embed(), root, dimensions)));
            }
        }

        if (roots.isEmpty()) {
            roots.addAll(required);
        } else {
            for (String sourceKey : required) {
                if (!dimensions.contains(sourceKey)) {
                    roots.add(sourceKey);
                }
            }
        }
        roots.retainAll(required);
        if (roots.isEmpty()) {
            refuse("the selected output has no bounded source anchor");
        }

        List<String> orderedRoots = roots.stream().sorted().toList();
        if (orderedRoots.size() > rootLimit) {
            refuse("rootLimit must allow at least one row from every required root source");
        }
        List<String> selectedRoots = new ArrayList<>();
        for (int index = 0; index < orderedRoots.size(); index++) {
            int allocation = rootLimit / orderedRoots.size()
                    + (index < rootLimit % orderedRoots.size() ? 1 : 0);
            if (allocation == 0) {
                continue;
            }
            selectedRoots.add(orderedRoots.get(index));
            context.read(orderedRoots.get(index), new BoundedSnapshotQueryRequest.AllRows(), allocation, true);
        }

        for (NestSelection nest : nests) {
            List<Map<String, Object>> rootRows = context.logicalRows(nest.root());
            for (RelationSelection relation : nest.relations()) {
                context.loadRelation(relation, rootRows);
            }
        }
        for (Map.Entry<String, StoreBackedDagSource.CompiledJoin> entry : joins.entrySet()) {
            context.loadJoin(context.steps.get(entry.getKey()), entry.getValue());
        }

        Map<String, List<Envelope>> rows = new LinkedHashMap<>();
        context.rows.forEach((key, value) -> {
            List<Envelope> ordered = new ArrayList<>(value.size());
            long sequence = 0;
            for (Envelope row : value) {
                ordered.add(row.withOrder(new SourceOrder(0, sequence++)));
            }
            rows.put(key, List.copyOf(ordered));
        });
        for (String sourceKey : required) {
            rows.putIfAbsent(sourceKey, List.of());
        }
        return new Sample(Map.copyOf(rows), context.rootRows, context.rowsRead, context.bytesRead,
                context.queryCount, context.rootTruncated, context.repeatable,
                context.sourceReads > 0 && context.cachedReads == context.sourceReads, context.cachedReads,
                List.copyOf(selectedRoots));
    }

    private final class Context {

        private final PipelineResource pipeline;
        private final String principal;
        private final String pipelineId;
        private final String sampleId;
        private final Map<String, PreviewSourceTable> tablesByKey = new LinkedHashMap<>();
        private final Map<String, PreviewSourceTable> tablesByName = new LinkedHashMap<>();
        private final Set<String> ambiguousTableNames = new HashSet<>();
        private final Map<String, Step> steps = new LinkedHashMap<>();
        private final Map<String, List<Envelope>> rows = new LinkedHashMap<>();
        private final Instant deadline;
        private final BoundedQueryCancellation cancellation;
        private int rowsRead;
        private int rootRows;
        private int queryCount;
        private int sourceReads;
        private int cachedReads;
        private long bytesRead;
        private boolean rootTruncated;
        private boolean repeatable = true;

        Context(String principal, String pipelineId, String sampleId,
                PipelineResource pipeline, SourceView sources, Instant deadline,
                BoundedQueryCancellation cancellation) {
            this.principal = Objects.requireNonNull(principal, "principal");
            this.pipelineId = Objects.requireNonNull(pipelineId, "pipelineId");
            this.sampleId = Objects.requireNonNull(sampleId, "sampleId");
            this.pipeline = pipeline;
            this.deadline = deadline;
            this.cancellation = cancellation;
            sources.sourceTables(pipeline).forEach(table -> {
                tablesByKey.put(table.sourceKey(), table);
                String tableName = table.schema().name();
                if (!ambiguousTableNames.contains(tableName)) {
                    PreviewSourceTable previous = tablesByName.putIfAbsent(tableName, table);
                    if (previous != null && previous != table) {
                        tablesByName.remove(tableName);
                        ambiguousTableNames.add(tableName);
                    }
                }
                tablesByName.put(table.sourceId() + "." + table.schema().name(), table);
            });
            if (pipeline.transforms() != null) {
                pipeline.transforms().forEach(step -> steps.put(step.id(), step));
            }
        }

        Lineage lineage(FromRef ref) {
            if (!(ref instanceof FromRef.Literal)) {
                refuse("regex-linked inputs cannot be sampled with exact relation keys");
            }
            FromRef.Literal literal = (FromRef.Literal) ref;
            String name = literal.ref();
            if (ambiguousTableNames.contains(name)) {
                refuse("source table reference '" + name + "' is ambiguous across the candidate sources");
            }
            PreviewSourceTable source = tablesByKey.get(name);
            if (source == null) {
                source = tablesByName.get(name);
            }
            if (source != null) {
                Map<String, String> fields = new LinkedHashMap<>();
                source.model().fields().forEach(field -> fields.put(field.name(), field.name()));
                return new Lineage(source.sourceKey(), Map.copyOf(fields), Map.of());
            }
            Step step = steps.get(name);
            if (!(step instanceof Step.Inline)) {
                refuse("preview input '" + name + "' does not resolve to a discovered source table");
            }
            Step.Inline inline = (Step.Inline) step;
            List<FromRef> refs = refs(inline.from());
            if (refs.size() != 1) {
                refuse("multi-input transforms cannot supply exact relation keys");
            }
            Lineage upstream = lineage(refs.getFirst());
            if (inline.body() instanceof TransformBody.Filter) {
                return upstream;
            }
            if (inline.body() instanceof TransformBody.MapProjection projection) {
                return mapLineage(upstream, projection, inline.id());
            }
            if (inline.body() instanceof TransformBody.Unwind unwind) {
                Map<String, String> fields = new LinkedHashMap<>(upstream.physicalByLogical());
                Map<String, DroppedField> drops = new LinkedHashMap<>(upstream.droppedByLogical());
                if (unwind.elementKey() != null) {
                    fields.remove(unwind.elementKey());
                    drops.remove(unwind.elementKey());
                }
                if (unwind.includeArrayIndex() != null) {
                    fields.remove(unwind.includeArrayIndex());
                    drops.remove(unwind.includeArrayIndex());
                }
                return new Lineage(upstream.sourceKey(), Map.copyOf(fields), Map.copyOf(drops));
            }
            refuse("transform '" + name + "' does not expose reversible source-key lineage");
            throw new IllegalStateException("unreachable");
        }

        private Lineage mapLineage(Lineage upstream, TransformBody.MapProjection projection, String stepId) {
            Map<String, String> fields = new LinkedHashMap<>(upstream.physicalByLogical());
            Map<String, DroppedField> drops = new LinkedHashMap<>(upstream.droppedByLogical());
            Set<String> consumed = new HashSet<>();
            projection.outputFields().forEach((output, rule) -> {
                fields.remove(output);
                drops.remove(output);
                switch (rule) {
                    case FieldRule.Rename rename -> {
                        String input = rename.sourceField();
                        String physical = upstream.physicalByLogical().get(input);
                        if (physical != null) {
                            fields.put(output, physical);
                        } else if (upstream.droppedByLogical().containsKey(input)) {
                            drops.put(output, upstream.droppedByLogical().get(input));
                        }
                        consumed.add(input);
                    }
                    case FieldRule.Drop ignored -> {
                        DroppedField removed = upstream.physicalByLogical().containsKey(output)
                                ? new DroppedField(stepId, output) : upstream.droppedByLogical().get(output);
                        if (removed != null) {
                            drops.put(output, removed);
                        }
                    }
                    case FieldRule.Literal ignored -> { }
                    case FieldRule.Computed ignored -> { }
                }
            });
            consumed.stream().filter(field -> !projection.fields().containsKey(field)).forEach(field -> {
                fields.remove(field);
                drops.remove(field);
            });
            return new Lineage(upstream.sourceKey(), Map.copyOf(fields), Map.copyOf(drops));
        }

        private List<RelationSelection> resolveRelations(FromClause.Aliases aliases, List<Embed> embeds,
                Lineage parent, Set<String> dimensions) {
            if (embeds == null) {
                return List.of();
            }
            List<RelationSelection> relations = new ArrayList<>();
            for (Embed embed : embeds) {
                Lineage child = lineage(aliases.aliases().get(embed.from()));
                dimensions.add(child.sourceKey());
                if (embed.on().isEmpty()) {
                    refuse("Nest relation '" + embed.from() + "' has no exact join keys");
                }
                List<List<RelationKey>> alternatives = new ArrayList<>();
                for (Map<String, String> alternative : embed.alternatives()) {
                    List<RelationKey> keys = new ArrayList<>();
                    for (Map.Entry<String, String> pair : alternative.entrySet()) {
                        String physical = requireRelationKey(embed.from(), child, pair.getKey());
                        requireRelationKey(embed.from(), parent, pair.getValue());
                        keys.add(new RelationKey(pair.getValue(), physical));
                    }
                    alternatives.add(keys);
                }
                relations.add(new RelationSelection(child, alternatives,
                        resolveRelations(aliases, embed.embed(), child, dimensions)));
            }
            return relations;
        }

        private String requireRelationKey(String relation, Lineage lineage, String field) {
            String physical = lineage.physicalByLogical().get(field);
            if (physical == null) {
                DroppedField dropped = lineage.droppedByLogical().get(field);
                if (dropped != null) {
                    throw new TapstateException(ActuationError.PREVIEW_ASSOCIATION_KEY_DROPPED,
                            Map.of("relation", relation, "field", field, "transform", dropped.transform(),
                                    "droppedField", dropped.field()), null);
                }
                refuse("Nest relation keys must resolve through source fields and direct Map renames");
            }
            return physical;
        }

        private void loadRelation(RelationSelection relation, List<Map<String, Object>> parentRows) {
            // Each alternative needs a separate read because its tuples can use different child columns.
            List<Envelope> childRows = new ArrayList<>();
            for (List<RelationKey> alternative : relation.alternatives()) {
                TupleSet tuples = new TupleSet();
                for (Map<String, Object> row : parentRows) {
                    Map<String, Object> tuple = new LinkedHashMap<>();
                    boolean usable = true;
                    for (RelationKey key : alternative) {
                        Object value = row.get(key.parentField());
                        if (value == null) {
                            usable = false;
                            break;
                        }
                        tuple.put(key.physicalChildField(), value);
                    }
                    if (usable) {
                        tuples.add(tuple);
                    }
                }
                childRows.addAll(read(relation.child().sourceKey(),
                        new BoundedSnapshotQueryRequest.ExactTuples(tuples.values()),
                        Math.max(1, MAX_INPUT_ROWS - rowsRead), false));
            }
            List<Map<String, Object>> logicalChildren = logicalRows(relation.child(), childRows);
            for (RelationSelection nested : relation.children()) {
                loadRelation(nested, logicalChildren);
            }
        }

        private List<Map<String, Object>> logicalRows(Lineage lineage) {
            return logicalRows(lineage, rows.getOrDefault(lineage.sourceKey(), List.of()));
        }

        private List<Map<String, Object>> logicalRows(Lineage lineage, List<Envelope> sourceRows) {
            List<Map<String, Object>> logical = new ArrayList<>(sourceRows.size());
            for (Envelope sourceRow : sourceRows) {
                Map<String, Object> mapped = new LinkedHashMap<>();
                lineage.physicalByLogical().forEach((name, physical) -> {
                    if (sourceRow.after().containsKey(physical)) {
                        mapped.put(name, sourceRow.after().get(physical));
                    }
                });
                logical.add(mapped);
            }
            return logical;
        }

        private void loadJoin(Step step, StoreBackedDagSource.CompiledJoin compiled) {
            if (!(step.from() instanceof FromClause.Aliases)) {
                refuse("Join preview requires an alias map");
            }
            FromClause.Aliases aliases = (FromClause.Aliases) step.from();
            Map<String, Lineage> lineages = new LinkedHashMap<>();
            aliases.aliases().forEach((name, ref) -> lineages.put(name, lineage(ref)));
            // an OR alternative the front end added samples the rows of the alias it stands in for
            compiled.plan().from().sources().forEach(source -> {
                if (!source.declared().equals(source.name()) && lineages.containsKey(source.declared())) {
                    lineages.put(source.name(), lineages.get(source.declared()));
                }
            });
            Map<String, List<Map<String, Object>>> available = new LinkedHashMap<>();
            String fact = compiled.plan().factSource().name();
            available.put(fact, logicalRows(lineages.get(fact)));
            List<JoinTree.Join> nodes = new ArrayList<>();
            collectJoins(compiled.plan().from(), nodes);
            Set<String> expected = new LinkedHashSet<>();
            compiled.plan().from().sources().forEach(source -> expected.add(source.name()));
            for (int pass = 0; pass <= expected.size(); pass++) {
                boolean progressed = false;
                for (JoinTree.Join node : nodes) {
                    if (node.hasUncapturedCondition() || node.on().isEmpty()) {
                        refuse("Join preview cannot safely sample a non-equality or unbounded join condition");
                    }
                    Map<String, List<JoinTree.KeyPair>> pairsByTarget = new LinkedHashMap<>();
                    Map<String, String> dataAliasByTarget = new LinkedHashMap<>();
                    for (JoinTree.KeyPair pair : node.on()) {
                        boolean leftReady = available.containsKey(pair.left().source());
                        boolean rightReady = available.containsKey(pair.right().source());
                        if (leftReady && !rightReady) {
                            pairsByTarget.computeIfAbsent(pair.right().source(), ignored -> new ArrayList<>()).add(pair);
                            dataAliasByTarget.put(pair.right().source(), pair.left().source());
                        } else if (rightReady && !leftReady) {
                            pairsByTarget.computeIfAbsent(pair.left().source(), ignored -> new ArrayList<>()).add(pair);
                            dataAliasByTarget.put(pair.left().source(), pair.right().source());
                        }
                    }
                    for (Map.Entry<String, List<JoinTree.KeyPair>> target : pairsByTarget.entrySet()) {
                        String targetAlias = target.getKey();
                        if (available.containsKey(targetAlias)) {
                            continue;
                        }
                        String dataAlias = dataAliasByTarget.get(targetAlias);
                        for (JoinTree.KeyPair pair : target.getValue()) {
                            String pairDataAlias = pair.left().source().equals(targetAlias)
                                    ? pair.right().source() : pair.left().source();
                            if (!dataAlias.equals(pairDataAlias)) {
                                refuse("Join preview requires each bounded equality to use one sampled source per side");
                            }
                        }
                        TupleSet tuples = new TupleSet();
                        Lineage targetLineage = lineages.get(targetAlias);
                        Lineage dataLineage = lineages.get(dataAlias);
                        if (targetLineage == null || dataLineage == null) {
                            refuse("Join preview could not resolve a source alias");
                        }
                        for (Map<String, Object> row : available.get(dataAlias)) {
                            Map<String, Object> tuple = new LinkedHashMap<>();
                            boolean usable = true;
                            for (JoinTree.KeyPair pair : target.getValue()) {
                                boolean targetIsLeft = pair.left().source().equals(targetAlias);
                                String targetField = targetIsLeft ? pair.left().column() : pair.right().column();
                                String dataField = targetIsLeft ? pair.right().column() : pair.left().column();
                                String logicalData = dataLineage.physicalByLogical().get(dataField);
                                String physicalTarget = targetLineage.physicalByLogical().get(targetField);
                                Object value = logicalData == null ? null : row.get(dataField);
                                if (physicalTarget == null || value == null) {
                                    usable = false;
                                    break;
                                }
                                tuple.put(physicalTarget, value);
                            }
                            if (usable) {
                                tuples.add(tuple);
                            }
                        }
                        List<Envelope> found = read(targetLineage.sourceKey(),
                                new BoundedSnapshotQueryRequest.ExactTuples(tuples.values()),
                                Math.max(1, MAX_INPUT_ROWS - rowsRead), false);
                        available.put(targetAlias, logicalRows(targetLineage, found));
                        progressed = true;
                    }
                }
                if (expected.stream().allMatch(available::containsKey)) {
                    break;
                }
                if (!progressed) {
                    refuse("Join preview could not derive exact keys for every dimension source");
                }
            }
        }

        private List<Envelope> read(String sourceKey, BoundedSnapshotQueryRequest.Selection selection,
                int requestedLimit, boolean root) {
            cancellation.throwIfCancelled();
            if (!clock.instant().isBefore(deadline)) {
                refuse("the preview exceeded its 45 second execution deadline");
            }
            PreviewSourceTable source = tablesByKey.get(sourceKey);
            if (source == null) {
                refuse("preview source '" + sourceKey + "' was not discovered");
            }
            if (selection instanceof BoundedSnapshotQueryRequest.ExactTuples exact && exact.tuples().isEmpty()) {
                rows.putIfAbsent(sourceKey, List.of());
                return List.of();
            }
            int remaining = MAX_INPUT_ROWS - rowsRead;
            if (remaining < 1) {
                refuse("the preview exceeded 20,000 total source rows");
            }
            int limit = Math.min(remaining, Math.min(BoundedSnapshotQueryRequest.MAX_ROWS, requestedLimit));
            if (limit < 1) {
                refuse("the preview has no remaining source-row budget");
            }
            long remainingBytes = MAX_SAMPLE_BYTES - bytesRead;
            if (remainingBytes < 1) {
                refuse("the preview input sample exceeds 32 MiB");
            }
            SourceTable model = source.model();
            BoundedSnapshotQueryRequest request = new BoundedSnapshotQueryRequest(
                    source.sourceId(), source.connectorId(), source.settings(), source.schema(),
                    model.primaryKey(), selection, List.of(), limit, remainingBytes, deadline);
            sourceReads++;
            String connectorIdentity = queries.cacheIdentity(request);
            BoundedSnapshotQueryResult result = sampleCache.get(
                    principal, pipelineId, sampleId, connectorIdentity, request);
            if (result == null) {
                result = queries.query(request, cancellation);
                sampleCache.put(principal, pipelineId, sampleId, connectorIdentity, request, result);
            } else {
                cachedReads++;
            }
            queryCount += result.queryCount();
            repeatable &= result.repeatable();
            if (!root && !result.complete()) {
                refuse("an exact relation query was truncated and cannot produce a complete preview");
            }
            if (root && result.hasMore()) {
                rootTruncated = true;
            }
            if (root) {
                rootRows += result.rows().size();
            }
            rowsRead += result.rows().size();
            for (Envelope row : result.rows()) {
                String encoded = JsonWriter.write(PreviewJsonValues.normalize(row.after()));
                bytesRead += encoded.getBytes(StandardCharsets.UTF_8).length;
                if (bytesRead > MAX_SAMPLE_BYTES) {
                    refuse("the preview input sample exceeds 32 MiB");
                }
            }
            append(sourceKey, result.rows(), model.primaryKey());
            return result.rows();
        }

        private void append(String sourceKey, List<Envelope> additions, List<String> primaryKey) {
            List<Envelope> existing = rows.computeIfAbsent(sourceKey, ignored -> new ArrayList<>());
            if (existing.isEmpty()) {
                existing.addAll(additions);
                return;
            }
            if (primaryKey.isEmpty()) {
                refuse("a source used by multiple preview relations needs a discovered primary key");
            }
            Set<String> seen = new HashSet<>();
            for (Envelope row : existing) {
                seen.add(rowIdentity(row.after(), primaryKey));
            }
            for (Envelope row : additions) {
                if (seen.add(rowIdentity(row.after(), primaryKey))) {
                    existing.add(row);
                }
            }
        }

        private String rowIdentity(Map<String, Object> row, List<String> primaryKey) {
            List<Object> values = new ArrayList<>(primaryKey.size());
            for (String field : primaryKey) {
                if (!row.containsKey(field) || row.get(field) == null) {
                    refuse("a sampled source row is missing its discovered primary key");
                }
                values.add(PreviewJsonValues.normalize(row.get(field)));
            }
            return JsonWriter.write(values);
        }
    }

    private static List<FromRef> refs(FromClause clause) {
        if (clause instanceof FromClause.Flow flow) {
            return flow.refs();
        }
        return ((FromClause.Aliases) clause).aliases().values().stream().toList();
    }

    private static void collectJoins(JoinTree tree, List<JoinTree.Join> into) {
        if (tree instanceof JoinTree.Join join) {
            collectJoins(join.left(), into);
            collectJoins(join.right(), into);
            into.add(join);
        }
    }

    private static final class TupleSet {
        private final List<Map<String, Object>> values = new ArrayList<>();
        private final Set<String> fingerprints = new HashSet<>();

        void add(Map<String, Object> tuple) {
            String fingerprint = JsonWriter.write(PreviewJsonValues.normalize(tuple));
            if (fingerprints.add(fingerprint)) {
                values.add(Map.copyOf(tuple));
            }
        }

        List<Map<String, Object>> values() {
            return List.copyOf(values);
        }
    }

    private static void refuse(String reason) {
        throw new TapstateException(ActuationError.PREVIEW_REFUSED, Map.of("reason", reason), null);
    }

    private record Lineage(String sourceKey, Map<String, String> physicalByLogical,
            Map<String, DroppedField> droppedByLogical) {
    }

    private record DroppedField(String transform, String field) {
    }

    private record NestSelection(Lineage root, List<RelationSelection> relations) {
    }

    private record RelationSelection(Lineage child, List<List<RelationKey>> alternatives,
            List<RelationSelection> children) {
    }

    private record RelationKey(String parentField, String physicalChildField) {
    }

    record Sample(Map<String, List<Envelope>> rowsBySourceKey, int rootRows, int inputRows,
            long inputBytes, int queryCount, boolean rootTruncated, boolean repeatable,
            boolean cacheHit, int cachedReads, List<String> rootSourceKeys) {
        Sample {
            Map<String, List<Envelope>> copy = new LinkedHashMap<>();
            rowsBySourceKey.forEach((key, value) -> copy.put(key, List.copyOf(value)));
            rowsBySourceKey = Map.copyOf(copy);
            rootSourceKeys = List.copyOf(rootSourceKeys);
        }
    }
}
