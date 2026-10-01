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
import io.tapstate.spi.store.SourceField;
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

    PreviewSelectionPlanner(BoundedSnapshotQueryPort queries, PreviewSampleCache sampleCache, Clock clock) {
        this.queries = Objects.requireNonNull(queries, "queries");
        this.sampleCache = Objects.requireNonNull(sampleCache, "sampleCache");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    Sample load(String principal, String pipelineId, String sampleId,
            PipelineResource pipeline, StoreBackedDagSource dagSource, int rootLimit,
            Instant deadline, BoundedQueryCancellation cancellation) {
        Context context = new Context(principal, pipelineId, sampleId, pipeline, dagSource, deadline, cancellation);
        Set<String> required = dagSource.previewInputSourceKeys(pipeline);
        Map<String, StoreBackedDagSource.CompiledJoin> joins = dagSource.previewCompiledJoins(pipeline);
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
                    .map(JoinTree.Source::name).toList()) {
                if (!alias.equals(factAlias)) {
                    dimensions.add(context.lineage(aliases.aliases().get(alias)).sourceKey());
                }
            }
        }

        for (Step step : context.steps.values()) {
            if (step instanceof Step.Inline inline && inline.body() instanceof TransformBody.Nest nest) {
                if (!(step.from() instanceof FromClause.Aliases)) {
                    refuse("a Nest preview could not resolve its declared input aliases");
                }
                FromClause.Aliases aliases = (FromClause.Aliases) step.from();
                roots.add(context.lineage(aliases.aliases().get(nest.root().from())).sourceKey());
                collectNestDimensions(context, aliases, nest.root().embed(), dimensions);
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

        List<String> orderedRoots = List.copyOf(roots);
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

        for (Step step : context.steps.values()) {
            if (step instanceof Step.Inline inline && inline.body() instanceof TransformBody.Nest nest) {
                context.loadNest(step, nest);
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

    private static void collectNestDimensions(Context context, FromClause.Aliases aliases,
            List<Embed> embeds, Set<String> dimensions) {
        for (Embed embed : embeds == null ? List.<Embed>of() : embeds) {
            dimensions.add(context.lineage(aliases.aliases().get(embed.from())).sourceKey());
            collectNestDimensions(context, aliases, embed.embed(), dimensions);
        }
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
                PipelineResource pipeline, StoreBackedDagSource dagSource, Instant deadline,
                BoundedQueryCancellation cancellation) {
            this.principal = Objects.requireNonNull(principal, "principal");
            this.pipelineId = Objects.requireNonNull(pipelineId, "pipelineId");
            this.sampleId = Objects.requireNonNull(sampleId, "sampleId");
            this.pipeline = pipeline;
            this.deadline = deadline;
            this.cancellation = cancellation;
            dagSource.previewSourceTables(pipeline).forEach(table -> {
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
                return new Lineage(source.sourceKey(), Map.copyOf(fields));
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
                return mapLineage(upstream, projection);
            }
            if (inline.body() instanceof TransformBody.Unwind unwind) {
                Map<String, String> fields = new LinkedHashMap<>(upstream.physicalByLogical());
                if (unwind.elementKey() != null) {
                    fields.remove(unwind.elementKey());
                }
                if (unwind.includeArrayIndex() != null) {
                    fields.remove(unwind.includeArrayIndex());
                }
                return new Lineage(upstream.sourceKey(), Map.copyOf(fields));
            }
            refuse("transform '" + name + "' does not expose reversible source-key lineage");
            throw new IllegalStateException("unreachable");
        }

        private Lineage mapLineage(Lineage upstream, TransformBody.MapProjection projection) {
            Map<String, String> fields = new LinkedHashMap<>(upstream.physicalByLogical());
            Set<String> consumed = new HashSet<>();
            projection.fields().forEach((output, rule) -> {
                switch (rule) {
                    case FieldRule.Rename rename -> {
                        String physical = upstream.physicalByLogical().get(rename.sourceField());
                        fields.remove(output);
                        if (physical != null) {
                            fields.put(output, physical);
                        }
                        consumed.add(rename.sourceField());
                    }
                    case FieldRule.Drop ignored -> fields.remove(output);
                    case FieldRule.Literal ignored -> fields.remove(output);
                    case FieldRule.Computed ignored -> fields.remove(output);
                }
            });
            consumed.stream().filter(field -> !projection.fields().containsKey(field)).forEach(fields::remove);
            return new Lineage(upstream.sourceKey(), Map.copyOf(fields));
        }

        private void loadNest(Step step, TransformBody.Nest nest) {
            FromClause.Aliases aliases = (FromClause.Aliases) step.from();
            Lineage root = lineage(aliases.aliases().get(nest.root().from()));
            List<Map<String, Object>> parentRows = logicalRows(root);
            for (Embed embed : nest.root().embed() == null ? List.<Embed>of() : nest.root().embed()) {
                loadEmbed(aliases, embed, root, parentRows);
            }
        }

        private void loadEmbed(FromClause.Aliases aliases, Embed embed, Lineage parent,
                List<Map<String, Object>> parentRows) {
            Lineage child = lineage(aliases.aliases().get(embed.from()));
            if (embed.on().isEmpty()) {
                refuse("Nest relation '" + embed.from() + "' has no exact join keys");
            }
            TupleSet tuples = new TupleSet();
            for (Map<String, Object> row : parentRows) {
                Map<String, Object> tuple = new LinkedHashMap<>();
                boolean usable = true;
                for (Map.Entry<String, String> pair : embed.on().entrySet()) {
                    String physicalChild = child.physicalByLogical().get(pair.getKey());
                    if (parent.physicalByLogical().get(pair.getValue()) == null || physicalChild == null) {
                        refuse("Nest relation keys must resolve through source fields and direct Map renames");
                    }
                    Object value = row.get(pair.getValue());
                    if (value == null) {
                        usable = false;
                        break;
                    }
                    tuple.put(physicalChild, value);
                }
                if (usable) {
                    tuples.add(tuple);
                }
            }
            List<Envelope> childRows = read(child.sourceKey(),
                    new BoundedSnapshotQueryRequest.ExactTuples(tuples.values()),
                    Math.max(1, MAX_INPUT_ROWS - rowsRead), false);
            List<Map<String, Object>> logicalChildren = logicalRows(child, childRows);
            for (Embed nested : embed.embed() == null ? List.<Embed>of() : embed.embed()) {
                loadEmbed(aliases, nested, child, logicalChildren);
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
                refuse("the preview exceeded its 15 second execution deadline");
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
            if (!result.complete() || (!root && result.hasMore())) {
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

    private record Lineage(String sourceKey, Map<String, String> physicalByLogical) {
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
