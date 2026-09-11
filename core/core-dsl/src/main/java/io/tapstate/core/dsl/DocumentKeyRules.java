package io.tapstate.core.dsl;

import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.ServeResource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reports the columns a batch would write into a store that reads a dot in a key as a step into a
 * nested document, when the column's own name holds one.
 *
 * <p>Such a column is written correctly and is really there, and every ordinary way of addressing it
 * takes the other reading: a read by path asks for a field inside a parent that does not exist, and no
 * index can be declared over it at all, because an index key is written in that same path syntax.
 *
 * <p><b>Reported, not refused.</b> The name is legal in the source and legal in the target, and the
 * rows cross correctly — refusing would also refuse every pipeline whose data already sits in a
 * collection this way. What makes it worth saying is that nothing downstream will: an empty answer to
 * a read for that column is indistinguishable from a column that happens to hold nothing.
 *
 * <p><b>Apply is the moment.</b> The column name is already in the discovered model of the source, so
 * nothing has to be run and no query has to be waited for to know that the name will not address
 * itself once written.
 *
 * <p><b>It judges the columns the sync reads, not the fields the sink finally writes.</b> A projection
 * step between the two can rename the column to something without a dot, and this does not follow it —
 * which field names come out of a step is a runtime answer, and for a script step there is no offline
 * answer at all. So a pipeline that already renames the column away is reported anyway. That is the
 * deliberate side to err on: over-reporting costs an author one line to read, and the alternative is
 * the silence this exists to remove.
 */
public final class DocumentKeyRules {

    /**
     * The connectors that read a dot in a key as a step into a nested document — the one place this
     * rule's set is written down.
     *
     * <p>A closed list rather than something derived, because there is nothing to derive it from: a
     * catalog row says what a connector can read and write, never how it addresses a key. Adding one
     * here is therefore a decision somebody makes and a reviewer sees, which is the shape the other
     * closed connector sets in this product take and for the same reason.
     */
    private static final Set<String> KEYS_READ_AS_PATHS = Set.of(
            "mongodb", "mongodb-atlas", "mongodb3", "aliyun-db-mongodb", "tencent-db-mongodb");

    private static final char STEP = '.';

    private DocumentKeyRules() {
    }

    /**
     * The findings over {@code batch}, in the order the columns were discovered. {@code tablesBySource}
     * is what each source was discovered to hold, keyed by the source's id; a source it omits has not
     * been discovered, and nothing can be said about its columns.
     */
    public static List<Advisory> review(
            Collection<Resource> batch, Map<String, List<DiscoveredTable>> tablesBySource) {
        Map<String, Resource> byId = new LinkedHashMap<>();
        for (Resource resource : batch) {
            byId.putIfAbsent(resource.id(), resource);
        }
        List<Advisory> findings = new ArrayList<>();
        Set<List<String>> reported = new LinkedHashSet<>();
        for (Resource resource : batch) {
            if (resource instanceof PipelineResource pipeline) {
                reviewPipeline(pipeline, byId, tablesBySource, reported, findings);
            }
        }
        return List.copyOf(findings);
    }

    private static void reviewPipeline(PipelineResource pipeline, Map<String, Resource> byId,
            Map<String, List<DiscoveredTable>> tablesBySource, Set<List<String>> reported,
            List<Advisory> findings) {
        ServeBlock serve = pipeline.serve();
        List<SyncElement> sync = syncOf(serve, byId);
        if (sync == null || sync.isEmpty()) {
            return;
        }
        FromClause from = switch (serve) {
            case ServeBlock.Inline inline -> inline.from();
            case ServeBlock.Use use -> use.from();
        };
        Set<Upstream> upstream = named(new Wiring(pipeline, byId).reaching(from), byId);
        for (SyncElement element : sync) {
            if (!(byId.get(element.source()) instanceof SourceResource target)
                    || !KEYS_READ_AS_PATHS.contains(target.connector())) {
                continue;
            }
            for (Upstream up : upstream) {
                for (DiscoveredTable table : tablesReaching(up, tablesBySource)) {
                    report(pipeline, up.source(), table, target.id(), reported, findings);
                }
            }
        }
    }

    /**
     * The upstreams whose discovered column names are names. A source that addresses a key by path
     * reports a nested field as the path leading to it, so {@code address.city} arriving from one is a
     * document shape the connector flattened, not a column somebody named with a dot in it — the
     * nesting crosses to the target unchanged and a read for that same path answers. Nothing in the
     * discovered model tells the two apart, so an upstream that addresses keys by path contributes
     * none: reporting there would say "unreachable" about every nested field of every document source,
     * on every apply, which is the noise that stops the channel being read at all.
     */
    private static Set<Upstream> named(Set<Upstream> upstream, Map<String, Resource> byId) {
        Set<Upstream> kept = new LinkedHashSet<>();
        for (Upstream up : upstream) {
            if (byId.get(up.source()) instanceof SourceResource origin
                    && KEYS_READ_AS_PATHS.contains(origin.connector())) {
                continue;
            }
            kept.add(up);
        }
        return kept;
    }

    /**
     * The sync elements the serve block declares. A block that names a reusable definition holds its
     * own {@code from:}, so the declarations come from the definition while the wiring comes from the
     * block using it — the same split a step naming a reusable transform takes.
     */
    private static List<SyncElement> syncOf(ServeBlock serve, Map<String, Resource> byId) {
        return switch (serve) {
            case null -> null;
            case ServeBlock.Inline inline -> inline.sync();
            case ServeBlock.Use use -> byId.get(use.use()) instanceof ServeResource definition
                    ? definition.sync()
                    : null;
        };
    }

    /**
     * The discovered tables {@code up} reads. An upstream the wiring could not narrow to one table
     * reads whichever tables that source holds, so every one of them is in play; a source nobody
     * discovered contributes none, because an undiscovered source has no column names to judge.
     */
    private static List<DiscoveredTable> tablesReaching(
            Upstream up, Map<String, List<DiscoveredTable>> tablesBySource) {
        List<DiscoveredTable> discovered = tablesBySource.get(up.source());
        if (discovered == null) {
            return List.of();
        }
        if (up.table() == null) {
            return discovered;
        }
        return discovered.stream().filter(table -> table.name().equals(up.table())).toList();
    }

    /** Reports each column of {@code table} whose own name holds a step, once per target. */
    private static void report(PipelineResource pipeline, String sourceId, DiscoveredTable table,
            String targetId, Set<List<String>> reported, List<Advisory> findings) {
        for (String column : table.columns().keySet()) {
            if (column.indexOf(STEP) < 0) {
                continue;
            }
            // One column can be reached by several upstreams of one pipeline - a table named directly
            // and again through a step that reads it. Saying it twice would read as two columns.
            if (!reported.add(List.of(pipeline.id(), sourceId, table.name(), column, targetId))) {
                continue;
            }
            findings.add(new Advisory(DocumentKeyError.COLUMN_NAME_READS_AS_A_PATH, Map.of(
                    "pipeline", pipeline.id(),
                    "source", sourceId,
                    "table", table.name(),
                    "column", column,
                    "target", targetId)));
        }
    }
}
