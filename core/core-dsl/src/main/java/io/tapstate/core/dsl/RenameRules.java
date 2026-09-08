package io.tapstate.core.dsl;

import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.RenameSpec;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TableRef;
import io.tapstate.core.model.TableRename;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The sink-side target-name gate (ADR-0016 §8, X4). A {@code serve.sync} rename decides which table a sink
 * creates and writes, so two rules hold at validate time rather than at first write:
 *
 * <ul>
 *   <li>every rename must yield a name — an explicit map entry may not be blank, and a case transform may
 *       not consume the whole source name;</li>
 *   <li>no two source tables written to one connection may land on the same target name, and no two sync
 *       elements may claim one target name on one connection.</li>
 * </ul>
 *
 * <p>Both are judged over the tables the pipeline's sources <em>declare</em>. A source whose selector is
 * dynamic — a regex, or an omitted {@code tables} meaning every table — has no name set to judge, so only
 * the map entries, which are literal wherever they appear, are checked for it. That is the deliberate limit
 * of a static gate: the runtime universe is not knowable here, and a rule that guessed at it would reject
 * workspaces that are fine.
 */
final class RenameRules {

    private RenameRules() {
    }

    static void validate(Collection<Resource> batch) {
        Map<String, SourceResource> sources = new LinkedHashMap<>();
        for (Resource r : batch) {
            if (r instanceof SourceResource s) {
                sources.put(s.id(), s);
            }
        }
        for (Resource r : batch) {
            if (r instanceof PipelineResource p) {
                checkPipeline(p, sources);
            }
        }
    }

    private static void checkPipeline(PipelineResource pipeline, Map<String, SourceResource> sources) {
        if (!(pipeline.serve() instanceof ServeBlock.Inline serve) || serve.sync() == null) {
            return;
        }
        Set<String> tables = declaredTables(pipeline, sources);
        // Target names already claimed on each target connection, and which sync element claimed them.
        Map<String, Map<String, String>> claimed = new LinkedHashMap<>();
        List<SyncElement> sync = serve.sync();
        for (int i = 0; i < sync.size(); i++) {
            SyncElement element = sync.get(i);
            checkBlankMapEntries(element.rename(), "serve.sync[" + i + "].rename.map");
            for (String table : tables) {
                claim(TableRename.apply(table, element.rename()), table, element, i, claimed);
            }
        }
    }

    /** A map entry is literal, so a blank target name in one is wrong whatever the source declares. */
    private static void checkBlankMapEntries(RenameSpec rename, String path) {
        if (rename == null || rename.map() == null) {
            return;
        }
        for (Map.Entry<String, String> entry : rename.map().entrySet()) {
            if (entry.getValue() == null || entry.getValue().isBlank()) {
                throw illegalValue(path, entry.getKey() + ": " + entry.getValue());
            }
        }
    }

    /** Records one element's target name for one source table, refusing a blank name or a second claim. */
    private static void claim(String target, String table, SyncElement element, int index,
            Map<String, Map<String, String>> claimed) {
        String path = "serve.sync[" + index + "]" + (element.rename() == null ? "" : ".rename");
        if (target == null || target.isBlank()) {
            throw illegalValue(path, table);
        }
        String owner = element.id() != null ? element.id() : String.valueOf(index);
        String previous = claimed.computeIfAbsent(element.source(), k -> new LinkedHashMap<>())
                .putIfAbsent(target, owner);
        if (previous != null) {
            throw new DslException(DslError.COMPOSITION, path, 0, 0, null, Map.of("detail",
                    "sync '" + owner + "' writes target table '" + target + "' to '" + element.source()
                            + "', which sync '" + previous + "' already writes"));
        }
    }

    private static DslException illegalValue(String path, String value) {
        return new DslException(DslError.ILLEGAL_VALUE, path, 0, 0, null,
                Map.of("value", value, "expected", "a non-blank target table name"));
    }

    /**
     * The table names every source of this pipeline declares literally, or an empty set as soon as one
     * selector is dynamic — a regex or an omitted {@code tables}, either of which stands for a set only the
     * runtime knows.
     *
     * <p>A set, because the universe is one: a pipeline may list the same source twice, and one table
     * reached twice is still one table, not two colliding onto a name.
     */
    private static Set<String> declaredTables(PipelineResource pipeline, Map<String, SourceResource> sources) {
        Set<String> tables = new LinkedHashSet<>();
        for (String sourceId : pipeline.sourceIds()) {
            SourceResource source = sources.get(sourceId);
            if (source == null || source.tables() == null) {
                return Set.of();
            }
            for (TableRef ref : source.tables()) {
                switch (ref) {
                    case TableRef.Literal literal -> tables.add(literal.name());
                    case TableRef.Spec spec -> tables.add(spec.name());
                    case TableRef.Regex ignored -> {
                        return Set.of();
                    }
                }
            }
        }
        return tables;
    }
}
