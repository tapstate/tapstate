package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.spi.store.SourceModel;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Selects the declared source tables addressed by a pipeline's input graph. */
final class PipelineTableSelection {

    private PipelineTableSelection() {}

    static List<String> resolve(PipelineResource pipeline, SourceResource source, SourceModel discovered) {
        List<String> declared = SourceTableSelection.resolve(source, discovered);
        return referencedTables(pipeline, source.id(), declared, true);
    }

    /** Whether the graph could select this table after discovery, without pretending other tables are absent. */
    static boolean references(PipelineResource pipeline, String sourceId, String table) {
        return !referencedTables(pipeline, sourceId, List.of(table), false).isEmpty();
    }

    /** Filters a known subset without rejecting qualified references that may belong to unresolved selectors. */
    static List<String> forOutputNames(PipelineResource pipeline, SourceResource source, List<String> known) {
        return referencedTables(pipeline, source.id(), known, false);
    }

    private static List<String> referencedTables(PipelineResource pipeline, String sourceId,
            List<String> declared, boolean requireSelected) {
        if (pipeline.transforms() == null && pipeline.view() == null && pipeline.serve() == null) {
            return declared;
        }
        Set<String> referenced = new LinkedHashSet<>();
        if (pipeline.transforms() != null) {
            for (Step step : pipeline.transforms()) {
                collect(step.from(), sourceId, declared, referenced, requireSelected);
            }
        }
        if (pipeline.view() instanceof ViewBlock.Inline view) {
            collect(view.from(), sourceId, declared, referenced, requireSelected);
        } else if (pipeline.view() instanceof ViewBlock.Use view) {
            collect(view.from(), sourceId, declared, referenced, requireSelected);
        }
        if (pipeline.serve() instanceof ServeBlock.Inline serve) {
            collect(serve.from(), sourceId, declared, referenced, requireSelected);
        } else if (pipeline.serve() instanceof ServeBlock.Use serve) {
            collect(serve.from(), sourceId, declared, referenced, requireSelected);
        }
        return declared.stream().filter(referenced::contains).toList();
    }

    private static void collect(FromClause from, String sourceId, List<String> declared, Set<String> referenced,
            boolean requireSelected) {
        if (from instanceof FromClause.Flow flow) {
            flow.refs().forEach(ref -> collect(ref, sourceId, declared, referenced, requireSelected));
        } else if (from instanceof FromClause.Aliases aliases) {
            aliases.aliases().values().forEach(ref -> collect(ref, sourceId, declared, referenced, requireSelected));
        }
    }

    private static void collect(FromRef from, String sourceId, List<String> declared, Set<String> referenced,
            boolean requireSelected) {
        if (from instanceof FromRef.Literal literal) {
            String token = literal.ref();
            if (token.equals(sourceId)) {
                referenced.addAll(declared);
            } else {
                declared.stream().filter(table -> token.equals(table) || token.equals(sourceId + "." + table))
                        .forEach(referenced::add);
                if (requireSelected && token.startsWith(sourceId + ".")
                        && !referenced.contains(token.substring(sourceId.length() + 1))) {
                    throw new TapstateException(ActuationError.SOURCE_TABLE_NOT_DISCOVERED,
                            Map.of("source", sourceId, "table", token.substring(sourceId.length() + 1)), null);
                }
            }
            return;
        }
        FromRef.Regex regex = (FromRef.Regex) from;
        final Pattern pattern;
        try {
            pattern = Pattern.compile(regex.pattern());
        } catch (PatternSyntaxException exception) {
            throw new TapstateException(
                    ActuationError.FROM_REGEX_INVALID, Map.of("regex", regex.pattern()), exception);
        }
        if (pattern.matcher(sourceId).matches()) {
            referenced.addAll(declared);
        } else {
            declared.stream().filter(table -> pattern.matcher(table).matches()
                    || pattern.matcher(sourceId + "." + table).matches()).forEach(referenced::add);
        }
    }
}
