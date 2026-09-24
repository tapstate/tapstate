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
        if (pipeline.transforms() == null && pipeline.view() == null && pipeline.serve() == null) {
            return declared;
        }
        Set<String> referenced = new LinkedHashSet<>();
        if (pipeline.transforms() != null) {
            for (Step step : pipeline.transforms()) {
                collect(step.from(), source.id(), declared, referenced);
            }
        }
        if (pipeline.view() instanceof ViewBlock.Inline view) {
            collect(view.from(), source.id(), declared, referenced);
        } else if (pipeline.view() instanceof ViewBlock.Use view) {
            collect(view.from(), source.id(), declared, referenced);
        }
        if (pipeline.serve() instanceof ServeBlock.Inline serve) {
            collect(serve.from(), source.id(), declared, referenced);
        } else if (pipeline.serve() instanceof ServeBlock.Use serve) {
            collect(serve.from(), source.id(), declared, referenced);
        }
        return declared.stream().filter(referenced::contains).toList();
    }

    private static void collect(FromClause from, String sourceId, List<String> declared, Set<String> referenced) {
        if (from instanceof FromClause.Flow flow) {
            flow.refs().forEach(ref -> collect(ref, sourceId, declared, referenced));
        } else if (from instanceof FromClause.Aliases aliases) {
            aliases.aliases().values().forEach(ref -> collect(ref, sourceId, declared, referenced));
        }
    }

    private static void collect(FromRef from, String sourceId, List<String> declared, Set<String> referenced) {
        if (from instanceof FromRef.Literal literal) {
            String token = literal.ref();
            if (token.equals(sourceId)) {
                referenced.addAll(declared);
            } else {
                declared.stream().filter(table -> token.equals(table) || token.equals(sourceId + "." + table))
                        .forEach(referenced::add);
                if (token.startsWith(sourceId + ".") && !referenced.contains(token.substring(sourceId.length() + 1))) {
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
