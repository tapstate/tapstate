package io.tapstate.control.restapi;

import io.tapstate.control.core.NestExplanation;
import io.tapstate.core.lifecycle.NestTree;
import io.tapstate.messages.MessageCatalog;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A nest explanation on the wire: the tree, the build line, the samples, and each warning with its code,
 * its arguments by name and its text rendered from the catalog, so a reader needs no catalog of its own.
 */
record NestExplainResponse(NestTree tree, String build, List<NestExplanation.Sampled> samples,
        List<Warning> warnings) {

    record Warning(String code, String embed, Map<String, Object> args, String message, String solution) {
    }

    static NestExplainResponse of(NestExplanation explanation, MessageCatalog catalog) {
        List<Warning> warnings = explanation.warnings().stream().map(warning -> {
            MessageCatalog.Rendered rendered = catalog.render(warning.code(), warning.args());
            return new Warning(warning.code().code(), warning.embed(), new TreeMap<>(warning.args()),
                    rendered.message(), rendered.solution());
        }).toList();
        return new NestExplainResponse(explanation.tree(), explanation.build(), explanation.samples(), warnings);
    }
}
