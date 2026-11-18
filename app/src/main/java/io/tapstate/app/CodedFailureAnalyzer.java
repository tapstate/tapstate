package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.messages.MessageCatalog;
import org.springframework.boot.diagnostics.AbstractFailureAnalyzer;
import org.springframework.boot.diagnostics.FailureAnalysis;

/**
 * Turns a coded {@link TapstateException} that aborts startup (e.g. the store is unreachable) into a
 * clean, operator-facing diagnostic rendered through the shared message catalog — the same rendering
 * the CLI uses — instead of a raw stack trace. Programmer errors are not coded, so they keep their
 * stack trace and are unaffected.
 */
public class CodedFailureAnalyzer extends AbstractFailureAnalyzer<TapstateException> {

    @Override
    protected FailureAnalysis analyze(Throwable rootFailure, TapstateException cause) {
        // Render only declared text arguments; retain the complete exception as the diagnostic cause.
        var textArguments = new java.util.LinkedHashMap<String, Object>();
        for (String name : cause.code().placeholders()) {
            if (cause.args().containsKey(name)) textArguments.put(name, cause.args().get(name));
        }
        MessageCatalog.Rendered rendered = MessageCatalog.bundled().render(cause.code(), textArguments);
        // Early environment failures may never produce an exception stack. Keep their stable code
        // visible alongside the catalog text, without dumping parameters or the cause chain.
        return new FailureAnalysis("[" + cause.code().code() + "] " + rendered.message(), rendered.solution(), cause);
    }
}
