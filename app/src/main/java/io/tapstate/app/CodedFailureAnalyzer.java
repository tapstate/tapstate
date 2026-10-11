package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.messages.MessageCatalog;
import io.tapstate.spi.store.IoError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.diagnostics.AbstractFailureAnalyzer;
import org.springframework.boot.diagnostics.FailureAnalysis;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Turns a coded {@link TapstateException} that aborts startup (e.g. the store is unreachable) into a
 * clean, operator-facing diagnostic rendered through the shared message catalog — the same rendering
 * the CLI uses — instead of a raw stack trace. Programmer errors are not coded, so they keep their
 * stack trace and are unaffected.
 */
public class CodedFailureAnalyzer extends AbstractFailureAnalyzer<TapstateException> {

    private static final Logger LOG = LoggerFactory.getLogger(CodedFailureAnalyzer.class);
    private static final int CALLSITE_LIMIT = 8;
    private static final int INSPECTED_FRAME_LIMIT = 64;
    private static final Pattern COMMAND_DETAIL =
            Pattern.compile("MongoCommandException code=([1-9][0-9]{0,9})");
    private static final Pattern PRODUCT_CLASS =
            Pattern.compile("io\\.tapstate(?:\\.[A-Za-z_$][A-Za-z0-9_$]*)+");
    private static final Pattern METHOD =
            Pattern.compile("(?:[A-Za-z_$][A-Za-z0-9_$]*|<init>|<clinit>)");

    @Override
    protected FailureAnalysis analyze(Throwable rootFailure, TapstateException cause) {
        MessageCatalog.Rendered rendered = MessageCatalog.bundled().render(cause.code(), cause.args());
        logStoreCommandCallsites(cause);
        // Early environment failures may never produce an exception stack. Keep their stable code
        // visible alongside the catalog text, without dumping parameters or the cause chain.
        return new FailureAnalysis("[" + cause.code().code() + "] " + rendered.message(), rendered.solution(), cause);
    }

    private static void logStoreCommandCallsites(TapstateException cause) {
        if (cause.code() != IoError.STORE_UNAVAILABLE) return;
        Integer commandCode = commandCode(cause.args().get("detail"));
        if (commandCode == null) return;
        try {
            List<String> callsites = new ArrayList<>(CALLSITE_LIMIT);
            StackTraceElement[] stack = cause.getStackTrace();
            for (int index = 0; index < Math.min(stack.length, INSPECTED_FRAME_LIMIT)
                    && callsites.size() < CALLSITE_LIMIT; index++) {
                StackTraceElement frame = stack[index];
                String className = frame.getClassName();
                String methodName = frame.getMethodName();
                if (className.length() <= 192 && methodName.length() <= 128
                        && PRODUCT_CLASS.matcher(className).matches()
                        && METHOD.matcher(methodName).matches()) {
                    callsites.add(className + "#" + methodName + ":" + frame.getLineNumber());
                }
            }
            // Only bounded product locations and the numeric command code enter this extra diagnostic.
            // No argument maps, free-form detail, file names or cause chains enter this extra log.
            LOG.warn("Startup store command failed [code={}, driverCode={}, callsites={}]",
                    cause.code().code(), commandCode, List.copyOf(callsites));
        } catch (RuntimeException diagnosticUnavailable) {
            // Optional logging must not replace the original coded startup analysis.
        }
    }

    private static Integer commandCode(Object detail) {
        if (!(detail instanceof String text) || text.length() > 37) return null;
        var matched = COMMAND_DETAIL.matcher(text);
        if (!matched.matches()) return null;
        try {
            return Integer.valueOf(matched.group(1));
        } catch (NumberFormatException outsideDriverCodeRange) {
            return null;
        }
    }
}
