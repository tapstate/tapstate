package io.tapstate.app;

import io.tapstate.core.alerting.AlarmDiagnostics;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

/** Captures trusted source failures. Source getters and message safety remain the host's responsibility. */
final class AlarmDiagnosticCapture {
    private static final int MAX_FRAMES = 256;
    private static final int MAX_FIELD = 1024;
    private static final Pattern URI = Pattern.compile("(?i)[a-z][a-z0-9+.-]{1,24}://[^\\s<>\\\"']+");
    private static final Pattern SECRET_FIELD = Pattern.compile("(?i)(password|passwd|secret|token|credential|authorization|api[-_]?key)\\s*[:=]\\s*(?:\\\"[^\\\"]*\\\"|'[^']*'|[^\\s,;}]+)");
    private AlarmDiagnosticCapture() { }

    /** Messages are omitted unless a source-specific policy supplies text it has classified as safe. */
    static Optional<AlarmDiagnostics.SafeTrace> capture(Throwable original, List<String> knownSecrets,
            Function<Throwable, Optional<String>> safeMessages) {
        if (original == null) { return Optional.empty(); }
        Objects.requireNonNull(knownSecrets); Objects.requireNonNull(safeMessages);
        if (knownSecrets.size() > 64 || knownSecrets.stream().anyMatch(s -> s == null || s.isEmpty() || s.length() > 4096)) {
            throw CloudAlertEventWire.invalid("diagnosticSecrets");
        }
        State state = new State(knownSecrets);
        try {
            state.visit(original, "", safeMessages);
            return Optional.of(new AlarmDiagnostics.SafeTrace(state.text.toString(), state.nodes, state.redacted, state.truncated));
        } catch (RuntimeException unavailable) {
            return Optional.empty();
        }
    }
    static Optional<AlarmDiagnostics.SafeTrace> capture(Throwable original, List<String> knownSecrets) {
        return capture(original, knownSecrets, ignored -> Optional.empty());
    }
    private static final class State {
        private final List<String> secrets;
        private final Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        private final StringBuilder text = new StringBuilder();
        private int nodes;
        private int frames;
        private int bytes;
        private int lines;
        private boolean redacted;
        private boolean truncated;
        State(List<String> secrets) { this.secrets = List.copyOf(secrets); }
        void visit(Throwable failure, String prefix, Function<Throwable, Optional<String>> messages) {
            if (failure == null || truncated) { return; }
            if (!seen.add(failure)) { append(prefix + "[circular exception reference]"); return; }
            if (nodes == AlarmDiagnostics.MAX_EXCEPTION_NODES) { truncated = true; return; }
            nodes++;
            String header = prefix + safe(failure.getClass().getName());
            Optional<String> message = Objects.requireNonNull(messages.apply(failure));
            if (message.isPresent()) { header += ": " + safe(message.get()); }
            else { redacted = true; }
            append(header);
            StackTraceElement[] stack = failure.getStackTrace();
            for (StackTraceElement element : stack) {
                if (frames == MAX_FRAMES || truncated) { truncated = true; break; }
                frames++;
                append("\tat " + safe(element.getClassName()) + "." + safe(element.getMethodName()) + "("
                        + (element.getFileName() == null ? "Unknown Source" : safe(element.getFileName()))
                        + (element.getLineNumber() >= 0 ? ":" + element.getLineNumber() : "") + ")");
            }
            if (!truncated) {
                for (Throwable suppressed : failure.getSuppressed()) {
                    visit(suppressed, "Suppressed: ", messages);
                    if (truncated) { break; }
                }
                visit(failure.getCause(), "Caused by: ", messages);
            }
        }
        String safe(String input) {
            String value = Objects.requireNonNull(input);
            if (value.length() > MAX_FIELD) { truncated = true; redacted = true; return "[redacted-overlong-field]"; }
            // A clipped secret prefix is also unsafe, so redact before selecting the bounded output.
            for (String secret : secrets) { String next = value.replace(secret, "[redacted]"); redacted |= !next.equals(value); value = next; }
            String next = URI.matcher(value).replaceAll("[redacted-uri]");
            redacted |= !next.equals(value); value = next;
            next = SECRET_FIELD.matcher(value).replaceAll("$1=[redacted]");
            redacted |= !next.equals(value); value = next;
            if (value.length() > MAX_FIELD) { value = value.substring(0, MAX_FIELD); truncated = true; }
            StringBuilder safe = new StringBuilder();
            for (int i = 0; i < value.length(); ) {
                int cp = value.codePointAt(i); i += Character.charCount(cp);
                if (Character.isISOControl(cp) || cp == 0x2028 || cp == 0x2029 || (cp >= 0x202a && cp <= 0x202e) || (cp >= 0x2066 && cp <= 0x2069)) {
                    safe.append(' '); redacted = true;
                } else { safe.appendCodePoint(cp); }
            }
            return safe.toString();
        }
        void append(String line) {
            if (lines == AlarmDiagnostics.MAX_LINES) { truncated = true; return; }
            String value = (text.isEmpty() ? "" : "\n") + line;
            for (int i = 0; i < value.length(); ) {
                int cp = value.codePointAt(i); int count = new String(Character.toChars(cp)).getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                if (bytes + count > AlarmDiagnostics.MAX_TEXT_BYTES) { truncated = true; return; }
                text.appendCodePoint(cp); bytes += count; i += Character.charCount(cp);
            }
            lines++;
        }
    }
}
