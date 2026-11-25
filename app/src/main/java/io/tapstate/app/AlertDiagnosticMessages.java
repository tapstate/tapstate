package io.tapstate.app;

import io.tapstate.core.alerting.AlarmDiagnostics;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.logging.SecretRedactor;
import io.tapstate.messages.MessageCatalog;
import io.tapstate.spi.store.SourceBindingSnapshot;
import java.util.*;

/** Bounded messages from original source settings or authoritative coded observations. */
final class AlertDiagnosticMessages {
    private static final MessageCatalog MESSAGES = MessageCatalog.bundled();
    private final SecretRedactor redactor;
    AlertDiagnosticMessages(SecretRedactor redactor) { this.redactor = Objects.requireNonNull(redactor); }

    record SourceRedaction(List<String> values,boolean complete) {
        SourceRedaction { values=List.copyOf(values); }
    }
    SourceRedaction snapshot(SourceBindingSnapshot original) {
        var secrets = new ArrayList<String>();
        boolean complete = collect(original.source().config(), secrets, new int[1], 0);
        return new SourceRedaction(secrets,complete);
    }
    Optional<AlarmDiagnostics.SafeTrace> source(SourceRedaction original, Throwable failure) {
        var captured = AlarmDiagnosticCapture.capture(failure, original.values(),
                cause -> original.complete() ? message(cause) : Optional.empty());
        return captured.map(this::redact);
    }
    Optional<AlarmDiagnostics.SafeTrace> pipeline(Throwable cause, ObservationFailure reported) {
        var summary=reported==null?Optional.<AlarmDiagnostics.SafeTrace>empty():rendered(reported.code(),reported.params())
                .map(AlarmDiagnosticCapture::reported).map(this::redact);
        if (cause != null) {
            var captured = AlarmDiagnosticCapture.capture(cause, List.of(), failure -> failure instanceof TapstateException coded
                    ? Optional.of(coded.code().code()) : Optional.empty());
            if (captured.isPresent()) {
                var trace=captured.orElseThrow();
                if(summary.isPresent()) {
                    var message=summary.orElseThrow();
                    trace=new AlarmDiagnostics.SafeTrace("Reported pipeline failure: "+message.text()+"\n"+trace.text(),
                            trace.exceptionNodes(),trace.redacted()||message.redacted(),trace.truncated()||message.truncated());
                }
                return Optional.of(redact(trace));
            }
        }
        return summary.map(message -> {
            // This is the published coded failure, never a reconstructed Throwable or fabricated stack.
            return new AlarmDiagnostics.SafeTrace("Reported pipeline failure: "+message.text(),1,message.redacted(),message.truncated());
        });
    }
    private Optional<String> message(Throwable cause) {
        if (cause instanceof TapstateException coded) return rendered(coded.code().code(), coded.args());
        boolean platformNetwork = cause.getClass().getClassLoader() == null
                && (cause instanceof java.net.SocketException || cause instanceof java.net.SocketTimeoutException
                    || cause instanceof java.net.UnknownHostException || cause instanceof javax.net.ssl.SSLException);
        boolean mongoNetwork=List.of("com.mongodb.MongoSocketReadException","com.mongodb.MongoSocketOpenException",
                "com.mongodb.MongoSocketException").contains(cause.getClass().getName());
        if (!platformNetwork && !mongoNetwork) return Optional.empty();
        String text = cause.getMessage();
        if(text==null)return Optional.empty();
        if(mongoNetwork&&!Set.of("Prematurely reached end of stream","Exception receiving message","Exception opening socket").contains(text))return Optional.empty();
        return text.length() > 1024 ? Optional.empty() : Optional.of(text);
    }
    private Optional<String> rendered(String code, Map<String, ?> arguments) {
        if (code==null||code.length()>128||arguments==null||arguments.size() > 64) return Optional.empty();
        var safe = new LinkedHashMap<String, Object>();
        for (var entry : arguments.entrySet()) {
            if(entry.getKey()==null||entry.getKey().length()>128)return Optional.empty();
            Object value = entry.getValue();
            if (!(value instanceof String || value instanceof Number || value instanceof Boolean)) return Optional.empty();
            String text = String.valueOf(value);
            if(text.length()>1024||text.endsWith(" …"))return Optional.empty();
            safe.put(entry.getKey(), text);
        }
        String text = MESSAGES.render(code, safe).message();
        return text.equals(code) || text.length() > 1024 ? Optional.empty() : Optional.of(text);
    }
    private AlarmDiagnostics.SafeTrace redact(AlarmDiagnostics.SafeTrace trace) {
        String safe = redactor.redactLog(trace.text());
        return new AlarmDiagnostics.SafeTrace(safe, trace.exceptionNodes(), trace.redacted() || !safe.equals(trace.text()), trace.truncated());
    }
    private static boolean collect(Object value, List<String> secrets, int[] count, int depth) {
        if (++count[0] > 256 || depth > 8) return false;
        if (value instanceof Map<?, ?> map) {
            for (Object child : map.values()) if (!collect(child, secrets, count, depth + 1)) return false;
        } else if (value instanceof Iterable<?> list) {
            for (Object child : list) if (!collect(child, secrets, count, depth + 1)) return false;
        } else if (value instanceof String text && !text.isEmpty()) {
            if (text.length() > 4096 || secrets.size() == 64) return false;
            secrets.add(text);
            for(String credential:io.tapstate.core.logging.MongoUriUserInfo.secretValues(text)) {
                if(secrets.size()==64)return false;
                if(!credential.isEmpty()&&!secrets.contains(credential))secrets.add(credential);
            }
        } else if (value != null && !(value instanceof Number || value instanceof Boolean)) return false;
        return true;
    }
}
