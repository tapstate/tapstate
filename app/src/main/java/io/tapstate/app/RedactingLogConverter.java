package io.tapstate.app;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.pattern.CompositeConverter;
import io.tapstate.core.logging.SecretRedactor;

/** Applies the pipeline's redactor to operational messages and their complete exception text. */
public final class RedactingLogConverter extends CompositeConverter<ILoggingEvent> {

    private static final String REDACTOR_KEY = RedactingLogConverter.class.getName() + ".redactor";

    static SecretRedactor redactor(Context context) {
        synchronized (context) {
            Object registered = context.getObject(REDACTOR_KEY);
            if (registered instanceof SecretRedactor redactor) {
                return redactor;
            }
            SecretRedactor redactor = new SecretRedactor();
            context.putObject(REDACTOR_KEY, redactor);
            return redactor;
        }
    }

    @Override
    protected String transform(ILoggingEvent event, String text) {
        return redactor(getContext()).redactLog(text);
    }
}
