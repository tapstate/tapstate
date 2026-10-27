package io.tapstate.app;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.LoggingEvent;
import io.tapstate.core.logging.SecretRedactor;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RedactingLogConverterTest {

    @Test
    void preservesExceptionDetailsAndNewlineAfterACredentialFreeMongoUri() {
        LoggerContext context = new LoggerContext();
        try {
            PatternLayout layout = new PatternLayout();
            layout.setContext(context);
            layout.getInstanceConverterMap().put("redact", RedactingLogConverter::new);
            layout.setPattern("%redact(%m%n%ex){}%nopex");
            layout.start();
            LoggingEvent event = new LoggingEvent(getClass().getName(), context.getLogger("test.mongo"),
                    Level.ERROR, "Connection failed for mongodb://db.example/data",
                    new IllegalStateException("ops@example.com: connection refused",
                            new IllegalArgumentException("endpoint unavailable")), null);

            assertThat(layout.doLayout(event)).contains(
                    "Connection failed for mongodb://db.example/data",
                    "IllegalStateException: ops@example.com: connection refused",
                    "Caused by: java.lang.IllegalArgumentException: endpoint unavailable",
                    "\tat io.tapstate.app.RedactingLogConverterTest.")
                    .endsWith(System.lineSeparator());
        } finally {
            context.stop();
        }
    }

    @Test
    void redactorRegistrationDoesNotAcquireTheCallerOwnedContextMonitor() throws Exception {
        LoggerContext context = new LoggerContext();
        SecretRedactor registered = RedactingLogConverter.redactor(context);

        try (var executor = Executors.newSingleThreadExecutor()) {
            synchronized (context) {
                var lookup = executor.submit(() -> RedactingLogConverter.redactor(context));
                assertThat(lookup.get(5, TimeUnit.SECONDS)).isSameAs(registered);
            }
        }
        assertThat(RedactingLogConverter.redactor(new LoggerContext())).isNotSameAs(registered);
    }
}
