package io.tapstate.app;

import ch.qos.logback.classic.LoggerContext;
import io.tapstate.core.logging.SecretRedactor;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RedactingLogConverterTest {

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
