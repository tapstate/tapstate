package io.tapstate.app;

import io.tapstate.adapters.mongostore.StoreError;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.diagnostics.FailureAnalysis;

import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A coded startup failure is rendered through the shared catalog — the operator sees the same
 * message the CLI would show, not a stack trace.
 */
class CodedFailureAnalyzerTest {

    @Test
    void rendersACodedExceptionThroughTheCatalog() {
        TapstateException coded = new TapstateException(
                StoreError.UNREACHABLE, Map.of("target", "localhost:27017"), null);

        FailureAnalysis analysis = new CodedFailureAnalyzer().analyze(coded);

        assertThat(analysis).as("a coded startup failure produces an analysis").isNotNull();
        assertThat(analysis.getDescription()).isEqualTo("[store.unreachable] Cannot reach the store at localhost:27017.");
        assertThat(analysis.getAction())
                .isEqualTo("Check the store is running and the connection settings are correct, then restart.");
        assertThat(analysis.getCause()).isSameAs(coded);
    }

    @Test
    void earlyConfigurationFailuresRetainTheirCanonicalCodeWithoutDumpingArgumentsOrCause() {
        String sentinel = "startup-diagnostic-secret-sentinel";
        TapstateException coded = new TapstateException(BootError.CLOUD_CONFIG_INCOMPLETE,
                Map.of("unrendered", sentinel), new IllegalArgumentException(sentinel));
        FailureAnalysis analysis = new CodedFailureAnalyzer().analyze(coded);
        assertThat(analysis.getDescription()).startsWith("[boot.cloud-config-incomplete] ")
                .doesNotContain(sentinel, "unrendered");
        assertThat(analysis.getAction()).doesNotContain(sentinel);
    }

    @Test
    void programmerDefectsAreNotReclassifiedAsCodedStartupFailures() {
        assertThat(new CodedFailureAnalyzer().analyze(new IllegalStateException("programmer-defect"))).isNull();
    }

    @Test
    void isRegisteredSoSpringBootInvokesItOnStartupFailure() throws Exception {
        // Spring Boot discovers FailureAnalyzers through META-INF/spring.factories, not the
        // auto-configuration .imports mechanism; registering it the wrong way leaves startup failures
        // printing a raw stack trace. Assert the registration is present under the right key.
        String key = "org.springframework.boot.diagnostics.FailureAnalyzer";
        boolean registered = false;
        Enumeration<URL> resources = getClass().getClassLoader().getResources("META-INF/spring.factories");
        while (resources.hasMoreElements()) {
            Properties props = new Properties();
            try (InputStream in = resources.nextElement().openStream()) {
                props.load(in);
            }
            String value = props.getProperty(key);
            if (value != null && value.contains(CodedFailureAnalyzer.class.getName())) {
                registered = true;
                break;
            }
        }
        assertThat(registered).as("CodedFailureAnalyzer registered under the FailureAnalyzer key").isTrue();
    }
}
