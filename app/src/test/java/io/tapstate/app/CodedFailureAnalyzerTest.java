package io.tapstate.app;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.tapstate.adapters.mongostore.StoreError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.boot.diagnostics.FailureAnalysis;

import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A coded startup failure is rendered through the shared catalog — the operator sees the same
 * message the CLI would show, not a stack trace.
 */
class CodedFailureAnalyzerTest {

    private final ListAppender<ILoggingEvent> written = new ListAppender<>() {
        @Override
        protected void append(ILoggingEvent event) {
            event.prepareForDeferredProcessing();
            super.append(event);
        }
    };
    private Logger logger;
    private Level previousLevel;

    @BeforeEach
    void captureStartupDiagnostics() {
        logger = (Logger) LoggerFactory.getLogger(CodedFailureAnalyzer.class);
        previousLevel = logger.getLevel();
        logger.setLevel(Level.WARN);
        written.start();
        logger.addAppender(written);
    }

    @AfterEach
    void restoreLogging() {
        logger.detachAppender(written);
        logger.setLevel(previousLevel);
        written.stop();
    }

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
    @Test
    void storeCommandFailuresExposeProductCallersWithoutLoggingArgumentsOrCauses() {
        String secret = "startup-observation-secret-sentinel";
        TapstateException coded = commandFailure(secret);
        RuntimeException outer = new RuntimeException("mongodb://user:" + secret + "@db.invalid/test", coded);

        FailureAnalysis analysis = new CodedFailureAnalyzer().analyze(outer);

        assertThat(analysis.getDescription()).isEqualTo(
                "[io.store-unavailable] A store operation could not complete: MongoCommandException code=112.");
        assertThat(analysis.getAction()).isEqualTo("Check the store is reachable and healthy, then retry.");
        assertThat(analysis.getCause()).isSameAs(coded);
        assertThat(analysis.getDescription()).doesNotContain(secret, "unrendered");
        assertThat(written.list).singleElement().satisfies(event -> {
            assertThat(event.getLevel()).isEqualTo(Level.WARN);
            assertThat(event.getFormattedMessage()).contains("code=io.store-unavailable", "driverCode=112",
                    "io.tapstate.app.CodedFailureAnalyzerTest#commandFailure:");
            assertThat(event.getFormattedMessage()).doesNotContain(secret, "mongodb://", "unrendered",
                    "CodedFailureAnalyzerTest.java");
            assertThat(event.getArgumentArray()).hasSize(3);
            assertThat(event.getThrowableProxy()).isNull();
        });
        written.list.clear();
        TapstateException anotherCommand = new TapstateException(IoError.STORE_UNAVAILABLE,
                Map.of("detail", "MongoCommandException code=26"), null);
        assertThat(new CodedFailureAnalyzer().analyze(anotherCommand).getCause()).isSameAs(anotherCommand);
        assertThat(written.list).singleElement().satisfies(event -> {
            assertThat(event.getFormattedMessage()).contains("code=io.store-unavailable", "driverCode=26");
            assertThat(event.getThrowableProxy()).isNull();
        });
    }

    @Test
    void deeplyNestedActualFailuresExposeAtMostEightProductLocations() {
        TapstateException coded = nestedCommandFailure(12);

        assertThat(new CodedFailureAnalyzer().analyze(coded).getCause()).isSameAs(coded);

        assertThat(written.list).singleElement().satisfies(event -> {
            List<?> locations = (List<?>) event.getArgumentArray()[2];
            assertThat(locations).hasSize(8).allSatisfy(location -> {
                assertThat(location.toString()).startsWith(
                        "io.tapstate.app.CodedFailureAnalyzerTest#nestedCommandFailure:");
            });
            assertThat(event.getFormattedMessage().length()).isLessThan(3000);
            assertThat(event.getThrowableProxy()).isNull();
        });
    }

    @Test
    void callerMetadataCannotEchoFileNamesOrMalformedAndUnboundedNames() {
        TapstateException coded = commandFailure("driver-cause-secret");
        coded.setStackTrace(new StackTraceElement[]{
                new StackTraceElement("io.tapstate.app.CodedFailureAnalyzerTest", "safeCaller",
                        "mongodb://user:file-name-secret@db.invalid/test", 42),
                new StackTraceElement("io.tapstate.app.Bad\nclass-secret", "call", "ignored", 43),
                new StackTraceElement("io.tapstate.app.Bad", "call\nmethod-secret", "ignored", 44),
                new StackTraceElement("io.tapstate." + "A".repeat(193), "call", "ignored", 45),
                new StackTraceElement("io.tapstate.app.Bad", "A".repeat(129), "ignored", 46),
                new StackTraceElement("com.mongodb.Driver", "call", "external-file-secret", 47)
        });

        new CodedFailureAnalyzer().analyze(coded);

        assertThat(written.list).singleElement().satisfies(event -> {
            assertThat(((List<?>) event.getArgumentArray()[2]).stream().map(Object::toString).toList()).containsExactly(
                    "io.tapstate.app.CodedFailureAnalyzerTest#safeCaller:42");
            assertThat(event.getFormattedMessage()).doesNotContain("secret", "mongodb://", "com.mongodb",
                    "\n", "A".repeat(129));
            assertThat(event.getThrowableProxy()).isNull();
        });
    }

    @Test
    void aProductCallerOutsideTheInspectionBudgetRemainsExplicitlyUnavailable() {
        TapstateException coded = commandFailure("not-logged");
        StackTraceElement[] stack = new StackTraceElement[65];
        Arrays.fill(stack, new StackTraceElement("com.mongodb.Driver", "call", "not-logged", 1));
        stack[64] = new StackTraceElement("io.tapstate.app.CodedFailureAnalyzerTest", "outsideBudget",
                "not-logged", 65);
        coded.setStackTrace(stack);

        new CodedFailureAnalyzer().analyze(coded);

        assertThat(written.list).singleElement().satisfies(event -> {
            assertThat((List<?>) event.getArgumentArray()[2]).isEmpty();
            assertThat(event.getFormattedMessage()).contains("driverCode=112", "callsites=[]")
                    .doesNotContain("outsideBudget", "com.mongodb", "not-logged");
        });
    }

    @Test
    void otherErrorsAndUnrecognizedCommandMetadataKeepTheirExistingAnalysisWithoutExtraLogs() {
        List<TapstateException> otherErrors = List.of(
                new TapstateException(BootError.CLOUD_CONFIG_INCOMPLETE, Map.of("detail", "MongoCommandException code=112"), null),
                new TapstateException(StoreError.UNREACHABLE, Map.of("target", "localhost:27017"), null),
                new TapstateException(IoError.STORE_UNAUTHORIZED, Map.of(), null));
        for (TapstateException coded : otherErrors) {
            assertThat(new CodedFailureAnalyzer().analyze(coded).getCause()).isSameAs(coded);
        }
        for (String detail : List.of("MongoInterruptedException", "MongoCommandException code=0",
                "MongoCommandException code=-1", "MongoCommandException code=2147483648",
                "MongoCommandException code=112\ncredential-secret",
                "MongoCommandException code=112 mongodb://credential-secret@db.invalid/test")) {
            TapstateException coded = new TapstateException(IoError.STORE_UNAVAILABLE,
                    Map.of("detail", detail), null);
            assertThat(new CodedFailureAnalyzer().analyze(coded).getCause()).isSameAs(coded);
        }
        assertThat(new CodedFailureAnalyzer().analyze(new IllegalStateException("programmer-defect"))).isNull();
        assertThat(written.list).isEmpty();
    }

    @Test
    void optionalLoggingFailureCannotReplaceTheOriginalCodedAnalysis() {
        ListAppender<ILoggingEvent> unavailable = new ListAppender<>() {
            @Override
            public void doAppend(ILoggingEvent event) {
                throw new IllegalStateException("diagnostic-appender-unavailable");
            }
        };
        unavailable.start();
        logger.addAppender(unavailable);
        TapstateException coded = commandFailure("not-logged");
        try {
            FailureAnalysis analysis = new CodedFailureAnalyzer().analyze(coded);
            assertThat(analysis.getCause()).isSameAs(coded);
            assertThat(analysis.getDescription()).isEqualTo(
                    "[io.store-unavailable] A store operation could not complete: MongoCommandException code=112.");
            assertThat(analysis.getAction()).isEqualTo("Check the store is reachable and healthy, then retry.");
        } finally {
            logger.detachAppender(unavailable);
            unavailable.stop();
        }
    }

    private static TapstateException commandFailure(String secret) {
        return new TapstateException(IoError.STORE_UNAVAILABLE,
                Map.of("detail", "MongoCommandException code=112", "unrendered", secret),
                new IllegalArgumentException("mongodb://user:" + secret + "@db.invalid/test"));
    }

    private static TapstateException nestedCommandFailure(int remaining) {
        if (remaining > 0) return nestedCommandFailure(remaining - 1);
        return new TapstateException(IoError.STORE_UNAVAILABLE,
                Map.of("detail", "MongoCommandException code=112"), null);
    }
}
