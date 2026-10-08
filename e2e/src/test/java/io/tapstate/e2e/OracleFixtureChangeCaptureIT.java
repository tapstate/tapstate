package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Isolated;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.ImageNameSubstitutor;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises the unchanged source fixture while a real transaction delays supplemental logging. */
@Isolated("temporarily substitutes the Oracle image and isolates the shared fixture cache")
class OracleFixtureChangeCaptureIT {
    private static final String ORACLE_IMAGE = "gvenzl/oracle-free:23-slim-faststart";
    private static final String PRIOR_CONTEXT = "tapstate.e2e.oracle-fixture-probe.prior-normal";
    private static OracleContainer initialContextCache;
    private static OracleContainer contextOwnedPrior;
    private static boolean ownsPriorContext;

    @BeforeAll
    static void retainNormalOracleForTheNamedContextControl() throws Exception {
        if (!Boolean.getBoolean(PRIOR_CONTEXT)) { return; }
        DockerGate.require();
        Field sharedServer = accessibleField(SharedOracle.class, "container");
        initialContextCache = (OracleContainer) sharedServer.get(null);
        if (initialContextCache != null) {
            throw new AssertionError("the named two-Oracle control requires an empty initial fixture cache");
        }
        ownsPriorContext = true;
        try {
            System.out.printf("oracle-fixture-prior-context stage=before-create utc=%s jvmPid=%d%n",
                    java.time.Instant.now(), ProcessHandle.current().pid());
            SharedOracle.settings("logging_wait_prior_context");
            contextOwnedPrior = (OracleContainer) sharedServer.get(null);
            if (contextOwnedPrior == null || !contextOwnedPrior.isRunning()) {
                throw new AssertionError("the named two-Oracle control did not create its running prior fixture");
            }
            System.out.printf("oracle-fixture-prior-context stage=ready-before-test utc=%s id=%s image=%s jvmPid=%d%n",
                    java.time.Instant.now(), contextOwnedPrior.getContainerId(),
                    contextOwnedPrior.getDockerImageName(), ProcessHandle.current().pid());
        } catch (Exception | Error failure) {
            try { stopOnlyTheContextOwnedPrior(); }
            catch (Exception | Error cleanupFailure) { failure.addSuppressed(cleanupFailure); }
            throw failure;
        }
    }

    @AfterAll
    static void stopOnlyTheContextOwnedPrior() throws Exception {
        if (!ownsPriorContext) { return; }
        Field sharedServer = accessibleField(SharedOracle.class, "container");
        // Schema provisioning can fail after the normal server has been cached.
        OracleContainer cleanupPrior = contextOwnedPrior != null
                ? contextOwnedPrior : (OracleContainer) sharedServer.get(null);
        try {
            if (cleanupPrior != null && cleanupPrior != initialContextCache) {
                String priorId = cleanupPrior.getContainerId();
                System.out.printf("oracle-fixture-prior-context stage=before-owned-cleanup utc=%s id=%s%n",
                        java.time.Instant.now(), priorId);
                cleanupPrior.stop();
                System.out.printf("oracle-fixture-prior-context stage=owned-cleanup-returned utc=%s id=%s%n",
                        java.time.Instant.now(), priorId);
            }
        } finally {
            sharedServer.set(null, initialContextCache);
            contextOwnedPrior = null;
            ownsPriorContext = false;
        }
    }

    @Test
    @Timeout(value = 6, unit = TimeUnit.MINUTES)
    void anInflightTransactionDoesNotAbortChangeCaptureSetup() throws Exception {
        DockerGate.require();
        ImageNameSubstitutor originalSubstitutor = ImageNameSubstitutor.instance();
        String delayedImage = new ImageFromDockerfile()
                .withFileFromString("Dockerfile", """
                        FROM %s
                        USER root
                        RUN mv "$ORACLE_HOME/bin/sqlplus" "$ORACLE_HOME/bin/sqlplus-probe-real"
                        COPY sqlplus /tmp/tapstate-probe-sqlplus
                        COPY hold-transaction.sh /tmp/tapstate-hold-transaction.sh
                        COPY probe-stage.sh /tmp/tapstate-probe-stage.sh
                        COPY probe-waits.sh /tmp/tapstate-probe-waits.sh
                        COPY collect-probe.sh /tmp/tapstate-collect-probe.sh
                        RUN cp /tmp/tapstate-probe-sqlplus "$ORACLE_HOME/bin/sqlplus"
                        RUN chmod 755 "$ORACLE_HOME/bin/sqlplus"
                        USER oracle
                        """.formatted(ORACLE_IMAGE))
                .withFileFromString("sqlplus", probeScript("sqlplus"))
                .withFileFromString("hold-transaction.sh", probeScript("hold-transaction.sh"))
                .withFileFromString("probe-stage.sh", probeScript("probe-stage.sh"))
                .withFileFromString("probe-waits.sh", probeScript("probe-waits.sh"))
                .withFileFromString("collect-probe.sh", probeScript("collect-probe.sh"))
                .get();

        // Image substitution keeps SharedOracle's initialization and failure handling under test.
        Field substitutor = accessibleField(ImageNameSubstitutor.class, "instance");
        Field sharedServer = accessibleField(SharedOracle.class, "container");
        OracleContainer previousServer = (OracleContainer) sharedServer.get(null);
        if (ownsPriorContext) {
            if (previousServer != contextOwnedPrior) {
                throw new AssertionError("the named context prior was not retained before controlled initialization");
            }
            System.out.printf("oracle-fixture-prior-context stage=retained-before-controlled utc=%s id=%s jvmPid=%d%n",
                    java.time.Instant.now(), previousServer.getContainerId(), ProcessHandle.current().pid());
        }
        substitutor.set(null, new ImageNameSubstitutor() {
            @Override
            public DockerImageName apply(DockerImageName original) {
                return original.asCanonicalNameString().equals(ORACLE_IMAGE)
                        ? DockerImageName.parse(delayedImage).asCompatibleSubstituteFor(original)
                        : originalSubstitutor.apply(original);
            }

            @Override
            protected String getDescription() {
                return "a test-owned Oracle server with an in-flight logging transaction";
            }
        });
        sharedServer.set(null, null);
        try {
            var settings = SharedOracle.settings("logging_wait_probe");
            try (var connection = DriverManager.getConnection(
                    "jdbc:oracle:thin:@//" + settings.get("host") + ":" + settings.get("port") + "/FREE",
                    settings.get("user").toString(), settings.get("password").toString());
                 var statement = connection.createStatement();
                 var logging = statement.executeQuery(
                         "SELECT LOG_MODE, SUPPLEMENTAL_LOG_DATA_MIN, FORCE_LOGGING FROM V$DATABASE")) {
                assertThat(logging.next()).isTrue();
                assertThat(logging.getString(1)).isEqualTo("ARCHIVELOG");
                assertThat(logging.getString(2)).isEqualTo("YES");
                assertThat(logging.getString(3)).isEqualTo("YES");
            }
        } finally {
            try {
                OracleContainer ownedServer = (OracleContainer) sharedServer.get(null);
                if (ownedServer != null && ownedServer != previousServer) {
                    boolean diagnosticInterrupted = false;
                    try { diagnosticInterrupted = SharedOracle.captureProbeDiagnostics(ownedServer); }
                    finally { ownedServer.stop(); }
                    if (diagnosticInterrupted) { Thread.currentThread().interrupt(); }
                }
            } finally {
                sharedServer.set(null, previousServer);
                substitutor.set(null, originalSubstitutor);
            }
        }
    }

    private static String probeScript(String name) throws IOException {
        try (var input = OracleFixtureChangeCaptureIT.class.getResourceAsStream(
                "/oracle-fixture-probe/" + name)) {
            if (input == null) { throw new AssertionError("controlled Oracle probe script is absent: " + name); }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static Field accessibleField(Class<?> owner, String name) throws NoSuchFieldException {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }
}
