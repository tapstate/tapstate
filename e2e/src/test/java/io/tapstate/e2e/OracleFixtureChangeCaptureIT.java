package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Isolated;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.ImageNameSubstitutor;

import java.lang.reflect.Field;
import java.sql.DriverManager;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Exercises the unchanged source fixture while a real transaction delays supplemental logging. */
@Isolated("temporarily substitutes the Oracle image and isolates the shared fixture cache")
class OracleFixtureChangeCaptureIT {
    private static final String ORACLE_IMAGE = "gvenzl/oracle-free:23-slim-faststart";

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
                        RUN cp /tmp/tapstate-probe-sqlplus "$ORACLE_HOME/bin/sqlplus"
                        RUN chmod 755 "$ORACLE_HOME/bin/sqlplus"
                        USER oracle
                        """.formatted(ORACLE_IMAGE))
                .withFileFromString("sqlplus", """
                        #!/bin/bash
                        set -eu
                        input=$(mktemp)
                        cat > "$input"
                        if grep -q 'ALTER DATABASE ADD SUPPLEMENTAL LOG DATA' "$input"; then
                          awk '/ALTER DATABASE ADD SUPPLEMENTAL LOG DATA/ {
                            print "HOST bash /tmp/tapstate-hold-transaction.sh"
                          } { print }' "$input" > "$input.delayed"
                          input="$input.delayed"
                        fi
                        exec "$ORACLE_HOME/bin/sqlplus-probe-real" "$@" < "$input"
                        """)
                .withFileFromString("hold-transaction.sh", """
                        #!/bin/bash
                        set -eu
                        "$ORACLE_HOME/bin/sqlplus-probe-real" -s / as sysdba \
                          > /tmp/tapstate-transaction.log 2>&1 <<'SQL' &
                        WHENEVER SQLERROR EXIT FAILURE ROLLBACK
                        CREATE TABLE SYSTEM.TAPSTATE_LOGGING_HOLD (ID NUMBER);
                        INSERT INTO SYSTEM.TAPSTATE_LOGGING_HOLD VALUES (1);
                        HOST touch /tmp/tapstate-transaction-ready
                        BEGIN DBMS_SESSION.SLEEP(130); END;
                        /
                        ROLLBACK;
                        EXIT;
                        SQL
                        holder=$!
                        for attempt in {1..30}; do
                          if [ -f /tmp/tapstate-transaction-ready ]; then
                            printf 'capture-probe: real transaction active; bounded rollback in 130 seconds\\n'
                            exit 0
                          fi
                          if ! kill -0 "$holder" 2>/dev/null; then
                            cat /tmp/tapstate-transaction.log
                            exit 1
                          fi
                          sleep 1
                        done
                        cat /tmp/tapstate-transaction.log
                        exit 1
                        """)
                .get();

        // Image substitution keeps SharedOracle's initialization and failure handling under test.
        Field substitutor = accessibleField(ImageNameSubstitutor.class, "instance");
        Field sharedServer = accessibleField(SharedOracle.class, "container");
        OracleContainer previousServer = (OracleContainer) sharedServer.get(null);
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
                    ownedServer.stop();
                }
            } finally {
                sharedServer.set(null, previousServer);
                substitutor.set(null, originalSubstitutor);
            }
        }
    }

    private static Field accessibleField(Class<?> owner, String name) throws NoSuchFieldException {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }
}
