package io.tapstate.e2e;

import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Observes one actual survivor branch after an owned submitted-but-unbound controller crash. */
@RequiresDocker
@EnabledIfSystemProperty(named = "tapstate.e2e.claimed-submit-pre-bind-diagnostic.jar", matches = ".+")
class RealClaimedSubmittedHandoffBranchDiagnosticIT {
    private static final String PREFIX = "tapstate.e2e.claimed-submit-pre-bind-diagnostic.";

    @BeforeAll
    static void requireInputs() {
        RealConnectorGate.require("mysql", "mongodb");
        assertThat(Path.of(required("jar"))).isRegularFile();
        assertThat(Path.of(required("output")).isAbsolute()).isTrue();
        assertThat(required("sha256")).matches("[0-9a-f]{64}");
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.MINUTES)
    void anOwnedSubmittedButUnboundCrashRevealsItsFirstActualSurvivorBranch() throws Exception {
        RealClaimedRebuildHandoffCrashIT.diagnoseSubmittedFirstBranch(Path.of(required("jar")),
                required("sha256"), Path.of(required("output")));
    }

    private static String required(String key) {
        String value = System.getProperty(PREFIX + key);
        if (value == null || value.isBlank()) { throw new AssertionError("missing diagnostic input: " + key); }
        return value;
    }
}
