package io.tapstate.e2e;

import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Qualifies a failed successor's actual unknown native frame while its known source floor remains intact. */
@RequiresDocker
@EnabledIfSystemProperty(named = "tapstate.e2e.claimed-failed-handoff.jar", matches = ".+")
class RealClaimedFailedHandoffQualificationIT {
    private static final String PREFIX = "tapstate.e2e.claimed-failed-handoff.";

    @BeforeAll
    static void requireInputs() {
        RealConnectorGate.require("mysql", "mongodb");
        assertThat(Path.of(required("jar"))).isRegularFile();
        assertThat(Path.of(required("output")).isAbsolute()).isTrue();
        assertThat(required("sha256")).matches("[0-9a-f]{64}");
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.MINUTES)
    void aMatchingFailedClaimedSuccessorWithActualUnknownNativeFactsPreservesItsKnownSourceFloor() throws Exception {
        RealClaimedRebuildHandoffCrashIT.qualifySubmittedFailedUnknownNativeHandoff(Path.of(required("jar")),
                required("sha256"), Path.of(required("output")));
    }

    private static String required(String key) {
        String value = System.getProperty(PREFIX + key);
        if (value == null || value.isBlank()) { throw new AssertionError("missing qualification input: " + key); }
        return value;
    }
}
