package io.tapstate.e2e;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The shipped CLI still exposes the one-shot workspace convergence command.
 *
 * <p>The full-screen workbench owns a bare launch, but it must not make scripts lose {@code up}: a
 * process invoking the command must reach its own command contract before any workspace or server is
 * needed. Launching the packaged CLI catches a missing command registration, which calling the command
 * class directly would not.
 */
@DisplayName("the one-shot workspace convergence command remains available")
class CliUpCommandIT {

    @Test
    void upIsARegisteredCommandWithItsConvergenceStages() {
        CliOnce.Run run = CliOnce.run("up", "--help");

        assertThat(run.exitCode()).as("stderr was:%n%s", run.stderr()).isZero();
        assertThat(run.stdout())
                .contains("Usage: tapstate up")
                .contains("preflight, apply sources, discover, apply workspace, start");
        assertThat(run.stderr()).isEmpty();
    }
}
