package io.tapstate.e2e;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The shipped CLI exposes one-shot workspace convergence and points session actions to the workbench
 * Shell. Both cases launch the packaged CLI in separate processes, covering command registration,
 * loaded message resources and process exit codes.
 */
@DisplayName("the one-shot CLI keeps command entry points and session recovery available")
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

    @Test
    void anInteractiveActionExplainsHowToOpenTheWorkbenchShell() {
        CliOnce.Run run = CliOnce.run("login");

        assertThat(run.exitCode()).as("the one-shot action must be refused").isEqualTo(3);
        assertThat(run.stdout()).isEmpty();
        assertThat(run.stderr())
                .contains("error: TS0330023")
                .contains("press F6 to open Shell")
                .contains("enter `login` at the `tapstate>` prompt");
    }

}
