package io.tapstate.cli;

import io.tapstate.core.catalog.OfficialConnectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Naming a connector the catalog carries but this release does not install is its own refusal, apart
 * from naming one that does not exist at all. Reporting the second for the first is untrue — the id
 * is right there in the catalog — and it flattens two situations a user resolves differently: fixing
 * a typo, versus learning this build does not ship that connector.
 */
class NewCmdNotOfficialTest {

    private record Run(int code, String out, String err) {
        String all() {
            return out + err;
        }
    }

    private static Run run(String... args) {
        CommandLine cl = Cli.newCommandLine();
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        cl.setOut(new PrintWriter(out));
        cl.setErr(new PrintWriter(err));
        int code = cl.execute(args);
        return new Run(code, out.toString(), err.toString());
    }

    @Test
    void refusesACatalogConnectorThisReleaseDoesNotInstall(@TempDir Path dir) {
        Run r = run("new", "--non-interactive", "--kind", "source",
                "--connector", "kafka", "--id", "src_k", "--out", dir.toString());

        assertThat(r.code()).isEqualTo(1);
        assertThat(r.all()).contains("TS0330014")
                .doesNotContain("TS0330013");
        assertThat(dir.resolve("src_k.tap.yml")).doesNotExist();
    }

    @Test
    void theRefusalNamesWhatIsAvailable() {
        // Without this the user learns only that kafka is out, not what would work instead — and the
        // list they were about to pick from has just been narrowed.
        Run r = run("new", "--non-interactive", "--kind", "source",
                "--connector", "kafka", "--id", "src_k", "--dry-run");

        assertThat(r.all()).contains(OfficialConnectors.IDS);
    }

    @Test
    void aConnectorNobodyShipsIsStillReportedAsUnknown() {
        // The other half of the split: an id absent from the catalog is a different mistake and keeps
        // its own code.
        Run r = run("new", "--non-interactive", "--kind", "source",
                "--connector", "not-a-real-connector", "--id", "src_x", "--dry-run");

        assertThat(r.all()).contains("TS0330013")
                .doesNotContain("TS0330014");
    }
}
