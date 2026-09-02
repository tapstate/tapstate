package io.tapstate.control.restapi;

import io.tapstate.core.model.Resource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What the endpoint reports as this build's version, and every way the build can fail to give it one.
 *
 * <p>The server has no other way to know which version it is: the number is filtered into a resource at
 * package time. So each refusal here is a build defect, thrown bare rather than turned into a coded
 * diagnostic — nothing a caller did produced it, and there is no remedy to render. That is also why
 * each one needs a case. A guard against a state only a broken build reaches is a guard nothing ever
 * puts into its failing state, and the failure it exists to prevent is silent: an unsubstituted
 * placeholder, or a blank, served to clients as though it were a version.
 */
@DisplayName("the version this build reports, and the build defects that leave it without one")
class VersionControllerTest {

    @Test
    void allThreeNumbersAreReportedAndNoneIsDerivedFromAnother() {
        Map<String, Object> body = new VersionController(() -> 7).version().getBody();

        assertThat(body).containsOnlyKeys("version", "dslVersions", "dataVersion");
        assertThat(body.get("dslVersions"))
                .as("the grammar versions reported are the ones the parser accepts, not a second list "
                        + "kept beside it -- a client deciding what it may send and the parser deciding "
                        + "what it will read must not be able to disagree")
                .isEqualTo(Resource.SUPPORTED_VERSIONS);
        assertThat(body).containsEntry("dataVersion", 7);
    }

    /**
     * A run with no store -- a substrate check, say -- has no data version to report. It has to travel
     * as an explicit absence: zero is a real answer, and it means a store nothing has migrated yet.
     */
    @Test
    void aRunWithNoStoreReportsNoDataVersionRatherThanZero() {
        Map<String, Object> body = new VersionController(null).version().getBody();

        assertThat(body).containsKey("dataVersion");
        assertThat(body.get("dataVersion")).isNull();
    }

    @Test
    void theVersionIsTheSubstitutedValueTheBuildFilteredIn() {
        assertThat(VersionController.versionIn(properties("version=0.4.0\n"))).isEqualTo("0.4.0");
    }

    @Test
    void aMissingResourceIsRefusedRatherThanLeavingTheVersionNull() {
        assertThatThrownBy(() -> VersionController.versionIn(null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not on the classpath");
    }

    /**
     * The one this guard was written for. Turning the resource filter off leaves the literal
     * {@code ${project.version}} in the file, which parses, is not blank, and is what every client
     * would then be told the server is running.
     */
    @Test
    void anUnsubstitutedPlaceholderIsRefusedRatherThanServed() {
        assertThatThrownBy(() -> VersionController.versionIn(properties("version=${project.version}\n")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("${project.version}");
    }

    @Test
    void aFileWithNoVersionKeyIsRefused() {
        assertThatThrownBy(() -> VersionController.versionIn(properties("built=yesterday\n")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aBlankVersionIsRefused() {
        assertThatThrownBy(() -> VersionController.versionIn(properties("version=   \n")))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aResourceThatCannotBeReadFailsAsAnIoFailureRatherThanAsNoVersion() {
        assertThatThrownBy(() -> VersionController.versionIn(new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("the jar is truncated");
            }
        })).isInstanceOf(UncheckedIOException.class);
    }

    private static InputStream properties(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }
}
