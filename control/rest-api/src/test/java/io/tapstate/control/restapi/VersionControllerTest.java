package io.tapstate.control.restapi;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

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
