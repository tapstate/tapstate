package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The process-role parser: at L1 the single binary runs only {@code --role=all}; an absent role
 * defaults to {@code all} and any other value is a coded {@code role.unsupported} error rather than
 * a silent fallback. Both the {@code --role=VALUE} and {@code --role VALUE} forms are accepted.
 */
class RoleArgumentsTest {

    @Test
    void absentRoleDefaultsToAll() {
        assertThat(RoleArguments.parse(new String[] {})).isEqualTo("all");
    }

    @Test
    void equalsFormAllIsAccepted() {
        assertThat(RoleArguments.parse(new String[] {"--role=all"})).isEqualTo("all");
    }

    @Test
    void separatedFormAllIsAccepted() {
        assertThat(RoleArguments.parse(new String[] {"--role", "all"})).isEqualTo("all");
    }

    @Test
    void otherArgumentsArePassedThrough() {
        assertThat(RoleArguments.parse(new String[] {"--server.port=8080", "--role=all", "x"}))
                .isEqualTo("all");
    }

    @Test
    void lastRoleOccurrenceWins() {
        assertThat(RoleArguments.parse(new String[] {"--role=api", "--role=all"})).isEqualTo("all");
    }

    @Test
    void unsupportedEqualsFormIsCoded() {
        assertThatThrownBy(() -> RoleArguments.parse(new String[] {"--role=api"}))
                .isInstanceOfSatisfying(TapstateException.class, e -> {
                    assertThat(e.code().code()).isEqualTo("TS0320001");
                    assertThat(e.args()).containsEntry("role", "api");
                });
    }

    @Test
    void unsupportedSeparatedFormIsCoded() {
        assertThatThrownBy(() -> RoleArguments.parse(new String[] {"--role", "engine"}))
                .isInstanceOfSatisfying(TapstateException.class, e -> {
                    assertThat(e.code().code()).isEqualTo("TS0320001");
                    assertThat(e.args()).containsEntry("role", "engine");
                });
    }

    @Test
    void allFollowedByAnUnsupportedOverrideIsRejected() {
        assertThatThrownBy(() -> RoleArguments.parse(new String[] {"--role=all", "--role=api"}))
                .isInstanceOf(TapstateException.class);
    }

    @Test
    void roleFlagWithoutAValueIsCodedAsEmpty() {
        assertThatThrownBy(() -> RoleArguments.parse(new String[] {"--role"}))
                .isInstanceOfSatisfying(TapstateException.class, e -> {
                    assertThat(e.code().code()).isEqualTo("TS0320001");
                    assertThat(e.args()).containsEntry("role", "");
                });
    }

    @Test
    void emptyValueAfterEqualsIsCodedAsEmpty() {
        // the --role= form: substring after '=' yields "", a distinct branch from the bare flag
        assertThatThrownBy(() -> RoleArguments.parse(new String[] {"--role="}))
                .isInstanceOfSatisfying(TapstateException.class, e -> {
                    assertThat(e.code().code()).isEqualTo("TS0320001");
                    assertThat(e.args()).containsEntry("role", "");
                });
    }

    @Test
    void separatedFormConsumesTheNextTokenEvenWhenItIsAFlag() {
        // the separated form takes the next token unconditionally; a forgotten value surfaces as a
        // coded unsupported-role rather than being silently swallowed
        assertThatThrownBy(() -> RoleArguments.parse(new String[] {"--role", "--server.port=8080"}))
                .isInstanceOfSatisfying(TapstateException.class, e -> {
                    assertThat(e.code().code()).isEqualTo("TS0320001");
                    assertThat(e.args()).containsEntry("role", "--server.port=8080");
                });
    }
}
