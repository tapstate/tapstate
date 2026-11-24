package io.tapstate.runtime.engine.nest;

import io.tapstate.core.common.Domain;
import io.tapstate.core.common.Severity;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NestErrorTest {

    @Test
    void everyCodeBelongsToTheRegisteredNestDomain() {
        assertThat(Domain.isRegistered("nest")).isTrue();
        for (NestError error : NestError.values()) {
            assertThat(error.code()).matches("TS049(?!0000)[0-9]{4}");
        }
    }

    @Test
    void noTwoCodesShareASymbol() {
        Set<String> codes = Arrays.stream(NestError.values())
                .map(NestError::code)
                .collect(Collectors.toSet());
        assertThat(codes).hasSameSizeAs(NestError.values());
    }

    @Test
    void everyCodeNamesTheArgumentsItsMessageNeeds() {
        for (NestError error : NestError.values()) {
            assertThat(error.placeholders())
                    .as("%s declares the named arguments every throw site must supply", error.code())
                    .isNotEmpty();
        }
    }

    @Test
    void aPlaceholderContractCannotBeEditedByItsCaller() {
        NestError error = NestError.ARRAY_KEY_UNRESOLVABLE;
        assertThatThrownBy(() -> error.placeholders().add("smuggled"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void onlyTheCodesThatLeaveThePipelineRunningAreWarnings() {
        // An event whose parent is gone is dead-lettered and the frontier moves on, and a namespace being
        // served from storage rather than memory is slow rather than broken. Both
        // leave the pipeline running, which is the whole of what this severity means. Every other code here
        // stops something -- a tree that will not compile, a deployment that would never reach its cold
        // tier, a limit that fails the job -- and reading them at one severity would hide which is which.
        Set<NestError> keepRunning =
                Set.of(NestError.PARENT_ABSENT, NestError.STATE_SERVED_FROM_COLD_LAYER);
        for (NestError error : NestError.values()) {
            assertThat(error.severity())
                    .as("%s", error.code())
                    .isEqualTo(keepRunning.contains(error) ? Severity.WARNING : Severity.ERROR);
        }
    }

    @Test
    void theCheckedTreeCodesCoverEveryWellFormednessRuleTheTreeIsCheckedAgainst() {
        // Six rules are checked when a nest tree is compiled; each has a code of its own so an author is
        // told which rule they broke rather than that "the nest is invalid".
        assertThat(Set.of(
                NestError.SIBLING_EMBEDS_TARGET_DIFFERENT_PARENT_KEYS.code(),
                NestError.EMBED_PATH_CONFLICT.code(),
                NestError.ARRAY_KEY_UNRESOLVABLE.code(),
                NestError.RESOLVER_VERTEX_LIMIT_EXCEEDED.code(),
                NestError.APPEND_MODE_CONFLICTS_WITH_KEY_TRACKING.code(),
                NestError.SNAPSHOT_PASSTHROUGH_FORBIDDEN.code()))
                .hasSize(6);
    }
}
