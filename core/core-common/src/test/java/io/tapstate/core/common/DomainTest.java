package io.tapstate.core.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DomainTest {

    @Test
    void idIsTheLowerKebabName() {
        assertThat(Domain.DSL.id()).isEqualTo("dsl");
        assertThat(Domain.CATALOG.id()).isEqualTo("catalog");
        // a multi-word constant separates with '_' but its id must be kebab: the code format admits
        // a hyphen in either segment and never an underscore, so name().toLowerCase() alone would
        // mint a code no catalog entry can match
        assertThat(Domain.DATA_BROWSER.id()).isEqualTo("data-browser");
        assertThat(Domain.DATA_BROWSER.id()).doesNotContain("_");
    }

    @Test
    void registryHoldsTheRegisteredDomains() {
        assertThat(Domain.ids())
                .containsExactlyInAnyOrder(
                        "dsl", "cli", "core", "catalog", "schema", "lifecycle", "role", "boot",
                        "actuation", "store", "connector", "transform", "io", "control",
                        "engine", "monitor", "data-browser", "artifact", "source", "mcp", "position",
                        "capture", "nest");
    }

    @Test
    void registeredAcceptsKnownDomains() {
        assertThat(Domain.isRegistered("dsl")).isTrue();
        assertThat(Domain.isRegistered("schema")).isTrue();
        assertThat(Domain.isRegistered("lifecycle")).isTrue();
        assertThat(Domain.isRegistered("role")).isTrue();
        assertThat(Domain.isRegistered("boot")).isTrue();
        assertThat(Domain.isRegistered("store")).isTrue();
        assertThat(Domain.isRegistered("io")).isTrue();
        assertThat(Domain.isRegistered("engine")).isTrue();
        assertThat(Domain.isRegistered("monitor")).isTrue();
        assertThat(Domain.isRegistered("data-browser")).isTrue();
        // the constant's own spelling is not a registered id -- only its kebab form is
        assertThat(Domain.isRegistered("data_browser")).isFalse();
        assertThat(Domain.isRegistered("artifact")).isTrue();
        assertThat(Domain.isRegistered("source")).isTrue();
        assertThat(Domain.isRegistered("mcp")).isTrue();
    }

    @Test
    void registeredRejectsTyposAndWrongCase() {
        // the legacy "dls." typo that silently minted a new namespace — must be caught now
        assertThat(Domain.isRegistered("dls")).isFalse();
        assertThat(Domain.isRegistered("DSL")).isFalse();
        assertThat(Domain.isRegistered("")).isFalse();
    }
}
