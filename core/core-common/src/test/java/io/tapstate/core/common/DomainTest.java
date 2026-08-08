package io.tapstate.core.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DomainTest {

    @Test
    void idIsTheLowerCaseName() {
        assertThat(Domain.DSL.id()).isEqualTo("dsl");
        assertThat(Domain.CATALOG.id()).isEqualTo("catalog");
    }

    @Test
    void registryHoldsTheRegisteredDomains() {
        assertThat(Domain.ids())
                .containsExactlyInAnyOrder(
                        "dsl", "cli", "core", "catalog", "schema", "lifecycle", "role", "boot",
                        "actuation", "store", "connector", "transform", "io", "control",
                        "engine", "monitor", "source", "mcp", "capture", "nest");
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
