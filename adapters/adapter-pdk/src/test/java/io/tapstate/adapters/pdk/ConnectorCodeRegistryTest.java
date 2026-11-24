package io.tapstate.adapters.pdk;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The connector-code namespace: codes a connector's own jar declares occupy a reserved
 * {@code connector.<connector-id>.<symbol>} namespace, merged at runtime with duplicate detection —
 * never the silent last-writer-wins the legacy scanner did. These codes come from jars the build
 * cannot see, so they are guarded here at runtime rather than by the build-time gates.
 */
class ConnectorCodeRegistryTest {

    @Test
    void namespacedConnectorCodesCannotCollideWithFirstPartyConnectorCodes() {
        Set<String> firstParty = Arrays.stream(ConnectorError.values())
                .map(ConnectorError::code).collect(Collectors.toSet());
        // A product code uses the allocated numeric module; connector-authored codes retain their
        // connector-id namespace. Reusing a product symbol cannot mint a product identity.
        for (ConnectorError e : ConnectorError.values()) {
            String symbol = e.name().toLowerCase(java.util.Locale.ROOT).replace('_', '-');
            String namespaced = ConnectorCodeRegistry.namespaced("mysql", symbol);
            assertThat(namespaced).isEqualTo("connector.mysql." + symbol);
            assertThat(firstParty).doesNotContain(namespaced);
        }
        assertThat(ConnectorCodeRegistry.namespaced("mysql", "boom").chars().filter(c -> c == '.').count())
                .isEqualTo(2);
    }

    @Test
    void rejectsADuplicateConnectorCodeInsteadOfSilentlyOverwriting() {
        ConnectorCodeRegistry registry = new ConnectorCodeRegistry();
        assertThat(registry.register("mysql", "violate-unique")).isEqualTo("connector.mysql.violate-unique");
        assertThatThrownBy(() -> registry.register("mysql", "violate-unique"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("connector.mysql.violate-unique");
    }

    @Test
    void differentConnectorsMayReuseTheSameSymbol() {
        ConnectorCodeRegistry registry = new ConnectorCodeRegistry();
        assertThat(registry.register("mysql", "violate-unique")).isEqualTo("connector.mysql.violate-unique");
        assertThat(registry.register("mongo", "violate-unique")).isEqualTo("connector.mongo.violate-unique");
    }

    @Test
    void anIdOrSymbolCarryingADotWouldBreakTheNamespaceAndIsRejected() {
        assertThatThrownBy(() -> ConnectorCodeRegistry.namespaced("my.sql", "boom"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ConnectorCodeRegistry.namespaced("mysql", "a.b"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
