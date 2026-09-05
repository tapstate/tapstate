package io.tapstate.runtime.srs;

import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.CaptureConfig;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The mining chain identity: the key every SRS ring and meta record is namespaced under. Two cdc sources
 * resolve to the same chain — and so are force-merged onto one shared change stream — exactly when they
 * read the same physical source. Identity derives from the connector and its settings (a config hash),
 * never from the table subset a source reads, so different table subsets of one database share a chain. A
 * manual {@code srs.key} overrides the hash, asserting same-chain when config derivation would not.
 */
class MiningChainIdTest {

    private static CaptureConfig config(Map<String, Object> settings, List<String> streams) {
        return new CaptureConfig("mysql", settings, streams);
    }

    @Test
    void sameConnectorAndSettingsResolveToOneChainRegardlessOfKeyOrder() {
        // Two authors write the same connection in different key orders (top level and nested).
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("host", "db1");
        a.put("port", 3306);
        a.put("ssl", new LinkedHashMap<>(Map.of("mode", "require", "ca", "x")));
        Map<String, Object> b = new LinkedHashMap<>();
        b.put("port", 3306);
        b.put("ssl", new LinkedHashMap<>(Map.of("ca", "x", "mode", "require")));
        b.put("host", "db1");

        assertThat(MiningChainId.of(config(a, List.of("orders"))))
                .isEqualTo(MiningChainId.of(config(b, List.of("orders"))));
    }

    @Test
    void differentSettingsResolveToDifferentChains() {
        assertThat(MiningChainId.of(config(Map.of("host", "db1"), List.of())))
                .isNotEqualTo(MiningChainId.of(config(Map.of("host", "db2"), List.of())));
    }

    @Test
    void differentConnectorResolvesToADifferentChainForTheSameSettings() {
        Map<String, Object> settings = Map.of("host", "db1");
        MiningChainId mysql = MiningChainId.of(new CaptureConfig("mysql", settings, List.of()));
        MiningChainId postgres = MiningChainId.of(new CaptureConfig("postgres", settings, List.of()));

        assertThat(mysql).isNotEqualTo(postgres);
    }

    @Test
    void theTableSubsetDoesNotChangeTheChain() {
        // Force-merge basis: two sources on the same database reading different tables share one chain.
        Map<String, Object> settings = Map.of("host", "db1");

        assertThat(MiningChainId.of(config(settings, List.of("orders"))))
                .isEqualTo(MiningChainId.of(config(settings, List.of("customers", "orders"))));
    }

    @Test
    void aChainIdIsDeterministicAndNonBlank() {
        MiningChainId id = MiningChainId.of(config(Map.of("host", "db1"), List.of("orders")));

        assertThat(id.value()).isNotBlank();
        assertThat(id).isEqualTo(MiningChainId.of(config(Map.of("host", "db1"), List.of("orders"))));
    }

    @Test
    void anExplicitKeyAssertsOneChainAcrossDivergentConfigs() {
        // Same srs.key merges two sources whose configs differ (config derivation would not catch it).
        MiningChainId k1 = MiningChainId.ofKey("prod-orders");
        MiningChainId k2 = MiningChainId.ofKey("prod-orders");

        assertThat(k1).isEqualTo(k2);
        assertThat(MiningChainId.ofKey("prod-orders")).isNotEqualTo(MiningChainId.ofKey("prod-billing"));
    }

    @Test
    void anExplicitKeyIdIsDistinctFromtheDerivedIdForTheSameSource() {
        CaptureConfig cfg = config(Map.of("host", "db1"), List.of("orders"));

        assertThat(MiningChainId.ofKey("db1")).isNotEqualTo(MiningChainId.of(cfg));
    }

    @Test
    void resolvePrefersTheExplicitKeyAndSkipsTheHash() {
        CaptureConfig cfg = config(Map.of("host", "db1"), List.of("orders"));

        assertThat(MiningChainId.resolve(cfg, "prod-orders")).isEqualTo(MiningChainId.ofKey("prod-orders"));
        assertThat(MiningChainId.resolve(cfg, "prod-orders")).isNotEqualTo(MiningChainId.of(cfg));
    }

    @Test
    void resolveFallsBackToTheConfigHashWhenNoKeyIsGiven() {
        CaptureConfig cfg = config(Map.of("host", "db1"), List.of("orders"));

        assertThat(MiningChainId.resolve(cfg, null)).isEqualTo(MiningChainId.of(cfg));
        assertThat(MiningChainId.resolve(cfg, "  ")).isEqualTo(MiningChainId.of(cfg));
    }

    /**
     * The node a config is read for is not part of what the chain is keyed by, and this is the case that
     * says so. Two pipelines reading one database through one set of settings differ in exactly that
     * field, and merging them onto a single mined stream is the whole point of deriving identity from
     * the physical coordinate — so a derivation that folded the node in would give each pipeline a chain
     * of its own and mine the same log once per reader. Nothing above would say so: every ring name
     * would still be well-formed, every pipeline would still receive its rows, and the only trace would
     * be the source's log being read twice.
     */
    @Test
    void theNodeAConfigIsReadForIsNotPartOfTheChainIdentity() {
        Map<String, Object> settings = Map.of("host", "db1");
        CaptureConfig one = config(settings, List.of("orders")).at(new PipelineNode("p1", "src_a"));
        CaptureConfig other = config(settings, List.of("orders")).at(new PipelineNode("p2", "src_b"));

        assertThat(MiningChainId.of(one)).isEqualTo(MiningChainId.of(other));
        assertThat(MiningChainId.of(one)).isEqualTo(MiningChainId.of(config(settings, List.of("orders"))));
    }

    @Test
    void aBlankExplicitKeyIsAProgrammerError() {
        // ofKey is the explicit-assertion entry; a blank key is a bug at the call site, surfaced bare.
        assertThatThrownBy(() -> MiningChainId.ofKey("")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MiningChainId.ofKey(null)).isInstanceOf(IllegalArgumentException.class);
    }
}
