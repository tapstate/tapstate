package io.tapstate.spi.store;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class DetachedStoreValuesTest {
    @Test
    void pendingKeyDiagnosticsDoNotExposeBusinessPartitionValues() {
        String businessKey = "{\"account\":\"private-account\"}";
        var pending = new NestReleaseStore.PendingKey(businessKey, new byte[]{1, 2});
        assertThat(pending.toString()).doesNotContain(businessKey, "private-account");
    }

    @Test
    void independentlyDecodedNoteValuesHaveStableContentIdentityWithoutExposingPayloads() {
        byte[] payload = "private-note".getBytes(StandardCharsets.UTF_8);
        var first = new RequiredNoteValue(8, payload);
        var second = new RequiredNoteValue(8, payload.clone());
        var values = new HashSet<>(List.of(first, second));
        assertThat(values).hasSize(1);
        payload[0] = 0;
        first.bytes()[0] = 1;
        assertThat(values).contains(second);
        assertThat(first).isNotEqualTo(new RequiredNoteValue(9, second.bytes()));
        assertThat(first.toString()).doesNotContain("private-note");
    }

    @Test
    void independentlyDecodedPendingKeysDeduplicateWithoutSharingTheirMutableArrays() {
        byte[] payload = "private-routing-key".getBytes(StandardCharsets.UTF_8);
        var first = new NestReleaseStore.PendingKey("state-key", payload);
        var second = new NestReleaseStore.PendingKey("state-key", payload.clone());
        var values = new HashSet<>(List.of(first, second));
        assertThat(values).hasSize(1);
        payload[0] = 0;
        first.routingKey()[0] = 1;
        assertThat(values).contains(second);
        assertThat(first).isNotEqualTo(new NestReleaseStore.PendingKey("other-key", second.routingKey()));
        assertThat(first.toString()).doesNotContain("private-routing-key");
    }
}
