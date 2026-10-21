package io.tapstate.app;

import io.tapstate.adapters.mongostore.SourceConfigCipher;
import io.tapstate.core.common.*;
import io.tapstate.core.model.*;
import io.tapstate.spi.store.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class EncryptedConfigurationTransferTest {
    private static final byte[] KEY_BYTES = new byte[32];
    private static final String KEY = Base64.getUrlEncoder().withoutPadding().encodeToString(KEY_BYTES);
    private final EncryptedConfigurationTransfer codec = new EncryptedConfigurationTransfer();
    private static SourceResource source(String ca) {
        return new SourceResource("input", null, "mongodb", Map.of("uri", "mongodb://user:sentinel-secret@host/source", "tlsCaPem", ca,
                "unchanged", "views"), SourceMode.CDC, List.of(TableRef.literal("orders")), null, null);
    }
    private static ConfigurationImportStore.Payload payload(String ca) {
        var pipeline = new PipelineResource("p", null, List.of(SourceRef.spec("input", true)), null, null,
                new ServeBlock.Inline(null, FromRef.literal("orders"), List.of(new SyncElement("sync", "views", null, null, null)), null, null), null, null);
        return new ConfigurationImportStore.Payload(List.of(source(ca), pipeline),
                List.of(new User("imported", "$2a$04$" + "A".repeat(53), "admin")),
                List.of(new PipelineLayout("p", Map.of("input", new PipelineLayout.NodePosition(1, 2)), new PipelineLayout.Viewport(3, 4, 1))), Map.of(), Map.of());
    }
    private String exported(String ca) {
        return codec.sealExport(new ConfigurationExportStore.Snapshot(StorageBinding.Mode.OP, "views", payload(ca)), KEY);
    }
    @Test void independentEncryptedTransportRemapsOnlyTheConfirmedTargetAndPreservesCredentialsAndLayout() {
        String encrypted = exported("ca-pem");
        assertThat(encrypted).doesNotContain("sentinel-secret", "imported", "ca-pem", KEY);
        var decoded = codec.open(encrypted, KEY, Map.of("views", "atlas-store"));
        assertThat(decoded.users()).isEqualTo(payload("ca-pem").users());
        assertThat(decoded.layouts()).isEqualTo(payload("ca-pem").layouts());
        var input = (SourceResource)decoded.resources().stream().filter(r -> r.id().equals("input")).findFirst().orElseThrow();
        assertThat(input.config()).containsEntry("unchanged", "views").containsEntry("tlsCaPem", "ca-pem");
        var pipeline = (PipelineResource)decoded.resources().stream().filter(r -> r.id().equals("p")).findFirst().orElseThrow();
        assertThat(((ServeBlock.Inline)pipeline.serve()).sync().getFirst().source()).isEqualTo("atlas-store");
        var prepared = new ConfigurationImportStore.Payload(decoded.resources(), decoded.users(), decoded.layouts(),
                Map.of("atlas-store", "a".repeat(64)), decoded.defaultSourceMapping());
        String sealed = codec.seal(prepared, KEY);
        assertThat(codec.open(sealed, KEY, prepared.defaultSourceMapping())).isEqualTo(prepared);
        assertThatThrownBy(() -> codec.open(sealed, KEY, Map.of("views", "different"))).isInstanceOf(TapstateException.class);
    }
    @Test void malformedKeysTamperingWrongVersionsAndUnconfirmedMappingsAreRefusedWithoutSecrets() {
        String encrypted = exported("ca");
        var other = new byte[32]; Arrays.fill(other, (byte)1);
        for (String key : List.of("password", KEY + "=", Base64.getUrlEncoder().withoutPadding().encodeToString(other)))
            assertThatThrownBy(() -> codec.open(encrypted, key, Map.of("views", "atlas-store"))).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.toString()).doesNotContain("sentinel-secret", key));
        assertThatThrownBy(() -> codec.open(encrypted.replace("export1:", "export2:"), KEY, Map.of("views", "atlas-store"))).isInstanceOf(TapstateException.class);
        int change = encrypted.length() - 10;
        String tampered = encrypted.substring(0, change) + (encrypted.charAt(change) == 'A' ? 'B' : 'A') + encrypted.substring(change + 1);
        assertThatThrownBy(() -> codec.open(tampered, KEY, Map.of("views", "atlas-store"))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> codec.open(encrypted, KEY, Map.of())).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> codec.open(encrypted, KEY, Map.of("views", "atlas-store", "unrelated", "another"))).isInstanceOf(TapstateException.class);
    }
    @Test void caBudgetIsMeasuredInUtf8BytesOnEveryTransportDirection() {
        String boundary = "\u00e9".repeat(32 * 1024);
        String accepted = exported(boundary);
        assertThat(codec.open(accepted, KEY, Map.of("views", "atlas-store")).resources()).hasSize(2);
        assertThatThrownBy(() -> exported(boundary + "a")).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> codec.seal(payload(boundary + "a"), KEY)).isInstanceOf(TapstateException.class);
        String clear = new SourceConfigCipher(KEY_BYTES).decrypt("configuration-transfer", "export1", accepted.substring("export1:".length()));
        @SuppressWarnings("unchecked") var tree = (Map<String,Object>)JsonReader.parse(clear);
        @SuppressWarnings("unchecked") var resources = (List<Map<String,Object>>)tree.get("resources");
        @SuppressWarnings("unchecked") var settings = (Map<String,Object>)resources.stream().filter(r -> "source".equals(r.get("kind"))).findFirst().orElseThrow().get("config");
        settings.put("tlsCaPem", boundary + "a");
        String forged = "export1:" + new SourceConfigCipher(KEY_BYTES).encrypt("configuration-transfer", "export1", JsonWriter.write(tree));
        assertThatThrownBy(() -> codec.open(forged, KEY, Map.of("views", "atlas-store"))).isInstanceOf(TapstateException.class);
    }
    @Test void otherConnectorSettingsRemainOpaqueToTheHostCaConvention() {
        var source = new SourceResource("custom", null, "custom", Map.of("tlsCaPem", Map.of("opaque", "connector-owned")), null, null, null, null);
        var payload = new ConfigurationImportStore.Payload(List.of(source), List.of(), List.of(), Map.of(), Map.of());
        String exported = codec.sealExport(new ConfigurationExportStore.Snapshot(StorageBinding.Mode.OP, null, payload), KEY);
        assertThat(((SourceResource)codec.open(exported, KEY, Map.of()).resources().getFirst()).config()).isEqualTo(source.config());
        assertThat(((SourceResource)codec.open(codec.seal(payload, KEY), KEY, Map.of()).resources().getFirst()).config()).isEqualTo(source.config());
        var mongo = new SourceResource("mongo", null, "mongodb", source.config(), null, null, null, null);
        assertThatThrownBy(() -> codec.seal(new ConfigurationImportStore.Payload(List.of(mongo), List.of(), List.of(), Map.of(), Map.of()), KEY))
                .isInstanceOf(TapstateException.class);
    }

    @Test void cloudUsersUnknownFieldsAndOversizedObjectsNeverBecomeAValidTransfer() {
        assertThatThrownBy(() -> codec.sealExport(new ConfigurationExportStore.Snapshot(StorageBinding.Mode.CLOUD, "views", payload("ca")), KEY)).isInstanceOf(TapstateException.class);
        String accepted = exported("ca");
        String clear = new SourceConfigCipher(KEY_BYTES).decrypt("configuration-transfer", "export1", accepted.substring("export1:".length()));
        @SuppressWarnings("unchecked") var tree = (Map<String,Object>)JsonReader.parse(clear);
        tree.put("keyring", "must-not-travel");
        String forged = "export1:" + new SourceConfigCipher(KEY_BYTES).encrypt("configuration-transfer", "export1", JsonWriter.write(tree));
        assertThatThrownBy(() -> codec.open(forged, KEY, Map.of("views", "atlas-store"))).isInstanceOf(TapstateException.class);
        var large = new SourceResource("oversized", null, "mongodb", Map.of("uri", "x".repeat(ConfigurationImportStore.MAX_OBJECT_BYTES)), null, null, null, null);
        assertThatThrownBy(() -> codec.seal(new ConfigurationImportStore.Payload(List.of(large), List.of(), List.of(), Map.of(), Map.of()), KEY)).isInstanceOf(TapstateException.class);
    }
}
