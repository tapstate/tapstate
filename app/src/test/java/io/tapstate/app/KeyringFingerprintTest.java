package io.tapstate.app;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KeyringFingerprintTest {
    @Test
    void normalWriteFenceChangesDoNotChangeThePersistedKeyFingerprint() {
        Document original = keyring();
        Document written = new Document(original).append("sourceWriteFence", 7L);
        Document withoutFence = new Document(original);
        withoutFence.remove("sourceWriteFence");

        assertThat(KeyringFingerprint.of(written)).isEqualTo(KeyringFingerprint.of(original));
        assertThat(KeyringFingerprint.of(withoutFence)).isEqualTo(KeyringFingerprint.of(original));
        assertThat(original.getLong("sourceWriteFence")).isEqualTo(6L);
        assertThat(written.getLong("sourceWriteFence")).isEqualTo(7L);
    }

    @Test
    void everyOtherFieldStillParticipatesIncludingUnknownFields() {
        Document original = keyring().append("futureField", new Document("sourceWriteFence", 3L));
        String fingerprint = KeyringFingerprint.of(original);

        for (String field : original.keySet()) {
            if (field.equals("sourceWriteFence")) continue;
            Document changed = new Document(original).append(field, "changed-" + field);
            Document missing = new Document(original);
            missing.remove(field);
            assertThat(KeyringFingerprint.of(changed)).as("changed %s", field).isNotEqualTo(fingerprint);
            assertThat(KeyringFingerprint.of(missing)).as("missing %s", field).isNotEqualTo(fingerprint);
        }
        Document added = new Document(original).append("anotherUnknownField", "unexpected");
        assertThat(KeyringFingerprint.of(added)).isNotEqualTo(fingerprint);
        Document nestedChange = new Document(original)
                .append("futureField", new Document("sourceWriteFence", 4L));
        assertThat(KeyringFingerprint.of(nestedChange)).isNotEqualTo(fingerprint);
    }

    private static Document keyring() {
        return new Document("_id", "cluster")
                .append("clusterId", "fixture-cluster")
                .append("formatVersion", 1)
                .append("keyId", "fixture-key")
                .append("material", "fixture-key-material")
                .append("sourceWriteFence", 6L);
    }
}
