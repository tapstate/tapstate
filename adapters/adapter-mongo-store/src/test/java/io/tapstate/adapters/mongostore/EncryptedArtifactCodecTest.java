package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EncryptedArtifactCodecTest {
    private final EncryptedArtifactCodec codec = new EncryptedArtifactCodec(new SourceConfigCipher(key()));

    @Test
    void everySourceConnectorAndInternalSourceUseOneEncryptedStringAndKeepLogicalIdentity() {
        for (String connector : List.of("mongodb-atlas", "mongodb", "mysql", "oracle", "aws-rds-mysql", "unknown")) {
            SourceResource source = source(connector);
            Document first = codec.encode(source);
            Document second = codec.encode(source);
            assertThat(first.get("body", Document.class).get("config"))
                    .isInstanceOf(String.class).isNotEqualTo(second.get("body", Document.class).get("config"));
            assertThat(first.toJson()).doesNotContain("config-secret", "example.invalid", "nested-secret");
            assertThat(first.getString("contentHash")).isEqualTo(CanonicalHash.of(source));
            Resource decoded = codec.decode(first);
            assertThat(new CanonicalWriter().tree(decoded)).isEqualTo(new CanonicalWriter().tree(source));
            assertThat(CanonicalHash.of(decoded)).isEqualTo(CanonicalHash.of(source));
            assertThat(codec.browse(first).readable()).isTrue();
            assertThat(codec.browse(first).contentHash()).isEqualTo(CanonicalHash.of(source));
        }
    }

    @Test
    void emptySourceConfigIsEncryptedAndNonSourceResourcesAreUnchanged() {
        SourceResource empty = new SourceResource("empty", null, "unknown", Map.of(), null, null, null, null);
        Document stored = codec.encode(empty);
        assertThat(stored.get("body", Document.class).get("config")).isInstanceOf(String.class);
        assertThat(((SourceResource) codec.decode(stored)).config()).isEmpty();
        Resource nonSource = new DslParser().parse("""
                version: tapstate/v1
                kind: transform
                id: t
                type: map
                fields:
                  full_name: $name
                """);
        assertThat(codec.encode(nonSource)).isEqualTo(MongoArtifactStore.toDocument(nonSource));
    }

    @Test
    void sourcePlaintextOrTamperingIsUnreadableInBothStrictAndTolerantPaths() {
        SourceResource source = source("unknown");
        Document legacyPlaintext = MongoArtifactStore.toDocument(source);
        refuses(legacyPlaintext);
        Document corrupt = codec.encode(source);
        corrupt.put("contentHash", "not-the-resource-hash");
        refuses(corrupt);
        Document swapped = codec.encode(source);
        swapped.put("_id", "another-source");
        refuses(swapped);
    }

    @Test
    void aSourceCannotMasqueradeAsAnotherIndexedKindAndBypassProtectedBrowseProjection() {
        for (Object wrongKind : Arrays.asList(null, "pipeline", new Document("kind", "untrusted-kind-input"))) {
            Document stored = codec.encode(source("unknown"));
            stored.put("kind", wrongKind);
            refuses(stored);
        }
    }

    @Test
    void aMalformedSourceHashIsAStorageDiagnosticRatherThanABareTypeError() {
        for (Object invalidHash : Arrays.asList(null, 123, new Document("input", "config-secret"))) {
            Document stored = codec.encode(source("unknown"));
            stored.put("contentHash", invalidHash);
            refuses(stored);
        }
    }

    private void refuses(Document document) {
        assertThatThrownBy(() -> codec.decode(document)).isInstanceOfSatisfying(TapstateException.class, error -> {
            assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
            assertThat(error.getCause()).isNull();
            assertThat(error.toString()).doesNotContain("config-secret", "nested-secret", "example.invalid");
        });
        assertThat(codec.browse(document).readable()).isFalse();
        assertThat(codec.browse(document).canonicalForm()).isNull();
    }

    private static SourceResource source(String connector) {
        return new SourceResource("views", null, connector, Map.of(
                "password", "config-secret", "host", "example.invalid",
                "nested", Map.of("value", "nested-secret"), "values", List.of(true, 12, 1.5)),
                null, null, null, null);
    }

    private static byte[] key() {
        byte[] key = new byte[32];
        Arrays.fill(key, (byte) 7);
        return key;
    }
}
