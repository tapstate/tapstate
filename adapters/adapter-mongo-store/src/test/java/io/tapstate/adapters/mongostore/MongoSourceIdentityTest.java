package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.IoError;
import java.util.List;
import java.util.Map;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MongoSourceIdentityTest {
    private static final SourceConfigCipher CIPHER = new SourceConfigCipher(new byte[32]);
    private static final EncryptedArtifactCodec CODEC = new EncryptedArtifactCodec(CIPHER);
    private static final MongoSourceIdentitySchema.State READY = new MongoSourceIdentitySchema.State(
            MongoSourceIdentitySchema.Phase.READY, "operation-a", "cluster-a", CIPHER.activeKeyId(), null, 0);

    @Test void presentationAndCipherChangesKeepLifetimeAndBindingButSemanticAbaDoesNot() {
        SourceResource original = source(null, "first");
        Document first = write(null, original);
        var identity = MongoSourceIdentity.read(first);
        SourceResource display = source(new Metadata(Map.of("team", "changed"), "New description"), "first");
        Document renamed = write(first, display);
        assertThat(MongoSourceIdentity.read(renamed)).isEqualTo(identity);
        assertThat(first.get("body", Document.class).get("config")).isNotEqualTo(renamed.get("body", Document.class).get("config"));
        Document changed = write(renamed, source(null, "second"));
        assertThat(MongoSourceIdentity.read(changed).binding().incarnation()).isEqualTo(identity.binding().incarnation());
        assertThat(MongoSourceIdentity.read(changed).binding().bindingRevision()).isEqualTo(2);
        Document reverted = write(changed, original);
        assertThat(reverted.get("contentHash")).isEqualTo(first.get("contentHash"));
        assertThat(MongoSourceIdentity.read(reverted).binding().bindingRevision()).isEqualTo(3);
        assertThat(CODEC.decode(reverted)).isEqualTo(original);
    }

    @Test void aNewSameIdAndSameContentLifetimeDoesNotReuseItsPredecessor() {
        SourceResource source = source(null, "same");
        Document first = write(null, source);
        Document recreated = write(null, source);
        assertThat(recreated.get("contentHash")).isEqualTo(first.get("contentHash"));
        assertThat(MongoSourceIdentity.read(recreated).binding().incarnation())
                .isNotEqualTo(MongoSourceIdentity.read(first).binding().incarnation());
    }

    @Test void fenceDoesNotBecomeAProducerRevisionOrResetDuringAnEdit() {
        Document first = write(null, source(null, "same"));
        first.get("sourceIdentity", Document.class).put("writeFence", 42L);
        Document rewritten = write(first, source(new Metadata(Map.of(), "Display only"), "same"));
        assertThat(MongoSourceIdentity.read(rewritten).writeFence()).isEqualTo(42);
        assertThat(MongoSourceIdentity.read(rewritten).binding().bindingRevision()).isEqualTo(1);
    }

    @Test void strictVersionedHeaderRefusesMissingUnknownOrWrongNumericTypes() {
        Document first = write(null, source(null, "same"));
        for (String field : List.of("formatVersion", "incarnation", "bindingRevision", "writeFence")) {
            Document broken = new Document(first);
            broken.put("sourceIdentity", new Document(first.get("sourceIdentity", Document.class)));
            broken.get("sourceIdentity", Document.class).remove(field);
            assertUnreadable(() -> MongoSourceIdentity.read(broken));
        }
        Document wrong = new Document(first);
        wrong.put("sourceIdentity", new Document(first.get("sourceIdentity", Document.class)).append("bindingRevision", 1));
        assertUnreadable(() -> MongoSourceIdentity.read(wrong));
        wrong.put("sourceIdentity", new Document(first.get("sourceIdentity", Document.class)).append("formatVersion", 2));
        assertUnreadable(() -> MongoSourceIdentity.read(wrong));
    }

    @Test void legacyAndPreparationNeverStripOrInventEnrolledFacts() {
        SourceResource source = source(null, "same");
        Document legacy = MongoSourceIdentity.decorate(null, null, source, CODEC.encode(source), CIPHER);
        assertThat(legacy).doesNotContainKey("sourceIdentity");
        Document enrolled = write(null, source);
        assertUnreadable(() -> MongoSourceIdentity.decorate(null, enrolled, source, CODEC.encode(source), CIPHER));
        var preparing = new MongoSourceIdentitySchema.State(MongoSourceIdentitySchema.Phase.PREPARING,
                "operation-a", "cluster-a", CIPHER.activeKeyId(), null, 0);
        assertThatThrownBy(() -> MongoSourceIdentity.decorate(preparing, null, source, CODEC.encode(source), CIPHER))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.SOURCE_IDENTITY_UNAVAILABLE));
    }

    @Test void completeBsonHeaderStaysWithinTheApprovedBudgetEvenAtCounterBounds() {
        Document source = write(null, source(null, "same"));
        Document header = source.get("sourceIdentity", Document.class);
        header.put("bindingRevision", Long.MAX_VALUE); header.put("writeFence", Long.MAX_VALUE);
        try (var output = new BasicOutputBuffer(); var writer = new BsonBinaryWriter(output)) {
            new DocumentCodec().encode(writer, new Document("sourceIdentity", header), EncoderContext.builder().build());
            assertThat(output.getSize()).isLessThanOrEqualTo(MongoSourceIdentity.MAX_HEADER_BYTES);
        }
        assertThatThrownBy(() -> write(source, source(null, "changed")))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.SOURCE_IDENTITY_EXHAUSTED));
    }

    private static SourceResource source(Metadata metadata, String database) {
        return new SourceResource("orders", metadata, "mongodb", Map.of("database", database), null, null, null, null);
    }
    private static Document write(Document old, SourceResource next) {
        return MongoSourceIdentity.decorate(READY, old, next, CODEC.encode(next), CIPHER);
    }
    private static void assertUnreadable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
}
