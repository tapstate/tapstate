package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.*;
import org.bson.codecs.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MongoValidationRawReadTest {
    @Test void oversizeIsRefusedBeforeTheCollectionDecoderCanMaterializeAnyValues(){
        var decoded=new AtomicBoolean();var codec=new Codec<Document>(){
            @Override public Document decode(BsonReader reader,DecoderContext context){decoded.set(true);throw new AssertionError("oversize cannot decode");}
            @Override public void encode(BsonWriter writer,Document value,EncoderContext context){throw new AssertionError("read only");}
            @Override public Class<Document> getEncoderClass(){return Document.class;}
        };
        var raw=new RawBsonDocument(new Document("payload","x".repeat(2048)),new DocumentCodec());
        assertThatThrownBy(()->MongoValidationResultStore.decodeBounded(raw,codec,1024,"root-bson-size"))
                .isInstanceOf(TapstateException.class).hasMessageContaining("root-bson-size");assertThat(decoded).isFalse();
    }
    @Test void theProductionRootByteBoundaryIsAppliedBeforeDecodedObjectGrowth(){
        var codec=new DocumentCodec();int header=new RawBsonDocument(new Document("payload",""),codec).getByteLength();
        var exact=new RawBsonDocument(new Document("payload","x".repeat(MongoValidationResultStore.MAX_ROOT_BYTES-header)),codec);
        assertThat(exact.getByteLength()).isEqualTo(MongoValidationResultStore.MAX_ROOT_BYTES);
        assertThat(MongoValidationResultStore.decodeBounded(exact,codec,MongoValidationResultStore.MAX_ROOT_BYTES,"root-bson-size").getString("payload"))
                .hasSize(MongoValidationResultStore.MAX_ROOT_BYTES-header);
        var excess=new RawBsonDocument(new Document("payload","x".repeat(MongoValidationResultStore.MAX_ROOT_BYTES-header+1)),codec);
        assertThatThrownBy(()->MongoValidationResultStore.decodeBounded(excess,codec,MongoValidationResultStore.MAX_ROOT_BYTES,"root-bson-size"))
                .isInstanceOf(TapstateException.class).hasMessageContaining("root-bson-size");
    }
    @Test void exactBoundaryUsesTheOriginalCodecAndMissingRowsStayMissing(){
        var codec=new DocumentCodec();var expected=new Document("count",9007199254740993L);var raw=new RawBsonDocument(expected,codec);
        assertThat(MongoValidationResultStore.decodeBounded(raw,codec,raw.getByteLength(),"report-bson-size")).isEqualTo(expected);
        assertThat(MongoValidationResultStore.decodeBounded(null,codec,1,"report-bson-size")).isNull();
    }
}
