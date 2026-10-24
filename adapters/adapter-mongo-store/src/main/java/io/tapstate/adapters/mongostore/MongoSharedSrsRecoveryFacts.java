package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SharedSrsRecoverySnapshot;
import io.tapstate.spi.store.StorageRecoveryCommands;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Bounded original control facts; decoding never grants a CACHE replacement permission. */
final class MongoSharedSrsRecoveryFacts {
    private static final int MAX_REQUEST_BYTES=262144;
    private MongoSharedSrsRecoveryFacts() {}

    static Document encode(SharedSrsRecoverySnapshot snapshot) {
        var records=new MongoSrsConsumerRecords(snapshot.previous().scope());
        var pipelines=new ArrayList<Document>();
        snapshot.pipelines().values().forEach(pipeline -> pipelines.add(new Document("scope",MongoPipelinePreparationFacts.scope(pipeline.scope()))
                .append("claim",WorkloadClaimDocuments.stored(pipeline.claim()))));
        return new Document("formatVersion",1).append("previous",MongoCapturePreparationFacts.row(snapshot.previous()))
                .append("captureFence",WorkloadClaimDocuments.stored(snapshot.captureFence())).append("controlHash",snapshot.controlHash())
                .append("consumers",snapshot.consumers().stream().map(records::membership).toList()).append("pipelines",pipelines);
    }

    static SharedSrsRecoverySnapshot decode(Document row) {
        try {
            if(row==null || !row.keySet().equals(Set.of("formatVersion","previous","captureFence","controlHash","consumers","pipelines"))
                    || !Integer.valueOf(1).equals(row.get("formatVersion")))throw unreadable();
            var previous=MongoCapturePreparationFacts.annex(row.get("previous",Document.class));
            var records=new MongoSrsConsumerRecords(previous.scope());
            var consumers=documents(row.get("consumers")).stream().map(records::membership).toList();
            var pipelines=new TreeMap<String,SharedSrsRecoverySnapshot.Pipeline>();
            for(var entry:documents(row.get("pipelines"))) {
                if(!entry.keySet().equals(Set.of("scope","claim")))throw unreadable();
                var scope=MongoPipelinePreparationFacts.scope(entry.get("scope",Document.class));
                var pipeline=new SharedSrsRecoverySnapshot.Pipeline(scope,WorkloadClaimDocuments.readPipeline(entry.get("claim",Document.class)));
                if(pipelines.put(scope.pipelineId(),pipeline)!=null)throw unreadable();
            }
            var result=new SharedSrsRecoverySnapshot(previous,WorkloadClaimDocuments.readCapture(row.get("captureFence",Document.class)),
                    row.getString("controlHash"),consumers,pipelines);
            if(!encode(result).equals(row))throw unreadable();
            return result;
        }catch(IllegalArgumentException|NullPointerException|ClassCastException invalid){throw unreadable();}
    }

    static String hash(StorageRecoveryCommands.Manifest manifest) {
        var selection=manifest.selection();
        return CanonicalHash.ofText(JsonWriter.write(List.of("shared-srs-v2",selection.canonicalChain(),selection.physicalChain(),
                new ArrayList<>(manifest.affectedPipelineIds()),manifest.definitionHashes(),manifest.dependencyHash(),manifest.configurationRevision(),
                encode(manifest.sharedSrs()).toJson())));
    }

    /** Bound the complete pending request, including selection, dependency names and its control annex. */
    static void requireSize(Document pending) {
        try(var output=new BasicOutputBuffer();var writer=new BsonBinaryWriter(output)) {
            new DocumentCodec().encode(writer,pending,EncoderContext.builder().build());
            if(output.getSize()>MAX_REQUEST_BYTES)
                throw new TapstateException(IoError.DOCUMENT_TOO_LARGE,Map.of("id","storage-recovery"),null);
        }
    }
    private static List<Document> documents(Object value) {
        if(!(value instanceof List<?> values) || values.stream().anyMatch(entry -> !(entry instanceof Document)))throw unreadable();
        return values.stream().map(Document.class::cast).toList();
    }
    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,Map.of("id","storage-recovery","field","sharedSrs"),null);
    }
}
