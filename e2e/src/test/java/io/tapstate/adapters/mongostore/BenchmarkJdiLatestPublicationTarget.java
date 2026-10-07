package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.ObservationStore;
import org.bson.codecs.DocumentCodec;

/** Runs one actual latest publication beside only the current build's selected artifact libraries. */
public final class BenchmarkJdiLatestPublicationTarget {
    private BenchmarkJdiLatestPublicationTarget() { }
    public static void main(String[] args) {
        try { run(args[0]); }
        catch (Throwable failure) { BenchmarkJdiEncoderTarget.reportFailure(failure); System.exit(2); }
    }

    private static void run(String mode) throws Exception {
        BenchmarkJdiEncoderTarget.origin(MongoObservationStore.class);
        BenchmarkJdiEncoderTarget.origin(MongoRateHistoryStore.class);
        BenchmarkJdiEncoderTarget.origin(DocumentCodec.class);
        BenchmarkJdiEncoderTarget.origin(Observation.class);
        BenchmarkJdiEncoderTarget.origin(LatestObservationPayloadCodec.class);
        BenchmarkJdiEncoderTarget.phase("WIRE_SETUP");
        String uri = System.getenv("TAPSTATE_JDI_WITNESS_MONGO_URI");
        if (uri == null || uri.isBlank()) { throw new AssertionError("latest store witness has no private connection input"); }
        try (var client = MongoClients.create(uri)) {
            var database = client.getDatabase("jdi_cost_witness");
            var collection = database.getCollection("pipeline_observation");
            var chunks = database.getCollection("pipeline_observation_chunks");
            collection.drop(); chunks.drop();
            var store = new MongoObservationStore(client, collection, chunks);
            var scope = new ObservationStore.Scope("reactor-cost-owner", 7L);
            Observation first = BenchmarkJdiEncoderTarget.observation();
            if (!store.saveScoped(first, scope)) { throw new AssertionError("latest setup publication was rejected"); }
            Observation next = new Observation(first.pipelineId(), first.state(), first.metrics(), first.snapshot(),
                    first.positions(), first.failure(), first.observedAt().plusSeconds(1));
            BenchmarkJdiEncoderTarget.ready();
            BenchmarkJdiEncoderTarget.phase("LATEST_ENCODING");
            if (!store.saveScoped(next, scope)) { throw new AssertionError("latest measured publication was rejected"); }
            if (mode.equals("store-latest-extra")) { LatestObservationPayloadCodec.encode(next, InlineChunks.INSTANCE); }
            BenchmarkJdiEncoderTarget.done();
            if (!store.readStored(next.pipelineId()).orElseThrow().observation().equals(next)) {
                throw new AssertionError("latest measured publication did not round trip");
            }
        }
    }

    private enum InlineChunks implements LatestObservationPayloadCodec.ChunkWriter {
        INSTANCE;
        @Override public void begin() { throw new AssertionError("small redundant encoding used chunks"); }
        @Override public void write(LatestObservationPayloadCodec.Chunk chunk) { throw new AssertionError("small redundant encoding used chunks"); }
    }
}
