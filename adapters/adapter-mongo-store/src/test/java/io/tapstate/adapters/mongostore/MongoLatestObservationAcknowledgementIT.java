package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Lost client acknowledgements are injected only after the real majority write has completed. */
@RequiresDocker
class MongoLatestObservationAcknowledgementIT {

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    private static final Instant FIRST = Instant.parse("2026-09-28T04:00:00Z");
    private static final ObservationStore.Scope RUN = new ObservationStore.Scope("inc", 1);

    @Test
    void aLostManifestAcknowledgementConfirmsOnlyTheExactlyCommittedCurrent() {
        try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("latest_manifest_ack_it");
            database.drop();
            AtomicBoolean armed = new AtomicBoolean();
            AtomicInteger injected = new AtomicInteger();
            MongoCollection<Document> manifests = loseOneAcknowledgement(
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                    "updateOne", armed, injected);
            MongoObservationStore store = new MongoObservationStore(client, manifests,
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
            assertThat(store.saveScoped(observation(FIRST, "before"), RUN)).isTrue();
            Observation current = observation(FIRST.plusSeconds(1), "after");
            armed.set(true);

            assertThat(store.saveScoped(current, RUN)).isTrue();

            assertThat(injected).hasValue(1);
            assertThat(store.readStored("flow")).contains(new ObservationStore.Stored(current, Optional.of(RUN)));
            assertThat(store.saveScoped(observation(current.observedAt(), "different"), RUN)).isFalse();
            assertThat(store.readStored("flow")).contains(new ObservationStore.Stored(current, Optional.of(RUN)));
        }
    }

    @Test
    void aLostChunkAcknowledgementConfirmsExactImmutableBytesBeforePromoting() {
        try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("latest_chunk_ack_it");
            database.drop();
            AtomicBoolean armed = new AtomicBoolean(true);
            AtomicInteger injected = new AtomicInteger();
            MongoCollection<Document> chunks = loseOneAcknowledgement(
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS),
                    "insertOne", armed, injected);
            MongoObservationStore store = new MongoObservationStore(client,
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION), chunks);
            Observation current = observation(FIRST, "x".repeat(600 * 1024));

            assertThat(store.saveScoped(current, RUN)).isTrue();

            assertThat(injected).hasValue(1);
            assertThat(chunks.countDocuments()).isEqualTo(1);
            assertThat(store.readStored("flow")).contains(new ObservationStore.Stored(current, Optional.of(RUN)));
        }
    }

    @Test
    void retryingAChunkCannotRepairDifferentBytesOrResurrectRetiredState() {
        try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("latest_chunk_immutable_it");
            database.drop();
            MongoCollection<Document> chunks = database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS);
            MongoLatestObservationStorage storage = new MongoLatestObservationStorage(client,
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION), chunks);
            Observation current = observation(FIRST, "x".repeat(600 * 1024));
            var publication = storage.publication(current, RUN);
            LatestObservationPayloadCodec.encode(current, publication);
            Document stored = chunks.find().first();
            assertThat(stored).isNotNull();
            byte[] payload = stored.get("payload", Binary.class).getData();
            byte[] digest = stored.get("chunkDigest", Binary.class).getData();
            var exact = new LatestObservationPayloadCodec.Chunk(0, payload, digest);

            publication.write(exact);
            assertThat(chunks.countDocuments()).isEqualTo(1);
            byte[] different = exact.payload();
            different[0] ^= 1;
            assertThatThrownBy(() -> publication.write(new LatestObservationPayloadCodec.Chunk(0, different, digest)))
                    .isInstanceOfSatisfying(TapstateException.class, error ->
                            assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(chunks.find().first()).isEqualTo(stored);

            chunks.updateOne(new Document("_id", stored.get("_id")),
                    new Document("$set", new Document("state", "RETIRED")));
            assertThatThrownBy(() -> publication.write(exact))
                    .isInstanceOfSatisfying(TapstateException.class, error ->
                            assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(chunks.find().first().getString("state")).isEqualTo("RETIRED");
        }
    }

    @SuppressWarnings("unchecked")
    private static MongoCollection<Document> loseOneAcknowledgement(MongoCollection<Document> target,
            String operation, AtomicBoolean armed, AtomicInteger injected) {
        return (MongoCollection<Document>) Proxy.newProxyInstance(MongoCollection.class.getClassLoader(),
                new Class<?>[] {MongoCollection.class}, (proxy, method, arguments) -> {
                    Object result;
                    try {
                        result = method.invoke(target, arguments);
                    } catch (InvocationTargetException failure) {
                        throw failure.getCause();
                    }
                    if (operation.equals(method.getName()) && armed.compareAndSet(true, false)) {
                        injected.incrementAndGet();
                        throw new MongoException("client acknowledgement lost after the real write");
                    }
                    if (result instanceof MongoCollection<?> collection) {
                        return loseOneAcknowledgement((MongoCollection<Document>) collection,
                                operation, armed, injected);
                    }
                    return result;
                });
    }

    private static Observation observation(Instant at, String position) {
        return new Observation("flow", PipelineState.RUNNING, Map.of(), Map.of(),
                Map.of("orders", position), null, at);
    }
}
