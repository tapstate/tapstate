package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The public precheck must accept BSON decimal fields on either side of a collection mapping. */
@RequiresDocker
class AtlasDecimalValidationIT {
    @Test
    void fullColumnPrecheckAcceptsTheDecimalModelFromAtlas() {
        RealConnectorGate.require("mongodb-atlas");
        try (var storage = PreparedNativeStore.create();
                var server = startWithValidation(storage);
                var mongo = MongoClients.create(storage.masterUri())) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector("mongodb-atlas", ConnectorJars.bytesFor("mongodb-atlas"));

            var resources = new LinkedHashMap<String, String>();
            for (String id : List.of("input", "target")) {
                String database = id.equals("input") ? storage.masterDatabase() : storage.cacheDatabase();
                mongo.getDatabase(database).getCollection("customer").insertOne(
                        new Document("_id", "customer-1").append("balance", Decimal128.parse("123456789012345678.99")));
                var settings = Map.of("isUri", true, "uri", SharedMongo.replicaSetUrl(database), "database", database);
                resources.put(id + ".tap.yml", """
                        version: tapstate/v1
                        kind: source
                        id: %s
                        connector: mongodb-atlas
                        config: %s
                        %s
                        """.formatted(id, JsonWriter.write(settings), id.equals("input") ? "mode: snapshot\ntables: [customer]" : ""));
            }
            resources.put("flow.tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: flow
                    source: input
                    serve:
                      from: input.customer
                      sync:
                        - { id: out, source: target, write_mode: upsert }
                    """);
            control.apply(resources);
            var mapping = Map.of("mappingId", "pair", "outputId", "out", "sourceId", "input", "table", "customer");
            var keys = Map.of("id", UUID.randomUUID().toString(), "fields", List.of(Map.of("source", "_id", "target", "_id")));
            var input = Map.of("pipelineId", "flow", "taskId", "check", "expectedRevision", "0", "method", "FULL_COLUMN",
                    "mappings", List.of(mapping), "confirmations", Map.of("pair", keys));
            var response = control.sourceRequest("POST", "/api/validation:evaluate", input, null, 200);
            var evaluated = (Map<?, ?>) JsonReader.parse(response.body());
            assertThat(evaluated.get("fingerprint")).isInstanceOf(String.class);
            assertThat((List<?>) evaluated.get("mappings")).hasSize(1);
        }
    }

    private static ServerHandle startWithValidation(PreparedNativeStore storage) {
        var timeout = Duration.ofSeconds(5);
        var connection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                storage.cacheUri(), storage.cacheDatabase(), null);
        try (var schema = MongoConfigurationDomain.prepareValidationSchema(
                new MongoConnectionSettings(storage.configurationUri(), null, timeout), storage.clusterId(), UUID.randomUUID());
                var cache = MongoCacheDomain.connect(connection, timeout, "validation-test")) {
            schema.prepareCache(cache);
            while (!schema.advance(20).ready()) {
                // Drain the bounded migration batches before exposing the validation API.
            }
        }
        return InProcessServer.startPrepared(storage);
    }
}
