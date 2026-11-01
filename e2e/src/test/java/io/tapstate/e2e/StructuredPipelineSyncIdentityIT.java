package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.http.HttpResponse;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Structured Pipeline writes keep their sync identities through the real configuration store. */
class StructuredPipelineSyncIdentityIT {

    @BeforeAll
    static void prerequisites() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void anonymousAndNamedOutputsKeepTheirIdentityEtagAndIntegrityChecks(Tiers tier) {
        try (var storage = PreparedNativeStore.create(); ServerHandle server = tier.launch(storage);
                var mongo = MongoClients.create(storage.configurationUri())) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(Map.of("input.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: orders_input
                    connector: mongodb
                    config: { isUri: true, uri: "%s" }
                    mode: cdc
                    tables: [orders]
                    """.formatted(storage.masterUri())));
            try (var source = MongoClients.create(storage.masterUri())) {
                source.getDatabase(storage.masterDatabase()).getCollection("orders")
                        .insertOne(new Document("_id", 1).append("value", "first"));
            }
            control.discoverSchema("orders_input", "mongodb", Map.of("isUri", true, "uri", storage.masterUri()));

            List<List<Map<String, Object>>> outputs = List.of(
                    List.of(Map.of("source", "views")),
                    List.of(Map.of("source", "views", "rename", Map.of("prefix", "first_")),
                            Map.of("id", "warehouse", "source", "views", "rename", Map.of("prefix", "second_")),
                            Map.of("source", "views", "rename", Map.of("prefix", "third_"))),
                    List.of(Map.of("id", "authored", "source", "views")));
            List<List<String>> identities = List.of(List.of("sync_1"),
                    List.of("sync_1", "warehouse", "sync_3"), List.of("authored"));
            var pipelines = SystemCollections.PIPELINES.on(mongo.getDatabase(storage.configurationDatabase()));
            for (int index = 0; index < outputs.size(); index++) {
                String id = "sync_identity_" + index;
                String path = "/api/pipelines/" + id;
                Map<String, Object> input = pipeline(id, outputs.get(index));
                HttpResponse<String> created = control.sourceRequest("POST", "/api/pipelines", input, null, 201);
                String etag = assertIdentity(created, id, identities.get(index));
                assertThat(created.headers().firstValue("Location")).contains(path);
                assertThat(assertIdentity(control.sourceRequest("GET", path, null, null, 200),
                        id, identities.get(index))).isEqualTo(etag);

                HttpResponse<String> echoed = control.sourceRequest("PUT", path, json(created.body()), etag, 200);
                assertThat(assertIdentity(echoed, id, identities.get(index))).isEqualTo(etag);
                assertThat(assertIdentity(control.sourceRequest("PUT", path, input, etag, 200),
                        id, identities.get(index))).isEqualTo(etag);
                Map<String, Object> changed = new LinkedHashMap<>(input);
                changed.put("metadata", Map.of("description", "Updated sync pipeline"));
                HttpResponse<String> replaced = control.sourceRequest("PUT", path, changed, etag, 200);
                String updatedEtag = assertIdentity(replaced, id, identities.get(index));
                assertThat(updatedEtag).isNotEqualTo(etag);
                assertThat(assertIdentity(control.sourceRequest("GET", path, null, null, 200),
                        id, identities.get(index))).isEqualTo(updatedEtag);
                assertThat(json(control.sourceRequest("PUT", path, input, etag, 412).body()))
                        .containsEntry("code", "pipeline.version-conflict");

                Document key = new Document("_id", id);
                Document stored = pipelines.find(key).first();
                assertThat(stored).isNotNull();
                Document bsonReadback = Document.parse(stored.toJson());
                PipelineResource reconstructed = (PipelineResource) new DslParser()
                        .fromTree(bsonReadback.get("body", Document.class));
                assertThat(CanonicalHash.of(reconstructed)).isEqualTo(stored.getString("contentHash"))
                        .isEqualTo(json(replaced.body()).get("contentHash"));
                assertThat(((ServeBlock.Inline) reconstructed.serve()).sync()).extracting(SyncElement::id)
                        .containsExactlyElementsOf(identities.get(index));

                for (Document corruption : List.of(new Document("body.id", "another_pipeline"),
                        new Document("body.serve.sync.0.source", "another_target"),
                        new Document("contentHash", "incorrect"))) {
                    try {
                        assertThat(pipelines.updateOne(key, new Document("$set", corruption)).getMatchedCount())
                                .isEqualTo(1);
                        Map<String, Object> error = json(control.sourceRequest("GET", path, null, null, 500).body());
                        assertThat(error).containsEntry("code", "io.document-unreadable");
                        assertThat(((Map<?, ?>) error.get("params")).get("field")).isEqualTo("body");
                    } finally {
                        pipelines.replaceOne(key, stored);
                    }
                }
                assertThat(assertIdentity(control.sourceRequest("GET", path, null, null, 200),
                        id, identities.get(index))).isEqualTo(updatedEtag);
            }

            Map<String, Object> collision = pipeline("sync_collision", List.of(
                    Map.of("source", "views"), Map.of("id", "sync_1", "source", "views")));
            assertThat(json(control.sourceRequest("POST", "/api/pipelines", collision, null, 400).body()))
                    .containsEntry("code", "dsl.duplicate-id");
            control.sourceRequest("GET", "/api/pipelines/sync_collision", null, null, 404);
        }
    }

    private static Map<String, Object> pipeline(String id, List<Map<String, Object>> sync) {
        return Map.of("id", id, "sources", List.of("orders_input"),
                "serve", Map.of("from", "orders", "sync", sync));
    }

    private static String assertIdentity(HttpResponse<String> response, String id, List<String> identities) {
        Map<String, Object> body = json(response.body());
        assertThat(body).containsEntry("id", id);
        var serve = (Map<?, ?>) body.get("serve");
        List<?> sync = (List<?>) serve.get("sync");
        assertThat(sync.stream().map(value -> (String) ((Map<?, ?>) value).get("id")).toList())
                .containsExactlyElementsOf(identities);
        String hash = (String) body.get("contentHash");
        assertThat(hash).matches("[0-9a-f]{64}");
        assertThat(response.headers().firstValue("ETag")).contains("\"" + hash + "\"");
        return "\"" + hash + "\"";
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> json(String value) {
        return (Map<String, Object>) JsonReader.parse(value);
    }
}
