package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import io.tapstate.core.common.JsonReader;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.http.HttpResponse;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Both Source forms use saved, encrypted credentials through the shipped HTTP and real Atlas PDK. */
@RequiresDocker
class RealAtlasSourceHttpIT {

    private static final String CONNECTOR = "mongodb-atlas";
    private static final String INVALID_PASSWORD = "invalid-atlas-http-password";

    @BeforeAll
    static void requireRealInputs() {
        assumeTrue(System.getenv("TAPSTATE_ATLAS_STANDARD_URI") != null,
                "a controlled resolved-host Atlas URI is required");
        RealConnectorGate.require(CONNECTOR);
    }

    @ParameterizedTest(name = "{0}/URI+STANDARD")
    @EnumSource(AtlasRuntime.Mode.class)
    void bothSourceFormsSurviveFailedPreviewsAndUseTheRealPdk(AtlasRuntime.Mode mode) throws Exception {
        String suffix = (mode == AtlasRuntime.Mode.CLOUD ? "cl" : "op") + "_"
                + UUID.randomUUID().toString().substring(0, 8);
        String database = "ts_plan_hs_" + suffix;
        String base = System.getenv("TAPSTATE_ATLAS_STANDARD_URI");
        int slash = base.indexOf('/', base.indexOf("://") + 3);
        int query = base.indexOf('?', slash);
        String uri = base.substring(0, slash + 1) + database + (query < 0 ? "" : base.substring(query));
        String metadataUri = SharedMongo.replicaSetUrl("ts_plan_hm_" + suffix);
        try (var atlas = MongoClients.create(uri);
             AutoCloseable cleanup = () -> atlas.getDatabase(database).drop();
             var rawMetadata = MongoClients.create(metadataUri);
             AtlasRuntime runtime = new AtlasRuntime(mode, metadataUri);
             ServerHandle server = runtime.launch(Tiers.REAL_PROCESS)) {
            atlas.getDatabase(database).getCollection("probe").insertOne(new Document("_id", "http-proof").append("value", 1));
            ControlPlane control = runtime.control(server, true);
            runtime.register(control, CONNECTOR);
            Map<String, Object> standardSettings = standardSettings(uri);
            String password = standardSettings.get("password").toString();
            for (boolean standard : List.of(false, true)) {
                String id = standard ? "atlas_standard" : "atlas_uri";
                Map<String, Object> settings = standard ? standardSettings : Map.of("isUri", true, "uri", uri);
                HttpResponse<String> created = control.sourceRequest("POST", "/api/sources",
                        draft(id, settings, "initial"), null, 201);
                String etag = created.headers().firstValue("ETag").orElseThrow();
                assertThat(etag).matches("\"[0-9a-f]{64}\"");
                Document stored = SystemCollections.ARTIFACTS.on(rawMetadata.getDatabase("ts_plan_hm_" + suffix))
                        .find(new Document("_id", id)).first();
                assertThat(stored != null && stored.get("body", Document.class).get("config") instanceof String)
                        .as("the PDK receives a saved config whose database representation is ciphertext").isTrue();
                HttpResponse<String> saved = control.sourceRequest("GET", "/api/sources/" + id, null, null, 200);
                assertNoSecrets(saved, password);
                Map<String, Object> view = object(saved.body());
                Map<String, Object> metadata = object(view.get("metadata"));
                if (mode == AtlasRuntime.Mode.CLOUD) {
                    assertThat(metadata).containsEntry("cloud", true).containsEntry("user_id", "atlas-data-user");
                } else assertThat(metadata).doesNotContainKeys("cloud", "user_id");

                Map<String, Object> connection = Map.of("id", id, "connectorId", CONNECTOR,
                        "settings", object(view.get("config")));
                test(control, connection, "PASSED", password);
                HttpResponse<String> discovered = control.sourceRequest("POST", "/api/connections:discover-schema",
                        connection, null, 200);
                assertNoSecrets(discovered, password);
                assertThat((List<?>) object(discovered.body()).get("tables")).anySatisfy(table ->
                        assertThat(object(table)).containsEntry("name", "probe"));
                HttpResponse<String> storedSchema = control.sourceRequest("GET", "/api/sources/" + id + "/schema",
                        null, null, 200);
                assertThat(object(storedSchema.body()).get("tables")).isEqualTo(object(discovered.body()).get("tables"));

                Map<String, Object> bad = new LinkedHashMap<>(settings);
                if (standard) bad.put("password", INVALID_PASSWORD);
                else {
                    int authority = uri.indexOf("://") + 3;
                    int at = uri.indexOf('@', authority);
                    int separator = uri.indexOf(':', authority);
                    assertThat(at > separator && separator > authority).isTrue();
                    bad.put("uri", uri.substring(0, separator + 1) + INVALID_PASSWORD + uri.substring(at));
                }
                test(control, Map.of("id", id, "connectorId", CONNECTOR, "settings", bad), "FAILED", password);
                if (standard) {
                    Map<String, Object> unreachable = new LinkedHashMap<>(standardSettings);
                    unreachable.put("host", "127.0.0.1:1");
                    unreachable.put("additionalString", "authSource=admin&tls=true&serverSelectionTimeoutMS=1000&connectTimeoutMS=1000");
                    test(control, Map.of("id", id, "connectorId", CONNECTOR, "settings", unreachable), "FAILED", password);
                }
                HttpResponse<String> afterFailure = control.sourceRequest("GET", "/api/sources/" + id, null, null, 200);
                assertThat(afterFailure.headers().firstValue("ETag")).contains(etag);
                assertNoSecrets(afterFailure, password);
                test(control, connection, "PASSED", password);
                HttpResponse<String> replaced = control.sourceRequest("PUT", "/api/sources/" + id,
                        draft(id, settings, "updated"), etag, 200);
                String changed = replaced.headers().firstValue("ETag").orElseThrow();
                assertThat(changed).isNotEqualTo(etag);
                assertNoSecrets(replaced, password);
                control.sourceRequest("DELETE", "/api/sources/" + id, null, changed, 204);
                control.sourceRequest("GET", "/api/sources/" + id, null, null, 404);
            }
            runtime.assertAuthenticationBoundary();
        }
    }

    private static void test(ControlPlane control, Map<String, Object> connection, String outcome, String password) {
        HttpResponse<String> response = control.sourceRequest("POST", "/api/connections:test", connection, null, 200);
        assertNoSecrets(response, password);
        Map<String, Object> report = object(response.body());
        assertThat(report.get("outcome")).isEqualTo(outcome);
        assertThat((List<?>) report.get("checks")).isNotEmpty();
        if (outcome.equals("FAILED")) assertThat((List<?>) report.get("checks")).anySatisfy(check ->
                assertThat(object(check)).containsEntry("status", "FAILED"));
    }

    private static void assertNoSecrets(HttpResponse<String> response, String password) {
        assertThat(response.body().contains(password)).as("HTTP never returns the saved password").isFalse();
        assertThat(response.body().contains(INVALID_PASSWORD)).as("diagnostics never echo the submitted password").isFalse();
    }

    private static Map<String, Object> draft(String id, Map<String, Object> settings, String description) {
        return Map.of("id", id, "metadata", Map.of("description", description), "connector", CONNECTOR,
                "config", settings, "mode", "cdc", "tables", List.of(Map.of("type", "literal", "name", "probe")),
                "options", Map.of(), "experimental", Map.of(), "clearSecrets", List.of());
    }

    private static Map<String, Object> standardSettings(String uri) {
        ConnectionString connection = new ConnectionString(uri);
        int query = uri.indexOf('?');
        return Map.of("isUri", false, "host", String.join(",", connection.getHosts()), "database", connection.getDatabase(),
                "user", connection.getCredential().getUserName(), "password", new String(connection.getCredential().getPassword()),
                "additionalString", query < 0 ? "" : uri.substring(query + 1));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Object value) {
        Object parsed = value instanceof String json ? JsonReader.parse(json) : value;
        if (!(parsed instanceof Map<?, ?>)) throw new AssertionError("the Source wire returned no object");
        return (Map<String, Object>) parsed;
    }
}
