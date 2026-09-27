package io.tapstate.app;

import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.MongoDBContainer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The store-backed control plane, started over a real replica set and signed in to, for the cases that
 * apply batches from projects. Each instance gets a database of its own, so cases sharing one container
 * never see each other's resources.
 */
final class ProjectApplyServer implements AutoCloseable {

    /** One HTTP answer: its status and its decoded body. */
    record Answer(int status, Map<?, ?> body) {
    }

    private final ConfigurableApplicationContext context;
    private final RestClient client;
    private final String token;

    private ProjectApplyServer(ConfigurableApplicationContext context, RestClient client, String token) {
        this.context = context;
        this.client = client;
        this.token = token;
    }

    static ProjectApplyServer start(MongoDBContainer replicaSet) {
        String database = "project_" + Long.toUnsignedString(System.nanoTime(), 16);
        ConfigurableApplicationContext context = new SpringApplicationBuilder(AssemblyApp.class)
                .properties(
                        "tapstate.store.mongo.enabled=true",
                        "tapstate.store.mongo.uri=" + replicaSet.getReplicaSetUrl(database),
                        "tapstate.store.mongo.server-selection-timeout=5s")
                .run("--server.address=127.0.0.1", "--server.port=0");
        int port = ((WebServerApplicationContext) context).getWebServer().getPort();
        RestClient client = RestClient.create("http://127.0.0.1:" + port);
        client.post().uri("/auth/bootstrap").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "admin", "password", "s3cret")).retrieve().toBodilessEntity();
        Map<?, ?> login = client.post().uri("/auth/login").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "admin", "password", "s3cret")).retrieve().body(Map.class);
        return new ProjectApplyServer(context, client, (String) login.get("token"));
    }

    /** Applies {@code documents} as one batch from {@code project}, or from no project when it is null. */
    Answer apply(String project, String... documents) {
        List<Map<String, Object>> drafts = new ArrayList<>();
        for (String document : documents) {
            drafts.add(Map.of("content", document));
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("drafts", drafts);
        if (project != null) {
            body.put("project", project);
        }
        return client.post().uri("/api/artifacts:apply")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .exchange((request, response) -> new Answer(response.getStatusCode().value(),
                        response.bodyTo(Map.class)));
    }

    /** The stored artifact's canonical form, as the server returns it. */
    String canonical(String id) {
        Map<?, ?> got = client.get().uri("/api/artifacts/" + id)
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .retrieve().body(Map.class);
        return (String) got.get("canonicalForm");
    }

    @Override
    public void close() {
        context.close();
    }

    /** The store bridge and the control plane, without the rest of the process. */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({StoreConfiguration.class, ControlPlaneConfiguration.class})
    static class AssemblyApp {
    }
}
