package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.testsupport.RequiresDocker;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The specification vocabulary cannot send an authenticated ad hoc HTTP GET and assert its exact response. */
@RequiresDocker
class APipelineIdBecomesUnavailableAfterCreatingADraftIT {
    @Test
    void theProbeTracksNewDraftsWithoutReservingTheirId() throws Exception {
        try (var storage = PreparedNativeStore.create();
                var server = InProcessServer.startPrepared(storage);
                var client = HttpClient.newHttpClient()) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            String authorization = "Bearer " + control.mintToken("WRITE");
            var probe = HttpRequest.newBuilder(server.baseUrl().resolve("/api/pipelines:id-availability?id=new-pipeline"))
                    .timeout(Duration.ofSeconds(20)).header("Authorization", authorization).GET().build();
            assertAvailability(client, probe, true);

            var draft = HttpRequest.newBuilder(server.baseUrl().resolve("/api/pipelines/new-pipeline/draft"))
                    .timeout(Duration.ofSeconds(20)).header("Authorization", authorization)
                    .header("Origin", server.baseUrl().toString()).header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("""
                            {"pipelineId":"new-pipeline","mode":"dag","name":"New Pipeline",
                             "graph":{"nodes":[],"edges":[],"viewport":{"x":0,"y":0,"zoom":1}}}
                            """)).build();
            var created = client.send(draft, HttpResponse.BodyHandlers.ofString());
            assertThat(created.statusCode()).as(created.body()).isEqualTo(201);
            assertAvailability(client, probe, false);
        }
    }

    private static void assertAvailability(HttpClient client, HttpRequest request, boolean available) throws Exception {
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        assertThat(JsonReader.parse(response.body())).isEqualTo(Map.of("available", available));
    }
}
