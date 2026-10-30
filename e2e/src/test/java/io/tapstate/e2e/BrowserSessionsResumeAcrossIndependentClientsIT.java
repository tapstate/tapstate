package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;

/** The declarative e2e vocabulary cannot express cookie transport, exchange and revocation. */
class BrowserSessionsResumeAcrossIndependentClientsIT {
    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void independentClientsResumeUntilTheSharedSessionIsRevoked() throws Exception {
        try (PreparedNativeStore storage = PreparedNativeStore.create("browser-admin", "browser-password");
                ServerHandle server = RealProcessServer.startPrepared(storage.configurationUri(), "127.0.0.1",
                        port -> java.util.List.of("--tapstate.cluster.id=" + storage.clusterId(),
                                "--tapstate.browser-auth.allow-insecure-loopback=true", "--tapstate.hz.member-port=0",
                                "--tapstate.hz.jet.cooperative-thread-count=2"))) {
            storage.loginAndAwaitBusiness(new ControlPlane(server.baseUrl()));
            String origin = server.baseUrl().toString().replaceAll("/$", "");
            HttpClient first = HttpClient.newHttpClient();
            HttpClient second = HttpClient.newHttpClient();
            HttpResponse<String> login = first.send(HttpRequest.newBuilder(server.baseUrl().resolve("/auth/browser/login"))
                    .header("Origin", origin).header("X-Tapstate-Browser-Request", "1")
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString("{\"username\":\"browser-admin\",\"password\":\"browser-password\"}"))
                    .build(), HttpResponse.BodyHandlers.ofString());
            assertThat(login.statusCode()).isEqualTo(200);
            String cookie = login.headers().firstValue("Set-Cookie").orElseThrow().split(";", 2)[0];
            assertThat(login.body()).contains("\"principal\":\"browser-admin\"", "sessionId")
                    .doesNotContain("tss_", "sessionToken");
            for (HttpClient client : new HttpClient[] {first, second}) {
                HttpResponse<String> resumed = client.send(request(origin, "session", cookie), HttpResponse.BodyHandlers.ofString());
                assertThat(resumed.statusCode()).isEqualTo(200);
                assertThat(resumed.headers().allValues("Set-Cookie")).isEmpty();
                assertThat(resumed.body()).contains("\"principal\":\"browser-admin\"");
            }
            assertThat(first.send(request(origin, "logout", cookie), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(204);
            assertThat(second.send(request(origin, "session", cookie), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(401);
        }
    }

    private HttpRequest request(String origin, String action, String cookie) {
        return HttpRequest.newBuilder(java.net.URI.create(origin + "/auth/browser/" + action))
                .header("Origin", origin).header("X-Tapstate-Browser-Request", "1")
                .header("Cookie", cookie).POST(HttpRequest.BodyPublishers.noBody()).build();
    }
}
