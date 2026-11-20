package io.tapstate.cli;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.common.JsonReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The CLI's half of the three data-browser reads, against a tiny in-JVM server so the real request is
 * built and the real answer decoded — the JDK's own {@code HttpServer}, so the test carries no
 * dependency.
 *
 * <p>What is worth pinning here is not that a well-formed answer decodes. It is the three places this
 * decoder deliberately refuses to guess, each of which fails by rendering something plausible: an
 * answer it does not recognise must not read as an empty collection, a count the server never reported
 * must not read as zero, and a preview that does not say whether more rows remain must not read as a
 * whole collection. All three would print a sentence the reader has no reason to doubt.
 */
class HttpControlPlaneClientDataBrowserTest {

    private HttpServer server;
    private HttpControlPlaneClient client;
    private volatile String method;
    private volatile String rawPath;
    private volatile String authorization;
    private volatile String requestBody;
    private volatile int status = 200;
    private volatile String responseBody = "{}";

    @BeforeEach
    void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            method = exchange.getRequestMethod();
            rawPath = exchange.getRequestURI().getRawPath();
            authorization = exchange.getRequestHeaders().getFirst("Authorization");
            requestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
            exchange.close();
        });
        server.start();
        client = new HttpControlPlaneClient();
    }

    @AfterEach
    void stopServerAndClient() {
        // The client holds an HTTP client and its executor. Ten of these left open per run is a leak
        // the sibling test in this package already avoids by wrapping the same type.
        client.close();
        server.stop(0);
    }

    private URI base() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    private void serverAnswers(int status, String body) {
        this.status = status;
        this.responseBody = body;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> sentBody() {
        // The reader produces a string-keyed object map for a JSON object, which is what a request body
        // always is here; anything else fails the cast loudly rather than being asserted around.
        return (Map<String, Object>) JsonReader.parse(requestBody);
    }

    @Test
    void listsTheCollectionsTheServerNamedAndReachesTheRightAddressToDoIt() {
        serverAnswers(200, "{\"collections\":[{\"name\":\"order_state\",\"kind\":\"view\"},"
                + "{\"kind\":\"view\"},{\"name\":\"orders\"}]}");

        DataBrowserOutcome.Collections outcome =
                client.collections(base(), "tok", "views");

        assertThat(outcome).isEqualTo(
                new DataBrowserOutcome.Collections.Listed(List.of("order_state", "orders")));
        assertThat(method).isEqualTo("GET");
        assertThat(rawPath).isEqualTo("/api/sources/views/collections");
        assertThat(authorization).isEqualTo("Bearer tok");
    }

    @Test
    void doesNotReadAnAnswerItCannotRecogniseAsASourceWithNoCollections() {
        serverAnswers(200, "{\"items\":[]}");

        assertThat(client.collections(base(), "tok", "views"))
                .as("an empty listing states as fact that the source holds nothing; an answer this "
                        + "shell did not understand is not that fact")
                .isEqualTo(new DataBrowserOutcome.Collections.Unreachable());
    }

    @Test
    void carriesACodedRefusalBackInsteadOfAnEmptyAnswer() {
        serverAnswers(404, "{\"code\":\"TS0390001\","
                + "\"message\":\"no collection named orders\"}");

        assertThat(client.collections(base(), "tok", "views"))
                .isEqualTo(new DataBrowserOutcome.Collections.Rejected(
                        "TS0390001", "no collection named orders"));
    }

    @Test
    void reportsACountTheServerOmittedAsNothingRatherThanAsZero() {
        // A size nobody would report and an empty collection are different answers, and rendering the
        // first as the second states it as fact.
        serverAnswers(200, "{\"numOfRows\":0,\"avgObjSize\":128}");

        assertThat(client.stats(base(), "tok", "views", "order_state"))
                .isEqualTo(new DataBrowserOutcome.Stats.Reported(0L, null, 128L));
        assertThat(rawPath).isEqualTo("/api/sources/views/collections/order_state/stats");
    }

    @Test
    void doesNotReadAnAnswerThatIsNotAReportAsAReport() {
        serverAnswers(200, "[]");

        assertThat(client.stats(base(), "tok", "views", "order_state"))
                .isEqualTo(new DataBrowserOutcome.Stats.Unreachable());
    }

    @Test
    void sendsOnlyTheRequestKeysThatWereActuallyAskedFor() {
        // An absent key is the request -- no filter reads every row, no order leaves the order to the
        // database. Sending a null under the key instead says something else to the face reading it.
        serverAnswers(200, "{\"rows\":[],\"moreAvailable\":false}");

        client.find(base(), "tok", "views", "order_state", null, null, null);

        assertThat(method).isEqualTo("POST");
        assertThat(rawPath).isEqualTo("/api/sources/views/collections/order_state:find");
        assertThat(sentBody()).isEmpty();
    }

    @Test
    void sendsTheFilterOrderAndSizeItWasGiven() {
        serverAnswers(200, "{\"rows\":[],\"moreAvailable\":false}");

        client.find(base(), "tok", "views", "order_state",
                Map.of("field", "status", "op", "eq", "value", "paid"),
                new DataBrowserCall.Order("total", "desc"), 25);

        assertThat(sentBody()).containsEntry("limit", 25L);
        assertThat(sentBody()).containsEntry("sort", Map.of("field", "total", "dir", "desc"));
        assertThat(sentBody()).containsEntry("filter",
                Map.of("field", "status", "op", "eq", "value", "paid"));
    }

    @Test
    void readsTheRowsTheTotalAndWhetherMoreRemain() {
        serverAnswers(200, "{\"rows\":[{\"order_id\":\"ord_1\"}],"
                + "\"approximateTotal\":42,\"moreAvailable\":true}");

        assertThat(client
                .find(base(), "tok", "views", "order_state", null, null, null))
                .isEqualTo(new DataBrowserOutcome.Find.Read(
                        List.of(Map.of("order_id", "ord_1")), 42L, true));
    }

    @Test
    void doesNotReadAPreviewThatNeverSaidWhetherMoreRemainAsAWholeCollection() {
        // Read as false when it is missing, a truncated preview renders as the whole collection, which
        // is the one thing this field is carried to prevent.
        serverAnswers(200, "{\"rows\":[{\"order_id\":\"ord_1\"}]}");

        assertThat(client
                .find(base(), "tok", "views", "order_state", null, null, null))
                .isEqualTo(new DataBrowserOutcome.Find.Unreachable());
    }

    @Test
    void escapesASpaceInACollectionNameAsAnEscapeRatherThanAsAPlus() {
        // Form encoding spells a space as `+`, and a `+` in a path is a plus -- so the server would go
        // looking for a collection nobody named and answer, correctly, that there is no such thing.
        serverAnswers(200, "{\"numOfRows\":1}");

        client.stats(base(), "tok", "views", "order state");

        assertThat(rawPath).isEqualTo("/api/sources/views/collections/order%20state/stats");
    }
}
