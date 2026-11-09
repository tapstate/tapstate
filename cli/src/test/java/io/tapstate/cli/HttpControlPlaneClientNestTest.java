package io.tapstate.cli;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The three nest reads over HTTP: each sends the bearer credential to its own path, decodes a 200 into what
 * the REPL renders, turns a coded refusal into its code and message, reads a body of the wrong shape as
 * unreachable rather than as an empty answer, and never throws when nothing answers.
 */
class HttpControlPlaneClientNestTest {

    private static final String REFUSAL =
            "{\"code\":\"monitor.unknown-nest-step\",\"params\":{},\"message\":\"No nest step x.\"}";

    private record Seen(String path, String query, String authorization) {
    }

    private static HttpServer server(int status, String body, AtomicReference<Seen> seen) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/pipelines/", exchange -> {
            seen.set(new Seen(exchange.getRequestURI().getRawPath(), exchange.getRequestURI().getRawQuery(),
                    exchange.getRequestHeaders().getFirst("Authorization")));
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
            exchange.close();
        });
        server.start();
        return server;
    }

    private static URI base(HttpServer server) {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    private static final URI NOBODY = URI.create("http://127.0.0.1:0");

    @Test
    void progressDecodesEachStepAndItsTablesAndNarrowsToOneStep() throws Exception {
        AtomicReference<Seen> seen = new AtomicReference<>();
        HttpServer server = server(200, """
                {"pipelineId":"order_doc","state":"RUNNING","phase":"initial load",
                 "steps":[{"step":"doc","rootTable":"orders","rootsRendered":3,"rootsEstimate":300,
                   "activity":"LOADING","waitingFor":["orders"],
                   "tables":[{"table":"orders","path":"","stage":"pending","rowsRead":null,"rowsEstimate":300,
                              "rowsStaged":null}]}]}
                """, seen);
        try {
            NestProgressOutcome outcome = new HttpControlPlaneClient().nestProgress(base(server), "tok", "order_doc",
                    "doc");

            assertThat(seen.get()).isEqualTo(new Seen("/api/pipelines/order_doc/nest/progress", "step=doc",
                    "Bearer tok"));
            assertThat(outcome).isEqualTo(new NestProgressOutcome.Found("order_doc", "RUNNING", "initial load",
                    List.of(new NestProgressOutcome.Step("doc", "orders", 3L, 300L, "LOADING", List.of("orders"),
                            List.of(new NestProgressOutcome.Table("orders", "", "pending", null, 300L, null))))));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void progressOfABodyWithNoStepsListIsUnreachableNotEmpty() throws Exception {
        HttpServer server = server(200, "{\"pipelineId\":\"order_doc\"}", new AtomicReference<>());
        try {
            assertThat(new HttpControlPlaneClient().nestProgress(base(server), "tok", "order_doc", null))
                    .isInstanceOf(NestProgressOutcome.Unreachable.class);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void inspectSendsOneKeyParameterPerColumnAndKeepsTheView() throws Exception {
        AtomicReference<Seen> seen = new AtomicReference<>();
        HttpServer server = server(200, "{\"step\":\"doc\",\"rootKey\":[1,\"a b\"]}", seen);
        try {
            NestInspectOutcome outcome = new HttpControlPlaneClient().nestInspect(base(server), "tok", "order_doc",
                    "doc", List.of("1", "a b"));

            assertThat(seen.get().path()).isEqualTo("/api/pipelines/order_doc/nest/doc/documents");
            assertThat(seen.get().query()).isEqualTo("key=1&key=a+b");
            assertThat(outcome).isInstanceOfSatisfying(NestInspectOutcome.Found.class,
                    found -> assertThat(found.view().get("step")).isEqualTo("doc"));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void explainKeepsTheAnswerWithItsTree() throws Exception {
        AtomicReference<Seen> seen = new AtomicReference<>();
        HttpServer server = server(200, "{\"tree\":{\"step\":\"doc\"},\"build\":\"no\",\"samples\":[],\"warnings\":[]}",
                seen);
        try {
            NestExplainOutcome outcome = new HttpControlPlaneClient().nestExplain(base(server), "tok", "order_doc",
                    "doc");

            assertThat(seen.get().path()).isEqualTo("/api/pipelines/order_doc/nest/doc/explain");
            assertThat(outcome).isInstanceOfSatisfying(NestExplainOutcome.Found.class,
                    found -> assertThat(found.view().get("tree")).isEqualTo(Map.of("step", "doc")));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void anAnswerWithoutWhatEachReadPromisesIsUnreachable() throws Exception {
        HttpServer server = server(200, "{\"something\":\"else\"}", new AtomicReference<>());
        try {
            HttpControlPlaneClient client = new HttpControlPlaneClient();
            assertThat(client.nestInspect(base(server), "tok", "p", "doc", List.of("1")))
                    .isInstanceOf(NestInspectOutcome.Unreachable.class);
            assertThat(client.nestExplain(base(server), "tok", "p", "doc"))
                    .isInstanceOf(NestExplainOutcome.Unreachable.class);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void aCodedRefusalComesBackAsItsCodeAndMessageOnEveryRead() throws Exception {
        HttpServer server = server(404, REFUSAL, new AtomicReference<>());
        try {
            HttpControlPlaneClient client = new HttpControlPlaneClient();
            assertThat(client.nestProgress(base(server), "tok", "p", "x"))
                    .isEqualTo(new NestProgressOutcome.Rejected("monitor.unknown-nest-step", "No nest step x."));
            assertThat(client.nestInspect(base(server), "tok", "p", "x", List.of("1")))
                    .isEqualTo(new NestInspectOutcome.Rejected("monitor.unknown-nest-step", "No nest step x."));
            assertThat(client.nestExplain(base(server), "tok", "p", "x"))
                    .isEqualTo(new NestExplainOutcome.Rejected("monitor.unknown-nest-step", "No nest step x."));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void nothingAnsweringIsUnreachableAndNeverThrows() {
        HttpControlPlaneClient client = new HttpControlPlaneClient();
        assertThat(client.nestProgress(NOBODY, "tok", "p", null)).isInstanceOf(NestProgressOutcome.Unreachable.class);
        assertThat(client.nestInspect(NOBODY, "tok", "p", "doc", List.of("1")))
                .isInstanceOf(NestInspectOutcome.Unreachable.class);
        assertThat(client.nestExplain(NOBODY, "tok", "p", "doc")).isInstanceOf(NestExplainOutcome.Unreachable.class);
    }
}
