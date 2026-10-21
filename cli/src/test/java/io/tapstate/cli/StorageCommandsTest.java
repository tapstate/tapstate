package io.tapstate.cli;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.control.client.ControlResponse;
import io.tapstate.core.common.JsonReader;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class StorageCommandsTest {
    @Test void theActualConnectedCommandParsesASecretFileAndSendsOnePostWithoutFailover(@TempDir Path work) throws Exception {
        var paths = new ArrayList<String>(); var bodies = new ArrayList<Object>(); var calls = new AtomicInteger();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/", exchange -> {
            paths.add(exchange.getRequestURI().toString()); calls.incrementAndGet();
            assertThat(exchange.getRequestMethod()).isEqualTo("POST");
            assertThat(exchange.getRequestHeaders().getFirst("Authorization")).isEqualTo("Bearer machine-sentinel");
            bodies.add(JsonReader.parse(new String(exchange.getRequestBody().readAllBytes(),java.nio.charset.StandardCharsets.UTF_8)));
            byte[] result = (exchange.getRequestURI().getPath().equals("/api/configuration/export")
                    ? "{\"transfer\":\"opaque-ciphertext\",\"defaultSourceId\":\"views\"}"
                    : "{\"operationId\":\"original\",\"phase\":\"CONFIRMED\"}").getBytes();
            exchange.sendResponseHeaders(200,result.length); try(var out=exchange.getResponseBody()) { out.write(result); }
        }); server.start();
        try (var client = new HttpControlPlaneClient()) {
            var cli = Cli.newCommandLine(); var output = new StringWriter(); cli.setOut(new PrintWriter(output)); cli.setErr(new PrintWriter(output));
            var repl = new Repl(cli,work,client); var base = URI.create("http://127.0.0.1:"+server.getAddress().getPort());
            repl.session().connect(List.of(base),base,null); repl.session().authenticateMachine("machine-sentinel",List.of(base));
            Files.writeString(work.resolve("request.json"),"{\"importId\":\"original\",\"transferKey\":\"sentinel-secret\"}");
            repl.dispatch("configuration import commit --file request.json");
            assertThat(repl.lastExitCode()).isEqualTo(Cli.EXIT_OK); assertThat(paths).containsExactly("/api/configuration/import/commit");
            assertThat(output.toString()).doesNotContain("sentinel-secret","machine-sentinel"); assertThat(bodies).hasSize(1);
            String transferKey = "A".repeat(43);
            Files.writeString(work.resolve("export.json"), io.tapstate.core.common.JsonWriter.write(Map.of("resourceIds", List.of("flow"), "usernames", List.of(), "transferKey", transferKey)));
            repl.dispatch("configuration export --file export.json");
            assertThat(repl.lastExitCode()).isEqualTo(Cli.EXIT_OK); assertThat(paths.getLast()).isEqualTo("/api/configuration/export");
            assertThat(output.toString()).contains("opaque-ciphertext").doesNotContain(transferKey);
            repl.dispatch("storage status"); assertThat(repl.lastExitCode()).isEqualTo(Cli.EXIT_OK);
            repl.dispatch("storage test mongodb://sentinel-secret"); assertThat(repl.lastExitCode()).isEqualTo(Cli.EXIT_USAGE);
            assertThat(calls).hasValue(3); assertThat(output.toString()).contains("cli.storage-usage").doesNotContain("sentinel-secret");
        } finally { server.stop(0); }
    }
    @Test void anUnknownWriteResultIsNotRetriedAgainstTheSameOrAnotherMember(@TempDir Path work) throws Exception {
        var count = new AtomicInteger(); var server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/",exchange -> { count.incrementAndGet(); exchange.getRequestBody().readAllBytes(); exchange.close(); }); server.start();
        try(var client=new HttpControlPlaneClient()) {
            var cli=Cli.newCommandLine(); var output=new StringWriter(); cli.setOut(new PrintWriter(output)); cli.setErr(new PrintWriter(output));
            var repl=new Repl(cli,work,client); var base=URI.create("http://127.0.0.1:"+server.getAddress().getPort());
            repl.session().connect(List.of(base),base,null); repl.session().authenticateMachine("machine",List.of(base));
            Files.writeString(work.resolve("request.json"),"{\"scope\":\"CLUSTER\",\"pipelineIds\":[]}");
            repl.dispatch("storage recovery plan --file request.json");
            assertThat(repl.lastExitCode()).isNotEqualTo(Cli.EXIT_OK); assertThat(count).hasValue(1);
        } finally { server.stop(0); }
    }
}
