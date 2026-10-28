package io.tapstate.adapters.alerting;

import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.common.TapstateException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class WebhookStartupGuardTest {
    @Test void perClientTransportDoesNotDependOnCachedJdkHeaderConfiguration() throws Exception {
        probe("65536", "false", "header");
    }
    @Test void perClientRetryPolicyDoesNotDependOnCachedJdkRetryFlag() throws Exception {
        probe("16384", "true", "retry");
    }
    @Test void perClientRetryPolicyDoesNotDependOnEmptyJdkProperty() throws Exception {
        probe("16384", "false", "empty");
    }
    private static void probe(String headerLimit, String allRetry, String mode) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process process = new ProcessBuilder(java, "-Djdk.http.maxHeaderSize=" + headerLimit, "-Djdk.httpclient.disableRetryConnect=true",
                "-Djdk.httpclient.enableAllMethodRetry=" + allRetry, "-cp", System.getProperty("java.class.path"), Probe.class.getName(), mode)
                .redirectErrorStream(true).start();
        if (!process.waitFor(10, TimeUnit.SECONDS)) { process.destroyForcibly(); throw new AssertionError("startup probe timed out"); }
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(process.exitValue()).as(output).isZero();
        assertThat(output).contains("PER_CLIENT_BOUNDED");
    }
    public static final class Probe {
        public static void main(String[] args) throws Exception {
            if (args[0].equals("empty")) { System.setProperty("jdk.httpclient.enableAllMethodRetry", ""); }
            try (ServerSocket server = new ServerSocket(0, 2, InetAddress.getLoopbackAddress());
                    HttpClient warm = HttpClient.newHttpClient()) {
                Thread responder = Thread.ofPlatform().daemon(true).start(() -> {
                    try {
                        for (int i = 0; i < (args[0].equals("header") ? 2 : 1); i++) {
                            try (var socket = server.accept()) {
                                socket.setSoTimeout(3000); WebhookChannelTest.readRequest(socket.getInputStream());
                                String large = i == 0 ? "" : "X-Large: " + "x".repeat(17000) + "\r\n";
                                socket.getOutputStream().write(("HTTP/1.1 200 OK\r\n" + large + "Content-Length: 0\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
                                socket.getOutputStream().flush();
                            }
                        }
                    } catch (Exception failure) { throw new AssertionError(failure); }
                });
                URI local = URI.create("http://localhost:" + server.getLocalPort() + "/");
                warm.send(HttpRequest.newBuilder(local).GET().build(), HttpResponse.BodyHandlers.discarding());
                System.setProperty("jdk.http.maxHeaderSize", "16384");
                System.setProperty("jdk.httpclient.enableAllMethodRetry", args[0].equals("empty") ? "" : "false");
                if (args[0].equals("header")) {
                int cached = warm.send(HttpRequest.newBuilder(local).GET().build(), HttpResponse.BodyHandlers.discarding()).statusCode();
                if (cached != 200) { throw new AssertionError("cached parser fixture did not accept its original limit"); }
                }
                var binding = new TargetBinding(new AlarmKey.Scope("org", "cluster"), "receiver", "target", "binding");
                try (var channel = new WebhookChannel(io.tapstate.core.alerting.AlertChannelPolicy.Deployment.ON_PREM, binding, URI.create("https://localhost/notify"),
                        Set.of(new WebhookChannel.Origin("localhost", 443)), SSLContext.getDefault(), null, null, Clock.systemUTC())) {
                    System.out.println("PER_CLIENT_BOUNDED");
                } catch (TapstateException refused) { System.out.println("GUARD_REFUSED"); }
                responder.join(1000);
            }
        }
    }
}
