package io.tapstate.control.restapi;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.context.annotation.Bean;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;

import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A single embedded web context boots on one port, and every {@code @RestController} handler is
 * served under the {@code /api} prefix and nowhere else. This witnesses the single-context,
 * single-port HTTP substrate and the one route prefix the control verbs project onto.
 *
 * <p>The context is booted programmatically (not through the Spring JUnit extension) so the module
 * needs no test-harness dependency beyond the reactor's JUnit line.
 */
class RestApiWebContextTest {

    private static ConfigurableApplicationContext context;
    private static int port;

    @BeforeAll
    static void startServer() {
        context = new SpringApplicationBuilder(TestApp.class)
                .properties("server.port=0")
                .run();
        port = ((WebServerApplicationContext) context).getWebServer().getPort();
    }

    @AfterAll
    static void stopServer() {
        if (context != null) {
            context.close();
        }
    }

    private RestClient client() {
        return RestClient.create("http://127.0.0.1:" + port);
    }

    @Test
    void restControllerHandlersAreServedUnderTheApiPrefix() {
        ResponseEntity<String> underApi = client().get().uri("/api/probe").retrieve().toEntity(String.class);
        assertThat(underApi.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(underApi.getBody()).isEqualTo("ok");
    }

    @Test
    void restControllerHandlersAreNotServedAtTheRoot() {
        // exchange() reads the raw response without the default 4xx/5xx throwing, so a 404 is an assertable value.
        HttpStatusCode atRoot = client().get().uri("/probe")
                .exchange((request, response) -> response.getStatusCode());
        assertThat(atRoot).isEqualTo(HttpStatus.NOT_FOUND);
    }

    /**
     * The embedded test server owns its port on the loopback address the client dials, not merely on the
     * wildcard. A wildcard listener does not reserve {@code 127.0.0.1:<port>}: the kernel will hand a
     * wildcard bind a port that another local process already holds on the loopback, and a connection to
     * {@code 127.0.0.1} is then routed to that more specific holder rather than to this server. A class
     * whose port was captured once at boot would then send every request to a stranger, which answers each
     * one with the same bare status and no body -- a failure that reads as the feature being broken rather
     * than as a port collision.
     *
     * <p>Binding the loopback explicitly is what closes it: the ephemeral port allocator will not hand out
     * a loopback port that is already taken, so the address the client dials is the address this server
     * owns for as long as it runs.
     */
    @Test
    void theTestServerOwnsItsPortOnTheLoopbackAddressTheClientDials() throws Exception {
        try (ServerSocket shadow = new ServerSocket()) {
            shadow.setReuseAddress(true);
            assertThatThrownBy(() -> shadow.bind(
                    new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port)))
                    .as("no other socket may take 127.0.0.1:%d out from under the running test server", port)
                    .isInstanceOf(BindException.class);
        }
    }

    /**
     * A minimal boot config: auto-configures Web MVC + the embedded servlet container, imports the
     * configuration under test and a probe endpoint. No component scan, so nothing else is pulled in.
     */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({RestApiConfiguration.class, ProbeController.class})
    static class TestApp {

        @Bean
        SecurityFilterChain permitAllSecurity(HttpSecurity http) throws Exception {
            http.csrf(AbstractHttpConfigurer::disable)
                    .formLogin(AbstractHttpConfigurer::disable)
                    .httpBasic(AbstractHttpConfigurer::disable)
                    .authorizeHttpRequests(authorize -> authorize.anyRequest().permitAll());
            return http.build();
        }
    }

    @RestController
    static class ProbeController {

        @GetMapping("/probe")
        String probe() {
            return "ok";
        }
    }
}
