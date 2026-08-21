package io.tapstate.control.restapi;

import io.tapstate.control.core.ClusterIdentityService;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.ClusterIdentityStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.client.RestClient;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class IssuerDiscoveryApiTest {

    private static ConfigurableApplicationContext context;
    private static int port;

    @BeforeAll
    static void startServer() {
        context = new SpringApplicationBuilder(TestApp.class).properties("server.port=0").run();
        port = ((WebServerApplicationContext) context).getWebServer().getPort();
    }

    @AfterAll
    static void stopServer() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void anonymousGetReturnsTheExactDiscoveryJsonWithoutAnAuthorizationHeader() {
        String body = RestClient.create("http://localhost:" + port).get()
                .uri(AuthWire.DISCOVERY_PATH)
                .retrieve()
                .body(String.class);

        assertThat(body).isEqualTo("{\"issuer\":\"urn:tapstate:cluster:01J5FIXTURE\","
                + "\"clusterId\":\"01J5FIXTURE\",\"apiVersion\":\"tapstate/v1\","
                + "\"authModes\":[\"password\",\"machine_token\"]}");
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({RestApiConfiguration.class, IssuerDiscoveryController.class})
    static class TestApp {
        @Bean
        ClusterIdentityService clusterIdentityService() {
            ClusterIdentity fixed = new ClusterIdentity("01J5FIXTURE");
            ClusterIdentityStore store = new ClusterIdentityStore() {
                @Override
                public Optional<ClusterIdentity> find() {
                    return Optional.of(fixed);
                }

                @Override
                public ClusterIdentity createIfAbsent(ClusterIdentity proposed) {
                    return fixed;
                }
            };
            return new ClusterIdentityService(store, () -> "unused");
        }

        @Bean
        SecurityFilterChain permitAllSecurity(HttpSecurity http) throws Exception {
            http.csrf(AbstractHttpConfigurer::disable)
                    .formLogin(AbstractHttpConfigurer::disable)
                    .httpBasic(AbstractHttpConfigurer::disable)
                    .authorizeHttpRequests(authorize -> authorize.anyRequest().permitAll());
            return http.build();
        }
    }
}
