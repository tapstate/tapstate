package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.JsonReader;
import io.tapstate.control.restapi.LoginResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import static org.assertj.core.api.Assertions.*;

/** A separate JVM recovers only durable authentication and logs in without inheriting an application session. */
public final class AuthenticationRecoveryRestartProcess {
    private AuthenticationRecoveryRestartProcess() {}
    public static void main(String[] args) throws Exception {
        var input=(Map<?,?>)JsonReader.parse(Files.readString(Path.of(args[0])));
        boolean cloud=Boolean.TRUE.equals(input.get("cloud"));
        var fixture=new ManagedMongoStoreIsolationIT(); fixture.work=Path.of((String)input.get("work"));
        try(var application=fixture.restartedAuthenticationRoot((String)input.get("configurationUri"),(String)input.get("cluster"),cloud)) {
            var root=application.getBean(StorageRoot.class);
            assertThat(root.phase()).isEqualTo(StorageRoot.Phase.AUTH_READY);
            assertThat(root.runtime().current().orElseThrow().preparation().authScope().authGeneration()).isEqualTo(2);
            assertThat(root.runtime().business()).isEmpty();
            assertThat(application.getBeansOfType(HazelcastInstance.class)).isEmpty();
            int port=((WebServerApplicationContext)application).getWebServer().getPort();
            var client=RestClient.builder().baseUrl("http://127.0.0.1:"+port)
                    .requestFactory(new org.springframework.http.client.JdkClientHttpRequestFactory(java.net.http.HttpClient.newBuilder()
                            .followRedirects(java.net.http.HttpClient.Redirect.NEVER).build())).build();
            String status;
            if(cloud) {
                var response=client.get().uri("/auth/exchange?code=late").retrieve().toBodilessEntity();
                assertThat(response.getStatusCode().value()).isEqualTo(302);
                String cookie=response.getHeaders().getFirst("Set-Cookie"); assertThat(cookie).contains("Secure","HttpOnly");
                status=client.post().uri("/api/storage/status").header("Cookie",cookie.split(";",2)[0])
                        .header("Origin","http://127.0.0.1:"+port).body(Map.of()).retrieve().body(String.class);
                assertThat(application.getBeansOfType(io.tapstate.spi.store.UserStore.class)).isEmpty();
            } else {
                assertThatThrownBy(() -> client.post().uri("/auth/login").body(Map.of("username","admin","password","incorrect-recovery-password","createSession",false))
                        .retrieve().body(String.class)).isInstanceOfSatisfying(RestClientResponseException.class,
                                failure -> assertThat(failure.getStatusCode().value()).isEqualTo(401));
                var login=client.post().uri("/auth/login").body(Map.of("username","admin","password","sentinel-login-password","createSession",false))
                        .retrieve().body(LoginResponse.class);
                status=client.post().uri("/api/storage/status").header("Authorization","Bearer "+login.token()).body(Map.of()).retrieve().body(String.class);
            }
            assertThat(((Map<?,?>)JsonReader.parse(status)).get("businessPhase")).isEqualTo("AWAITING_REBUILD_CONFIRMATION");
            assertThat(root.runtime().business()).isEmpty();
            assertThat(application.getBeansOfType(HazelcastInstance.class)).isEmpty();
        }
        System.out.println("AUTH_RECOVERY_PROCESS_VERIFIED "+ProcessHandle.current().pid()+" "+cloud);
    }
}
