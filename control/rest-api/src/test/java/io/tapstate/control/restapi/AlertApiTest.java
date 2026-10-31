package io.tapstate.control.restapi;

import io.tapstate.control.core.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.http.*;
import org.springframework.web.client.RestClient;
import static org.assertj.core.api.Assertions.*;

/** Real HTTP/security transport; store and outbound behavior have their own integration witnesses. */
class AlertApiTest {
    static final AtomicInteger CALLS=new AtomicInteger();
    static String credential(AuthenticationMode mode,Scope scope){return (mode==AuthenticationMode.CLOUD?"tcs_alert-":"human-alert-")+scope+".fixture";}
    @ParameterizedTest @EnumSource(AuthenticationMode.class)
    void originalVerifiedIdentityAndNoStoreHeadersReachEveryAlertOperation(AuthenticationMode mode){
        try(var context=new SpringApplicationBuilder(TestApp.class).run("--server.port=0","--test.alert.mode="+mode.name())){
            int port=((WebServerApplicationContext)context).getWebServer().getPort();String origin="http://127.0.0.1:"+port;var client=RestClient.create(origin);CALLS.set(0);
            for(var scope:Scope.values()){
                var body=request(client,mode,scope,origin,"catalog","{}").header("X-Cloud-UserId","forged").exchange((r,p)->{assertThat(p.getStatusCode()).isEqualTo(HttpStatus.OK);assertThat(p.getHeaders().getCacheControl()).isEqualTo("no-store");return p.bodyTo(Map.class);});
                assertThat(body.get("subject")).isEqualTo("operator");assertThat(body.get("scope")).isEqualTo(scope.name());assertThat(body.toString()).doesNotContain("fixture","forged");
            }
            request(client,mode,Scope.READ,origin,"configuration/save","{}").exchange((r,p)->{assertThat(p.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);return null;});
            int before=CALLS.get();request(client,mode,Scope.ADMIN,origin,"test","{\"private\":\"fixture-secret\"").exchange((r,p)->{assertThat(p.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);assertThat(p.bodyTo(String.class)).doesNotContain("fixture-secret");return null;});assertThat(CALLS.get()).isEqualTo(before);
            request(client,mode,Scope.ADMIN,origin,"test","{\"value\":\""+"a".repeat(65536)+"\"}").exchange((r,p)->{assertThat(p.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);return null;});assertThat(CALLS.get()).isEqualTo(before);
            client.post().uri("/api/alerts/center").contentType(MediaType.APPLICATION_JSON).body("{}").exchange((r,p)->{assertThat(p.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);return null;});
            if(mode==AuthenticationMode.CLOUD)request(client,mode,Scope.ADMIN,"https://foreign.invalid","center","{}").exchange((r,p)->{assertThat(p.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);return null;});
        }
    }
    static RestClient.RequestBodySpec request(RestClient client,AuthenticationMode mode,Scope scope,String origin,String path,String body){
        var r=client.post().uri("/api/alerts/"+path).contentType(MediaType.APPLICATION_JSON).body(body);
        return mode==AuthenticationMode.CLOUD?r.header(HttpHeaders.COOKIE,CloudSessionCookies.NAME+"="+credential(mode,scope)).header(HttpHeaders.ORIGIN,origin):r.header(HttpHeaders.AUTHORIZATION,"Bearer "+credential(mode,scope));
    }
    @SpringBootConfiguration @EnableAutoConfiguration
    @Import({RestApiConfiguration.class,RestApiSecurityConfiguration.class,ApiExceptionHandler.class,AlertController.class})
    static class TestApp {
        @Bean AuthenticationMode mode(Environment e){return AuthenticationMode.valueOf(e.getRequiredProperty("test.alert.mode"));}
        @Bean OperationRegistry registry(){return ControlOperations.registry();}
        @Bean CredentialAuthenticator credentials(AuthenticationMode mode){return CredentialAuthenticator.scoped(p->{for(var scope:Scope.values())if(credential(mode,scope).equals(p))return Optional.of(new CredentialAuthentication(new VerifiedToken("operator",scope),Optional.empty(),Optional.empty(),()->{}));return Optional.empty();});}
        @Bean AlertControlDispatcher alerts(){return (op,caller,input)->{CALLS.incrementAndGet();return Map.of("subject",caller.token().subject(),"scope",caller.token().scope().name());};}
    }
}
