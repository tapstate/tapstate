package io.tapstate.app;

import io.tapstate.adapters.mongostore.CloudReadCommandProbe;
import io.tapstate.core.common.*;
import io.tapstate.testsupport.RequiresDocker;
import com.mongodb.client.MongoClients;
import java.net.URI;
import java.net.http.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.web.filter.OncePerRequestFilter;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Counts the real warm HTTP authentication and alert read path separately from background polling. */
@RequiresDocker
class NativeAlertReadBudgetIT {
    @TempDir Path work;
    private static final Map<String,CompletableFuture<CloudReadCommandProbe.Observation>> TRACES=new ConcurrentHashMap<>();
    private record Read(Map<String,Object> body,CloudReadCommandProbe.Observation commands){ }
    @Test void alertListReadsStayBoundedAcrossPagesAndUnrelatedRowsAndKeepRevocation()throws Exception{
        try(var mongo=new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withTmpFs(Map.of("/data/db","rw","/data/configdb","rw"))
                .withCreateContainerCmdModifier(cmd->cmd.getHostConfig().withMemory(768L*1024*1024).withNanoCPUs(1_000_000_000L))
                .withCommand("--replSet","docker-rs","--bind_ip_all","--wiredTigerCacheSizeGB","0.25")){
            mongo.start();var storage=NativeAppStorage.createOnPremWithAdmin(mongo,"alert_read_budget","operator","isolated-budget-password");
            try(var observer=MongoClients.create(storage.configurationUri());var http=HttpClient.newHttpClient();
                    var app=new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated())
                    .initializers(context->context.getBeanFactory().registerSingleton("alertReadBudgetProbe",probeFilter())).run(
                        "--server.address=127.0.0.1","--server.port=0","--logging.level.root=ERROR",
                        "--tapstate.store.mongo.uri="+storage.configurationUri(),"--tapstate.hz.member-port=0","--tapstate.hz.jet.cooperative-thread-count=2",
                        "--tapstate.connectors.seed-dir="+work.resolve("no-seeds"),"--tapstate.connectors.plugins-dir="+work.resolve("plugins"),
                        "--tapstate.control.auth.jwt-secret=isolated-alert-budget-signing-key")){
                NativeAppStorage.awaitBusiness(app);String root="http://127.0.0.1:"+((WebServerApplicationContext)app).getWebServer().getPort();
                var login=post(http,root,"/auth/login",null,Map.of("username","operator","password","isolated-budget-password"),null);
                assertThat(login.statusCode()).isEqualTo(200);String token=(String)AlertHttpModel.object(JsonReader.parse(login.body())).get("token");
                for(int i=0;i<6;i++){
                    var form=Map.of("name","Budget rule "+i,"typeId","pipeline.failed","typeVersion",1,"resourceKinds",List.of("pipeline"),
                            "allResources",true,"resources",List.of(),"parameters",Map.of(),"severity","DEFAULT","enabled",true);
                    var saved=post(http,root,"/api/alerts/configuration/save",token,Map.of("kind","RULE","id","budget-rule-"+i,"etag","0","form",form),null);
                    assertThat(saved.statusCode()).as(saved.body()).isEqualTo(200);
                }
                read(http,root,token,1);var small=read(http,root,token,1);var full=read(http,root,token,100);
                assertThat((List<?>)small.body().get("rows")).hasSize(1);assertThat((List<?>)full.body().get("rows")).hasSizeGreaterThanOrEqualTo(6);
                assertThat(small.body().get("nextCursor")).isNotNull();
                var rules=observer.getDatabase(storage.configurationDatabase()).getCollection("alert_rules");var original=rules.find(new Document("_id.id","budget-rule-0")).first();
                var unrelated=new ArrayList<Document>();
                for(int i=0;i<100;i++){
                    var copy=Document.parse(original.toJson());copy.get("_id",Document.class).put("authorityId","unrelated-authority");
                    copy.get("_id",Document.class).put("id","unrelated-"+i);unrelated.add(copy);
                }
                rules.insertMany(unrelated);var grown=read(http,root,token,100);
                assertThat(grown.body()).isEqualTo(full.body());
                assertThat(grown.commands().started()).as("unrelated scope growth adds no commands").isLessThanOrEqualTo(full.commands().started()+2);
                assertThat(grown.commands().bytes()).as("unrelated rows are not returned for enrichment").isLessThanOrEqualTo(full.commands().bytes()+4096);
                observer.getDatabase(storage.configurationDatabase()).getCollection("users").updateOne(new Document("_id","operator"),new Document("$set",new Document("role","read")));
                var refused=post(http,root,"/api/alerts/configuration/list",token,Map.of("kind","RULE","limit",1),null);
                assertThat(refused.statusCode()).as(refused.body()).isEqualTo(409);
                assertThat(refused.body()).contains("alert.stale-action","actor-user-permission");
            }
        }
    }
    private static FilterRegistrationBean<OncePerRequestFilter> probeFilter(){
        var filter=new OncePerRequestFilter(){
            @Override protected void doFilterInternal(jakarta.servlet.http.HttpServletRequest request,jakarta.servlet.http.HttpServletResponse response,jakarta.servlet.FilterChain chain)
                    throws jakarta.servlet.ServletException,java.io.IOException{
                String id=request.getHeader("X-Alert-Read-Probe");var target=id==null?null:TRACES.get(id);
                if(target==null){chain.doFilter(request,response);return;}
                try(var probe=new CloudReadCommandProbe()){
                    try{chain.doFilter(request,response);}finally{target.complete(probe.observation());}
                }
            }
        };
        var registration=new FilterRegistrationBean<OncePerRequestFilter>(filter);registration.setOrder(Integer.MIN_VALUE);return registration;
    }
    private static Read read(HttpClient http,String root,String token,int limit)throws Exception{
        String id=UUID.randomUUID().toString();var result=new CompletableFuture<CloudReadCommandProbe.Observation>();TRACES.put(id,result);
        try{
            var response=post(http,root,"/api/alerts/configuration/list",token,Map.of("kind","RULE","limit",limit),id);
            assertThat(response.statusCode()).as(response.body()).isEqualTo(200);var observed=result.get(5,TimeUnit.SECONDS);
            assertThat(observed.completed()).isEqualTo(observed.started());assertThat(observed.started()).isPositive().isLessThanOrEqualTo(64);
            System.out.println("ALERT_HTTP_READ_BUDGET limit="+limit+" commands="+observed.commands()+" bytes="+observed.bytes());
            return new Read(AlertHttpModel.object(JsonReader.parse(response.body())),observed);
        }finally{TRACES.remove(id);}
    }
    private static HttpResponse<String> post(HttpClient http,String root,String path,String token,Map<String,?> body,String probe)throws Exception{
        var request=HttpRequest.newBuilder(URI.create(root+path)).timeout(Duration.ofSeconds(20)).header("Content-Type","application/json");
        if(token!=null)request.header("Authorization","Bearer "+token);if(probe!=null)request.header("X-Alert-Read-Probe",probe);
        return http.send(request.POST(HttpRequest.BodyPublishers.ofString(JsonWriter.write(body))).build(),HttpResponse.BodyHandlers.ofString());
    }
}
