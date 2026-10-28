package io.tapstate.app;

import io.tapstate.control.core.*;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.app.AlertHttpModel.*;
import static io.tapstate.app.AlertNotificationPlannerIT.*;

@RequiresDocker
class AlertManagementIT {
    @org.testcontainers.junit.jupiter.Container static final org.testcontainers.containers.MongoDBContainer MONGO=AlertEvaluationHostIT.MONGO;
    static CredentialAuthentication caller(Scope scope,Runnable guard){return new CredentialAuthentication(new VerifiedToken("operator",scope),Optional.empty(),Optional.empty(),guard);}
    static AlertManagement service(AlertEvaluationHostIT.Fixture f){
        var host=f.storeHost();return new AlertManagement(()->new PipelineAlertRuntime.Services(host.deployment()==AlertChannelPolicy.Deployment.ON_PREM?sendingStore(f):f.store(),f.evaluator()),c->host.deployment()==AlertChannelPolicy.Deployment.ON_PREM?access():AlertStore.Access.cloud("operator",host.scope().authorityId(),c.token().scope().name(),c.requireActive(),ACTIVE),
                ()->new PipelineAlertRuntime.Health(false,false,0,null,Map.of(),0,0),host.scope(),host.deployment(),f.clock(),new AuditGate(record->{},f.clock()));
    }
    static Map<String,Object> call(AlertManagement s,String op,Map<String,Object> input){return s.dispatch("alert."+op,caller(Scope.ADMIN,ACTIVE),input);}
    static Map<String,Object> email(){return map("name","SMTP","enabled",true,"transport",map("channelType","email","host","smtp.example.test","port",465,"encryption","IMPLICIT_TLS","authentication","PLAIN","username","operator","from","alerts@example.test","recipients",List.of("a@example.test"),"trust","SYSTEM"),"secret","private-test-password");}
    @Test void readerGetsReadOnlyActiveCenterAndCannotMutateConfiguration(){
        AlertEvaluationHostIT.withFixture(f->{bad(f);var s=service(f);var before=f.store().evaluationStates(null,100).rows();
            var result=s.dispatch("alert.center",caller(Scope.READ,ACTIVE),Map.of("limit",5));assertThat(result.get("total")).isEqualTo(1L);assertThat((List<?>)result.get("rows")).hasSize(1);
            assertThat(f.store().evaluationStates(null,100).rows()).isEqualTo(before);
            assertThatThrownBy(()->s.dispatch("alert.configuration.save",caller(Scope.READ,ACTIVE),map("kind","RULE","id","rule","etag","1","form",Map.of()))).isInstanceOf(TapstateException.class);
            assertThat(call(s,"health",Map.of())).containsEntry("cleanupAvailable",false);
        });
    }
    @Test void existingManagedWriteScopeCanAuthorLocalRulesWhileOpStillRequiresAdmin(){
        AlertEvaluationHostIT.withFixture(false,true,f->{var s=service(f);var form=map("name","Managed authoring","typeId","pipeline.failed","resourceKinds",List.of("pipeline"),"allResources",true,"resources",List.of(),"parameters",Map.of(),"enabled",true);
            assertThat(s.dispatch("alert.configuration.save",caller(Scope.WRITE,ACTIVE),map("kind","RULE","id","managed-authoring","etag","0","form",form))).containsEntry("id","managed-authoring");
            assertThat((List<?>)s.dispatch("alert.configuration.list",caller(Scope.WRITE,ACTIVE),map("kind","RULE")).get("rows")).hasSize(2);
            assertThatThrownBy(()->s.dispatch("alert.configuration.save",caller(Scope.READ,ACTIVE),map("kind","RULE","id","reader","etag","0","form",form))).isInstanceOf(TapstateException.class);
        });
        AlertEvaluationHostIT.withFixture(f->{assertThatThrownBy(()->service(f).dispatch("alert.configuration.save",caller(Scope.WRITE,ACTIVE),map("kind","RULE","id","writer","etag","0","form",map("name","Valid OP rule","typeId","pipeline.failed","resourceKinds",List.of("pipeline"),"allResources",true,"resources",List.of(),"parameters",Map.of(),"enabled",true)))).isInstanceOfSatisfying(TapstateException.class,failure->{assertThat(failure.code()).isEqualTo(AlertError.STALE_ACTION);assertThat(failure.args()).containsEntry("action","permission");});});
    }
    @Test void saveUsesCasAndNeverReturnsOrAuditsPrivateMaterial(){
        AlertEvaluationHostIT.withFixture(f->{var s=service(f);var saved=call(s,"configuration.save",map("kind","RECEIVER","id","smtp","etag","0","form",email()));
            assertThat(JsonWriter.write(saved)).doesNotContain("private-test-password","credentialId","ciphertext");assertThat(saved.get("etag")).isEqualTo("1");
            var replacement=email();replacement.put("name","changed");replacement.put("retainMaterial",true);replacement.remove("secret");
            var next=call(s,"configuration.save",map("kind","RECEIVER","id","smtp","etag","1","form",replacement));assertThat(next.get("etag")).isEqualTo("2");
            assertThatThrownBy(()->call(s,"configuration.save",map("kind","RECEIVER","id","smtp","etag","1","form",email()))).isInstanceOf(TapstateException.class);
            assertThat(call(s,"configuration.list",map("kind","RECEIVER")).toString()).contains("changed").doesNotContain("private-test-password");
        });
    }
    @Test void unsavedCandidateTestsDoNotSaveOrCreateAnAlarmAndReplayOneCommand(){
        AlertEvaluationHostIT.withFixture(f->{var s=service(f);String command=new AlertTestCommand.ClientId(T,"1".repeat(32)).value();var input=map("commandId",command,"candidateId","candidate","form",email(),"target","a@example.test");
            var first=call(s,"test",input);var replay=call(s,"test",input);assertThat(replay.get("id")).isEqualTo(first.get("id"));
            assertThat(f.store().definitions(AlertConfiguration.Kind.RECEIVER,null,100,access()).rows()).isEmpty();assertThat(f.store().active(null,10,f.reads()).total()).isZero();
            assertThat(f.store().dueIntents(T.plusSeconds(300),null,100).rows()).hasSize(1);assertThat(call(s,"test-result",map("commandId",command)).get("id")).isEqualTo(first.get("id"));
            object(input.get("form")).put("name","label-only");object(object(input.get("form")).get("transport")).put("host","other.example.test");
            assertThatThrownBy(()->call(s,"test",input)).isInstanceOf(TapstateException.class);
        });
    }
    @Test void candidateTargetNormalizesOnlyDomainCaseAndRetainsTheChosenRecipient(){
        AlertEvaluationHostIT.withFixture(f->{var form=email();object(form.get("transport")).put("recipients",List.of("first@example.test","Second@EXAMPLE.TEST"));
            var result=call(service(f),"test",map("commandId",new AlertTestCommand.ClientId(T,"a".repeat(32)).value(),"candidateId","chosen-recipient","form",form,"target","Second@EXAMPLE.TEST"));
            assertThat(result.get("id")).isNotNull();var queued=f.store().dueIntents(T.plusSeconds(300),null,100).rows();assertThat(queued).hasSize(1);assertThat(queued.getFirst().value().frozen().identity().targetId()).isEqualTo("Second@example.test");
        });
    }
    @Test void savedWebhookWithoutAuthenticationCanTestItsUnchangedPrivateEndpoint(){
        AlertEvaluationHostIT.withFixture(f->{var s=service(f);var form=map("name","Webhook","transport",map("channelType","webhook","authentication","NONE","trust","SYSTEM"),"endpoint","https://notify.example.test/private-token");
            var saved=call(s,"configuration.save",map("kind","RECEIVER","id","webhook","etag","0","form",form));assertThat(saved.toString()).doesNotContain("private-token");
            var result=call(s,"test",map("commandId",new AlertTestCommand.ClientId(T,"2".repeat(32)).value(),"candidateId","webhook-candidate","existingId","webhook","etag","1","form",saved));assertThat(result.get("id")).isNotNull();assertThat(text(result,"target")).startsWith("https://notify.example.test").doesNotContain("private-token");assertThat(JsonWriter.write(result)).doesNotContain("private-token");
        });
    }
    @Test void emptyHiddenPrivateFieldsFromChannelSwitchAreAbsentMaterial(){
        AlertEvaluationHostIT.withFixture(f->{var form=map("name","Webhook","transport",map("channelType","webhook","authentication","NONE","trust","SYSTEM"),"endpoint","https://notify.example.test/hook","secret","","privateCa","");
            assertThat(call(service(f),"configuration.save",map("kind","RECEIVER","id","webhook","etag","0","form",form)).get("id")).isEqualTo("webhook");
        });
    }
    @Test void unauthenticatedEmailCanBeSavedAfterTestingWithoutUnexpectedMaterial(){
        AlertEvaluationHostIT.withFixture(f->{var form=email();form.remove("secret");object(form.get("transport")).put("authentication","NONE");object(form.get("transport")).remove("username");
            assertThat(call(service(f),"configuration.save",map("kind","RECEIVER","id","simple-smtp","etag","0","form",form)).get("id")).isEqualTo("simple-smtp");
        });
    }
    @Test void managedModeRejectsExternalSurfacesAndDoesNotReturnCredentialSummaries(){
        AlertEvaluationHostIT.withFixture(false,true,f->{var s=service(f);
            for(var op:List.of("configuration.list","configuration.save","test","test-result","retry","preview","route-preview")){
                var input=switch(op){case "configuration.list"->map("kind","RECEIVER");case "configuration.save"->map("kind","RECEIVER","id","r","etag","0","form",email());case "route-preview"->map("form",Map.of());case "preview"->map("kind","FAULT","channel","email","messages",messages(MessageSettings.empty()));case "test-result"->map("commandId",new AlertTestCommand.ClientId(T,"1".repeat(32)).value());case "retry"->map("id","none","etag","1");default->map("commandId","none","candidateId","none","form",email(),"target","a@example.test");};
                assertThatThrownBy(()->call(s,op,input)).as(op).isInstanceOf(TapstateException.class);
            }
            assertThat(call(s,"catalog",Map.of()).get("deployment")).isEqualTo("CLOUD_MANAGED");assertThat(call(s,"center",Map.of()).get("total")).isEqualTo(0L);
        });
    }
    @Test void deliveryQueriesAreBoundedIntersectFiltersAndRefuseForeignCursorContext(){
        AlertEvaluationHostIT.withFixture(f->{AlertDeliveryRuntimeIT.seed(f);var s=service(f);var p=call(s,"deliveries",map("limit",1,"channel","email","result","NOT_ATTEMPTED"));assertThat((List<?>)p.get("rows")).hasSize(1);assertThat(p.get("nextCursor")).isNotNull();
            var next=call(s,"deliveries",map("limit",1,"channel","email","result","NOT_ATTEMPTED","cursor",p.get("nextCursor")));assertThat((List<?>)next.get("rows")).hasSize(1);
            assertThatThrownBy(()->call(s,"deliveries",map("channel","webhook","cursor",p.get("nextCursor")))).isInstanceOf(TapstateException.class);
            var row=object(((List<?>)p.get("rows")).getFirst());var detail=call(s,"deliveries",map("id",row.get("id")));assertThat(detail).doesNotContainKeys("payload","candidate","credentials");var original=sendingStore(f).delivery(text(row,"id"),f.reads()).orElseThrow();grant(f,original);accepted(f,original.value().frozen().identity());var received=call(s,"deliveries",map("id",row.get("id")));assertThat(object(received.get("diagnostic"))).containsEntry("phase","FINAL_RESPONSE").containsEntry("status",250);
            assertThat(f.store().deliveries(null,100,null,null,new AlertStore.ReadAccess(false,Set.of(),ACTIVE)).rows()).isEmpty();
        });
    }
    @Test void originalEndedEpisodeDetailsDoNotUseTheNextEpisodeObservation(){
        AlertEvaluationHostIT.withFixture(f->{bad(f);f.now().set(T.plusSeconds(1));var binding=f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));
            f.evaluator().accept(binding,2,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.GOOD,null));
            f.now().set(T.plusSeconds(2));f.evaluator().accept(binding,3,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.BAD,null));
            var s=service(f);var history=call(s,"center",map("view","history"));var row=object(((List<?>)history.get("rows")).getFirst());assertThat(row.get("status")).isEqualTo("RECOVERED");
            var detail=call(s,"detail",map("key",row.get("key"),"episode",1));assertThat(detail.get("episode")).isEqualTo(1L);assertThat(object(detail.get("basis")).get("observedAt")).isNull();assertThat(detail.get("status")).isEqualTo("RECOVERED");
        });
    }
    @Test void resourcesUseCurrentSourceUuidAndPipelineGenerationAndSaveRechecksThem(){
        AlertEvaluationHostIT.withFixture(true,f->{var s=service(f);var sources=call(s,"resources",map("kind","source"));var source=object(((List<?>)sources.get("rows")).getFirst());assertThat(UUID.fromString(text(source,"incarnation")).version()).isEqualTo(4);
            var pipelines=call(s,"resources",map("kind","pipeline"));assertThat(((List<?>)pipelines.get("rows")).contains(map("kind","pipeline","id","orders","incarnation","1"))).isTrue();
            var form=map("name","explicit","typeId","pipeline.failed","resourceKinds",List.of("pipeline"),"allResources",false,"resources",List.of(map("kind","pipeline","id","orders","incarnation","999")),"parameters",Map.of());
            assertThatThrownBy(()->call(s,"configuration.save",map("kind","RULE","id","bad","etag","0","form",form))).isInstanceOf(TapstateException.class);
        });
    }
    @Test void requestsCannotChooseScopeAndPreviewUsesTheRealEscapingRendererWithoutSending(){
        AlertEvaluationHostIT.withFixture(f->{var s=service(f);assertThatThrownBy(()->call(s,"center",map("authorityId","other"))).isInstanceOf(TapstateException.class);
            var p=call(s,"preview",map("kind","FAULT","channel","email","messages",messages(new MessageSettings("<script>","","<b>literal</b>",""))));
            assertThat(text(p,"html")).contains("&lt;script&gt;","&lt;b&gt;").doesNotContain("<script>","<b>literal</b>");assertThat(f.store().dueIntents(T.plusSeconds(300),null,100).rows()).isEmpty();
        });
    }
    @Test void currentEpisodeLinksRetainLiveQualityAndOriginalThresholds(){
        AlertEvaluationHostIT.withFixture(f->{bad(f);var s=service(f);var row=object(((List<?>)call(s,"center",Map.of()).get("rows")).getFirst());
            var detail=call(s,"detail",map("key",row.get("key"),"episode",row.get("episode")));assertThat(detail).containsEntry("quality","VALID").containsEntry("observedAt",T.toString());assertThat(detail.get("thresholds")).isNotNull();
        });
    }
    @Test void realHttpFaceReadsAndWritesThePreparedConfigurationStore(){
        AlertEvaluationHostIT.withFixture(f->{f.now().set(Instant.now());var s=service(f);var browserPlanner=planner(f,"browser");route(f,Duration.ZERO);f.evaluator().withPlanner(browserPlanner);
            var binding=f.binding(()->new AlertStore.EvaluationProof(f.intent(),ACTIVE));
            f.evaluator().accept(binding,1,new AlertInput.Conditions(f.clock().instant(),AlertSignal.Condition.BAD,new AlarmDiagnostics.SafeTrace("java.lang.IllegalStateException: isolated failure\n\tat example.Pipeline.run(Pipeline.java:12)",1,false,false)));browserPlanner.tick();
            var app=new org.springframework.boot.builder.SpringApplicationBuilder(io.tapstate.control.restapi.AlertTestHttpFace.class)
                    .initializers(c->c.getBeanFactory().registerSingleton("alertControlDispatcher",s)).run("--server.port="+Integer.getInteger("tapstate.alert.browser-port",0),"--server.address=127.0.0.1");
            try(app){int port=((org.springframework.boot.web.server.context.WebServerApplicationContext)app).getWebServer().getPort();
                var client=java.net.http.HttpClient.newHttpClient();
                try{var req=java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:"+port+"/api/alerts/center"))
                            .header("Authorization","Bearer cyxt_alert-browser.fixture").header("Content-Type","application/json").POST(java.net.http.HttpRequest.BodyPublishers.ofString("{}" )).build();
                    var response=client.send(req,java.net.http.HttpResponse.BodyHandlers.ofString());assertThat(response.statusCode()).isEqualTo(200);assertThat(response.body()).contains("orders","MAJOR");
                    String control=System.getProperty("tapstate.alert.browser-control");
                    if(control!=null){var root=java.nio.file.Path.of(control);java.nio.file.Files.createDirectories(root);java.nio.file.Files.writeString(root.resolve("ready.json"),JsonWriter.write(map("port",port,"scope","isolated-config-fixture","transport","controlled-no-external-io")));
                        var channels=new AlertDeliveryRuntimeIT.Channels(sendingStore(f));
                        try(var worker=AlertDeliveryRuntimeIT.runtime(f,sendingStore(f),channels,"browser-worker")){worker.start();long until=System.nanoTime()+Duration.ofMinutes(15).toNanos();
                            while(!java.nio.file.Files.exists(root.resolve("stop"))&&System.nanoTime()<until){f.now().set(Instant.now());Thread.sleep(100);}}
                    }
                }catch(java.io.IOException|InterruptedException e){throw new AssertionError(e);}
            }
        });
    }
    @Test void searchSeverityAndRoutePreviewUseTheCurrentMatchWithoutWriting(){
        AlertEvaluationHostIT.withFixture(f->{bad(f);var s=service(f);var before=f.store().planning().version();
            assertThat(call(s,"center",map("search","orders","severity","MAJOR")).get("total")).isEqualTo(1L);
            assertThat(call(s,"center",map("search","[orders]","severity","MAJOR")).get("total")).isEqualTo(0L);
            assertThat(call(s,"center",map("severity","CRITICAL")).get("total")).isEqualTo(0L);
            var form=map("resourceKinds",List.of("pipeline"),"allResources",true,"resources",List.of(),"typeIds",List.of("pipeline.failed"),"severities",List.of("MAJOR"),"inhibition",List.of());
            var preview=call(s,"route-preview",map("form",form));assertThat(preview).containsEntry("total",1L).containsEntry("previewOnly",true);assertThat(f.store().planning().version()).isEqualTo(before);assertThat(intents(f)).isEmpty();
            var silence=new AlertConfiguration.Silence(AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.SILENCE,f.storeHost().scope(),"silence"),AlertChannelPolicy.Deployment.ON_PREM,T,"operator"),"Maintenance",new AlertSilenceWindow(f.storeHost().scope(),true,Set.of(),Set.of(),T,Duration.ofMinutes(1),null,null));
            assertThat(f.store().saveDefinition(null,silence,access())).isEqualTo(AlertStore.Outcome.STAGED);
            assertThat(object(((List<?>)call(s,"route-preview",map("form",form)).get("rows")).getFirst())).containsEntry("silenced",true);
        });
    }
    @Test void deliveryTitleUsesTheOriginalFoldedMimeSnapshotAndEpisodeFilter(){
        AlertEvaluationHostIT.withFixture(f->{var route=route(f,Duration.ZERO);String prefix="\u00e9".repeat(60);var channel=route.channels().getFirst();
            var changed=route.edit(route.name(),route.match(),true,List.of(new AlertConfiguration.RouteChannel(channel.channelType(),channel.receivers(),new MessageSettings(prefix,"","",""))),route.groupBy(),route.grouping(),route.inhibition(),T,"operator");
            assertThat(f.store().saveDefinition(route,changed,access())).isEqualTo(AlertStore.Outcome.STAGED);var planner=planner(f,"title");f.evaluator().withPlanner(planner);bad(f);planner.tick();var s=service(f);
            var rows=(List<?>)call(s,"deliveries",Map.of()).get("rows");assertThat(rows).hasSize(2);assertThat(text(object(rows.getFirst()),"title")).startsWith(prefix);
            var alarm=f.store().active(null,10,f.reads()).rows().getFirst().value().snapshot().rule().key();
            assertThat((List<?>)call(s,"deliveries",map("key",key(alarm),"episode",2)).get("rows")).isEmpty();assertThat((List<?>)call(s,"deliveries",map("key",key(alarm),"episode",1)).get("rows")).hasSize(2);
        });
    }
    @Test void equivalentCredentialRotationKeepsTheOriginalSemanticReference(){
        AlertEvaluationHostIT.withFixture(f->{var s=service(f);call(s,"configuration.save",map("kind","RECEIVER","id","rotating","etag","0","form",email()));var before=(AlertReceiver)f.store().definition(AlertConfiguration.Kind.RECEIVER,"rotating",access()).orElseThrow();
            var form=email();form.put("secret","rotated-private-value");call(s,"configuration.save",map("kind","RECEIVER","id","rotating","etag","1","form",form));var after=(AlertReceiver)f.store().definition(AlertConfiguration.Kind.RECEIVER,"rotating",access()).orElseThrow();assertThat(after.credentials()).isEqualTo(before.credentials());assertThat(after.bindingRevision()).isEqualTo(before.bindingRevision());assertThat(after.header().revision()).isEqualTo(2);
        });
    }
    @Test void administrativelyEndedHistoryIsNeverPresentedAsRecovered(){
        AlertEvaluationHostIT.withFixture(f->{bad(f);var key=f.store().active(null,10,f.reads()).rows().getFirst().value().snapshot().rule().key();var s=service(f);var form=AlertAuthoring.project(f.rule());form.put("enabled",false);call(s,"configuration.save",map("kind","RULE","id","rule","etag","1","form",form));f.evaluator().end(key,AlertSnapshot.EndReason.DISABLED,ACTIVE);
            assertThat((List<?>)call(s,"center",map("view","history")).get("rows")).isEmpty();var page=call(s,"center",map("view","history","endReason","DISABLED"));assertThat(object(((List<?>)page.get("rows")).getFirst())).containsEntry("status","ENDED").containsEntry("endReason","DISABLED");
        });
    }
    @Test void syntheticRuleExamplesExerciseRealLifecycleWithoutPublishingOrWriting(){
        AlertEvaluationHostIT.withFixture(f->{var s=service(f);var form=AlertAuthoring.project(f.rule());form.put("parameters",Map.of("trigger-for",3,"resolve-for",2));var before=f.store().planning().version();
            var bad=call(s,"rule-preview",map("form",form,"sample","BAD"));assertThat(bad).containsEntry("initialStatus","PENDING").containsEntry("status","FIRING").containsEntry("severity","MAJOR").containsEntry("persisted",false).containsEntry("sent",false);
            assertThat(call(s,"rule-preview",map("form",form,"sample","GOOD"))).containsEntry("status","RESOLVED");assertThat(call(s,"rule-preview",map("form",form,"sample","MISSING"))).containsEntry("status","FIRING").containsEntry("quality","UNKNOWN");
            assertThat(f.store().planning().version()).isEqualTo(before);assertThat(f.store().active(null,10,f.reads()).total()).isZero();assertThat(intents(f)).isEmpty();
        });
    }
    @Test void configurationRetirementPreservesTypedPayloadAcrossAllKinds(){
        AlertEvaluationHostIT.withFixture(f->{var s=service(f);route(f,Duration.ZERO);
            for(var pair:List.of(new String[]{"RULE","rule"},new String[]{"SUBSCRIPTION","route"},new String[]{"RECEIVER","receiver"})){
                var kind=AlertConfiguration.Kind.valueOf(pair[0]);var before=f.store().definition(kind,pair[1],access()).orElseThrow();var retired=call(s,"configuration.retire",map("kind",pair[0],"id",pair[1],"etag","1"));assertThat(retired).containsEntry("active",false);assertThat(f.store().definition(kind,pair[1],access()).orElseThrow().header().identity()).isEqualTo(before.header().identity());}
        });
    }
    @Test void malformedNullParametersHaveCodedValidationWithoutAParserOrInvariantLeak(){
        AlertEvaluationHostIT.withFixture(f->{var form=AlertAuthoring.project(f.rule());var params=new HashMap<String,Object>();params.put("trigger-for",null);form.put("parameters",params);
            assertThatThrownBy(()->call(service(f),"rule-preview",map("form",form,"sample","BAD"))).isInstanceOfSatisfying(TapstateException.class,e->assertThat(e.code()).isEqualTo(AlertError.INVALID_CONFIGURATION));
        });
    }
    @Test void aRevokedOriginalRequestCannotReturnItsSnapshot(){
        AlertEvaluationHostIT.withFixture(f->{var valid=new AtomicBoolean(true);var count=new java.util.concurrent.atomic.AtomicInteger();Runnable guard=()->{if(count.incrementAndGet()>=2)valid.set(false);if(!valid.get())throw stale("revoked");};
            assertThatThrownBy(()->service(f).dispatch("alert.center",caller(Scope.READ,guard),Map.of())).isInstanceOf(TapstateException.class);
        });
    }
}
