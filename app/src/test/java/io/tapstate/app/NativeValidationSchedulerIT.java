package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.core.*;
import io.tapstate.core.validation.*;
import io.tapstate.runtime.engine.validation.ValidationProcessorRegistry;
import io.tapstate.runtime.probe.ValidationOperation;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;

/** Real timer dispatch, native CONFIG/CACHE, member RPC, Jet and Mongo PDK; controlled UTC, not Cloud/PO acceptance. */
@RequiresDocker
class NativeValidationSchedulerIT {
    @Container static final MongoDBContainer MONGO=NativeValidationOwnerReservationsIT.MONGO;
    private static final class Time extends Clock {
        final AtomicReference<Instant> now=new AtomicReference<>(Instant.parse("2026-10-29T00:00:01Z"));
        @Override public ZoneId getZone(){return ZoneOffset.UTC;}@Override public Clock withZone(ZoneId zone){return this;}@Override public Instant instant(){return now.get();}
    }
    @Test @Timeout(90) void twoActualMembersDetectOneIntervalAndCreateOnePdkRun()throws Exception{
        try(var f=new NativeValidationOwnerReservationsIT.Fixture(true)){
            var source="schedule_source_"+f.suffix;var target="schedule_target_"+f.suffix;var time=new Time();HazelcastInstance peer=null;NodeSessionLease peerLease=null;
            try{
                NativeValidationMetadataIT.seed(f,source,target);var artifact=NativeValidationPdkCountIT.artifact("mongodb");
                var task=create(f,time,new ValidationSchedule.Interval(1,ValidationSchedule.Unit.MINUTES,time.instant(),"UTC"),artifact);
                peer=peer(f.member);var exactPeer=peer;var committed=new ClusterMembership("cluster",2,Set.of("node","peer"));f.gate.install(committed);f.gate.apply(f.member.getCluster().getMembers());
                long asked=System.nanoTime();var node=f.cache.workloadClaims(f.scope).acquire(new WorkloadClaimKey("cluster",WorkloadClaimType.NODE_SESSION,"peer"),new WorkloadOwner("peer","boot-peer"),2,Duration.ofMinutes(5)).claim();
                peerLease=new NodeSessionLease(f.cache.workloadClaims(f.scope),node,asked,Duration.ofMinutes(5),Duration.ofMinutes(1),exactPeer::shutdown);var lease=peerLease;
                var properties=new ClusterProperties();properties.setProfile(ClusterProperties.Profile.PROCESS_FAILURE_ONLY);var gate=new ClusterMembershipGate(properties);gate.observeNodeSession(lease::active);gate.install(committed);gate.apply(peer.getCluster().getMembers());
                var peerResources=f.process.bind("cluster","peer","boot-peer",4,()->{if(!lease.active())throw new io.tapstate.core.common.TapstateException(ValidationError.OWNER_NOT_AUTHORIZED,Map.of("pipeline","flow"),null);});
                try(var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers,time,new io.tapstate.core.logging.SecretRedactor());var processors=new ValidationProcessorRegistry(f.member,()->{});
                        var left=new ValidationOwnerRouter(f.member,f.gate,()->WorkloadClaimFence.from(f.node),f.owners::readOwner,f.owners,
                                new NativeValidationCommands(f.owners,runs,f.member,processors,f.process,new NativeValidationPdkPlans(id->artifact,time)),()->{},Duration.ofSeconds(5));
                        var right=new ValidationOwnerRouter(peer,gate,()->WorkloadClaimFence.from(node),f.owners::readOwner,request->ValidationOwnerRouter.Result.capacity(),()->{},Duration.ofSeconds(5));
                        var a=new NativeValidationStartCoordinator(f.member,f.configuration,left,f.resources,f.process,()->{},Duration.ofSeconds(12),time);
                        var b=new NativeValidationStartCoordinator(peer,f.configuration,right,peerResources,f.process,()->{},Duration.ofSeconds(12),time)){
                    var facts=new MemoryValidationSchedules();
                    try(var first=new NativeValidationScheduler(f.configuration,f.resources,f.process,()->a,facts,time,new ValidationScheduleCalculator(),()->{});
                            var second=new NativeValidationScheduler(f.configuration,peerResources,f.process,()->b,facts,time,new ValidationScheduleCalculator(),()->{})){
                        first.start();second.start();
                        await(()->fact(facts,a,task,1).isPresent()&&fact(facts,b,task,1).isPresent());
                        time.now.set(Instant.parse("2026-10-29T00:01:01Z"));
                        try{await(()->f.configuration.validationReports(()->{}).history("flow",task.taskId(),ValidationMode.FULL,new ValidationReports.Paging(0,10)).items().stream().anyMatch(row->row.terminal()==ValidationRunState.COMPLETED));}
                        catch(org.awaitility.core.ConditionTimeoutException failed){throw new AssertionError("scheduled original: a="+fact(facts,a,task,1)+"; b="+fact(facts,b,task,1)+"; mode="+f.tasks.read("flow",task.taskId()).orElseThrow().full()+"; resources="+f.resources.snapshot()+"; reports="+f.configuration.validationReports(()->{}).history("flow",task.taskId(),ValidationMode.FULL,new ValidationReports.Paging(0,10)),failed);}
                        assertThat(f.tasks.read("flow",task.taskId()).orElseThrow().full().nextStartSequence()).isEqualTo(2);
                        assertThat(f.configuration.validationReports(()->{}).history("flow",task.taskId(),ValidationMode.FULL,new ValidationReports.Paging(0,10)).total()).isEqualTo(1);
                        assertThat(f.member.getJet().getJobs()).hasSize(1);
                    }
                }finally{peerResources.close();}
            }finally{
                if(peerLease!=null){var stopped=peer;peerLease.stopAndClose(()->{if(stopped!=null)stopped.shutdown();});}else if(peer!=null)peer.shutdown();
                f.raw.getDatabase(source).drop();f.raw.getDatabase(target).drop();
            }
        }
    }
    @Test @Timeout(90) void cronSkipsBusyLateAndDowntimeOccurrencesAndOffFencesTheNextTick()throws Exception{
        try(var f=new NativeValidationOwnerReservationsIT.Fixture()){
            var source="cron_source_"+f.suffix;var target="cron_target_"+f.suffix;var time=new Time();var ready=new AtomicBoolean();
            try{
                NativeValidationMetadataIT.seed(f,source,target);var artifact=NativeValidationPdkCountIT.artifact("mongodb");var task=create(f,time,new ValidationSchedule.Cron("* * * * *","Asia/Shanghai"),artifact);
                f.startup=(saved,spec)->new NativeValidationOwnerReservations.StartupGate(true,()->ready.get()?ValidationRunLifecycle.Readiness.READY:ValidationRunLifecycle.Readiness.WAITING_FOR_INITIAL_LOAD);
                try(var runs=new NativeValidationRuns(f.owners,f.resources,f.process,f.timers,time,new io.tapstate.core.logging.SecretRedactor());var processors=new ValidationProcessorRegistry(f.member,()->{});
                        var router=new ValidationOwnerRouter(f.member,f.gate,()->WorkloadClaimFence.from(f.node),f.owners::readOwner,f.owners,
                                new NativeValidationCommands(f.owners,runs,f.member,processors,f.process,new NativeValidationPdkPlans(id->artifact,time)),()->{},Duration.ofSeconds(5));
                        var coordinator=new NativeValidationStartCoordinator(f.member,f.configuration,router,f.resources,f.process,()->{},Duration.ofSeconds(12),time)){
                    var facts=new MemoryValidationSchedules();
                    try(var scheduler=new NativeValidationScheduler(f.configuration,f.resources,f.process,()->coordinator,facts,time,new ValidationScheduleCalculator(),()->{})){
                        scheduler.start();await(()->fact(facts,coordinator,task,1).isPresent());
                        var manual=new ValidationOperation(coordinator.context(),"flow",task.taskId(),task.generation(),1,1,UUID.randomUUID());
                        try(var ignored=coordinator.start(manual)){}
                        await(()->f.tasks.read("flow",task.taskId()).orElseThrow().full().permission()!=null&&f.tasks.read("flow",task.taskId()).orElseThrow().full().permission().phase()==ValidationTaskStore.Phase.BORN);
                        time.now.set(Instant.parse("2026-10-29T00:01:00Z"));await(()->fact(facts,coordinator,task,1).map(value->value.state()==ValidationScheduleObservations.State.SKIPPED_BUSY).orElse(false));
                        assertThat(f.member.getJet().getJobs()).isEmpty();assertThat(f.tasks.read("flow",task.taskId()).orElseThrow().full().nextStartSequence()).isEqualTo(2);
                        try(var ignored=coordinator.stop(manual)){}
                        await(()->f.tasks.read("flow",task.taskId()).orElseThrow().full().permission().phase()==ValidationTaskStore.Phase.CLOSED);
                        time.now.set(Instant.parse("2026-10-29T00:03:10Z"));await(()->fact(facts,coordinator,task,1).map(value->value.state()==ValidationScheduleObservations.State.SKIPPED_LATE).orElse(false));
                        assertThat(f.member.getJet().getJobs()).isEmpty();ready.set(true);time.now.set(Instant.parse("2026-10-29T00:04:00Z"));
                        await(()->f.configuration.validationReports(()->{}).history("flow",task.taskId(),ValidationMode.FULL,new ValidationReports.Paging(0,10)).items().stream().anyMatch(row->row.terminal()==ValidationRunState.COMPLETED));
                        assertThat(f.member.getJet().getJobs()).hasSize(1);
                        scheduler.close();time.now.set(Instant.parse("2026-10-29T00:10:10Z"));
                        try(var restarted=new NativeValidationScheduler(f.configuration,f.resources,f.process,()->coordinator,facts,time,new ValidationScheduleCalculator(),()->{})){
                            restarted.start();await(()->fact(facts,coordinator,task,1).map(value->value.state()==ValidationScheduleObservations.State.WAITING&&value.nextDue().equals(Instant.parse("2026-10-29T00:11:00Z"))).orElse(false));
                            // The stopped manual report and the completed scheduled report both remain in FULL history.
                            assertThat(f.configuration.validationReports(()->{}).history("flow",task.taskId(),ValidationMode.FULL,new ValidationReports.Paging(0,10)).total()).isEqualTo(2);
                            assertThat(f.tasks.read("flow",task.taskId()).orElseThrow().full().nextStartSequence()).isEqualTo(3);
                            time.now.set(Instant.parse("2026-10-29T00:11:00Z"));
                            await(()->f.configuration.validationReports(()->{}).history("flow",task.taskId(),ValidationMode.FULL,new ValidationReports.Paging(0,10)).items().stream().filter(row->row.terminal()==ValidationRunState.COMPLETED).count()==2);
                            assertThat(f.tasks.disableSchedule(new ValidationTaskStore.DisableSchedule(task,1,UUID.randomUUID()))).isEqualTo(ValidationTaskStore.Write.APPLIED);
                            time.now.set(Instant.parse("2026-10-29T00:12:00Z"));
                            await(()->fact(facts,coordinator,task,1).isEmpty());
                            assertThat(f.tasks.read("flow",task.taskId()).orElseThrow().full().nextStartSequence()).isEqualTo(4);assertThat(f.member.getJet().getJobs()).hasSize(2);
                        }
                    }
                }
            }finally{f.raw.getDatabase(source).drop();f.raw.getDatabase(target).drop();}
        }
    }
    private static ValidationReportAdmission.Task create(NativeValidationOwnerReservationsIT.Fixture f,Time time,ValidationSchedule schedule,io.tapstate.adapters.pdk.ConnectorRef artifact){
        var metadata=new NativeValidationMetadata(f.configuration,f.resources,id->artifact,time);
        try(var scope=metadata.inspect(new ValidationTaskStore.Selection("flow",List.of("out")),List.of(new ValidationSelectedDeclarations.Choice("orders","out","input","orders")),ValidationMethod.COUNT,Map.of(),time.instant().plusSeconds(30))){
            var comparison=ValidationTaskCompiler.compile(scope,Map.of(),null).comparison();var task=new ValidationReportAdmission.Task("cluster","flow","scheduled",UUID.randomUUID());
            var settings=new ValidationTaskStore.Settings(new ValidationTaskStore.Full(ValidationMethod.COUNT,1,schedule,true,10),new ValidationTaskStore.Incremental(0,10));
            assertThat(f.tasks.create(new ValidationTaskStore.Create(task,comparison,settings,UUID.randomUUID()))).isEqualTo(ValidationTaskStore.Write.APPLIED);return task;
        }
    }
    private static Optional<ValidationScheduleObservations.Fact> fact(MemoryValidationSchedules facts,NativeValidationStartCoordinator coordinator,ValidationReportAdmission.Task task,long revision){
        var origin=coordinator.context();return facts.view(new ValidationScheduleObservations.Origin(origin.node(),origin.boot(),origin.member(),origin.incarnation())).read(task,revision);
    }
    private static HazelcastInstance peer(HazelcastInstance first){
        var config=new Config().setClusterName(first.getConfig().getClusterName());config.getJetConfig().setEnabled(true).setCooperativeThreadCount(4);
        config.addExecutorConfig(new ExecutorConfig(ValidationOwnerRouter.EXECUTOR).setPoolSize(1).setQueueCapacity(16));
        config.getMemberAttributeConfig().setAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE,"peer").setAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE,"boot-peer");
        config.setProperty("hazelcast.logging.type","none").setProperty("hazelcast.phone.home.enabled","false").setProperty("hazelcast.shutdownhook.enabled","false")
                .setProperty("hazelcast.operation.thread.count","2").setProperty("hazelcast.operation.generic.thread.count","2").setProperty("hazelcast.io.input.thread.count","1").setProperty("hazelcast.io.output.thread.count","1").setProperty("hazelcast.partition.count","17");
        var network=config.getNetworkConfig();network.setPort(0).setPortAutoIncrement(false).getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        var join=network.getJoin();join.getMulticastConfig().setEnabled(false);join.getAutoDetectionConfig().setEnabled(false);join.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1:"+first.getCluster().getLocalMember().getAddress().getPort());
        var peer=Hazelcast.newHazelcastInstance(config);await(()->first.getCluster().getMembers().size()==2&&peer.getCluster().getMembers().size()==2);return peer;
    }
    private static void await(java.util.function.BooleanSupplier predicate){org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100)).until(predicate::getAsBoolean);}
}
