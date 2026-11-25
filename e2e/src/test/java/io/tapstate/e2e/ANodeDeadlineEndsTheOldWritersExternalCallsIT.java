package io.tapstate.e2e;

import com.hazelcast.core.HazelcastInstance;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.context.ConfigurableApplicationContext;
import static org.assertj.core.api.Assertions.*;

/** Real native leases and PDK file I/O; only delivery of one actual Mongo renewal result is delayed. */
@RequiresDocker
class ANodeDeadlineEndsTheOldWritersExternalCallsIT {
    private static final String SOURCE="deadline_source", TARGET="deadline_target", PIPELINE="deadline_pipeline", TABLE="orders";
    private static final Duration BOUND=Duration.ofSeconds(90);
    private static final List<String> ARGUMENTS=List.of("--tapstate.cluster.node-session-ttl=10s",
            "--tapstate.cluster.node-session-renew-interval=1s");

    @Test void aLateSuccessfulRenewalCannotAuthorizeAnotherTargetCallOrConfirmTheHeldWrite(@TempDir Path directory) throws Exception {
        Path source=Files.createDirectory(directory.resolve("source")), target=Files.createDirectory(directory.resolve("target"));
        FileEndpoints.replaceTable(source.resolve(TABLE+".csv"),"id,priority\n1,initial\n");
        byte[] connector=CdcRecoveryFixture.connectorJar(E2eConnectorJar.buildInto(Files.createDirectory(directory.resolve("connector"))));
        var holds=new Holds(directory.resolve("holds")); var written=new WriteWitness(directory.resolve("written"));
        Path replies=Files.createDirectory(directory.resolve("replies"));
        var originalAcknowledgement=new AtomicReference<Document>();
        try (var storage=PreparedNativeStore.create(); var documents=new NativeSrsDocuments(storage)) {
            try (var server=InProcessServer.startPrepared(storage,ARGUMENTS)) {
                var control=new ControlPlane(server.baseUrl()); storage.loginAndAwaitBusiness(control);
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID,connector); control.apply(resources(source,target,holds,written,replies));
                control.discoverSchema(SOURCE,E2eConnectorJar.CONNECTOR_ID,Map.of("uri",source.toString()));
                control.lifecycle(PIPELINE,LifecycleVerb.START);
                Await.until("the native continuous pipeline to run",BOUND,() -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                        () -> control.state(PIPELINE)+" "+control.failureCode(PIPELINE));
                awaitValue(target,"1","initial");
                CdcRecoveryFixture.change(source,1,TABLE,"1","initial","confirmed"); awaitValue(target,"1","confirmed");
                Await.until("the first CDC change to have an actual durable acknowledgement",BOUND,() -> {
                    var progress=progress(documents); var ack=progress == null ? null : progress.get("sinkAcked",Document.class);
                    return ack != null && ack.getString("token") != null && !ack.getString("token").equals(progress.getString("cdcStartPosition"));
                },() -> String.valueOf(progress(documents)));
                var confirmed=progress(documents).get("sinkAcked",Document.class);
                originalAcknowledgement.set(confirmed);
                var child=businessContext(server); Object lease=child.getBean("nodeSessionLease");
                var member=child.getBean(HazelcastInstance.class);
                try (var delayed=new DelayedNodeRenewal(lease)) {
                    try {
                        CdcRecoveryFixture.change(source,2,TABLE,"2","","held");
                        Await.until("the actual target write to remain inside the connector after its effect",BOUND,
                                () -> Files.exists(replies.resolve("held")),
                                () -> written.rowsOf(TABLE).toString());
                        long effectObserved=System.nanoTime(); assertThat(value(target,"2")).isEqualTo("held");
                        assertThat(written.rowsOf(TABLE)).anySatisfy(row -> assertThat(row.id()).isEqualTo("2"));
                        var membership=documents.pipelineMembership(PIPELINE,SOURCE); String chain=membership.get("_id",Document.class).getString("chain");
                        var roster=documents.consumerOffset(chain,SrsConsumerId.of(PIPELINE,SOURCE).value()).get("writerRun",Document.class)
                                .getList("expected",Document.class).stream().filter(table -> TABLE.equals(table.getString("table")))
                                .flatMap(table -> table.getList("writers",String.class).stream()).toList();
                        assertThat(roster).as("the later row is queued behind this run's one real sink writer").hasSize(1);
                        long beforeQueued=documents.largestSequence(chain,TABLE);
                        holds.before(TABLE,3); CdcRecoveryFixture.change(source,3,TABLE,"3","","after-deadline");
                        Await.until("the later demand to enter the old run's read backlog while its sole target writer is held",BOUND,() -> {
                            long current=documents.largestSequence(chain,TABLE);
                            return current>beforeQueued && documents.consumerReadSeq(chain,SrsConsumerId.of(PIPELINE,SOURCE).value(),TABLE)>=current;
                        },() -> String.valueOf(progress(documents)));
                        assertThat(holds.holderBefore(TABLE,3)).isEmpty();
                        delayed.arm.set(true); assertThat(delayed.held.await(5,TimeUnit.SECONDS)).isTrue();
                        assertThat(delayed.accepted.get()).isPresent(); assertThat(delayed.deadline-effectObserved).isPositive();
                        assertThat(delayed.deadline-System.nanoTime()).isPositive();
                        assertThat(progress(documents).get("sinkAcked",Document.class)).isEqualTo(confirmed);
                        AtomicBoolean closed=(AtomicBoolean)readField(lease,"closed");
                        Await.until("the original monotonic proof to close despite its successful result still being held",Duration.ofSeconds(12),
                                () -> closed.get() && System.nanoTime()>=delayed.deadline,() -> "closed="+closed+", remaining="+(delayed.deadline-System.nanoTime()));
                        assertThat(delayed.returnedAt).isZero(); assertThat(readField(lease,"provenUntil")).isEqualTo(delayed.deadline);
                        assertThat(Files.exists(replies.resolve("returned"))).isFalse();
                        assertThat(value(target,"2")).as("an effect from the pre-deadline call remains visible after expiry").isEqualTo("held");
                        Files.writeString(replies.resolve("release"),"");
                        Await.until("the expired native member to stop",Duration.ofSeconds(20),
                                () -> !member.getLifecycleService().isRunning(),() -> "original member still running");
                        assertThat(holds.holderBefore(TABLE,3)).as("any connector entry for this new row leaves a persistent before-hold witness").isEmpty();
                        assertThat(written.rowsOf(TABLE)).noneMatch(row -> row.id().equals("3")); assertThat(value(target,"3")).isNull();
                        assertThat(progress(documents).get("sinkAcked",Document.class)).isEqualTo(confirmed);
                        delayed.release.countDown();
                        var renewer=(java.util.concurrent.ScheduledExecutorService)readField(lease,"renewer");
                        Await.until("the original renewer to finish processing its late Mongo result",Duration.ofSeconds(10),renewer::isTerminated,() -> "original renewal task still running");
                        assertThat(Files.exists(replies.resolve("returned"))).isTrue();
                        assertThat(delayed.returnedAt).isGreaterThanOrEqualTo(delayed.deadline);
                        assertThat(closed).isTrue(); assertThat(readField(lease,"provenUntil")).isEqualTo(delayed.deadline);
                        assertThat(member.getLifecycleService().isRunning()).isFalse();
                    } finally { delayed.release.countDown(); Files.writeString(replies.resolve("release"),""); }
                }
            }
            // The same native state must still carry the unconfirmed change and the later demand.
            try (var observer=MongoClients.create(storage.cacheUri())) {
                Await.until("the original stable node's actual Mongo lease to expire before a new boot",Duration.ofSeconds(15),() -> {
                    var rows=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(observer.getDatabase(storage.cacheDatabase())).aggregate(List.of(
                            new Document("$match",new Document("resourceType","NODE_SESSION")),
                            new Document("$project",new Document("leased",new Document("$gt",List.of("$leaseUntil","$$NOW")))))).into(new ArrayList<>());
                    return rows.size()==1 && Boolean.FALSE.equals(rows.getFirst().get("leased"));
                },() -> "the original NODE lease remains live");
            }
            try (var replacement=InProcessServer.startPrepared(storage,ARGUMENTS)) {
                var control=new ControlPlane(replacement.baseUrl()); storage.loginAndAwaitBusiness(control);
                Await.until("the new native run to enter the previously refused target call",BOUND,
                        () -> holds.holderBefore(TABLE,3).isPresent(),() -> control.state(PIPELINE)+" "+control.failureCode(PIPELINE));
                holds.releaseBefore(TABLE,3); awaitValue(target,"2","held"); awaitValue(target,"3","after-deadline");
                assertThat(written.rowsOf(TABLE).stream().filter(row -> row.id().equals("2")).count())
                        .as("the write whose result was unconfirmed remained owed to its successor").isGreaterThanOrEqualTo(2);
                Await.until("the replacement to publish a later actual durable acknowledgement",BOUND,() -> {
                    var progress=progress(documents); var ack=progress == null ? null : progress.get("sinkAcked",Document.class);
                    return ack != null && !ack.equals(originalAcknowledgement.get());
                },() -> String.valueOf(progress(documents)));
            } finally { holds.releaseBefore(TABLE,3); }
        }
    }

    private static Document progress(NativeSrsDocuments documents) {
        var membership=documents.pipelineMembership(PIPELINE,SOURCE); if (membership==null) return null;
        var key=membership.get("_id",Document.class);
        var offset=documents.consumerOffset(key.getString("chain"),SrsConsumerId.of(PIPELINE,SOURCE).value());
        return offset==null ? null : offset.get("progress",Document.class);
    }
    private static ConfigurableApplicationContext businessContext(InProcessServer server) throws Exception {
        @SuppressWarnings("unchecked") var contexts=(List<ConfigurableApplicationContext>)readField(server,"refreshedContexts");
        return contexts.stream().filter(context -> context.containsLocalBean("nativeBusinessContext")).findFirst().orElseThrow();
    }
    private static Object readField(Object owner,String name) throws Exception {
        Field field=owner.getClass().getDeclaredField(name); field.setAccessible(true); return field.get(owner);
    }
    private static String value(Path target,String id) {
        var lines=CdcRecoveryFixture.lines(target.resolve(TABLE+".csv")); if (lines.isEmpty()) return null;
        var columns=List.of(lines.getFirst().split(",",-1)); int key=columns.indexOf("id"), value=columns.indexOf("priority");
        for (var line:lines.subList(1,lines.size())) { var cells=line.split(",",-1); if (cells.length>Math.max(key,value) && id.equals(cells[key])) return cells[value]; }
        return null;
    }
    private static void awaitValue(Path target,String id,String expected) {
        Await.until("the actual file target row "+id+" to equal "+expected,BOUND,() -> expected.equals(value(target,id)),
                () -> CdcRecoveryFixture.lines(target.resolve(TABLE+".csv")).toString());
    }
    private static Map<String,String> resources(Path source,Path target,Holds holds,WriteWitness written,Path replies) {
        return Map.of(SOURCE+".tap.yml","""
                version: tapstate/v1
                kind: source
                id: %s
                connector: e2e_file
                config: {uri: "%s"}
                mode: cdc
                tables: [orders]
                """.formatted(SOURCE,source),TARGET+".tap.yml",Workspaces.targetYaml(TARGET,target,
                        Map.of("hold",holds.directory().toString(),"write_witness",written.directory().toString(),
                                "hold_result",replies.toString(),"hold_result_id","2")),PIPELINE+".tap.yml","""
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [{id: %s, srs: true}]
                settings: {read_mode: snapshot_and_cdc}
                serve: {from: orders, sync: [{source: %s, execution: {parallelism: 1, batch: {max_records: 1}}}]}
                """.formatted(PIPELINE,SOURCE,TARGET));
    }

    private static final class DelayedNodeRenewal implements WorkloadClaimStore,AutoCloseable {
        private final Object lease;
        private final WorkloadClaimStore delegate;
        private final WorkloadClaim original;
        private final AtomicBoolean arm=new AtomicBoolean();
        private final CountDownLatch held=new CountDownLatch(1),release=new CountDownLatch(1);
        private final AtomicReference<Optional<WorkloadClaim>> accepted=new AtomicReference<>(Optional.empty());
        private volatile long deadline,returnedAt;
        DelayedNodeRenewal(Object lease) throws Exception {
            this.lease=lease; delegate=(WorkloadClaimStore)readField(lease,"store");
            @SuppressWarnings("unchecked") var current=(AtomicReference<WorkloadClaim>)readField(lease,"current"); original=current.get();
            assertThat(original.key().type()).isEqualTo(WorkloadClaimType.NODE_SESSION);
            Field field=lease.getClass().getDeclaredField("store"); field.setAccessible(true); field.set(lease,this);
        }
        @Override public Optional<WorkloadClaim> renew(WorkloadClaim expected,Duration ttl) {
            boolean delay=expected.key().equals(original.key()) && expected.owner().equals(original.owner())
                    && expected.claimGeneration()==original.claimGeneration() && arm.compareAndSet(true,false);
            if (delay) try { deadline=(long)readField(lease,"provenUntil"); } catch (Exception failure) { throw new AssertionError(failure); }
            var result=delegate.renew(expected,ttl);
            if (delay) {
                accepted.set(result); held.countDown(); boolean interrupted=false;
                try {
                    while (true) try { if (!release.await(40,TimeUnit.SECONDS)) throw new AssertionError("the test did not release its renewal result"); break; }
                    catch (InterruptedException stopping) { interrupted=true; }
                } finally { returnedAt=System.nanoTime(); if (interrupted) Thread.currentThread().interrupt(); }
            }
            return result;
        }
        @Override public WorkloadClaimAttempt acquire(WorkloadClaimKey key,WorkloadOwner owner,long topology,Duration ttl) { return delegate.acquire(key,owner,topology,ttl); }
        @Override public WorkloadClaimAttempt acquirePipeline(PipelineIntentScope scope,WorkloadOwner owner,long topology,Duration ttl) { return delegate.acquirePipeline(scope,owner,topology,ttl); }
        @Override public Optional<WorkloadClaimReading> readPipeline(PipelineIntentScope scope) { return delegate.readPipeline(scope); }
        @Override public boolean release(WorkloadClaim expected) { return delegate.release(expected); }
        @Override public Optional<WorkloadClaim> advanceExecution(WorkloadClaim expected,long topology,Set<String> nodes) { return delegate.advanceExecution(expected,topology,nodes); }
        @Override public Optional<WorkloadClaim> recordExecutionFailure(WorkloadClaim expected,boolean lost) { return delegate.recordExecutionFailure(expected,lost); }
        @Override public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) { return delegate.read(key); }
        @Override public Map<WorkloadClaimKey,WorkloadClaimReading> readAll(Collection<WorkloadClaimKey> keys) { return delegate.readAll(keys); }
        @Override public void close() { release.countDown(); }
    }
}
