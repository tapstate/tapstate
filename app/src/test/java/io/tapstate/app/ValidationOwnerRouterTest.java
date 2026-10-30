package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.validation.ValidationOwner;
import io.tapstate.core.validation.ValidationOwnerRanking;
import io.tapstate.spi.store.*;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Function;
import static io.tapstate.app.ValidationOwnerRouter.*;
import static org.assertj.core.api.Assertions.*;

/** Real member RPC/identity tests. The local lease port is a fixture, not durable acquisition proof. */
class ValidationOwnerRouterTest {
    @Test void originalCommandsUseOnlyTheExactOwnerAndNeverReserveOrRerank(){
        try(var f=new Fixture(Duration.ofSeconds(5))){
            var request=request();var seen=new CopyOnWriteArrayList<Command>();
            f.commands.put("b",command->{seen.add(command);return new CommandReply(Outcome.ACCEPTED,command,"STARTING",true,true,false,false,false,null);});
            for(var action:Action.values()){
                var command=Command.of(action,request,accepted("b",request).owner());
                assertThat(f.left.command(command)).satisfies(reply->{assertThat(reply.outcome()).isEqualTo(Outcome.ACCEPTED);assertThat(reply.original()).isEqualTo(command);assertThat(reply.waiting()).isTrue();});
            }
            assertThat(seen).extracting(Command::action).containsExactly(Action.ACTIVATE,Action.STATUS,Action.STOP,Action.CANCEL_UNUSED);
            assertThat(f.calls.get("a")).hasValue(0);assertThat(f.calls.get("b")).hasValue(0);
            var wrongBoot=new Command(Action.ACTIVATE,request,"cluster","b","other-boot",3,4,1);
            assertThat(f.left.command(wrongBoot).outcome()).isEqualTo(Outcome.REJECTED);assertThat(seen).hasSize(4);
        }
    }
    @Test void aLateOriginalCommandNeverFallsBackToAnotherMember()throws Exception{
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var finished=new CountDownLatch(1);
        try(var f=new Fixture(Duration.ofMillis(200))){
            var request=request();var command=Command.of(Action.ACTIVATE,request,accepted("b",request).owner());var calls=new AtomicInteger();
            f.commands.put("b",original->{calls.incrementAndGet();entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("command gate timeout");}
                catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}finished.countDown();return new CommandReply(Outcome.ACCEPTED,original,"STARTING",false,false,false,false,false,null);});
            try{assertThat(f.left.command(command).outcome()).isEqualTo(Outcome.UNKNOWN);assertThat(entered.await(2,TimeUnit.SECONDS)).isTrue();}
            finally{release.countDown();}
            assertThat(finished.await(5,TimeUnit.SECONDS)).isTrue();assertThat(calls).hasValue(1);assertThat(f.calls.get("a")).hasValue(0);assertThat(f.calls.get("b")).hasValue(0);
        }finally{release.countDown();}
    }
    @Test void aCommandProbedBeforeContextReplacementCannotEnterTheReplacement()throws Exception{
        try(var f=new Fixture(Duration.ofSeconds(5))){
            var target=f.b.getCluster().getLocalMember();var expected=new Identity("b","boot-b",target.getUuid());var executor=f.a.getExecutorService(EXECUTOR);
            var original=executor.submitToMember(new Probe(expected,1),target).get(5,TimeUnit.SECONDS);f.right.close();var calls=new AtomicInteger();
            f.commands.put("b",command->{calls.incrementAndGet();return commandUnavailable(command,Outcome.REJECTED,null);});
            try(var replacement=f.router(f.b,f.gb,"b",Duration.ofSeconds(5))){
                var request=request();var command=Command.of(Action.STOP,request,accepted("b",request).owner());
                assertThat(executor.submitToMember(new CommandCall(command,expected,1,original.incarnation()),target).get(5,TimeUnit.SECONDS).outcome()).isEqualTo(Outcome.REJECTED);
                assertThat(calls).hasValue(0);
            }
        }
    }
    @Test void anExistingOwnerIsReusedAndItsCapacityRefusalNeverMovesThePipeline() {
        try (var f = new Fixture(Duration.ofSeconds(5))) {
            var request = request(); var ranked = f.ranked(request); var owner = ranked.getLast();
            f.reading.set(reading(owner, request.pipeline()));
            var reply = f.left.reserve(request);
            assertThat(reply.outcome()).isEqualTo(Outcome.ACCEPTED); assertThat(reply.receiver().node()).isEqualTo(owner);
            assertThat(reply.claimGeneration()).isEqualTo(3); assertThat(reply.executionGeneration()).isEqualTo(4);
            assertThat(f.calls.get(ranked.getFirst())).hasValue(0);
            f.behaviors.put(owner, ignored -> Result.capacity());
            assertThat(f.left.reserve(request).outcome()).isEqualTo(Outcome.CAPACITY);
            assertThat(f.calls.get(ranked.getFirst())).hasValue(0);
        }
    }

    @Test void everyOriginalCoordinateSurvivesTheRealReservationRpc() {
        try (var f = new Fixture(Duration.ofSeconds(5))) {
            var request = new Request("other-pipeline", "other-task", UUID.randomUUID(), 9, 17, "INCREMENTAL", UUID.randomUUID(), "original-run");
            var received = new AtomicReference<Request>(); String chosen = f.ranked(request).getFirst();
            f.behaviors.put(chosen, value -> { received.set(value); return accepted(chosen, value); });
            assertThat(f.left.reserve(request).outcome()).isEqualTo(Outcome.ACCEPTED); assertThat(received.get()).isEqualTo(request);
        }
    }

    @Test void losingEitherNodeProofAfterAcceptanceKeepsTheOriginalOutcomeUnknown() {
        for (String lost : List.of("a", "b")) try (var f = new Fixture(Duration.ofSeconds(5))) {
            var request = request(); f.reading.set(reading("b", request.pipeline()));
            f.behaviors.put("b", value -> { f.live.get(lost).set(false); return accepted("b", value); });
            var reply = f.left.reserve(request); assertThat(reply.outcome()).isEqualTo(Outcome.UNKNOWN);
            assertThat(reply.command()).isEqualTo(request.command()); assertThat(f.calls.get("a")).hasValue(0); assertThat(f.calls.get("b")).hasValue(1);
        }
    }

    @Test void anOldReceiverStampCannotEnterAReplacementContextOnTheSameMember() throws Exception {
        try (var f = new Fixture(Duration.ofSeconds(5))) {
            var member = f.b.getCluster().getLocalMember(); var expected = new Identity("b", "boot-b", member.getUuid());
            var executor = f.a.getExecutorService(EXECUTOR);
            var original = executor.submitToMember(new Probe(expected, 1), member).get(5, TimeUnit.SECONDS);
            f.right.close();
            try (var replacement = f.router(f.b, f.gb, "b", Duration.ofSeconds(5))) {
                var request = request();
                var reply = executor.submitToMember(new Call(request, expected, 1, original.incarnation()), member).get(5, TimeUnit.SECONDS);
                assertThat(reply.outcome()).isEqualTo(Outcome.REJECTED); assertThat(f.calls.get("b")).hasValue(0);
                var current = executor.submitToMember(new Probe(expected, 1), member).get(5, TimeUnit.SECONDS);
                assertThat(current.incarnation()).isNotEqualTo(original.incarnation());
            }
        }
    }

    @Test void aSaturatedRealControlExecutorProducesUnknownWithoutDispatchingAnotherMember() throws Exception {
        try (var f = new Fixture(Duration.ofMillis(300))) {
            var request = request(); String chosen = f.ranked(request).getFirst(); var target = chosen.equals("a") ? f.a : f.b;
            var entered = new CountDownLatch(2); var release = new CountDownLatch(1); String key = UUID.randomUUID().toString();
            target.getUserContext().put(key, List.of(entered, release)); var executor = target.getExecutorService(EXECUTOR);
            var futures = new ArrayList<Future<Boolean>>();
            try {
                for (int index = 0; index < 2; index++) futures.add(executor.submitToMember(new Block(key), target.getCluster().getLocalMember()));
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                for (int index = 0; index < 16; index++) futures.add(executor.submitToMember(new Block(key), target.getCluster().getLocalMember()));
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> executor.getLocalExecutorStats().getPendingTaskCount() == 16);
                var reply = f.left.reserve(request); assertThat(reply.outcome()).isEqualTo(Outcome.UNKNOWN);
                assertThat(reply.command()).isEqualTo(request.command()); assertThat(f.calls.get("a")).hasValue(0); assertThat(f.calls.get("b")).hasValue(0);
            } finally {
                release.countDown(); for (var future : futures) assertThat(future.get(5, TimeUnit.SECONDS)).isTrue(); target.getUserContext().remove(key);
            }
        }
    }

    @Test void onlyKnownFreshCapacityRefusalTriesTheNextStableCandidate() {
        try (var f = new Fixture(Duration.ofSeconds(5))) {
            var request = request(); var ranked = f.ranked(request);
            f.behaviors.put(ranked.getFirst(), ignored -> Result.capacity());
            var reply = f.left.reserve(request);
            assertThat(reply.outcome()).isEqualTo(Outcome.ACCEPTED); assertThat(reply.receiver().node()).isEqualTo(ranked.getLast());
            assertThat(f.calls.get(ranked.getFirst())).hasValue(1); assertThat(f.calls.get(ranked.getLast())).hasValue(1);
            assertThat(reply.command()).isEqualTo(request.command());
        }
    }

    @Test void anUnknownReplyNeverDispatchesAnotherCandidateAndTheLateOriginalMayStillFinish() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var finished = new CountDownLatch(1);
        try (var f = new Fixture(Duration.ofMillis(200))) {
            var request = request(); var ranked = f.ranked(request);
            f.behaviors.put(ranked.getFirst(), value -> {
                entered.countDown();
                try { if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("test receiver was not released"); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
                finished.countDown(); return accepted(ranked.getFirst(), value);
            });
            try {
                var reply = f.left.reserve(request); assertThat(reply.outcome()).isEqualTo(Outcome.UNKNOWN);
                assertThat(reply.command()).isEqualTo(request.command()); assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
                assertThat(f.calls.get(ranked.getLast())).hasValue(0);
            } finally { release.countDown(); }
            assertThat(finished.await(5, TimeUnit.SECONDS)).isTrue(); assertThat(f.calls.get(ranked.getLast())).hasValue(0);
        } finally { release.countDown(); }
    }

    @Test void aClaimWinnerDiscoveredAfterAnExplicitRedirectReceivesTheSameOriginalRequest() {
        try (var f = new Fixture(Duration.ofSeconds(5))) {
            var request = request(); var ranked = f.ranked(request);
            f.behaviors.put(ranked.getFirst(), ignored -> {
                f.reading.set(reading(ranked.getLast(), request.pipeline())); return new Result(Outcome.REDIRECT, null, null);
            });
            var reply = f.left.reserve(request);
            assertThat(reply.outcome()).isEqualTo(Outcome.ACCEPTED); assertThat(reply.receiver().node()).isEqualTo(ranked.getLast());
            assertThat(reply.command()).isEqualTo(request.command());
        }
    }

    @Test void aVisibleUncommittedMemberIsNeverAValidationCandidate() {
        try (var f = new Fixture(Duration.ofSeconds(5))) {
            f.install(2, Set.of("a"));
            var reply = f.left.reserve(request());
            assertThat(reply.outcome()).isEqualTo(Outcome.ACCEPTED); assertThat(reply.receiver().node()).isEqualTo("a");
            assertThat(f.calls.get("b")).hasValue(0);
        }
    }

    @Test void aChangedReceiverBootProofCannotEnterTheLocalRuntime() {
        try (var f = new Fixture(Duration.ofSeconds(5))) {
            var request = request(); f.reading.set(reading("b", request.pipeline()));
            f.nodes.get("b").set(new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, "b"),
                    new WorkloadOwner("b", "another-boot"), 1, 0, 1));
            assertThat(f.left.reserve(request).outcome()).isEqualTo(Outcome.REJECTED);
            assertThat(f.calls.get("b")).hasValue(0);
        }
    }

    @Test void aTopologyChangeAfterLocalAcceptanceReportsTheOriginalOperationAsUnknown() {
        try (var f = new Fixture(Duration.ofSeconds(5))) {
            var request = request(); String chosen = f.ranked(request).getFirst();
            f.behaviors.put(chosen, value -> { f.install(2, Set.of("a", "b")); return accepted(chosen, value); });
            var reply = f.left.reserve(request);
            assertThat(reply.outcome()).isEqualTo(Outcome.UNKNOWN); assertThat(reply.command()).isEqualTo(request.command());
            assertThat(f.calls.get(chosen)).hasValue(1);
            assertThat(f.calls.get(chosen.equals("a") ? "b" : "a")).hasValue(0);
        }
    }

    @Test void anAcceptedReceiptDoesNotAuthorizeAReplacementReceiverIncarnation(){
        try(var f=new Fixture(Duration.ofSeconds(5))){
            var request=request();var target=f.ranked(request).getFirst();var sender=target.equals("a")?f.right:f.left;
            var initial=sender.reserveAttempt(request);assertThat(initial.reply().outcome()).isEqualTo(Outcome.ACCEPTED);assertThat(initial.dispatched()).isTrue();
            var owner=new ValidationOwner("cluster",request.pipeline(),target,"boot-"+target,initial.reply().claimGeneration(),initial.reply().executionGeneration(),initial.reply().topologyRevision());
            f.receipts.put(target,original->new LocalReceipt(new Result(Outcome.ACCEPTED,owner,null),false));
            assertThat(sender.receipt(request,initial.receiver()).reply().outcome()).isEqualTo(Outcome.ACCEPTED);
            int reservations=f.calls.values().stream().mapToInt(AtomicInteger::get).sum();var commands=new AtomicInteger();
            f.commands.put(target,command->{commands.incrementAndGet();return new CommandReply(Outcome.ACCEPTED,command,"STARTING",false,false,false,false,false,null);});
            var receiver=target.equals("a")?f.left:f.right;receiver.close();
            try(var replacement=f.router(target.equals("a")?f.a:f.b,target.equals("a")?f.ga:f.gb,target,Duration.ofSeconds(5))){
                assertThat(sender.command(Command.of(Action.ACTIVATE,request,owner),initial.receiver()).outcome()).isEqualTo(Outcome.REJECTED);
                assertThat(sender.receipt(request,initial.receiver()).reply().outcome()).isEqualTo(Outcome.UNKNOWN);
                assertThat(commands).hasValue(0);assertThat(f.calls.values().stream().mapToInt(AtomicInteger::get).sum()).isEqualTo(reservations);
            }
        }
    }
    private static Request request() { return new Request("pipeline", "task", UUID.randomUUID(), 1, 1, "FULL", UUID.randomUUID(), UUID.randomUUID().toString()); }
    private static Result accepted(String node, Request request) {
        return new Result(Outcome.ACCEPTED, new ValidationOwner("cluster", request.pipeline(), node, "boot-" + node, 3, 4, 1), null);
    }
    private static WorkloadClaimReading reading(String node, String pipeline) {
        var claim = new WorkloadClaim(ValidationClaims.key("cluster", pipeline), new WorkloadOwner(node, "boot-" + node), 3, 4, 1, Instant.now().plusSeconds(30));
        return new WorkloadClaimReading(claim, Duration.ofSeconds(30));
    }

    static final class Fixture implements AutoCloseable {
        final HazelcastInstance a;
        HazelcastInstance b;
        final ClusterMembershipGate ga = gate(), gb = gate();
        final Map<String, AtomicInteger> calls = Map.of("a", new AtomicInteger(), "b", new AtomicInteger());
        final Map<String, AtomicBoolean> live = Map.of("a", new AtomicBoolean(true), "b", new AtomicBoolean(true));
        final Map<String, AtomicReference<WorkloadClaimFence>> nodes = new HashMap<>();
        final Map<String, Function<Request, Result>> behaviors = new ConcurrentHashMap<>();
        final Map<String,Function<Command,CommandReply>> commands=new ConcurrentHashMap<>();
        final Map<String,Function<Request,LocalReceipt>> receipts=new ConcurrentHashMap<>();
        final AtomicReference<WorkloadClaimReading> reading = new AtomicReference<>();
        final ValidationOwnerRouter left, right;
        Fixture(Duration timeout) { this(timeout,2); }
        Fixture(Duration timeout,int executorWorkers) {
            ga.observeNodeSession(live.get("a")::get);gb.observeNodeSession(live.get("b")::get);
            String cluster = "validation-routing-" + UUID.randomUUID();
            a = Hazelcast.newHazelcastInstance(config(cluster, "a", null,executorWorkers));
            try {
                b = Hazelcast.newHazelcastInstance(config(cluster, "b", "127.0.0.1:" + a.getCluster().getLocalMember().getAddress().getPort(),executorWorkers));
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> a.getCluster().getMembers().size() == 2 && b.getCluster().getMembers().size() == 2);
                install(1, Set.of("a", "b"));
                for (String id : List.of("a", "b")) {
                    nodes.put(id, new AtomicReference<>(new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, id),
                            new WorkloadOwner(id, "boot-" + id), 1, 0, 1)));
                    behaviors.put(id, value -> accepted(id, value));
                    commands.put(id,value->commandUnavailable(value,Outcome.REJECTED,null));
                }
                left = router(a, ga, "a", timeout); right = router(b, gb, "b", timeout);
            } catch (RuntimeException | Error failed) {
                var diagnosis = new AssertionError("validation fixture join: a=" + a.getCluster().getLocalMember().getAddress() + " members=" + a.getCluster().getMembers()
                        + "; b=" + (b == null ? "absent" : b.getCluster().getLocalMember().getAddress() + " members=" + b.getCluster().getMembers()), failed);
                if (b != null) b.shutdown(); a.shutdown(); throw diagnosis;
            }
        }
        ValidationOwnerRouter router(HazelcastInstance member, ClusterMembershipGate gate, String id, Duration timeout) {
            return new ValidationOwnerRouter(member, gate, () -> nodes.get(id).get(), ignored -> Optional.ofNullable(reading.get()),
                    new Local(){public Result reserve(Request request){calls.get(id).incrementAndGet();return behaviors.get(id).apply(request);}
                        public LocalReceipt receipt(Request request){var read=receipts.get(id);return read==null?LocalReceipt.unknown():read.apply(request);}}, command->commands.get(id).apply(command),() -> {
                        if (!live.get(id).get()) throw new io.tapstate.core.common.TapstateException(io.tapstate.core.validation.ValidationError.OWNER_NOT_AUTHORIZED,
                                Map.of("pipeline", "test"), null);
                    }, timeout);
        }
        List<String> ranked(Request request) { return ValidationOwnerRanking.rank("cluster", request.pipeline(), List.of("a", "b")); }
        void install(long revision, Set<String> nodes) {
            var committed = new ClusterMembership("cluster", revision, nodes);
            ga.install(committed); gb.install(committed); ga.apply(a.getCluster().getMembers()); gb.apply(b.getCluster().getMembers());
        }
        @Override public void close() { left.close(); right.close(); b.shutdown(); a.shutdown(); }
        private static ClusterMembershipGate gate() {
            var properties = new ClusterProperties(); properties.setProfile(ClusterProperties.Profile.PROCESS_FAILURE_ONLY); return new ClusterMembershipGate(properties);
        }
        private static Config config(String cluster, String id, String seed,int executorWorkers) {
            var config = new Config().setClusterName(cluster);
            config.getMemberAttributeConfig().setAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE, id).setAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE, "boot-" + id);
            config.addExecutorConfig(new ExecutorConfig(EXECUTOR).setPoolSize(executorWorkers).setQueueCapacity(16));
            config.getJetConfig().setCooperativeThreadCount(2);
            config.setProperty("hazelcast.partition.count", "17").setProperty("hazelcast.phone.home.enabled", "false")
                    .setProperty("hazelcast.shutdownhook.enabled", "false").setProperty("hazelcast.logging.type", System.getProperty("test.validation.routing.logging", "none"))
                    .setProperty("hazelcast.operation.thread.count", "2").setProperty("hazelcast.operation.generic.thread.count", "2")
                    .setProperty("hazelcast.io.input.thread.count", "1").setProperty("hazelcast.io.output.thread.count", "1");
            var network = config.getNetworkConfig(); network.setPort(0).setPortAutoIncrement(false); network.getInterfaces().setEnabled(true).addInterface("127.0.0.1");
            var join = network.getJoin(); join.getMulticastConfig().setEnabled(false); join.getAutoDetectionConfig().setEnabled(false);
            if (seed != null) join.getTcpIpConfig().setEnabled(true).addMember(seed); else join.getTcpIpConfig().setEnabled(true);
            return config;
        }
    }
    private static final class Block implements Callable<Boolean>, java.io.Serializable, com.hazelcast.core.HazelcastInstanceAware {
        private static final long serialVersionUID = 1L;
        private final String key; private transient HazelcastInstance member;
        Block(String key) { this.key = key; }
        @Override public void setHazelcastInstance(HazelcastInstance member) { this.member = member; }
        @Override public Boolean call() throws Exception {
            var gates = (List<?>) member.getUserContext().get(key); ((CountDownLatch) gates.getFirst()).countDown();
            if (!((CountDownLatch) gates.getLast()).await(10, TimeUnit.SECONDS)) throw new AssertionError("test executor was not released");
            return true;
        }
    }
}
