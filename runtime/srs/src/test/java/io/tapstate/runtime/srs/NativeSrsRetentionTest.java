package io.tapstate.runtime.srs;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.*;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import static org.assertj.core.api.Assertions.*;

class NativeSrsRetentionTest {
    private static final CacheBusinessScope SCOPE = new CacheBusinessScope("cluster", 1, "cache");
    private static final String CHAIN = "chain";
    private static final WorkloadClaimFence CAPTURE = fence(WorkloadClaimType.CAPTURE, "capture");

    @Test void nativeRetentionReusesTheChainValidatedWithItsTopology() {
        var f = new Fixture(Set.of("a"));
        f.combined = true;
        f.progress(f.attach("pipeline", Set.of("a")), Map.of("a", 7L));
        try (var unit = f.unit()) { f.retain(unit); }
        assertThat(f.chainReads).as("the coherent topology already contains its validated chain").isZero();
        assertThat(f.combinedReads).isEqualTo(1);
        assertThat(f.trims).containsOnly(entry(f.ring("a"), 7L));
        assertThat(f.reclaims).isEqualTo(1);
    }

    @Test void jointRetentionFloorsDoNotReopenIndividualConsumerReads() {
        var f = new Fixture(Set.of("a", "b"));
        f.jointFloors = Map.of("a", 4L, "b", -1L);
        f.attach("first", Set.of("a", "b"));
        f.attach("second", Set.of("a"));
        try (var unit = f.unit()) { f.retain(unit); }
        assertThat(f.combinedReads).isEqualTo(1);
        assertThat(f.chainReads).isZero();
        assertThat(f.reads).isEmpty();
        assertThat(f.trims).containsOnly(entry(f.ring("a"), 4L));
        assertThat(f.reclaims).isEqualTo(1);
    }

    @Test void aHundredTablesReadEachRelevantConsumerOnlyOncePerPass() {
        var tables = IntStream.range(0, 100).mapToObj(i -> "table-" + i).collect(Collectors.toSet());
        var f = new Fixture(tables);
        var consumer = f.attach("pipeline", tables);
        f.progress(consumer, tables.stream().collect(Collectors.toMap(name -> name, name -> -1L)));
        try (var unit = f.unit()) { f.retain(unit); }
        assertThat(f.reads).containsOnly(entry(consumer.context(), 1));
        assertThat(f.trims).isEmpty();
        assertThat(f.reclaims).isEqualTo(1);
    }

    @Test void subscribersBoundTheirOwnTablesAndUnconfirmedTablesDoNotBorrowAnotherFloor() {
        var f = new Fixture(Set.of("a", "b", "unconfirmed", "unsubscribed"));
        var first = f.attach("first", Set.of("a", "b", "unconfirmed"));
        var second = f.attach("second", Set.of("a"));
        f.attach("not-serving", Set.of("future"));
        f.progress(first, Map.of("a", 12L, "b", 7L));
        f.progress(second, Map.of("a", 4L));
        try (var unit = f.unit()) { f.retain(unit); }
        assertThat(f.trims).containsOnly(entry(f.ring("a"), 4L), entry(f.ring("b"), 7L));
        assertThat(f.reads).containsOnly(entry(first.context(), 1), entry(second.context(), 1));
        assertThat(f.reclaims).isEqualTo(1);
    }

    @Test void coherentSnapshotUsesTheLowestConfirmedFloorWithoutSeparateReads() {
        var f = new Fixture(Set.of("a", "b", "unconfirmed", "unsubscribed"));
        f.coherentSnapshot = true;
        var faster = f.attach("faster", Set.of("a", "b", "unconfirmed"));
        var slower = f.attach("slower", Set.of("a"));
        var future = f.attach("future", Set.of("not-serving"));
        f.progress(faster, Map.of("a", 12L, "b", 7L));
        f.progress(slower, Map.of("a", 4L));
        f.progress(future, Map.of("not-serving", 99L));

        try (var unit = f.unit()) {
            f.retain(unit);
            assertThat(f.trims).containsOnly(entry(f.ring("a"), 4L), entry(f.ring("b"), 7L));
            f.progress(slower, Map.of("a", 9L));
            f.retain(unit);
        }

        assertThat(f.trims).containsOnly(entry(f.ring("a"), 9L), entry(f.ring("b"), 7L));
        assertThat(f.reads).isEmpty();
        assertThat(f.snapshotReads).isEqualTo(2);
        assertThat(f.reclaims).isEqualTo(2);
    }

    @Test void aDetachAfterTheTopologySnapshotAbandonsThePass() {
        var f = new Fixture(Set.of("a"));
        f.attach("detached", Set.of("a"));
        try (var unit = f.unit()) { f.retain(unit); }
        assertThat(f.trims).isEmpty();
        assertThat(f.reclaims).isZero();
    }

    @Test void aReplacedMembershipCannotSupplyTheOriginalTopologysFloor() {
        var f = new Fixture(Set.of("a"));
        var original = f.attach("pipeline", Set.of("a"));
        f.progress(new SrsConsumerMembership(original.context(), 2, false, original.tables(), original.commandFence()), Map.of("a", 7L));
        try (var unit = f.unit()) { f.retain(unit); }
        assertThat(f.trims).isEmpty();
        assertThat(f.reclaims).isZero();
    }

    @Test void losingTheOriginalAuthorityDuringBoundsReadCannotIssueATrim() {
        var f = new Fixture(Set.of("a"));
        f.progress(f.attach("pipeline", Set.of("a")), Map.of("a", 7L));
        f.afterBounds = () -> f.active = false;
        try (var unit = f.unit()) {
            assertThatThrownBy(() -> f.retain(unit)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        }
        assertThat(f.trims).isEmpty();
        assertThat(f.reclaims).isZero();
    }

    @Test void theNextPassReadsFreshProgressAndKeepsTheOriginalCaptureFence() {
        var f = new Fixture(Set.of("a"));
        var consumer = f.attach("pipeline", Set.of("a"));
        f.progress(consumer, Map.of("a", 3L));
        try (var unit = f.unit()) {
            f.retain(unit);
            assertThat(f.trims).containsOnly(entry(f.ring("a"), 3L));
            f.progress(consumer, Map.of("a", 9L));
            f.retain(unit);
        }
        assertThat(f.trims).containsOnly(entry(f.ring("a"), 9L));
        assertThat(f.reads).containsOnly(entry(consumer.context(), 2));
        assertThat(f.reclaims).isEqualTo(2);
    }

    private static WorkloadClaimFence fence(WorkloadClaimType type, String resource) {
        return new WorkloadClaimFence(new WorkloadClaimKey("cluster", type, resource), new WorkloadOwner("node", "boot"), 4,
                type == WorkloadClaimType.PIPELINE_ACTUATION ? 1 : 0, 1);
    }

    private static final class Fixture {
        final Set<String> tables;
        final List<SrsConsumerMembership> members = new ArrayList<>();
        final Map<SrsConsumerContext, SrsConsumerProgress> progress = new HashMap<>();
        final Map<SrsConsumerContext, Integer> reads = new HashMap<>();
        final Map<String, Long> trims = new HashMap<>();
        boolean active = true;
        boolean coherentSnapshot;
        int snapshotReads;
        int reclaims;
        boolean combined;
        Map<String,Long> jointFloors;
        int chainReads, combinedReads;
        Runnable afterBounds = () -> {};

        Fixture(Set<String> tables) { this.tables = tables; }
        String ring(String table) { return SrsRingbuffer.ringName(CHAIN, table); }
        void requireActive() { if (!active) throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }
        SrsConsumerMembership attach(String pipeline, Set<String> selected) {
            var context = new SrsConsumerContext(new SrsConsumerKey(CHAIN, pipeline, "source"), 1);
            var member = new SrsConsumerMembership(context, 1, false, selected, fence(WorkloadClaimType.PIPELINE_ACTUATION, pipeline));
            members.add(member); return member;
        }
        void progress(SrsConsumerMembership member, Map<String, Long> floors) {
            var readAhead = member.tables().stream().collect(Collectors.toMap(name -> name, name -> 99L));
            var cursor = new ConsumerOffset(member.context().key().consumerId(), readAhead, null, List.of(), null, 0, Map.of(), ConsumerProgressKind.SRS);
            progress.put(member.context(), new SrsConsumerProgress(member.context(), member.revision(), 1, cursor, null, null, floors));
        }
        void retain(NativeSrsCaptureRunUnit unit) { unit.retain(SCOPE, CHAIN, CAPTURE, this::requireActive); }
        private SrsChainState chainState() {
            return new SrsChainState(CHAIN, "capture", "durable", 1, tables, tables, 0, null, null, members.size(), members.size());
        }

        NativeSrsCaptureRunUnit unit() {
            var chains = proxy(SrsChainStore.class, (name, args) -> {
                if (!name.equals("load") || coherentSnapshot) throw new AssertionError(name);
                chainReads++;
                assertThat(args[0]).isEqualTo(CHAIN);
                return Optional.of(chainState());
            });
            var consumers = proxy(SrsConsumerStore.class, (name, args) -> switch (name) {
                case "retentionFloors" -> {
                    if (jointFloors == null) yield null;
                    combinedReads++;
                    yield Optional.of(jointFloors);
                }
                case "retentionSnapshot" -> {
                    if (args.length == 2) {
                        if (combined) {
                            combinedReads++;
                            yield new SrsConsumerStore.AttachedChain(new SrsConsumerTopology(SCOPE, CHAIN, members.size(), members), chainState());
                        }
                        var chain = ((SrsChainStore) args[1]).load(CHAIN).orElseThrow();
                        yield new SrsConsumerStore.AttachedChain(new SrsConsumerTopology(SCOPE, CHAIN, members.size(), members), chain);
                    }
                    if (!coherentSnapshot) yield Optional.empty();
                    assertThat(args[0]).isEqualTo(CHAIN);
                    snapshotReads++;
                    var captured = members.stream().map(member -> new SrsConsumerStore.RetentionConsumer(
                            member, progress.get(member.context()).ringDoneThrough())).toList();
                    yield Optional.of(new SrsConsumerStore.RetentionSnapshot(chainState(), captured));
                }
                case "attached" -> {
                    if (coherentSnapshot) throw new AssertionError(name);
                    yield new SrsConsumerTopology(SCOPE, CHAIN, members.size(), members);
                }
                case "progress" -> {
                    var context = (SrsConsumerContext) args[0]; reads.merge(context, 1, Integer::sum);
                    yield Optional.ofNullable(progress.get(context));
                }
                default -> throw new AssertionError(name);
            });
            var log = proxy(SrsLogStore.class, (name, args) -> switch (name) {
                case "bounds" -> { afterBounds.run(); yield new SrsLogBounds(99, -1); }
                case "trim" -> {
                    assertThat(args).hasSize(3); assertThat(args[2]).isEqualTo(CAPTURE);
                    trims.put((String) args[0], (Long) args[1]); yield null;
                }
                default -> throw new AssertionError(name);
            });
            var models = proxy(SrsModelStore.class, (name, args) -> {
                if (!name.equals("reclaim")) throw new AssertionError(name);
                assertThat(args[0]).isEqualTo(CHAIN); assertThat(args[1]).isEqualTo(CAPTURE); reclaims++; return 0L;
            });
            var member = proxy(HazelcastInstance.class, (name, args) -> { throw new AssertionError(name); });
            return new NativeSrsCaptureRunUnit(SCOPE, new SrsStores(chains, models, log, consumers), member);
        }
    }

    private static <T> T proxy(Class<T> type, BiFunction<String, Object[], Object> call) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> {
            if (method.getName().equals("guardedBy")) {
                var guard = (Runnable) args[0];
                return proxy(type, (name, arguments) -> { guard.run(); var result = call.apply(name, arguments); guard.run(); return result; });
            }
            var result = call.apply(method.getName(), args);
            if (result == null && method.getName().equals("retentionFloors"))
                return java.lang.reflect.InvocationHandler.invokeDefault(proxy, method, args);
            return result;
        }));
    }
}
