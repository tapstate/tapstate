package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.common.TapstateException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class AdmittedMemberPlanMetaSupplierTest {

    @Test
    void aProperSubsetIsRefusedBeforeTheDagIsDecorated() throws Exception {
        Address a = address(5701);
        Address b = address(5702);
        RecordingMetaSupplier original = new RecordingMetaSupplier();
        DAG dag = dag(original);

        assertRefused(() -> AdmittedMemberPlanMetaSupplier.install("orders", dag, Set.of("a"),
                List.of(member("a", a, 1), member("b", b, 2))));

        assertThat(dag.getVertex("source").getMetaSupplier()).isSameAs(original);
        assertThat(original.initCalls).isZero();
    }

    @Test
    void unknownAndDuplicateNativeIdentitiesAreRefusedBeforeDecoration() throws Exception {
        Address a = address(5701);
        Address b = address(5702);
        List<List<Member>> invalidViews = List.of(
                List.of(member("a", a, 1), member(null, b, 2)),
                List.of(member("a", a, 1), member(" ", b, 2)),
                List.of(member("a", a, 1), member("a", b, 2)),
                List.of(member("a", a, 1), member("b", b, 1)),
                List.of(member("a", a, 1), member("b", a, 2)));
        for (List<Member> view : invalidViews) {
            RecordingMetaSupplier original = new RecordingMetaSupplier();
            DAG dag = dag(original);

            assertRefused(() -> AdmittedMemberPlanMetaSupplier.install("orders", dag, Set.of("a", "b"), view));

            assertThat(dag.getVertex("source").getMetaSupplier()).isSameAs(original);
        }
    }

    @Test
    void thePhysicalSnapshotContainsOnlyNativeDataMembers() throws Exception {
        Address a = address(5701);
        Member owner = member("a", a, 1);
        Member lite = member(null, address(5702), 2, true);
        RecordingMetaSupplier original = new RecordingMetaSupplier();
        ProcessorMetaSupplier guarded = guarded(original, List.of(owner, lite), Set.of("a"));

        guarded.init(context(List.of(owner, lite), Map.of(a, new int[] {0})));

        assertThat(original.initCalls).isEqualTo(1);
    }

    @Test
    void aMemberAddedToTheNativePlanCannotInitializeAnyOriginalVertex() throws Exception {
        Address a = address(5701);
        Address b = address(5702);
        Address c = address(5703);
        List<Member> admitted = List.of(member("a", a, 1), member("b", b, 2));
        List<Member> expanded = List.of(admitted.get(0), admitted.get(1), member("c", c, 3));
        RecordingMetaSupplier source = new RecordingMetaSupplier();
        RecordingMetaSupplier writer = new RecordingMetaSupplier();
        DAG dag = dag(source);
        dag.newVertex("writer", writer);
        AdmittedMemberPlanMetaSupplier.install("orders", dag, Set.of("a", "b"), admitted);
        var wrongContext = context(expanded, Map.of(a, new int[] {0}, b, new int[] {1}, c, new int[] {2}));

        for (var vertex : dag) {
            assertRefused(() -> vertex.getMetaSupplier().init(wrongContext));
            vertex.getMetaSupplier().close(new IllegalStateException("plan refused"));
        }

        assertThat(source.initCalls).isZero();
        assertThat(writer.writerPreparations).isZero();
        assertThat(source.closeCalls).isZero();
        assertThat(writer.closeCalls).isZero();
    }

    @Test
    void aMissingNativeParticipantCannotRunOriginalInitialization() throws Exception {
        Address a = address(5701);
        Address b = address(5702);
        List<Member> admitted = List.of(member("a", a, 1), member("b", b, 2));
        RecordingMetaSupplier original = new RecordingMetaSupplier();
        ProcessorMetaSupplier guarded = guarded(original, admitted, Set.of("a", "b"));

        assertRefused(() -> guarded.init(context(admitted, Map.of(a, new int[] {0}))));

        assertThat(original.initCalls).isZero();
    }

    @Test
    void aReplacementAtTheSameAddressIsRefusedBeforeWriterPreparation() throws Exception {
        Address a = address(5701);
        Address b = address(5702);
        Member first = member("a", a, 1);
        List<Member> admitted = List.of(first, member("b", b, 2));
        for (Member replacement : List.of(member("b", b, 3), member("other", b, 2))) {
            RecordingMetaSupplier original = new RecordingMetaSupplier();
            ProcessorMetaSupplier guarded = guarded(original, admitted, Set.of("a", "b"));

            assertRefused(() -> guarded.init(context(List.of(first, replacement),
                    Map.of(a, new int[] {0}, b, new int[] {1}))));

            assertThat(original.writerPreparations).isZero();
            guarded.close(null);
            assertThat(original.closeCalls).isZero();
        }
    }

    @Test
    void aLaterJoinDoesNotChangeTheAlreadyFrozenNativePlan() throws Exception {
        Address a = address(5701);
        Address b = address(5702);
        List<Member> admitted = List.of(member("a", a, 1), member("b", b, 2));
        List<Member> visible = List.of(admitted.get(0), admitted.get(1), member("c", address(5703), 3));
        RecordingMetaSupplier original = new RecordingMetaSupplier();
        ProcessorMetaSupplier guarded = guarded(original, admitted, Set.of("a", "b"));

        // An empty partition array still names a participant when the native plan contains its entry.
        guarded.init(context(visible, Map.of(a, new int[] {0}, b, new int[0])));
        guarded.get(List.of(b, a));

        assertThat(original.initCalls).isEqualTo(1);
        assertThat(original.getCalls).isEqualTo(1);
        assertThat(original.addresses).containsExactly(b, a);
    }

    @Test
    void getRejectsChangedMissingOrRepeatedAddressesBeforeCallingTheOriginal() throws Exception {
        Address a = address(5701);
        Address b = address(5702);
        Address c = address(5703);
        List<Member> admitted = List.of(member("a", a, 1), member("b", b, 2));
        RecordingMetaSupplier original = new RecordingMetaSupplier();
        ProcessorMetaSupplier guarded = guarded(original, admitted, Set.of("a", "b"));
        guarded.init(context(admitted, Map.of(a, new int[] {0}, b, new int[] {1})));

        for (List<Address> addresses : List.of(List.of(a), List.of(a, c), List.of(a, a), List.of(a, b, c))) {
            assertRefused(() -> guarded.get(addresses));
        }

        assertThat(original.getCalls).isZero();
    }

    @Test
    void aReplacementBetweenInitAndGetCannotObtainOriginalSuppliers() throws Exception {
        Address a = address(5701);
        Address b = address(5702);
        List<Member> visible = new ArrayList<>(List.of(member("a", a, 1), member("b", b, 2)));
        RecordingMetaSupplier original = new RecordingMetaSupplier();
        ProcessorMetaSupplier guarded = guarded(original, visible, Set.of("a", "b"));
        guarded.init(context(visible, Map.of(a, new int[] {0}, b, new int[] {1})));
        visible.set(1, member("b", b, 3));

        assertRefused(() -> guarded.get(List.of(a, b)));

        assertThat(original.initCalls).isEqualTo(1);
        assertThat(original.getCalls).isZero();
        guarded.close(null);
        assertThat(original.closeCalls).isEqualTo(1);
    }

    @Test
    void metadataAndLifecycleCallbacksArePreserved() throws Exception {
        Address a = address(5701);
        List<Member> admitted = List.of(member("a", a, 1));
        RecordingMetaSupplier original = new RecordingMetaSupplier();
        ProcessorMetaSupplier guarded = guarded(original, admitted, Set.of("a"));
        var suppliedContext = context(admitted, Map.of(a, new int[] {0}));
        Throwable completion = new IllegalStateException("execution ended");

        assertThat(guarded.preferredLocalParallelism()).isEqualTo(original.preferredLocalParallelism());
        assertThat(guarded.getTags()).isSameAs(original.tags);
        assertThat(guarded.getRequiredPermission()).isSameAs(original.permission);
        assertThat(guarded.isReusable()).isTrue();
        assertThat(guarded.initIsCooperative()).isFalse();
        assertThat(guarded.closeIsCooperative()).isFalse();
        guarded.init(suppliedContext);
        guarded.get(List.of(a));
        guarded.close(completion);

        assertThat(original.context).isSameAs(suppliedContext);
        assertThat(original.initCalls).isEqualTo(1);
        assertThat(original.getCalls).isEqualTo(1);
        assertThat(original.closeCalls).isEqualTo(1);
        assertThat(original.closeError).isSameAs(completion);
    }

    @Test
    void aDelegateThatFailedDuringInitializationStillReceivesCleanup() throws Exception {
        Address a = address(5701);
        List<Member> admitted = List.of(member("a", a, 1));
        RecordingMetaSupplier original = new RecordingMetaSupplier();
        original.failInitialization = true;
        ProcessorMetaSupplier guarded = guarded(original, admitted, Set.of("a"));

        assertThatThrownBy(() -> guarded.init(context(admitted, Map.of(a, new int[] {0}))))
                .isInstanceOf(IllegalStateException.class).hasMessage("original initialization failed");
        guarded.close(null);

        assertThat(original.initCalls).isEqualTo(1);
        assertThat(original.closeCalls).isEqualTo(1);
    }

    @Test
    void nativeDagSerializationPreservesTheGuardAndTheNativePlacementDelegate() throws Exception {
        Address a = address(5701);
        Address b = address(5702);
        List<Member> admitted = List.of(member("a", a, 1), member("b", b, 2));
        ProcessorMetaSupplier nativePlacement = ProcessorMetaSupplier.forceTotalParallelismOne(
                ProcessorSupplier.of(OwnedProcessor::new), a);
        DAG dag = dag(nativePlacement);
        AdmittedMemberPlanMetaSupplier.install("orders", dag, Set.of("a", "b"), admitted);
        var serialization = new DefaultSerializationServiceBuilder().build();
        try {
            DAG restored = serialization.toObject(serialization.toData(dag));
            ProcessorMetaSupplier guarded = restored.getVertex("source").getMetaSupplier();
            assertThat(guarded).isInstanceOf(AdmittedMemberPlanMetaSupplier.class);
            assertThat(restored.memberSelector()).isNull();
            assertThat(guarded.preferredLocalParallelism()).isEqualTo(1);
            guarded.init(context(admitted, Map.of(a, new int[] {0}, b, new int[] {1})));
            var suppliers = guarded.get(List.of(b, a));

            assertThat(suppliers.apply(a).get(1)).singleElement().isInstanceOf(OwnedProcessor.class);
            assertThat(suppliers.apply(b).get(1)).singleElement().isNotInstanceOf(OwnedProcessor.class);
            guarded.close(null);
        } finally {
            serialization.dispose();
        }
    }

    private static DAG dag(ProcessorMetaSupplier source) {
        DAG dag = new DAG();
        dag.newVertex("source", source);
        return dag;
    }

    private static ProcessorMetaSupplier guarded(ProcessorMetaSupplier original, List<Member> members,
            Set<String> admitted) {
        DAG dag = dag(original);
        AdmittedMemberPlanMetaSupplier.install("orders", dag, admitted, members);
        return dag.getVertex("source").getMetaSupplier();
    }

    private static void assertRefused(org.assertj.core.api.ThrowableAssert.ThrowingCallable operation) {
        assertThatThrownBy(operation).isInstanceOfSatisfying(TapstateException.class, failure -> {
            assertThat(failure.code()).isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED);
            assertThat(failure.args()).containsEntry("pipeline", "orders");
        });
    }

    private static Address address(int port) throws Exception {
        return new Address("127.0.0.1", port);
    }

    private static Member member(String nodeId, Address address, long uuid) {
        return member(nodeId, address, uuid, false);
    }

    private static Member member(String nodeId, Address address, long uuid, boolean lite) {
        return proxy(Member.class, (self, method, arguments) -> switch (method.getName()) {
            case "getAttribute" -> nodeId;
            case "getAddress" -> address;
            case "getUuid" -> new UUID(0, uuid);
            case "isLiteMember", "isLocalMember" -> lite;
            case "equals" -> self == arguments[0];
            case "hashCode" -> System.identityHashCode(self);
            case "toString" -> "member-" + uuid;
            default -> throw new UnsupportedOperationException(method.getName());
        });
    }

    private static ProcessorMetaSupplier.Context context(Collection<Member> members,
            Map<Address, int[]> assignment) {
        Cluster cluster = proxy(Cluster.class, (self, method, arguments) -> switch (method.getName()) {
            case "getMembers" -> Set.copyOf(members);
            default -> throw new UnsupportedOperationException(method.getName());
        });
        HazelcastInstance member = proxy(HazelcastInstance.class, (self, method, arguments) -> switch (method.getName()) {
            case "getCluster" -> cluster;
            default -> throw new UnsupportedOperationException(method.getName());
        });
        return proxy(ProcessorMetaSupplier.Context.class, (self, method, arguments) -> switch (method.getName()) {
            case "hazelcastInstance" -> member;
            case "partitionAssignment" -> assignment;
            case "memberCount", "totalParallelism" -> assignment.size();
            case "localParallelism" -> 1;
            case "processingGuarantee" -> ProcessingGuarantee.NONE;
            case "checkPermission" -> null;
            default -> throw new UnsupportedOperationException(method.getName());
        });
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
    }

    private static final class OwnedProcessor extends AbstractProcessor { }

    private static final class RecordingMetaSupplier implements ProcessorMetaSupplier {
        private final Map<String, String> tags = Map.of("fixture", "original");
        private final Permission permission = new RuntimePermission("tapstate.test.original");
        private int initCalls;
        private int writerPreparations;
        private int getCalls;
        private int closeCalls;
        private Context context;
        private List<Address> addresses;
        private Throwable closeError;
        private boolean failInitialization;

        @Override public int preferredLocalParallelism() { return 3; }
        @Override public Map<String, String> getTags() { return tags; }
        @Override public Permission getRequiredPermission() { return permission; }
        @Override public boolean isReusable() { return true; }
        @Override public boolean initIsCooperative() { return false; }
        @Override public boolean closeIsCooperative() { return false; }
        @Override public void init(Context supplied) {
            initCalls++;
            writerPreparations++;
            context = supplied;
            if (failInitialization) {
                throw new IllegalStateException("original initialization failed");
            }
        }
        @Override public Function<Address, ProcessorSupplier> get(List<Address> supplied) {
            getCalls++;
            addresses = supplied;
            return address -> ProcessorSupplier.of(OwnedProcessor::new);
        }
        @Override public void close(Throwable error) {
            closeCalls++;
            closeError = error;
        }
    }
}
