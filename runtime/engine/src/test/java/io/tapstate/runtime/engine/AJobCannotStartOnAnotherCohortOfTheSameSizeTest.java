package io.tapstate.runtime.engine;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.test.TestProcessorMetaSupplierContext;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Proxy;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.Permission;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AJobCannotStartOnAnotherCohortOfTheSameSizeTest {

    private static final Address A = Address.createUnresolvedAddress("127.0.0.1", 5701);
    private static final Address B = Address.createUnresolvedAddress("127.0.0.1", 5702);
    private static final Address C = Address.createUnresolvedAddress("127.0.0.1", 5703);
    private static final String HASH = "profile-hash";

    @Test
    void aSameCountReplacementIsRefusedBeforeAnyProcessorSupplierCanBeCreated() {
        Fixture fixture = new Fixture();
        Probe delegate = new Probe();
        DAG dag = new DAG();
        dag.newVertex("native", delegate).localParallelism(2);
        install(dag, fixture);
        fixture.members.set(Set.of(member("a", "boot-a", A, 2, HASH), member("c", "boot-c", C, 2, HASH)));

        assertThatThrownBy(() -> dag.getVertex("native").getMetaSupplier().init(fixture.context(2)))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(EngineError.EXECUTION_COHORT_CHANGED_BEFORE_START);
                    assertThat(error.args()).containsEntry("pipeline", "orders").containsEntry("reason", "live-identities");
                    assertThat(JobFailureRegistry.of(fixture.member).get("orders")).contains(error);
                });
        assertThat(delegate.initializations).isZero();
        assertThat(delegate.suppliers).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = {"stable-node", "boot", "uuid", "address", "generation", "hash", "missing-profile"})
    void anyChangedIdentityOrProfileIsRefusedBeforeTheDelegateInitializes(String changed) {
        Fixture fixture = new Fixture();
        Probe delegate = new Probe();
        DAG dag = new DAG();
        dag.newVertex("native", delegate);
        install(dag, fixture);
        UUID originalUuid = UUID.nameUUIDFromBytes(("b:boot-b:" + B).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        Member replacement = member(changed.equals("stable-node") ? "c" : "b",
                changed.equals("boot") ? "boot-b2" : "boot-b", changed.equals("address") ? C : B,
                changed.equals("generation") ? 3 : 2,
                changed.equals("hash") ? "other-profile" : changed.equals("missing-profile") ? null : HASH,
                changed.equals("uuid") ? UUID.randomUUID() : originalUuid, false);
        fixture.members.set(Set.of(member("a", "boot-a", A, 2, HASH), replacement));

        assertThatThrownBy(() -> dag.getVertex("native").getMetaSupplier().init(fixture.context(2)))
                .isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.code()).isEqualTo(EngineError.EXECUTION_COHORT_CHANGED_BEFORE_START));
        assertThat(delegate.initializations).isZero();
        assertThat(delegate.suppliers).isZero();
    }

    @Test
    void aDifferentContextCountIsRefusedEvenWhileTheLiveCohortStillMatches() {
        Fixture fixture = new Fixture();
        Probe delegate = new Probe();
        DAG dag = new DAG();
        dag.newVertex("native", delegate);
        install(dag, fixture);

        assertThatThrownBy(() -> dag.getVertex("native").getMetaSupplier().init(fixture.context(1)))
                .isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.args()).containsEntry("reason", "context-member-count"));
        assertThat(delegate.initializations).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = {"subset", "replacement", "duplicate"})
    void actualExecutionAddressesMustMatchBeforeTheDelegateCreatesASupplier(String changed) throws Exception {
        Fixture fixture = new Fixture();
        Probe delegate = new Probe();
        DAG dag = new DAG();
        dag.newVertex("native", delegate);
        install(dag, fixture);
        ProcessorMetaSupplier guarded = dag.getVertex("native").getMetaSupplier();
        guarded.init(fixture.context(2));
        List<Address> addresses = switch (changed) {
            case "subset" -> List.of(A);
            case "replacement" -> List.of(A, C);
            case "duplicate" -> List.of(A, A);
            default -> throw new IllegalStateException(changed);
        };

        assertThatThrownBy(() -> guarded.get(addresses))
                .isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.args()).containsEntry("reason", "execution-addresses"));
        assertThat(delegate.initializations).isEqualTo(1);
        assertThat(delegate.suppliers).isZero();
    }

    @Test
    void aChangeAfterInitIsStillRefusedBeforeGet() throws Exception {
        Fixture fixture = new Fixture();
        Probe delegate = new Probe();
        DAG dag = new DAG();
        dag.newVertex("native", delegate);
        install(dag, fixture);
        ProcessorMetaSupplier guarded = dag.getVertex("native").getMetaSupplier();
        guarded.init(fixture.context(2));
        fixture.members.set(Set.of(member("a", "boot-a", A, 2, HASH), member("c", "boot-c", B, 2, HASH)));

        assertThatThrownBy(() -> guarded.get(List.of(A, B))).isInstanceOf(TapstateException.class);
        assertThat(delegate.suppliers).isZero();
    }

    @Test
    void matchingMembersPreserveTheOriginalHooksWidthsAndAddressOrder() throws Exception {
        Fixture fixture = new Fixture();
        Probe delegate = new Probe();
        DAG dag = new DAG();
        dag.newVertex("native", PlannedMembersGuard.of(delegate, 2)).localParallelism(7);
        install(dag, fixture);
        ProcessorMetaSupplier guarded = dag.getVertex("native").getMetaSupplier();
        TestProcessorMetaSupplierContext context = fixture.context(2);
        guarded.init(context);
        List<Address> addresses = List.of(B, A);

        assertThat(guarded.get(addresses).apply(A)).isSameAs(delegate.processorSupplier);
        assertThat(delegate.lastContext).isSameAs(context);
        assertThat(delegate.lastAddresses).isSameAs(addresses);
        assertThat(dag.getVertex("native").getLocalParallelism()).isEqualTo(7);
        assertThat(guarded.preferredLocalParallelism()).isEqualTo(3);
        assertThat(guarded.getRequiredPermission()).isSameAs(delegate.permission);
        assertThat(guarded.getTags()).isSameAs(delegate.tags);
        assertThat(guarded.initIsCooperative()).isFalse();
        assertThat(guarded.closeIsCooperative()).isFalse();
        assertThat(guarded.isReusable()).isFalse();
        Throwable end = new IllegalStateException("end");
        guarded.close(end);
        assertThat(delegate.closeError).isSameAs(end);
        assertThat(dag.memberSelector()).isNull();
    }

    @Test
    void totalOneKeepsItsPlacementAndCannotEscapeTheFullCohortGuard() throws Exception {
        Fixture fixture = new Fixture();
        Probe delegate = new Probe();
        DAG dag = new DAG();
        dag.newVertex("native", delegate).localParallelism(2);
        ProcessorMetaSupplier original = ProcessorMetaSupplier.forceTotalParallelismOne(delegate.processorSupplier, A);
        dag.newVertex("source", original).localParallelism(1);
        install(dag, fixture);
        ProcessorMetaSupplier guarded = dag.getVertex("source").getMetaSupplier();
        guarded.init(fixture.context(2));
        assertThat(guarded.get(List.of(A, B)).apply(A)).isSameAs(delegate.processorSupplier);
        assertThat(guarded.preferredLocalParallelism()).isEqualTo(original.preferredLocalParallelism());
        assertThat(dag.getVertex("source").getLocalParallelism()).isEqualTo(1);
        fixture.members.set(Set.of(member("a", "boot-a", A, 2, HASH), member("c", "boot-c", C, 2, HASH)));

        for (var vertex : dag) {
            assertThatThrownBy(() -> vertex.getMetaSupplier().init(fixture.context(2)))
                    .isInstanceOf(TapstateException.class);
        }
        assertThat(delegate.initializations).isZero();
        assertThat(delegate.suppliers).isZero();
    }

    @Test
    void aSerializedGuardRetainsIdentityProofWithoutCapturingALiveMember() throws Exception {
        Fixture fixture = new Fixture();
        DAG dag = new DAG();
        dag.newVertex("native", new Probe());
        install(dag, fixture);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(dag.getVertex("native").getMetaSupplier());
        }
        ProcessorMetaSupplier restored;
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (ProcessorMetaSupplier) input.readObject();
        }
        restored.init(fixture.context(2));
        assertThat(restored.get(List.of(A, B)).apply(A)).isNotNull();
        fixture.members.set(Set.of(member("a", "boot-a", A, 2, HASH), member("c", "boot-c", C, 2, HASH)));
        assertThatThrownBy(() -> restored.init(fixture.context(2))).isInstanceOf(TapstateException.class);
    }

    @Test
    void installRefusesAPlannedSubsetInsteadOfSelectingThoseMembers() {
        Fixture fixture = new Fixture();
        Probe delegate = new Probe();
        DAG dag = new DAG();
        dag.newVertex("native", delegate);

        assertThatThrownBy(() -> ExecutionCohortGuard.install(dag, fixture.member, "orders", List.of("a"), 2, HASH))
                .isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.args()).containsEntry("reason", "live-membership"));
        assertThat(dag.getVertex("native").getMetaSupplier()).isSameAs(delegate);
        assertThat(dag.memberSelector()).isNull();
    }

    @Test
    void missingRuntimeProofDoesNotActivateAnUnfencedDefault() {
        Fixture fixture = new Fixture();
        Probe delegate = new Probe();
        DAG dag = new DAG();
        dag.newVertex("native", delegate);
        install(dag, fixture);

        assertThatThrownBy(() -> dag.getVertex("native").getMetaSupplier().init(
                fixture.context(2).setHazelcastInstance(null)))
                .isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.args()).containsEntry("reason", "membership-unavailable"));
        assertThat(delegate.initializations).isZero();
        assertThat(delegate.suppliers).isZero();
    }

    @Test
    void liteMembersDoNotChangeTheFullDataMemberCohort() throws Exception {
        Fixture fixture = new Fixture();
        fixture.members.set(Set.of(member("a", "boot-a", A, 2, HASH), member("b", "boot-b", B, 2, HASH),
                member("lite", "boot-lite", C, 9, "other-profile", UUID.randomUUID(), true)));
        Probe delegate = new Probe();
        DAG dag = new DAG();
        dag.newVertex("native", delegate);
        install(dag, fixture);

        dag.getVertex("native").getMetaSupplier().init(fixture.context(2));
        assertThat(dag.getVertex("native").getMetaSupplier().get(List.of(A, B)).apply(A))
                .isSameAs(delegate.processorSupplier);
        assertThat(delegate.initializations).isEqualTo(1);
        assertThat(delegate.suppliers).isEqualTo(1);
    }

    private static void install(DAG dag, Fixture fixture) {
        ExecutionCohortGuard.install(dag, fixture.member, "orders", List.of("a", "b"), 2, HASH);
    }

    private static final class Fixture {
        final AtomicReference<Set<Member>> members = new AtomicReference<>(Set.of(
                member("a", "boot-a", A, 2, HASH), member("b", "boot-b", B, 2, HASH)));
        final Map<String, Object> userContext = new ConcurrentHashMap<>();
        final Cluster cluster = (Cluster) Proxy.newProxyInstance(Cluster.class.getClassLoader(),
                new Class<?>[]{Cluster.class}, (proxy, method, arguments) -> switch (method.getName()) {
                    case "getMembers" -> members.get();
                    default -> throw new UnsupportedOperationException("unexpected cluster call " + method.getName());
                });
        final HazelcastInstance member = (HazelcastInstance) Proxy.newProxyInstance(
                HazelcastInstance.class.getClassLoader(), new Class<?>[]{HazelcastInstance.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getCluster" -> cluster;
                    case "getUserContext" -> userContext;
                    default -> throw new UnsupportedOperationException("unexpected member call " + method.getName());
                });

        TestProcessorMetaSupplierContext context(int count) {
            return new TestProcessorMetaSupplierContext() {
                @Override
                public int memberCount() {
                    return count;
                }
            }.setHazelcastInstance(member).setJobConfig(new JobConfig().setName("orders"))
                    .setTotalParallelism(count).setLocalParallelism(1)
                    .setPartitionAssignment(Map.of(A, new int[]{0}, B, new int[]{1}));
        }
    }

    private static Member member(String node, String boot, Address address, long generation, String hash) {
        UUID uuid = UUID.nameUUIDFromBytes((node + ":" + boot + ":" + address).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return member(node, boot, address, generation, hash, uuid, false);
    }

    private static Member member(String node, String boot, Address address, long generation, String hash,
            UUID uuid, boolean lite) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("tapstate.node-id", node);
        attributes.put("tapstate.boot-id", boot);
        attributes.put("tapstate.profile-generation", String.valueOf(generation));
        if (hash != null) {
            attributes.put("tapstate.profile-hash", hash);
        }
        return (Member) Proxy.newProxyInstance(Member.class.getClassLoader(), new Class<?>[]{Member.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "isLiteMember" -> lite;
                    case "localMember" -> false;
                    case "getAddress" -> address;
                    case "getUuid" -> uuid;
                    case "getAttribute" -> attributes.get(arguments[0]);
                    case "getAttributes" -> attributes;
                    case "hashCode" -> uuid.hashCode();
                    case "equals" -> proxy == arguments[0];
                    case "toString" -> node + ":" + boot + ":" + address;
                    default -> throw new UnsupportedOperationException("unexpected identity call " + method.getName());
                });
    }

    private static final class Probe implements ProcessorMetaSupplier {
        private static final long serialVersionUID = 1L;
        int initializations;
        int suppliers;
        transient Context lastContext;
        transient List<Address> lastAddresses;
        transient Throwable closeError;
        final Permission permission = new RuntimePermission("cohort-test");
        final Map<String, String> tags = Map.of("stage", "source");
        final ProcessorSupplier processorSupplier = count -> List.of();

        @Override
        public void init(Context context) {
            initializations++;
            lastContext = context;
        }

        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(List<Address> addresses) {
            suppliers++;
            lastAddresses = addresses;
            return address -> processorSupplier;
        }

        @Override
        public int preferredLocalParallelism() {
            return 3;
        }

        @Override
        public Permission getRequiredPermission() {
            return permission;
        }

        @Override
        public Map<String, String> getTags() {
            return tags;
        }

        @Override
        public boolean initIsCooperative() {
            return false;
        }

        @Override
        public boolean closeIsCooperative() {
            return false;
        }

        @Override
        public boolean isReusable() {
            return false;
        }

        @Override
        public void close(Throwable error) {
            closeError = error;
        }
    }
}
