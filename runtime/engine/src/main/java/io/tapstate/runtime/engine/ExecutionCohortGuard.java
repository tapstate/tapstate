package io.tapstate.runtime.engine;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import io.tapstate.core.common.TapstateException;

import java.io.Serializable;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/** Holds every vertex to the exact admitted data-member cohort captured just before submission. */
public final class ExecutionCohortGuard {

    private static final String NODE_ID = "tapstate.node-id";
    private static final String BOOT_ID = "tapstate.boot-id";
    private static final String PROFILE_GENERATION = "tapstate.profile-generation";
    private static final String PROFILE_HASH = "tapstate.profile-hash";

    private ExecutionCohortGuard() {
    }

    /**
     * Captures all live data members and wraps every meta-supplier, including total-one vertices.
     * Existing placement, widths, and supplier hooks are preserved; no member selector is installed.
     */
    public static void install(DAG dag, HazelcastInstance member, String pipeline, List<String> plannedStableIds,
            long profileGeneration, String profileHash) {
        Objects.requireNonNull(dag, "dag");
        Objects.requireNonNull(member, "member");
        Objects.requireNonNull(pipeline, "pipeline");
        Objects.requireNonNull(plannedStableIds, "plannedStableIds");
        Objects.requireNonNull(profileHash, "profileHash");
        if (pipeline.isBlank() || plannedStableIds.isEmpty() || profileGeneration < 1 || profileHash.isBlank()
                || plannedStableIds.stream().anyMatch(id -> id == null || id.isBlank())
                || new HashSet<>(plannedStableIds).size() != plannedStableIds.size()) {
            throw new IllegalArgumentException("a cohort requires a pipeline, unique planned nodes, and a profile");
        }
        List<Identity> captured = identities(member, pipeline, profileGeneration, profileHash);
        List<String> planned = plannedStableIds.stream().sorted().toList();
        List<String> actual = captured.stream().map(Identity::nodeId).toList();
        if (!planned.equals(actual)) {
            throw refused(member, pipeline, "live-membership", planned, actual, null);
        }
        dag.forEach(vertex -> vertex.updateMetaSupplier(
                delegate -> new Guard(delegate, pipeline, captured, profileGeneration, profileHash)));
    }

    private static List<Identity> identities(
            HazelcastInstance member, String pipeline, long profileGeneration, String profileHash) {
        List<Identity> identities = new ArrayList<>();
        try {
            for (Member peer : member.getCluster().getMembers()) {
                if (peer.isLiteMember()) {
                    continue;
                }
                String nodeId = peer.getAttribute(NODE_ID);
                String bootId = peer.getAttribute(BOOT_ID);
                if (nodeId == null || nodeId.isBlank() || bootId == null || bootId.isBlank()
                        || peer.getUuid() == null || peer.getAddress() == null) {
                    throw refused(member, pipeline, "identity-unavailable", "stable/boot/UUID/address",
                            String.valueOf(peer), null);
                }
                String generation = peer.getAttribute(PROFILE_GENERATION);
                String hash = peer.getAttribute(PROFILE_HASH);
                if (!String.valueOf(profileGeneration).equals(generation) || !profileHash.equals(hash)) {
                    throw refused(member, pipeline, "execution-profile", profileGeneration + ":" + profileHash,
                            nodeId + ":" + generation + ":" + hash, null);
                }
                identities.add(new Identity(nodeId, bootId, peer.getUuid().toString(),
                        peer.getAddress().toString(), generation, hash));
            }
        } catch (HazelcastInstanceNotActiveException unavailable) {
            throw refused(member, pipeline, "membership-unavailable", "live data members", "unavailable", unavailable);
        }
        identities.sort(Comparator.comparing(Identity::nodeId).thenComparing(Identity::bootId)
                .thenComparing(Identity::uuid).thenComparing(Identity::address));
        if (identities.stream().map(Identity::uuid).distinct().count() != identities.size()
                || identities.stream().map(Identity::address).distinct().count() != identities.size()) {
            throw refused(member, pipeline, "duplicate-runtime-identity", "unique UUIDs and addresses", identities, null);
        }
        return List.copyOf(identities);
    }

    private static TapstateException refused(HazelcastInstance member, String pipeline, String reason,
            Object planned, Object actual, Throwable cause) {
        TapstateException failure = new TapstateException(EngineError.EXECUTION_COHORT_CHANGED_BEFORE_START,
                Map.of("pipeline", pipeline, "reason", reason,
                        "planned", String.valueOf(planned), "actual", String.valueOf(actual)), cause);
        if (member != null) {
            JobFailureRegistry.of(member).record(pipeline, failure);
        }
        return failure;
    }

    /** Address's canonical host/port spelling preserves its equality without serializing a live runtime object. */
    private record Identity(String nodeId, String bootId, String uuid, String address,
            String profileGeneration, String profileHash) implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    private static final class Guard implements ProcessorMetaSupplier {
        private static final long serialVersionUID = 1L;
        private final ProcessorMetaSupplier delegate;
        private final String pipeline;
        private final List<Identity> captured;
        private final long profileGeneration;
        private final String profileHash;
        private transient HazelcastInstance member;

        Guard(ProcessorMetaSupplier delegate, String pipeline, List<Identity> captured,
                long profileGeneration, String profileHash) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
            this.pipeline = pipeline;
            this.captured = captured;
            this.profileGeneration = profileGeneration;
            this.profileHash = profileHash;
        }

        @Override
        public void init(Context context) throws Exception {
            member = null;
            HazelcastInstance answering = context.hazelcastInstance();
            if (answering == null) {
                throw refused(null, pipeline, "membership-unavailable", captured, "unavailable", null);
            }
            if (context.memberCount() != captured.size()) {
                throw refused(answering, pipeline, "context-member-count", captured.size(), context.memberCount(), null);
            }
            verify(answering);
            delegate.init(context);
            member = answering;
        }

        private void verify(HazelcastInstance answering) {
            List<Identity> actual = identities(answering, pipeline, profileGeneration, profileHash);
            if (!captured.equals(actual)) {
                throw refused(answering, pipeline, "live-identities", captured, actual, null);
            }
        }

        @Override
        public Function<? super Address, ? extends ProcessorSupplier> get(List<Address> addresses) {
            if (member == null) {
                throw new IllegalStateException("the cohort meta-supplier must be initialized before get");
            }
            verify(member);
            Set<String> planned = new HashSet<>(captured.stream().map(Identity::address).toList());
            List<String> actual = addresses.stream().map(Address::toString).toList();
            if (actual.size() != planned.size() || !planned.equals(new HashSet<>(actual))) {
                throw refused(member, pipeline, "execution-addresses", planned.stream().sorted().toList(), actual, null);
            }
            return delegate.get(addresses);
        }

        @Override
        public int preferredLocalParallelism() {
            return delegate.preferredLocalParallelism();
        }

        @Override
        public Permission getRequiredPermission() {
            return delegate.getRequiredPermission();
        }

        @Override
        public Map<String, String> getTags() {
            return delegate.getTags();
        }

        @Override
        public boolean initIsCooperative() {
            return delegate.initIsCooperative();
        }

        @Override
        public boolean closeIsCooperative() {
            return delegate.closeIsCooperative();
        }

        @Override
        public boolean isReusable() {
            return delegate.isReusable();
        }

        @Override
        public void close(Throwable error) throws Exception {
            delegate.close(error);
        }
    }
}
