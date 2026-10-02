package io.tapstate.runtime.engine;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import io.tapstate.core.common.TapstateException;
import java.io.IOException;
import java.security.Permission;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

/** Verifies the admitted physical participants before any original vertex initialization runs. */
public final class AdmittedMemberPlanMetaSupplier implements ProcessorMetaSupplier, DataSerializable {

    private static final String NODE_ID_ATTRIBUTE = "tapstate.node-id";

    private record MemberIdentity(String nodeId, UUID uuid) { }

    private String pipelineId;
    private ProcessorMetaSupplier delegate;
    private Map<Address, MemberIdentity> participants;
    private transient boolean delegateInitStarted;
    private transient HazelcastInstance initializedOn;

    /** Constructor used by Hazelcast's native object deserialization. */
    public AdmittedMemberPlanMetaSupplier() { }

    private AdmittedMemberPlanMetaSupplier(String pipelineId, ProcessorMetaSupplier delegate,
            Map<Address, MemberIdentity> participants) {
        this.pipelineId = pipelineId;
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.participants = participants;
    }

    /** A proper subset cannot be represented by the ordinary native execution plan. */
    static void install(String pipelineId, DAG dag, Set<String> admittedNodeIds,
            Collection<? extends Member> nativeMembers) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(dag, "dag");
        Objects.requireNonNull(admittedNodeIds, "admittedNodeIds");
        Objects.requireNonNull(nativeMembers, "nativeMembers");
        Map<Address, MemberIdentity> snapshot = new HashMap<>();
        Set<String> nodeIds = new HashSet<>();
        Set<UUID> uuids = new HashSet<>();
        for (Member current : nativeMembers) {
            if (current.isLiteMember()) {
                continue;
            }
            String nodeId = current.getAttribute(NODE_ID_ATTRIBUTE);
            UUID uuid = current.getUuid();
            Address address = current.getAddress();
            if (nodeId == null || nodeId.isBlank() || uuid == null || address == null
                    || !nodeIds.add(nodeId) || !uuids.add(uuid)
                    || snapshot.put(new Address(address), new MemberIdentity(nodeId, uuid)) != null) {
                throw refused(pipelineId);
            }
        }
        if (snapshot.isEmpty() || !nodeIds.equals(admittedNodeIds)) {
            throw refused(pipelineId);
        }
        Map<Address, MemberIdentity> frozen = Map.copyOf(snapshot);
        for (var vertex : dag) {
            vertex.updateMetaSupplier(original -> new AdmittedMemberPlanMetaSupplier(pipelineId, original, frozen));
        }
    }

    @Override
    public void init(Context context) throws Exception {
        delegateInitStarted = false;
        initializedOn = null;
        if (context.memberCount() != participants.size()
                || !context.partitionAssignment().keySet().equals(participants.keySet())) {
            throw refused(pipelineId);
        }
        HazelcastInstance currentMember = context.hazelcastInstance();
        requirePhysicalMembers(currentMember);
        initializedOn = currentMember;
        // A delegate that fails partway through initialization still owns its cleanup.
        delegateInitStarted = true;
        delegate.init(context);
    }

    private void requirePhysicalMembers(HazelcastInstance member) {
        Map<Address, Member> visible = new HashMap<>();
        for (Member current : member.getCluster().getMembers()) {
            if (!current.isLiteMember() && visible.put(current.getAddress(), current) != null) {
                throw refused(pipelineId);
            }
        }
        for (var participant : participants.entrySet()) {
            Member current = visible.get(participant.getKey());
            MemberIdentity expected = participant.getValue();
            if (current == null || !expected.uuid().equals(current.getUuid())
                    || !expected.nodeId().equals(current.getAttribute(NODE_ID_ATTRIBUTE))) {
                throw refused(pipelineId);
            }
        }
    }

    @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(List<Address> addresses) {
        if (!delegateInitStarted || addresses.size() != participants.size()
                || !new HashSet<>(addresses).equals(participants.keySet())) {
            throw refused(pipelineId);
        }
        requirePhysicalMembers(initializedOn);
        return delegate.get(addresses);
    }

    @Override public int preferredLocalParallelism() { return delegate.preferredLocalParallelism(); }
    @Override public Map<String, String> getTags() { return delegate.getTags(); }
    @Override public Permission getRequiredPermission() { return delegate.getRequiredPermission(); }
    @Override public boolean isReusable() { return delegate.isReusable(); }
    @Override public boolean initIsCooperative() { return delegate.initIsCooperative(); }
    @Override public boolean closeIsCooperative() { return delegate.closeIsCooperative(); }

    @Override
    public void close(Throwable error) throws Exception {
        if (delegateInitStarted) {
            delegateInitStarted = false;
            try {
                delegate.close(error);
            } finally {
                initializedOn = null;
            }
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(pipelineId);
        out.writeObject(delegate);
        out.writeInt(participants.size());
        for (var participant : participants.entrySet()) {
            out.writeObject(participant.getKey());
            out.writeString(participant.getValue().nodeId());
            out.writeLong(participant.getValue().uuid().getMostSignificantBits());
            out.writeLong(participant.getValue().uuid().getLeastSignificantBits());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        pipelineId = in.readString();
        delegate = in.readObject();
        int count = in.readInt();
        if (count < 1) {
            throw new IOException("a physical member plan is empty");
        }
        Map<Address, MemberIdentity> snapshot = new HashMap<>();
        for (int index = 0; index < count; index++) {
            Address address = in.readObject();
            String nodeId = in.readString();
            UUID uuid = new UUID(in.readLong(), in.readLong());
            if (snapshot.put(address, new MemberIdentity(nodeId, uuid)) != null) {
                throw new IOException("a physical member plan contains a repeated address");
            }
        }
        participants = Map.copyOf(snapshot);
        delegateInitStarted = false;
        initializedOn = null;
    }

    private static TapstateException refused(String pipelineId) {
        return new TapstateException(EngineError.EXECUTION_NOT_AUTHORIZED, Map.of("pipeline", pipelineId), null);
    }
}
