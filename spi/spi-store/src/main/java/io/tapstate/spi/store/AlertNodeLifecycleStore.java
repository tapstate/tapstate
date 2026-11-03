package io.tapstate.spi.store;

import io.tapstate.core.alerting.AlarmKey;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** CONFIG authority for whether one exact admitted node boot is still expected. */
public interface AlertNodeLifecycleStore {
    int MAX_ROWS = 512;
    int MAX_ROW_BYTES = 4096;
    enum Phase { EXPECTED, RETIRING }
    record Key(AlarmKey.Scope scope,String nodeId,String bootId) {
        public Key { Objects.requireNonNull(scope); new AlarmKey.Resource("cluster-node",nodeId,bootId); }
        public AlarmKey.Resource resource(){return new AlarmKey.Resource("cluster-node",nodeId,bootId);}
    }
    record Registration(Key key,StorageSetupScope setup,CacheAuthScope cache,WorkloadClaimFence session,
            String memberId,long membershipRevision) {
        public Registration {
            Objects.requireNonNull(key);Objects.requireNonNull(setup);Objects.requireNonNull(cache);Objects.requireNonNull(session);
            if(!key.scope().clusterId().equals(setup.clusterId()) || !key.scope().clusterId().equals(cache.clusterId())
                    || session.key().type()!=WorkloadClaimType.NODE_SESSION || !key.scope().clusterId().equals(session.key().clusterId())
                    || !key.nodeId().equals(session.key().resourceId()) || !key.nodeId().equals(session.owner().nodeId())
                    || !key.bootId().equals(session.owner().bootId()) || membershipRevision<1
                    || !UUID.fromString(memberId).toString().equals(memberId))throw new IllegalArgumentException("foreign node registration");
        }
        public String producerIncarnation() {
            try {
                var digest=java.security.MessageDigest.getInstance("SHA-256");
                for(String part:List.of(key.scope().authorityId(),key.scope().clusterId(),key.nodeId(),key.bootId(),
                        setup.installationId(),setup.operationId(),Long.toString(setup.generation()),cache.cacheIncarnation(),
                        Long.toString(cache.bindingVersion()),Long.toString(cache.authGeneration()),memberId,
                        Long.toString(session.claimGeneration()),Long.toString(session.executionGeneration()),Long.toString(session.topologyRevision()))) {
                    byte[] bytes=part.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    digest.update(java.nio.ByteBuffer.allocate(4).putInt(bytes.length).array());digest.update(bytes);
                }
                return java.util.HexFormat.of().formatHex(digest.digest());
            }catch(java.security.NoSuchAlgorithmException impossible){throw new IllegalStateException(impossible);}
        }
    }
    record Row(Registration registration,long revision,Instant registeredAt,Phase phase,String commandId,Instant requestedAt) {
        public Row {
            Objects.requireNonNull(registration);Objects.requireNonNull(registeredAt);Objects.requireNonNull(phase);
            if(revision<1 || (phase==Phase.EXPECTED ? commandId!=null || requestedAt!=null
                    : commandId==null || requestedAt==null || requestedAt.isBefore(registeredAt)
                    || !UUID.fromString(commandId).toString().equals(commandId)))throw new IllegalArgumentException("invalid node lifecycle");
        }
    }
    /** The callback rechecks only the already captured original lease and deadline, without IO. */
    Row register(Registration original,Instant registeredAt,Runnable locallyCurrent);
    Row retire(Registration original,long revision,String commandId,Instant requestedAt,Runnable locallyCurrent);
    Optional<Row> read(Key key);
    /** Bounded by MAX_ROWS and scoped to the original trusted host. */
    List<Row> list();
}
