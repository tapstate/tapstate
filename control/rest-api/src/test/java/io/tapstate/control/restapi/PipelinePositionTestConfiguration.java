package io.tapstate.control.restapi;

import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.LivePipelines;
import io.tapstate.control.core.PipelineChains;
import io.tapstate.control.core.PipelinePositionService;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.StateStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * The position service, for contexts that import the whole HTTP face and do not exercise positions.
 *
 * <p>Every controller in that bundle brings its service with it as a requirement: a context that mounts
 * the face without one does not start. That is the design -- a face is either wired or it is not -- and
 * the alternative failed in the product rather than in a test, so it is worth the small cost here.
 *
 * <p>The collaborators are built inside the bean method rather than published as beans of their own. Two
 * of these types are already beans in the contexts that import this, and a second candidate would make
 * their injection ambiguous -- which would be this configuration breaking tests about something else.
 *
 * <p>The stores throw rather than answering. Nothing in those contexts reaches a position route, so an
 * answer here would be a fixture nobody asked for; a throw says plainly that a case which did reach one
 * should have supplied its own.
 */
@Configuration(proxyBeanMethods = false)
final class PipelinePositionTestConfiguration {

    @Bean
    PipelinePositionService pipelinePositionService(ArtifactStore artifacts, Clock clock) {
        PipelineChains chains = pipelineId -> List.of();
        return new PipelinePositionService(chains, new UnreachableMeta(), artifacts,
                new LivePipelines(new UnreachableDesired(), new UnreachableState()),
                new AuditGate(record -> { }, clock));
    }

    private static UnsupportedOperationException notHere() {
        return new UnsupportedOperationException(
                "no case in this context reads or writes a position; a case that does has to supply a store");
    }

    private static final class UnreachableMeta implements SrsMetaStore {

        @Override
        public Optional<SrsMeta> read(String miningChainId) {
            throw notHere();
        }

        @Override
        public void create(String miningChainId, String retention) {
            throw notHere();
        }

        @Override
        public void advanceSourceReadOffset(String miningChainId, ChainPosition position) {
            throw notHere();
        }

        @Override
        public void rewindSourceReadOffset(String miningChainId, String token) {
            throw notHere();
        }

        @Override
        public void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
            throw notHere();
        }

        @Override
        public void advanceConsumerReadSeq(
                String miningChainId, String pipelineId, String table, long lastReadSeq) {
            throw notHere();
        }

        @Override
        public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
            throw notHere();
        }

        @Override
        public void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch) {
            throw notHere();
        }

        @Override
        public long openEpoch(String miningChainId) {
            throw notHere();
        }

        @Override
        public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
            throw notHere();
        }

        @Override
        public void markSnapshotComplete(String miningChainId, String pipelineId, String table) {
            throw notHere();
        }

        @Override
        public List<String> miningChainIdsWithConsumer(String pipelineId) {
            throw notHere();
        }

        @Override
        public void detachConsumer(String miningChainId, String pipelineId) {
            throw notHere();
        }

        @Override
        public void dropChain(String miningChainId) {
            throw notHere();
        }
    }

    private static final class UnreachableDesired implements DesiredStore {

        @Override
        public Optional<DesiredState> read(String pipelineId) {
            throw notHere();
        }

        @Override
        public void save(DesiredState desired) {
            throw notHere();
        }

        @Override
        public List<String> pipelineIds() {
            throw notHere();
        }

        @Override
        public void delete(String pipelineId) {
            throw notHere();
        }
    }

    private static final class UnreachableState implements StateStore {

        @Override
        public Optional<CheckpointDoc> read(String pipelineId) {
            throw notHere();
        }

        @Override
        public void create(String pipelineId, String stateJson, Instant touchTime) {
            throw notHere();
        }

        @Override
        public CasOutcome compareAndSwap(
                String pipelineId, long expectedEpoch, String nextStateJson, Instant touchTime) {
            throw notHere();
        }

        @Override
        public void delete(String pipelineId) {
            throw notHere();
        }
    }
}
