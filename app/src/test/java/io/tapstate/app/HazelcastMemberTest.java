package io.tapstate.app;

import io.tapstate.core.event.ChainPosition;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.runtime.engine.nest.DurableNestDeadLetter;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsItem;
import io.tapstate.runtime.srs.SrsItemSerializer;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.NestDeadLetterStore;
import io.tapstate.spi.store.SchemaVersion;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The embedded Hazelcast member wired by the assembly root: exactly one full member per process,
 * structurally undiscoverable, cluster name pinned, and the Jet engine enabled with a configurable
 * cooperative thread count.
 *
 * <p>Join discovery must be off explicitly: a bare {@link Config} defaults to auto-detection,
 * which falls back to multicast discovery — and a stray same-subnet member joining silently
 * would break the single-member replay invariant the runtime is built on. The listen socket is
 * loopback-only: the same port also serves the client protocol, which has no authentication.
 */
class HazelcastMemberTest {

    @Test
    void memberConfigDisablesAllJoinDiscovery() {
        JoinConfig join = HazelcastConfiguration.memberConfig(new HazelcastProperties())
                .getNetworkConfig().getJoin();
        assertThat(join.getMulticastConfig().isEnabled()).isFalse();
        assertThat(join.getTcpIpConfig().isEnabled()).isFalse();
        assertThat(join.getAutoDetectionConfig().isEnabled()).isFalse();
    }

    @Test
    void memberConfigPinsTheClusterName() {
        // "tapstate" replaces the Hazelcast default ("dev") as a second isolation fence: even a
        // misconfigured join path cannot merge members that disagree on the cluster name.
        Config config = HazelcastConfiguration.memberConfig(new HazelcastProperties());
        assertThat(config.getClusterName()).isEqualTo("tapstate");
        // The name is an operator knob, not a constant: an override must reach the config.
        HazelcastProperties overridden = new HazelcastProperties();
        overridden.setClusterName("tapstate-two");
        assertThat(HazelcastConfiguration.memberConfig(overridden).getClusterName())
                .isEqualTo("tapstate-two");
    }

    @Test
    void memberBindsLoopbackOnly() {
        // The member port also serves the (unauthenticated) client protocol; a single local member
        // must not listen on a LAN interface. Widening the bind is a deliberate multi-node change.
        Config config = HazelcastConfiguration.memberConfig(new HazelcastProperties());
        assertThat(config.getProperty("hazelcast.socket.bind.any")).isEqualTo("false");
        assertThat(config.getNetworkConfig().getInterfaces().isEnabled()).isTrue();
        assertThat(config.getNetworkConfig().getInterfaces().getInterfaces())
                .containsExactly("127.0.0.1");
    }

    @Test
    void memberConfigPinsThePolicyProperties() {
        // Policy lines, not tuning: member logs flow through the process logging setup, the context
        // owns shutdown (no competing JVM hook), and an embedded member never reports usage data.
        Config config = HazelcastConfiguration.memberConfig(new HazelcastProperties());
        assertThat(config.getProperty("hazelcast.logging.type")).isEqualTo("slf4j");
        assertThat(config.getProperty("hazelcast.shutdownhook.enabled")).isEqualTo("false");
        assertThat(config.getProperty("hazelcast.phone.home.enabled")).isEqualTo("false");
    }

    @Test
    void memberConfigEnablesJetWithTheDefaultCooperativeThreadCount() {
        Config config = HazelcastConfiguration.memberConfig(new HazelcastProperties());
        assertThat(config.getJetConfig().isEnabled()).isTrue();
        assertThat(config.getJetConfig().getCooperativeThreadCount())
                .isEqualTo(Runtime.getRuntime().availableProcessors());
    }

    @Test
    void cooperativeThreadCountIsOverridable() {
        HazelcastProperties properties = new HazelcastProperties();
        properties.getJet().setCooperativeThreadCount(2);
        Config config = HazelcastConfiguration.memberConfig(properties);
        assertThat(config.getJetConfig().getCooperativeThreadCount()).isEqualTo(2);
    }

    @Test
    void memberConfigRegistersTheSrsChangeRingItemSerializer() {
        // The change-ring item is not zero-config serializable (its heterogeneous row map defeats Compact),
        // so its stream serializer must be registered on the member for ring storage and Jet transport alike.
        Config config = HazelcastConfiguration.memberConfig(new HazelcastProperties());
        assertThat(config.getSerializationConfig().getSerializerConfigs())
                .anySatisfy(serializer -> {
                    assertThat(serializer.getTypeClass()).isEqualTo(SrsItem.class);
                    assertThat(serializer.getImplementation()).isInstanceOf(SrsItemSerializer.class);
                });
    }

    @Test
    void memberConfigDefinesTheBoundedSrsChangeRing() {
        // The per-table change rings (srs.<chain>.<table>) are the SRS's only hot buffer: bounded, in
        // memory, no time expiry (headroom backpressure -- not TTL -- guards unread overwrites), no backups
        // (single node). The wildcard config applies to every ring the capture runtime names under srs.*.
        Config config = HazelcastConfiguration.memberConfig(new HazelcastProperties());
        RingbufferConfig ring = config.getRingbufferConfigs().get("srs.*");
        assertThat(ring).isNotNull();
        assertThat(ring.getTimeToLiveSeconds()).isZero();
        assertThat(ring.getBackupCount()).isZero();
        assertThat(ring.getInMemoryFormat()).isEqualTo(InMemoryFormat.OBJECT);
        assertThat(ring.getCapacity()).isEqualTo(1024);
    }

    @Test
    void memberConfigDeclaresWhatNestStateMapsAre() {
        // Nest state maps are created on demand as vertices ask for them, so what they are is decided here
        // or not at all -- and the substrate's own defaults are wrong for state a vertex must read back: a
        // backup replica costs a copy per write for redundancy this state does not need, and expiry would
        // drop entries that a later event is answered from, emitting a half-built document instead of
        // failing. Eviction is the one bound that is allowed, because the store behind the map is where an
        // evicted entry comes back from. The wildcard applies to every map the nest naming lands under, so
        // the engine owns its shape and the assembly root only installs it.
        Config config = HazelcastConfiguration.memberConfig(
                new HazelcastProperties(), new InMemoryKeyedStateStore());
        MapConfig state = config.getMapConfigs().get(NestSettings.defaults().stateMaps().getName());
        assertThat(state).isNotNull();
        assertThat(state.getBackupCount()).isZero();
        assertThat(state.getTimeToLiveSeconds()).isZero();
        assertThat(state.getMaxIdleSeconds()).isZero();
        assertThat(state.getMapStoreConfig().isEnabled())
                .describedAs("the only shape this member installs is one with a store behind it")
                .isTrue();
    }

    @Test
    void memberConfigPutsTheStoreBehindTheStateMapsWhenThereIsOne() {
        // With a store, nest state is written through it as a key is handled and read back per key on the
        // way up: a restart resumes instead of re-reading the sources, which is what the cold layer is for.
        // Write-through is the decision -- a queued write would live in memory, and these maps keep no
        // replica of it, so a crash would lose the tail with nothing reporting it.
        Config config = HazelcastConfiguration.memberConfig(
                new HazelcastProperties(), new InMemoryKeyedStateStore());
        MapStoreConfig store = config.getMapConfigs().get(NestSettings.defaults().stateMaps().getName()).getMapStoreConfig();
        assertThat(store).isNotNull();
        assertThat(store.isEnabled()).isTrue();
        assertThat(store.getWriteDelaySeconds()).isZero();
    }

    @Test
    void memberConfigDeclaresNoStateMapsAtAllWhenThereIsNoStore() {
        // Nest state that outlives nothing is not a lesser version of nest state, it is a way to emit a
        // half-built document and call it whole. So there is no shape for it: without a store there are no
        // state maps to declare. Nothing is lost by their absence -- a run with no store drives no pipeline,
        // so no vertex ever asks for one.
        Config config = HazelcastConfiguration.memberConfig(new HazelcastProperties(), null);
        assertThat(config.getMapConfigs())
                .describedAs("a state map with nothing behind it is not a shape this member offers")
                .doesNotContainKey(NestSettings.defaults().stateMaps().getName());
    }

    @Test
    void hazelcastMemberBindsTheMetaStoreIntoTheUserContext() {
        SrsMetaStore meta = new SentinelMetaStore();
        HazelcastInstance member = new HazelcastConfiguration()
                .hazelcastMember(new HazelcastProperties(), meta, null, null, null, NestSettings.defaults(), null);
        try {
            // The read-cursor publisher factory resolves the store member-side from the user context, so the
            // assembly root binds it under the well-known key -- otherwise cursor publishing silently no-ops.
            assertThat(member.getUserContext().get(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY)).isSameAs(meta);
        } finally {
            member.shutdown();
        }
    }

    @Test
    void hazelcastMemberLeavesTheUserContextUnboundWhenNoStoreIsConfigured() {
        HazelcastInstance member = new HazelcastConfiguration()
                .hazelcastMember(new HazelcastProperties(), null, null, null, null, NestSettings.defaults(), null);
        try {
            // A run with no store (mongo disabled) binds nothing; the publisher then resolves no store and
            // cursor publishing is a documented no-op rather than a failure.
            assertThat(member.getUserContext()).doesNotContainKey(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY);
        } finally {
            member.shutdown();
        }
    }

    @Test
    void hazelcastMemberBindsTheConnectorProvisionerIntoTheUserContext() {
        ConnectorProvisioner provisioner = connectorId -> {
            throw new UnsupportedOperationException("resolution is not exercised by this binding test");
        };
        HazelcastInstance member = new HazelcastConfiguration()
                .hazelcastMember(new HazelcastProperties(), null, provisioner, null, null, NestSettings.defaults(), null);
        try {
            // A sink-writer factory carried onto the Jet sink vertex resolves the provisioner member-side from
            // the user context, so the assembly root binds it under the well-known key -- otherwise the member
            // is not sink-capable and a sink open fails.
            assertThat(member.getUserContext().get(PdkSinkWriterFactory.CONNECTOR_PROVISIONER_USER_CONTEXT_KEY))
                    .isSameAs(provisioner);
        } finally {
            member.shutdown();
        }
    }

    @Test
    void hazelcastMemberLeavesTheProvisionerUnboundWhenNoneIsConfigured() {
        HazelcastInstance member = new HazelcastConfiguration()
                .hazelcastMember(new HazelcastProperties(), null, null, null, null, NestSettings.defaults(), null);
        try {
            // A run with no provisioner (mongo disabled) binds nothing; the member is then not sink-capable and
            // a sink open fails loudly rather than silently dropping writes.
            assertThat(member.getUserContext())
                    .doesNotContainKey(PdkSinkWriterFactory.CONNECTOR_PROVISIONER_USER_CONTEXT_KEY);
        } finally {
            member.shutdown();
        }
    }

    @Test
    void hazelcastMemberBindsTheConnectorStateStoreIntoTheUserContext() {
        KeyedStateStore store = new InMemoryKeyedStateStore();
        HazelcastInstance member = new HazelcastConfiguration()
                .hazelcastMember(new HazelcastProperties(), null, null, null, store, NestSettings.defaults(), null);
        try {
            // A sink-writer factory is serialized onto the sink vertex and opens its connector on whichever
            // member runs it, so the layer a connector's own notes are kept in cannot travel with the factory
            // and is resolved member-side instead. Unbound, every sink connector would be handed a map that
            // dies with the open -- and on a single member that is indistinguishable from working.
            assertThat(member.getUserContext().get(PdkSinkWriterFactory.CONNECTOR_STATE_STORE_USER_CONTEXT_KEY))
                    .isSameAs(store);
        } finally {
            member.shutdown();
        }
    }

    @Test
    void hazelcastMemberLeavesTheConnectorStateStoreUnboundWhenNoneIsConfigured() {
        HazelcastInstance member = new HazelcastConfiguration()
                .hazelcastMember(new HazelcastProperties(), null, null, null, null, NestSettings.defaults(), null);
        try {
            // A run with no store (mongo disabled) binds nothing, and a sink connector then keeps its notes
            // for the life of the open -- which is what every caller got before there was anywhere to file them.
            assertThat(member.getUserContext())
                    .doesNotContainKey(PdkSinkWriterFactory.CONNECTOR_STATE_STORE_USER_CONTEXT_KEY);
        } finally {
            member.shutdown();
        }
    }

    @Test
    void hazelcastMemberBindsTheSnapshotBufferIntoTheUserContext() {
        SnapshotBuffer buffer = new SnapshotBuffer();
        HazelcastInstance member = new HazelcastConfiguration()
                .hazelcastMember(new HazelcastProperties(), null, null, buffer, null, NestSettings.defaults(), null);
        try {
            // A source vertex resolves the buffer member-side from the user context to emit its ring's snapshot
            // rows ahead of the cdc tail, so the assembly root binds the same instance under the well-known key.
            assertThat(member.getUserContext().get(SnapshotBuffer.USER_CONTEXT_KEY)).isSameAs(buffer);
        } finally {
            member.shutdown();
        }
    }

    @Test
    void hazelcastMemberLeavesTheSnapshotBufferUnboundWhenNoneIsConfigured() {
        HazelcastInstance member = new HazelcastConfiguration()
                .hazelcastMember(new HazelcastProperties(), null, null, null, null, NestSettings.defaults(), null);
        try {
            // A run with no buffer (mongo disabled) binds nothing; a source then emits no snapshot ahead of the
            // tail rather than failing.
            assertThat(member.getUserContext()).doesNotContainKey(SnapshotBuffer.USER_CONTEXT_KEY);
        } finally {
            member.shutdown();
        }
    }

    @Test
    void hazelcastMemberBindsTheNestDeadLetterStoreIntoTheUserContext() {
        NestDeadLetterStore deadLetters = new InMemoryNestDeadLetterStore();
        HazelcastInstance member = new HazelcastConfiguration().hazelcastMember(
                new HazelcastProperties(), null, null, null, null, NestSettings.defaults(), deadLetters);
        try {
            // The channel carried onto a nest vertex resolves the store member-side from the user context,
            // so the assembly root binds it under the well-known key -- otherwise a vertex that cannot
            // assemble a change has nowhere to put it, which is the whole failure the channel exists for.
            assertThat(member.getUserContext().get(DurableNestDeadLetter.USER_CONTEXT_KEY))
                    .isSameAs(deadLetters);
        } finally {
            member.shutdown();
        }
    }

    @Test
    void hazelcastMemberLeavesTheNestDeadLetterStoreUnboundWhenNoneIsConfigured() {
        HazelcastInstance member = new HazelcastConfiguration()
                .hazelcastMember(new HazelcastProperties(), null, null, null, null, NestSettings.defaults(), null);
        try {
            // A run with no store (mongo disabled) binds nothing. Unlike the bindings above, the consequence
            // is a refusal rather than a quiet no-op: a nest vertex on such a member fails when it reaches
            // for the channel, which is the right end to fail at for a channel that exists to stop loss.
            assertThat(member.getUserContext()).doesNotContainKey(DurableNestDeadLetter.USER_CONTEXT_KEY);
        } finally {
            member.shutdown();
        }
    }

    @Test
    void embeddedMemberAndJetComeUpAndDownWithTheContext() {
        HazelcastInstance member;
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(Bootstrap.class)
                .web(WebApplicationType.NONE)
                .properties("tapstate.store.mongo.enabled=false",
                        "tapstate.hz.jet.cooperative-thread-count=2")
                .run()) {
            member = context.getBean(HazelcastInstance.class);
            assertThat(member.getLifecycleService().isRunning()).isTrue();
            assertThat(member.getConfig().getClusterName()).isEqualTo("tapstate");
            assertThat(member.getCluster().getMembers()).hasSize(1);
            // The live member picked the loopback address, not a LAN interface.
            assertThat(member.getCluster().getLocalMember().getAddress().getHost())
                    .isEqualTo("127.0.0.1");
            // The cooperative thread count property reaches the member through the binder.
            assertThat(member.getConfig().getJetConfig().getCooperativeThreadCount()).isEqualTo(2);
            // The Jet engine is up and reachable; no jobs exist at the substrate level.
            assertThat(member.getJet().getJobs()).isEmpty();
        }
        // The member's lifecycle is bound to the context: closing the context shuts it down.
        assertThat(member.getLifecycleService().isRunning()).isFalse();
    }

    /** A sentinel meta store: an identity to assert the user-context binding; its facets are never invoked here. */
    private static final class SentinelMetaStore implements SrsMetaStore {
        @Override
        public java.util.List<String> miningChainIdsWithConsumer(String pipelineId) {
            throw new UnsupportedOperationException("consumer detachment is not exercised by this double");
        }

        @Override
        public void detachConsumer(String miningChainId, String pipelineId) {
            throw new UnsupportedOperationException("consumer detachment is not exercised by this double");
        }

        @Override public Optional<SrsMeta> read(String miningChainId) {
            throw new UnsupportedOperationException();
        }

        @Override public void create(String miningChainId, String retention) {
            throw new UnsupportedOperationException();
        }

        @Override public void upsertConsumerOffset(String miningChainId, ConsumerOffset offset) {
            throw new UnsupportedOperationException();
        }

        @Override public void advanceConsumerReadSeq(String miningChainId, String pipelineId, String table, long lastReadSeq) {
            throw new UnsupportedOperationException();
        }

        @Override public void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position) {
            throw new UnsupportedOperationException();
        }

        @Override public void advanceSourceReadOffset(String miningChainId, String sourceReadOffset) {
            throw new UnsupportedOperationException();
        }

        @Override public void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch) {
            throw new UnsupportedOperationException();
        }

        @Override public long openEpoch(String miningChainId) {
            throw new UnsupportedOperationException();
        }

        @Override public void appendSchemaVersion(String miningChainId, SchemaVersion version) {
            throw new UnsupportedOperationException();
        }

        @Override public void markSnapshotComplete(String miningChainId, String table) {
            throw new UnsupportedOperationException();
        }
    }
}
