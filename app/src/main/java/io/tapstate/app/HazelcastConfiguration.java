package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestStateMapStoreFactory;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsItem;
import io.tapstate.runtime.srs.SrsItemSerializer;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.SrsMetaStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.Nullable;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Wires the embedded Hazelcast member into the assembly root: exactly one full member per process,
 * with the Jet execution engine enabled. The context owns the member's lifecycle — it is created
 * with the context and shut down when the context closes.
 *
 * <p>The member is structurally undiscoverable: every join-discovery path is disabled explicitly.
 * A bare {@link Config} defaults to auto-detection, which falls back to multicast discovery — a
 * stray same-subnet member joining silently would break the single-member replay invariant the
 * runtime is built on. The pinned cluster name is a second fence: members that disagree on the
 * name never merge. The listen socket is loopback-only: the member port also serves the
 * (unauthenticated) client protocol, so a single local member must not be reachable from the LAN;
 * widening the bind is a deliberate multi-node change.
 */
@Configuration
@EnableConfigurationProperties(HazelcastProperties.class)
class HazelcastConfiguration {

    /**
     * The bounded capacity of each per-table SRS change ring. Headroom backpressure, not size, is the
     * primary guard against overwriting an unread change, so this is a coarse single-node default rather
     * than a tuned figure.
     */
    private static final int SRS_RING_CAPACITY = 1024;

    @Bean(destroyMethod = "shutdown")
    HazelcastInstance hazelcastMember(HazelcastProperties properties, @Nullable SrsMetaStore srsMetaStore,
            @Nullable ConnectorProvisioner connectorProvisioner, @Nullable SnapshotBuffer snapshotBuffer,
            @Nullable KeyedStateStore nestStateStore, NestSettings nestSettings) {
        Config config = memberConfig(properties, nestStateStore, nestSettings);
        HazelcastInstance member = startMember(() -> Hazelcast.newHazelcastInstance(config));
        // Bind the SRS meta store onto the member so the read-cursor publisher factory -- carried onto the
        // Jet source and resolved member-side -- can reach it through the user context and publish durable
        // read cursors. A run with no store (mongo disabled) binds nothing, and the publisher then no-ops.
        if (srsMetaStore != null) {
            member.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, srsMetaStore);
        }
        // Bind the connector provisioner onto the member so a sink-writer factory -- carried onto the Jet
        // sink vertex and resolved member-side -- can reach it and open its target connector. A run with no
        // provisioner (mongo disabled) binds nothing, and the member is then not sink-capable: a sink open
        // fails loudly rather than silently dropping writes.
        if (connectorProvisioner != null) {
            member.getUserContext().put(
                    PdkSinkWriterFactory.CONNECTOR_PROVISIONER_USER_CONTEXT_KEY, connectorProvisioner);
        }
        // Bind the snapshot buffer onto the member so a source vertex -- resolved member-side by the ring name
        // it carries -- can drain this ring's snapshot rows and emit them ahead of the cdc tail. The coordinator
        // holds the same instance and fills it through the snapshot pass-through. A run with no buffer (mongo
        // disabled) binds nothing, and a source then emits no snapshot ahead of the tail.
        if (snapshotBuffer != null) {
            member.getUserContext().put(SnapshotBuffer.USER_CONTEXT_KEY, snapshotBuffer);
        }
        // Bind the layer behind the nest state maps onto the member, so a store named in a map's
        // configuration -- and built by the substrate on whichever member runs that map -- can reach it.
        // The configuration carries the name and not the instance because a configuration added once the
        // member is running is written down and broadcast, and a live store does not survive that. A run
        // with no store (mongo disabled) binds nothing, and its maps declare no store to resolve.
        if (nestStateStore != null) {
            NestStateMapStoreFactory.bindTo(member, nestStateStore);
        }
        return member;
    }

    /**
     * Starts the member, translating a Hazelcast startup failure — typically the loopback member
     * port being already in use — into a coded diagnostic so the operator sees a clean message
     * instead of a bare stack trace. Anything that is not a {@link HazelcastException} (a programmer
     * error while assembling the config) propagates unchanged: it must crash bare, not be laundered
     * into a code that hides the defect. The factory is a seam so the translation is unit-testable.
     */
    static HazelcastInstance startMember(Supplier<HazelcastInstance> factory) {
        try {
            return factory.get();
        } catch (HazelcastException cause) {
            throw new TapstateException(BootError.HAZELCAST_UNAVAILABLE, Map.of(), cause);
        }
    }

    /**
     * What every nest in this process is allowed to be, as one value rather than as one per place that
     * asks. The shape of the state maps and the limits the running vertices are held to are two halves of
     * the same capacity decision - taken from two instances they can be set into a combination that cannot
     * work, with neither able to see the other's number - so both are taken from this bean.
     */
    @Bean
    NestSettings nestSettings() {
        return NestSettings.defaults();
    }

    /** Builds the single-member config with nothing behind the nest state maps. */
    static Config memberConfig(HazelcastProperties properties) {
        return memberConfig(properties, null);
    }

    /** Builds the single-member config with the default limits, for a caller configuring none. */
    static Config memberConfig(HazelcastProperties properties, @Nullable KeyedStateStore nestStateStore) {
        return memberConfig(properties, nestStateStore, NestSettings.defaults());
    }

    /**
     * Builds the single-member config; pure function, exposed for direct assertion. A run with no store
     * ({@code nestStateStore} null) still gets the nest state maps, holding only what the member holds:
     * declaring a store that is not there would fail the map the first time a vertex asked it for a key.
     */
    static Config memberConfig(HazelcastProperties properties, @Nullable KeyedStateStore nestStateStore,
            NestSettings nestSettings) {
        Config config = new Config();
        config.setClusterName(properties.getClusterName());
        // Member logs flow through the same operational logging setup as the rest of the process.
        config.setProperty("hazelcast.logging.type", "slf4j");
        // The context owns the member lifecycle (bean destroy); Hazelcast's own JVM shutdown hook
        // would race the context's orderly shutdown.
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        // An embedded member of a server product must not report usage data anywhere.
        config.setProperty("hazelcast.phone.home.enabled", "false");
        // Loopback-only listen socket: the member port also serves the unauthenticated client
        // protocol, so a single local member must not expose it on a LAN interface.
        config.setProperty("hazelcast.socket.bind.any", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        config.getJetConfig().setEnabled(true);
        Integer cooperativeThreads = properties.getJet().getCooperativeThreadCount();
        if (cooperativeThreads != null) {
            config.getJetConfig().setCooperativeThreadCount(cooperativeThreads);
        }
        // Make the member SRS-capable. The change-ring item is not zero-config serializable (its
        // heterogeneous row map defeats Compact), so its stream serializer is registered for ring storage
        // and Jet cross-vertex transport alike. The per-table change rings under srs.* are the SRS's only
        // hot buffer: bounded, in memory, with no time expiry (headroom backpressure guards unread
        // overwrites, not TTL) and no backups (single node).
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig()
                .setTypeClass(SrsItem.class)
                .setImplementation(new SrsItemSerializer()));
        config.addRingBufferConfig(new RingbufferConfig("srs.*")
                .setCapacity(SRS_RING_CAPACITY)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setBackupCount(0));
        // Make the member nest-capable. A nest vertex's state map is created on demand, by the name the
        // compiled topology gave that vertex, so what those maps are has to be declared before any of them
        // exists. The engine owns their shape -- the assembly root only installs it here, next to the ring
        // it does the same for.
        config.addMapConfig(nestStateStore == null
                ? nestSettings.stateMaps()
                : nestSettings.backedStateMaps());
        return config;
    }
}
