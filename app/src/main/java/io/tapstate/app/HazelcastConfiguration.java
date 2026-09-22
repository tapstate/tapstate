package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.RingbufferStoreConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import io.tapstate.adapters.pdk.ConnectorProvisioner;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.runtime.engine.EnvelopeSerializer;
import io.tapstate.runtime.engine.nest.DurableNestDeadLetter;
import io.tapstate.runtime.engine.join.JoinMaps;
import io.tapstate.runtime.engine.join.JoinStateMapStoreFactory;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestStateMapStoreFactory;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.SnapshotBuffer;
import io.tapstate.runtime.srs.SrsItem;
import io.tapstate.runtime.srs.SrsItemSerializer;
import io.tapstate.runtime.srs.SrsLogRingbufferStoreFactory;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.NestDeadLetterStore;
import io.tapstate.spi.store.OperatorStateStores;
import io.tapstate.spi.store.SrsLogStore;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.ClusterMembershipStore;
import io.tapstate.spi.store.WorkloadClaimStore;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.lang.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
@EnableConfigurationProperties({HazelcastProperties.class, ControlEndpointProperties.class})
@Import(ClusterMembershipConfiguration.class)
class HazelcastConfiguration {

    static final String NODE_SESSION_CONTEXT_KEY = "tapstate.cluster.node-session";
    private static final Logger LOG = LoggerFactory.getLogger(HazelcastConfiguration.class);

    /** A single port, or an inclusive range, in the form the library itself accepts. */
    private static final Pattern PORT_DEFINITION = Pattern.compile("(\\d{1,5})(?:-(\\d{1,5}))?");

    /**
     * The bounded capacity of each per-table SRS change ring. Headroom backpressure, not size, is the
     * primary guard against overwriting an unread change, so this is a coarse single-node default rather
     * than a tuned figure.
     */
    private static final int SRS_RING_CAPACITY = 1024;

    @Bean(destroyMethod = "shutdown")
    HazelcastInstance hazelcastMember(HazelcastProperties properties, ClusterProperties clusterProperties,
            ControlEndpointProperties controlProperties, @Nullable SrsMetaStore srsMetaStore,
            @Nullable ConnectorProvisioner connectorProvisioner, @Nullable SnapshotBuffer snapshotBuffer,
            @Nullable KeyedStateStore nestStateStore, NestSettings nestSettings,
            @Nullable NestDeadLetterStore nestDeadLetterStore,
            @Nullable OperatorStateStores operatorStateStores, @Nullable SrsLogStore srsLogStore,
            ObjectProvider<ClusterIdentityStore> clusterIdentities,
            ObjectProvider<WorkloadClaimStore> workloadClaims,
            ClusterMembershipGate membershipGate) {
        ClusterMemberPreflight.Identity identity =
                ClusterMemberPreflight.validate(properties, clusterProperties, controlProperties);
        warnAboutClusterProfile(clusterProperties);
        WorkloadClaimStore claimStore = workloadClaims.getIfAvailable();
        if (identity != null) {
            identity = ClusterMemberPreflight.reserve(identity, clusterProperties,
                    clusterIdentities.getIfAvailable(), claimStore, UUID.randomUUID().toString());
        }
        Config config = memberConfig(properties, nestStateStore, nestSettings, srsLogStore);
        if (identity != null) {
            identify(config, identity);
            configureClusterProtection(config, membershipGate);
        }
        HazelcastInstance member;
        try {
            member = startMember(() -> Hazelcast.newHazelcastInstance(config));
        } catch (RuntimeException startupFailure) {
            if (identity != null && claimStore != null) {
                claimStore.release(identity.nodeSession());
            }
            throw startupFailure;
        }
        if (identity != null) {
            // What the rings and maps of this member will answer when work reaches them. Asked rather
            // than assumed, because the library caches its answer and recomputes it on its own schedule:
            // admitting work this member's data plane is still refusing produces a run that reaches
            // RUNNING and then dies on its first write.
            membershipGate.observeDataPlane(() -> dataPlaneAdmitsWork(member));
            member.getUserContext().put("tapstate.cluster.id", identity.clusterId());
            member.getUserContext().put("tapstate.cluster.node-id", identity.nodeId());
            member.getUserContext().put("tapstate.cluster.boot-id", identity.nodeSession().owner().bootId());
            member.getUserContext().put("tapstate.control.advertise-url", identity.controlUrl().toString());
            member.getUserContext().put(NODE_SESSION_CONTEXT_KEY, identity.nodeSession());
            member.getUserContext().put(
                    io.tapstate.runtime.engine.nest.NestMemoryBudget.SPLIT_BRAIN_PROTECTION_CONTEXT_KEY,
                    ClusterMembershipGate.PROTECTION_NAME);
        }
        // Bind the SRS meta store onto the member so the read-cursor publisher factory -- carried onto the
        // Jet source and resolved member-side -- can reach it through the user context and publish durable
        // read cursors. A run with no store (mongo disabled) binds nothing, and the publisher then no-ops.
        if (srsMetaStore != null) {
            member.getUserContext().put(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY, srsMetaStore);
        }
        // Bind the change log too, so the capture runtime can cut it back at the durable frontier. The
        // rings already reach it through their own configuration; this is the same store, reached the way
        // everything else the runtime resolves member-side is reached. A run with no store binds nothing,
        // and nothing is cut -- because nothing was written down either.
        if (srsLogStore != null) {
            member.getUserContext().put(CaptureRunUnit.SRS_LOG_USER_CONTEXT_KEY, srsLogStore);
        }
        // Bind the connector provisioner onto the member so a sink-writer factory -- carried onto the Jet
        // sink vertex and resolved member-side -- can reach it and open its target connector. A run with no
        // provisioner (mongo disabled) binds nothing, and the member is then not sink-capable: a sink open
        // fails loudly rather than silently dropping writes.
        if (connectorProvisioner != null) {
            member.getUserContext().put(
                    PdkSinkWriterFactory.CONNECTOR_PROVISIONER_USER_CONTEXT_KEY, connectorProvisioner);
        }
        // Bind the layer a connector's own notes are kept in onto the member, for the same reason the
        // provisioner is: the sink-writer factory crosses to whichever member runs the sink vertex and a live
        // store does not survive the crossing, so the node travels and the store is picked up where the
        // connector is actually opened. The read side needs no such indirection -- it opens its connector in
        // the process that holds the store -- which is why only the write side reaches through here. A run
        // with no store (mongo disabled) binds nothing, and a sink connector then keeps its notes for the life
        // of the open, exactly as it did before there was anywhere to file them.
        if (nestStateStore != null) {
            member.getUserContext().put(
                    PdkSinkWriterFactory.CONNECTOR_STATE_STORE_USER_CONTEXT_KEY, nestStateStore);
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
        if (operatorStateStores != null) {
            NestStateMapStoreFactory.bindTo(member, operatorStateStores);
        } else if (nestStateStore != null) {
            NestStateMapStoreFactory.bindTo(member, nestStateStore);
        }
        if (nestStateStore != null) {
            // The same layer, bound again under the join's own key. One key shared between them would
            // read as tidier and would make "these two are told about different layers" impossible to
            // say - which is a thing a deployment may one day want to say, and a thing neither of them
            // could then express without the other noticing.
            JoinStateMapStoreFactory.bindTo(member, nestStateStore);
        }
        if (operatorStateStores != null) {
            makeNestCapable(member, operatorStateStores, nestSettings);
        } else {
            makeNestCapable(member, nestStateStore, nestSettings);
        }
        makeJoinCapable(member, nestStateStore);
        // Bind the channel behind the nest dead letters onto the member for the same reason: the channel is
        // carried onto the vertex and resolved member-side, because somewhere durable to put a row is
        // reached through a handle that does not survive being written into a graph. A run with no store
        // (mongo disabled) binds nothing, and a nest vertex then refuses to start rather than running on
        // with nowhere to put what it cannot assemble -- which is the failure this channel exists to stop.
        if (operatorStateStores != null) {
            DurableNestDeadLetter.bindTo(member, operatorStateStores);
        } else if (nestDeadLetterStore != null) {
            DurableNestDeadLetter.bindTo(member, nestDeadLetterStore);
        }
        return member;
    }

    /** Renews and releases the claim that was acquired before this member was created. */
    @Bean(destroyMethod = "close")
    NodeSessionLease nodeSessionLease(
            HazelcastInstance member,
            ClusterProperties clusterProperties,
            ObjectProvider<WorkloadClaimStore> workloadClaims) {
        Object stored = member.getUserContext().get(NODE_SESSION_CONTEXT_KEY);
        if (!(stored instanceof io.tapstate.spi.store.WorkloadClaim claim)) {
            return NodeSessionLease.inactive();
        }
        WorkloadClaimStore store = workloadClaims.getIfAvailable();
        if (store == null) {
            throw new IllegalStateException("cluster member started without its workload-claim store");
        }
        return new NodeSessionLease(store, claim, clusterProperties.getNodeSessionTtl(),
                clusterProperties.getNodeSessionRenewInterval(), member::shutdown);
    }

    /** Keeps the local gate aligned with the majority-committed ACTIVE node set. */
    @Bean(destroyMethod = "close")
    ClusterMembershipController clusterMembershipController(
            HazelcastInstance member,
            ClusterProperties clusterProperties,
            ClusterMembershipGate membershipGate,
            ObjectProvider<ClusterMembershipStore> membershipStores) {
        if (clusterProperties.getProfile() == ClusterProperties.Profile.SINGLE) {
            return ClusterMembershipController.inactive();
        }
        ClusterMembershipStore store = membershipStores.getIfAvailable();
        if (store == null) {
            throw new IllegalStateException("cluster member started without its membership store");
        }
        return new ClusterMembershipController(clusterProperties.getId(), member, store, membershipGate,
                clusterProperties.getMembershipReconcileInterval());
    }

    /** Test seam retaining the single-member call shape that predates cluster identity configuration. */
    HazelcastInstance hazelcastMember(HazelcastProperties properties, @Nullable SrsMetaStore srsMetaStore,
            @Nullable ConnectorProvisioner connectorProvisioner, @Nullable SnapshotBuffer snapshotBuffer,
            @Nullable KeyedStateStore nestStateStore, NestSettings nestSettings,
            @Nullable NestDeadLetterStore nestDeadLetterStore, @Nullable SrsLogStore srsLogStore) {
        return hazelcastMember(properties, new ClusterProperties(), new ControlEndpointProperties(),
                srsMetaStore, connectorProvisioner, snapshotBuffer, nestStateStore, nestSettings,
                nestDeadLetterStore, null, srsLogStore, emptyProvider(), emptyProvider(),
                new ClusterMembershipGate(new ClusterProperties()));
    }

    private static <T> ObjectProvider<T> emptyProvider() {
        return new ObjectProvider<>() {
            @Override
            public T getObject(Object... args) {
                throw new org.springframework.beans.factory.NoSuchBeanDefinitionException(Object.class);
            }

            @Override
            public T getIfAvailable() {
                return null;
            }

            @Override
            public T getIfUnique() {
                return null;
            }

            @Override
            public T getObject() {
                throw new org.springframework.beans.factory.NoSuchBeanDefinitionException(Object.class);
            }
        };
    }

    /**
     * Declares what every nest state map on {@code member} is, once the member is already running.
     *
     * <p><b>After the member starts, and that is the whole of this method.</b> A namespace belongs to a
     * pipeline, so a per-pipeline budget can only be written once there is a pipeline - which is always
     * after this. The substrate resolves a map's configuration by looking through the static
     * configuration by pattern first and only then at what was added while it ran, so a pattern left in
     * the static configuration answers for every namespace and no exact configuration behind it is ever
     * reached. Declared here instead, the pattern and the exact names sit in the same place, where an
     * exact name wins over a pattern - which is what makes a pipeline's own number the one in force.
     *
     * <p>Measured before it was moved: a budget of 271 applied to a namespace read back as 271 from every
     * way of asking, while the map ran on the process-wide 4,000 and held all 700 entries written to it.
     * Neither the substrate nor the configuration says anything when that happens; the only trace is how
     * many entries are resident, which nothing was reading.
     *
     * <p>Only with a store behind them, for the reason the store binding above gives: nest state must
     * outlive the process, so a map that keeps it in memory alone is not a smaller version of this. A run
     * with no store drives no pipeline, so no vertex ever asks for a state map. {@code nestStateStore} is
     * therefore allowed to be null and is not annotated as such: this is not a bean method, so nothing
     * reads the annotation, and the one that would be written here is deprecated.
     */
    static void makeNestCapable(HazelcastInstance member, KeyedStateStore nestStateStore,
            NestSettings nestSettings) {
        if (nestStateStore == null) {
            return;
        }
        member.getConfig().addMapConfig(protectIfCluster(member, nestSettings.backedStateMaps()));
    }

    /** Declares the nest map pattern with the deployment's resolved default database. */
    static void makeNestCapable(HazelcastInstance member, OperatorStateStores stores,
            NestSettings nestSettings) {
        if (stores == null) {
            return;
        }
        member.getConfig().addMapConfig(
                nestSettings.backedStateMapsForDatabase(stores.defaultDatabase()));
    }

    /**
     * Declares what every join state map on {@code member} is, once the member is already running.
     *
     * <p><b>After the member starts, for the reason {@link #makeNestCapable} gives.</b> The substrate
     * resolves a map's configuration by looking through the static configuration by pattern first and
     * only then at what was added while it ran, so a pattern left in the static configuration answers
     * for every namespace and no exact configuration behind it is ever reached. Join state carries no
     * per-namespace configuration today, so nothing is being shadowed yet; it is declared here so that
     * the day one is added it is reached, rather than being ignored with nothing saying so.
     *
     * <p><b>Its own method rather than a line inside the nest one.</b> Nest and join are two mechanisms
     * that happen to want the same treatment here, not one mechanism; folded together, "these two are
     * configured differently" becomes a thing neither could express without the other noticing.
     *
     * <p>Only with a store behind them, for the reason the store binding gives: join state is what lets
     * a broken target table be rebuilt without reading the source again, so a map that keeps it in
     * memory alone is not a smaller version of this but a way to lose it quietly.
     */
    static void makeJoinCapable(HazelcastInstance member, KeyedStateStore joinStateStore) {
        if (joinStateStore == null) {
            return;
        }
        member.getConfig().addMapConfig(protectIfCluster(
                member, JoinMaps.backedStateMaps(JoinMaps.DEFAULT_ENTRIES_HELD_IN_MEMORY)));
    }

    /**
     * Starts the member, translating a Hazelcast startup failure — typically the loopback member
     * port being already in use — into a coded diagnostic so the operator sees a clean message
     * instead of a bare stack trace. Anything that is not a {@link HazelcastException} (a programmer
     * error while assembling the config) propagates unchanged: it must crash bare, not be laundered
     * into a code that hides the defect. The factory is a seam so the translation is unit-testable.
     */
    /**
     * Writes this node's Tapstate identity onto the member before it joins, as member attributes.
     *
     * <p>Attributes rather than a lookup: they travel with membership itself, so every member holds every
     * other's identity the moment it sees it, with no round trip and no second place for them to go
     * stale. That is what lets the topology read face answer the same on any node, and it is why they are
     * set here -- before the member joins -- rather than published afterwards, which would leave a window
     * in which a member is in the cluster and anonymous.
     */
    static Config identify(Config config, ClusterMemberPreflight.Identity identity) {
        config.setClusterName(identity.clusterId());
        config.getMemberAttributeConfig()
                .setAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE, identity.nodeId())
                .setAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE,
                        identity.nodeSession().owner().bootId())
                .setAttribute(ClusterMembershipGate.CONTROL_URL_ATTRIBUTE,
                        identity.controlUrl().toString());
        return config;
    }

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

    /** Builds the single-member config for a run with no store, and so with no nest state maps. */
    static Config memberConfig(HazelcastProperties properties) {
        return memberConfig(properties, null);
    }

    /** Builds the single-member config with the default limits, for a caller configuring none. */
    static Config memberConfig(HazelcastProperties properties, @Nullable KeyedStateStore nestStateStore) {
        return memberConfig(properties, nestStateStore, NestSettings.defaults(), null);
    }

    /** As above, with nest settings but no change log -- the shape a caller that has no store gets. */
    static Config memberConfig(HazelcastProperties properties, @Nullable KeyedStateStore nestStateStore,
            NestSettings nestSettings) {
        return memberConfig(properties, nestStateStore, nestSettings, null);
    }

    /**
     * Builds the single-member config; pure function, exposed for direct assertion. A run with no store
     * ({@code nestStateStore} null) gets no nest state maps at all -- see the comment where they are
     * installed.
     */
    static Config memberConfig(HazelcastProperties properties, @Nullable KeyedStateStore nestStateStore,
            NestSettings nestSettings, @Nullable SrsLogStore srsLogStore) {
        Config config = new Config();
        config.setClusterName(properties.getClusterName());
        // Member logs flow through the same operational logging setup as the rest of the process.
        config.setProperty("hazelcast.logging.type", "slf4j");
        // The context owns the member lifecycle (bean destroy); Hazelcast's own JVM shutdown hook
        // would race the context's orderly shutdown.
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        // An embedded member of a server product must not report usage data anywhere.
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.heartbeat.interval.seconds",
                Long.toString(properties.getHeartbeatInterval().toSeconds()));
        config.setProperty("hazelcast.max.no.heartbeat.seconds",
                Long.toString(properties.getMaximumNoHeartbeat().toSeconds()));
        // Loopback-only listen socket: the member port also serves the unauthenticated client
        // protocol, so a single local member must not expose it on a LAN interface.
        config.setProperty("hazelcast.socket.bind.any", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        if (!"127.0.0.1".equals(properties.getBindAddress())) {
            config.getNetworkConfig().getInterfaces().clear().addInterface(properties.getBindAddress());
            LOG.warn("Hazelcast member port is exposed on {} and serves an unauthenticated protocol. "
                    + "Keep it inside a private network or NetworkPolicy.", properties.getBindAddress());
        }
        applyAdvertisedMemberAddress(config, properties);
        applyOutboundMemberPorts(config, properties);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getKubernetesConfig().setEnabled(false);
        switch (properties.getDiscovery().getMode()) {
            case NONE -> {
                // The default path deliberately keeps every network value above byte-for-byte unchanged.
            }
            case TCP_IP -> {
                if (properties.getDiscovery().getTcpIp().getSeeds().isEmpty()) {
                    throw invalidDiscovery("tcp-ip requires at least one seed address");
                }
                config.getNetworkConfig().setPort(properties.getMemberPort()).setPortAutoIncrement(false);
                join.getTcpIpConfig().setEnabled(true)
                        .setMembers(properties.getDiscovery().getTcpIp().getSeeds());
            }
            case KUBERNETES -> {
                HazelcastProperties.Kubernetes kubernetes = properties.getDiscovery().getKubernetes();
                boolean hasDns = hasText(kubernetes.getServiceDns());
                boolean hasApiService = hasText(kubernetes.getServiceName());
                if (hasDns == hasApiService) {
                    throw invalidDiscovery("kubernetes requires exactly one of service-dns or service-name");
                }
                config.getNetworkConfig().setPort(properties.getMemberPort()).setPortAutoIncrement(false);
                join.getKubernetesConfig().setEnabled(true);
                if (hasDns) {
                    join.getKubernetesConfig().setProperty("service-dns", kubernetes.getServiceDns().trim());
                } else {
                    join.getKubernetesConfig().setProperty("service-name", kubernetes.getServiceName().trim());
                    if (hasText(kubernetes.getNamespace())) {
                        join.getKubernetesConfig().setProperty("namespace", kubernetes.getNamespace().trim());
                    }
                }
            }
        }
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
        // And a change itself, for the same reason and one more: a change is three row images of names to
        // whatever the source had, which the zero-configuration mechanism refuses outright. Registered
        // here rather than where it is needed, because where it is needed is any edge between two
        // members - and a member that cannot write one only finds out on the first event of the first
        // graph to be spread across two of them, at the edge carrying its root stream.
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig()
                .setTypeClass(Envelope.class)
                .setImplementation(new EnvelopeSerializer()));
        RingbufferConfig rings = new RingbufferConfig("srs.*")
                .setCapacity(SRS_RING_CAPACITY)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setBackupCount(0);
        // Put the change log behind them when there is one. The ring writes through it before admitting a
        // change, so every change in the ring is already written down, and a ring rebuilt on a later member
        // numbers on from what the record holds rather than reusing sequences it already named.
        //
        // Not a replay path, and nothing here reads a change back out: a restart re-mines the ring from the
        // durable source read offset instead.
        //
        // A factory rather than a single store: the ring's store hook is told a sequence and an item but
        // never which ring is asking, and only the factory call is given the name. A live instance is
        // allowed here because this configuration is built before the member starts; a configuration added
        // to a running member is written down and broadcast, which no live object survives.
        if (srsLogStore != null) {
            rings.setRingbufferStoreConfig(new RingbufferStoreConfig()
                    .setEnabled(true)
                    .setFactoryImplementation(new SrsLogRingbufferStoreFactory(srsLogStore)));
        }
        config.addRingBufferConfig(rings);
        // What a nest state map is is NOT declared here, and the omission is load-bearing: it is declared
        // once the member is running, by makeNestCapable. A pattern placed in this static configuration
        // answers for every namespace and shadows the per-pipeline budget added later, which the substrate
        // reports nowhere -- see that method.
        //
        // A join state map is declared by a pattern too, and is left out of here for the same reason and
        // under the same rule, by makeJoinCapable. Join state carries no per-namespace budget today, so
        // there is nothing behind the pattern being shadowed yet - which is exactly the state the nest
        // maps were in until the day one was added.
        return config;
    }

    /**
     * Whether this member's own rings and maps would accept a write right now.
     *
     * <p>Not a second opinion on the same question: it is the same call. What a ring does before it
     * accepts a write ends in {@code hasMinimumSize()}, which is what this reads - so the two cannot
     * drift apart without the library's own enforcement drifting with them.
     *
     * <p>A member on its way out answers no rather than throwing: shutdown order between the member and
     * whatever is still asking is not ours to fix here, and a member that is stopping is a member that
     * must not be taking work on anyway.
     */
    private static boolean dataPlaneAdmitsWork(HazelcastInstance member) {
        try {
            return member.getSplitBrainProtectionService()
                    .getSplitBrainProtection(ClusterMembershipGate.PROTECTION_NAME)
                    .hasMinimumSize();
        } catch (HazelcastInstanceNotActiveException stopping) {
            return false;
        }
    }

    static void configureClusterProtection(Config config, ClusterMembershipGate gate) {
        config.addSplitBrainProtectionConfig(new SplitBrainProtectionConfig(
                ClusterMembershipGate.PROTECTION_NAME, true)
                .setProtectOn(SplitBrainProtectionOn.READ_WRITE)
                .setFunctionImplementation(gate));
        config.getRingbufferConfigs().values().forEach(ring -> {
            ring.setBackupCount(Math.max(1, ring.getBackupCount()));
            ring.setSplitBrainProtectionName(ClusterMembershipGate.PROTECTION_NAME);
        });
        config.getMapConfigs().values().forEach(map ->
                map.setSplitBrainProtectionName(ClusterMembershipGate.PROTECTION_NAME));
    }

    static void warnAboutClusterProfile(ClusterProperties properties) {
        if (properties.getProfile() == ClusterProperties.Profile.PROCESS_FAILURE_ONLY) {
            LOG.warn("Cluster profile process-failure-only supports process-loss recovery but does not provide "
                    + "network-partition safety. Use production-ha with at least three members for HA.");
        }
    }

    private static MapConfig protectIfCluster(HazelcastInstance member, MapConfig config) {
        if (member.getConfig().getSplitBrainProtectionConfigs()
                .containsKey(ClusterMembershipGate.PROTECTION_NAME)) {
            config.setSplitBrainProtectionName(ClusterMembershipGate.PROTECTION_NAME);
        }
        return config;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    /**
     * Tells the member to report an address other than the one it binds, when the deployment says so.
     *
     * <p>What this changes is where the other members dial, and nothing else -- the interfaces above
     * still decide what this member listens on, and therefore who can reach it at all. Reporting an
     * address nobody can route to is not a way to hide a member that binds a routable interface.
     *
     * <p>Refused outright while discovery is off: a single loopback-only member has no others to be
     * reached by, so an address here is either a misconfiguration or a preparation for something this
     * mode does not do, and both are worth saying out loud rather than accepting silently.
     */
    private static void applyAdvertisedMemberAddress(Config config, HazelcastProperties properties) {
        String advertised = properties.getAdvertisedMemberAddress();
        if (!hasText(advertised)) {
            return;
        }
        if (properties.getDiscovery().getMode() == HazelcastProperties.DiscoveryMode.NONE) {
            throw invalidDiscovery(
                    "advertised-member-address needs a discovery mode: a loopback-only member has "
                            + "nobody to advertise to");
        }
        config.getNetworkConfig().setPublicAddress(advertised.trim());
        LOG.info("Hazelcast member advertises {} to the other members and binds {}.",
                advertised.trim(), properties.getBindAddress());
    }

    /**
     * Restricts which local ports this member dials the other members from, when the deployment says so.
     *
     * <p>An outgoing member connection takes an arbitrary ephemeral port otherwise, and an egress rule
     * written against that has to allow the whole ephemeral range. Naming the ports here is what lets
     * the rule name them too.
     *
     * <p>Refused outright while discovery is off, for the same reason as the reported address: a member
     * that never dials anybody has no outgoing connection for this to apply to, so a value here is
     * either a misconfiguration or a preparation for something this mode does not do.
     */
    private static void applyOutboundMemberPorts(Config config, HazelcastProperties properties) {
        List<String> definitions = properties.getOutboundMemberPorts();
        if (definitions.isEmpty()) {
            return;
        }
        if (properties.getDiscovery().getMode() == HazelcastProperties.DiscoveryMode.NONE) {
            throw invalidDiscovery(
                    "outbound-member-ports needs a discovery mode: a member that dials nobody has no "
                            + "outgoing connection to place");
        }
        for (String definition : definitions) {
            config.getNetworkConfig().addOutboundPortDefinition(checkedPortDefinition(definition));
        }
        LOG.info("Hazelcast member dials the other members from local port(s) {}.", definitions);
    }

    /**
     * Accepts {@code port} or {@code low-high} and refuses anything else here rather than deep inside
     * the library, where a malformed definition surfaces as a start-up failure naming neither the
     * setting nor the value.
     */
    private static String checkedPortDefinition(String definition) {
        String trimmed = definition == null ? "" : definition.trim();
        Matcher matcher = PORT_DEFINITION.matcher(trimmed);
        if (!matcher.matches()) {
            throw invalidDiscovery("outbound-member-ports takes a port or a low-high range, not '"
                    + trimmed + "'");
        }
        int low = Integer.parseInt(matcher.group(1));
        int high = matcher.group(2) == null ? low : Integer.parseInt(matcher.group(2));
        if (low < 1 || high > 65535 || low > high) {
            throw invalidDiscovery("outbound-member-ports needs 1-65535 with the low end first, not '"
                    + trimmed + "'");
        }
        return trimmed;
    }

    private static TapstateException invalidDiscovery(String detail) {
        return new TapstateException(BootError.DISCOVERY_CONFIG_INVALID, Map.of("detail", detail), null);
    }
}
