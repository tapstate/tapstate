package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.runtime.engine.*;
import io.tapstate.runtime.engine.nest.RequiredNestDeadLetter;
import io.tapstate.spi.store.*;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** The pre-join claim and conservative deadline remain the same proof when the ordinary lease takes over. */
final class NativeMemberStorage implements MemberStorage, AutoCloseable {
    private final StorageRuntime.Bound bound;
    private final MongoConfigurationDomain configuration;
    private final ClusterProperties properties;
    private final WorkloadClaimStore claims;
    private final SrsStores sources;
    private volatile NodeSessionLease startupLease;
    private final AtomicReference<NodeSessionLease> lease = new AtomicReference<>();
    // Publishes the joined member; Hazelcast provides concurrency safety for its operations.
    @SuppressWarnings("java:S3077")
    private volatile HazelcastInstance member;
    private volatile boolean closed;
    private volatile long initialDeadline;
    private ExecutionAuthorization authorization;
    private OperatorMapBindings maps;

    NativeMemberStorage(StorageRuntime.Bound bound, MongoConfigurationDomain configuration, ClusterProperties properties,
            WorkloadClaimStore claims, SrsStores sources) {
        this.bound = Objects.requireNonNull(bound); this.configuration = Objects.requireNonNull(configuration);
        this.properties = Objects.requireNonNull(properties); this.claims = Objects.requireNonNull(claims);
        this.sources = Objects.requireNonNull(sources);
    }
    @Override public synchronized void prepare(Config config, WorkloadClaim originalNode, long askedAt) {
        bound.requireBusiness();
        if (closed || authorization != null || originalNode == null) throw ThreeLayerStorePort.refused();
        initialDeadline = askedAt + properties.getNodeSessionTtl().toNanos();
        authorization = ExecutionAuthorization.scoped(bound.preparation().businessScope(), claims,
                properties.getWorkloadClaimRenewInterval(), System::nanoTime, this::nodeActive);
        config.getUserContext().put(ExecutionAuthorization.USER_CONTEXT_KEY, authorization);
        config.getUserContext().put(SrsStoreViews.USER_CONTEXT_KEY, sourceViews());
        maps = OperatorMapBindings.prepare(config, new ScopedOperatorStateProvider(bound.cache(), authorization));
        ScopedSinkStateProvider.bind(config, bound.cache(), configuration.requiredNotes());
        RequiredNestDeadLetter.bindTo(config, new ScopedRequiredDeadLetterProvider(configuration.requiredDeadLetters(), authorization));
    }
    @Override public synchronized void prepare(Config config, WorkloadClaim originalNode, long askedAt, NodeSessionLease proof) {
        if (proof == null || !proof.active()) throw ThreeLayerStorePort.refused();
        prepare(config, originalNode, askedAt);
        startupLease = proof;
    }
    @Override public void joined(HazelcastInstance member) { this.member = Objects.requireNonNull(member); }
    synchronized void admitted(NodeSessionLease original) {
        if (closed || lease.get() != null || startupLease != null && startupLease != original || !original.active() || member == null) throw ThreeLayerStorePort.refused();
        lease.set(original); requireNode();
    }
    boolean nodeActive() {
        if (closed) return false;
        var actual = lease.get();
        if (actual != null) return actual.active() && member != null && member.getLifecycleService().isRunning();
        var starting = startupLease;
        if (starting != null) return starting.active();
        return initialDeadline != 0 && System.nanoTime() - initialDeadline < 0;
    }
    void requireNode() { bound.requireBusiness(); if (!nodeActive()) throw ThreeLayerStorePort.refused(); }
    ExecutionAuthorization authorization() { if (authorization == null || closed) throw ThreeLayerStorePort.refused(); return authorization; }
    OperatorMapBindings maps() { if (maps == null || closed) throw ThreeLayerStorePort.refused(); return maps; }
    SrsStoreViews sourceViews() {
        return new SrsStoreViews() {
            @Override public SrsStores srs(CacheBusinessScope expected, Runnable active) {
                bound.requireBusiness(); active.run();
                if (!expected.equals(bound.preparation().businessScope())) throw ThreeLayerStorePort.refused();
                Runnable original = () -> { bound.requireBusiness(); active.run(); };
                return new SrsStores(sources.chains().guardedBy(original), sources.models().guardedBy(original), sources.log(), sources.consumers().guardedBy(original));
            }
            @Override public DirectChannelStore directChannels(CacheBusinessScope expected, Runnable active) {
                if (!expected.equals(bound.preparation().businessScope())) throw ThreeLayerStorePort.refused();
                return bound.cache().directChannels(expected, () -> { bound.requireBusiness(); active.run(); });
            }
        };
    }
    @Override public synchronized void close() {
        if (closed) return; closed = true;
        Throwable failure = null;
        try { if (maps != null) maps.close(); } catch (RuntimeException | Error cleanup) { failure = cleanup; }
        try { if (authorization != null) authorization.close(); } catch (RuntimeException | Error cleanup) {
            if (failure == null) failure = cleanup; else if (failure != cleanup) failure.addSuppressed(cleanup);
        }
        if (failure instanceof Error error) throw error;
        if (failure instanceof RuntimeException error) throw error;
    }
}
