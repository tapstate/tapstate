package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.NestDeadLetterStore;
import io.tapstate.spi.store.SrsLogStore;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.StorePort;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the store into the assembly root. Under {@code --role=all} the server connects to the store
 * at startup and fails fast — as a coded diagnostic — if it cannot (see {@link CodedFailureAnalyzer}),
 * then exposes the driver-free {@link StorePort} the service rings consume. The connection is closed
 * when the context shuts down.
 *
 * <p>Gated on {@code tapstate.store.mongo.enabled} (on by default): a run that has no store — a
 * substrate check, say — turns it off and starts without one.
 */
@Configuration
@EnableConfigurationProperties(MongoProperties.class)
class StoreConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "tapstate.store.mongo", name = "enabled", matchIfMissing = true)
    MongoConnection storeConnection(MongoProperties properties) {
        MongoConnection connection = new MongoConnection(new MongoConnectionSettings(
                properties.getUri(), properties.getTlsCaFile(),
                properties.getServerSelectionTimeout()));
        // Fail fast at startup: a coded diagnostic surfaces through CodedFailureAnalyzer if the
        // store is unreachable or is not a replica-set, rather than a bare driver stack trace.
        connection.verify();
        return connection;
    }

    /**
     * The persistence port the service rings depend on (the store bridge, R7-wired here at the
     * assembly root). It aggregates the artifact, state and catalog sub-stores over the verified
     * connection; the app sees only the driver-free {@link StorePort}, never a driver type.
     */
    @Bean
    @ConditionalOnProperty(prefix = "tapstate.store.mongo", name = "enabled", matchIfMissing = true)
    StorePort storePort(MongoConnection storeConnection) {
        return new MongoStorePort(storeConnection);
    }

    /**
     * The SRS meta store the assembly root binds onto the embedded Hazelcast member, so the capture
     * runtime's read-cursor publisher can resolve it member-side. It is the store's own meta facet, gated
     * with the store: a run without a store exposes none and the publisher no-ops.
     */
    @Bean
    @ConditionalOnProperty(prefix = "tapstate.store.mongo", name = "enabled", matchIfMissing = true)
    SrsMetaStore srsMetaStore(StorePort storePort) {
        return storePort.meta();
    }

    /**
     * The durable change log the assembly root puts behind every change ring, so a change that entered a
     * ring outlives the process that read it. Gated with the store, and the gate is what it means: a run
     * without one keeps changes in the member alone, and a restart has nothing to replay from -- which is
     * the state this log exists to end.
     */
    @Bean
    @ConditionalOnProperty(prefix = "tapstate.store.mongo", name = "enabled", matchIfMissing = true)
    SrsLogStore srsLogStore(StorePort storePort) {
        return storePort.srsLog();
    }

    /**
     * The cold layer the embedded member puts behind every nest state map: written through as a key is
     * handled, read back per key when a key that is no longer in memory is asked for. Gated with the
     * store, and the gate is what it means: a run without one keeps nest state in the member alone, so a
     * restart rebuilds it by re-reading the sources rather than resuming.
     */
    @Bean
    @ConditionalOnProperty(prefix = "tapstate.store.mongo", name = "enabled", matchIfMissing = true)
    KeyedStateStore nestStateStore(StorePort storePort) {
        return storePort.keyedState();
    }

    /**
     * Where a nest puts the changes it can never place in a document. Gated with the store beside it,
     * because it is the same gate: a run with no store has nowhere durable to put either, and a nest vertex
     * on such a member refuses to start rather than discarding rows with nothing to show for it.
     */
    @Bean
    @ConditionalOnProperty(prefix = "tapstate.store.mongo", name = "enabled", matchIfMissing = true)
    NestDeadLetterStore nestDeadLetterStore(StorePort storePort) {
        return storePort.nestDeadLetters();
    }
}
