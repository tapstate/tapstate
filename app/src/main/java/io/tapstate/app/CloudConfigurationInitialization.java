package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationBootstrap;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.StorageSetupStateStore;
import io.tapstate.spi.store.IoError;
import java.util.Objects;

/** One process owns its original platform creation permission; a response or database failure cannot mint another. */
final class CloudConfigurationInitialization implements AutoCloseable {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CloudConfigurationInitialization.class);
    private static final java.util.regex.Pattern DRIVER_DETAIL = java.util.regex.Pattern.compile(
            "Mongo[A-Za-z]{0,64}Exception(?: code=[1-9][0-9]{0,9})?");
    private final MongoConnectionSettings connection;
    private final CloudRuntimeSettings cloud;
    private final CloudSdkBridge bridge;
    private final String bootId;
    private final String hash;
    private CloudSdkBridge.ConfigurationInitialization authorization;
    private boolean creationInvoked;
    private boolean closed;

    CloudConfigurationInitialization(MongoConnectionSettings connection, CloudRuntimeSettings cloud, CloudSdkBridge bridge, BootId boot) {
        this.connection = Objects.requireNonNull(connection); this.cloud = Objects.requireNonNull(cloud);
        this.bridge = Objects.requireNonNull(bridge); bootId = boot.value(); hash = CanonicalHash.ofText(cloud.atlasUri());
    }

    synchronized MongoConfigurationDomain open() {
        String stage = "open-existing";
        try {
            active();
            MongoConfigurationDomain retained;
            try { retained = MongoConfigurationDomain.open(connection, cloud.clusterId()); }
            catch (TapstateException unavailable) { retained = null; }
            if (retained != null) { stage = "confirm"; return confirm(retained); }
            if (authorization == null) { stage = "claim"; authorization = bridge.claimConfiguration(bootId, hash); }
            active();
            MongoConfigurationDomain created;
            if (authorization.creationGranted() && !creationInvoked) {
                stage = "prepare";
                creationInvoked = true;
                created = MongoConfigurationBootstrap.prepare(connection, cloud.clusterId(), installationId(), authorization.operationId(), 1);
            } else {
                stage = "resume";
                created = MongoConfigurationBootstrap.resume(connection, cloud.clusterId(), installationId(), authorization.operationId(), 1);
            }
            stage = "confirm";
            return confirm(created);
        } catch (TapstateException failure) {
            LOG.warn("Cloud configuration initialization failed [stage={}, code={}, detail={}, creationGranted={}, creationInvoked={}]",
                    stage, failure.code().code(), driverDetail(failure),
                    authorization == null ? "unknown" : Boolean.toString(authorization.creationGranted()), creationInvoked);
            throw failure;
        }
    }

    private static String driverDetail(TapstateException failure) {
        if (failure.code() == IoError.STORE_UNAVAILABLE && failure.args().get("detail") instanceof String detail
                && detail.length() <= 96 && DRIVER_DETAIL.matcher(detail).matches()) return detail;
        return "none";
    }

    private MongoConfigurationDomain confirm(MongoConfigurationDomain original) {
        try {
            active(); var state = original.setupStates().load();
            if (state.scope().installationId().equals(installationId()) && state.phase() == StorageSetupStateStore.Phase.CONFIG_PREPARED) {
                bridge.completeConfiguration(state.scope().operationId(), hash);
            }
            active(); return original;
        } catch (RuntimeException | Error failed) {
            try { original.close(); } catch (RuntimeException | Error cleanup) { if (cleanup != failed) failed.addSuppressed(cleanup); }
            throw failed;
        }
    }
    private String installationId() { return "cloud-" + cloud.clusterId(); }
    private void active() { if (closed) throw StorageRuntime.unavailable(); }
    @Override public synchronized void close() { closed = true; }
}
