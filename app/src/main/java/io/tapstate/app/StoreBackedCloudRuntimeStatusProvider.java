package io.tapstate.app;

import io.tapstate.control.core.CloudRuntimeStatus;
import io.tapstate.control.core.CloudRuntimeStatusProvider;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.StorePort;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/** Projects the existing lifecycle observations into the credential-free C2 status shape. */
final class StoreBackedCloudRuntimeStatusProvider implements CloudRuntimeStatusProvider {

    private static final Set<PipelineState> ACTIVE = Set.of(PipelineState.RUNNING, PipelineState.PAUSED);

    private final StorePort store;
    private final String runtimeVersion;
    private final Clock clock;
    private final Instant startedAt;

    StoreBackedCloudRuntimeStatusProvider(
            StorePort store, String runtimeVersion, Clock clock, Instant startedAt) {
        this.store = Objects.requireNonNull(store, "store");
        if (runtimeVersion == null || runtimeVersion.isBlank()) {
            throw new IllegalArgumentException("runtimeVersion must be non-blank");
        }
        this.runtimeVersion = runtimeVersion;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.startedAt = Objects.requireNonNull(startedAt, "startedAt");
    }

    @Override
    public CloudRuntimeStatus snapshot() {
        int active = 0;
        Instant lastErrorAt = null;
        // A defensive set keeps a malformed adapter result from inflating the public count. The desired
        // store contract already promises unique ids; this is a projection boundary, not a repair write.
        for (String pipelineId : new LinkedHashSet<>(store.desired().pipelineIds())) {
            Observation observation = store.observations().read(pipelineId).orElse(null);
            if (observation == null) {
                continue;
            }
            if (ACTIVE.contains(observation.state())) {
                active++;
            }
            if (observation.failure() != null && observation.observedAt() != null
                    && (lastErrorAt == null || observation.observedAt().isAfter(lastErrorAt))) {
                lastErrorAt = observation.observedAt();
            }
        }
        long uptime = Math.max(0L, Duration.between(startedAt, clock.instant()).toMillis());
        return new CloudRuntimeStatus(runtimeVersion, uptime, active, lastErrorAt);
    }
}
