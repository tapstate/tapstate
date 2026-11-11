package io.tapstate.runtime.engine.aggregate;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.runtime.engine.OperatorMapBindings;
import io.tapstate.runtime.engine.OperatorMapCoordinates.AggregateStore;
import io.tapstate.runtime.engine.OperatorRunManifest;
import io.tapstate.spi.store.OrderedStateStore;
import java.io.Serializable;
import java.util.Objects;

/**
 * Where an aggregate vertex gets its two stores, on the member that runs it. Carried in the job, so it
 * holds coordinates and never a store.
 */
@FunctionalInterface
public interface AggregateStores extends Serializable {

    /** The store of one side of {@code step}, opened under the execution this job was built for. */
    OrderedStateStore open(HazelcastInstance member, String step, AggregateStore kind);

    /**
     * The stores this execution was bound to: each resolves through the member's installed bindings, so
     * a vertex of an execution that has been replaced is refused where it asks, and every later
     * operation of a store it already holds is refused as well.
     */
    static AggregateStores bound(OperatorRunManifest manifest) {
        Objects.requireNonNull(manifest, "manifest");
        return (member, step, kind) -> OperatorMapBindings.resolve(member, true)
                .aggregate(manifest.forStep(manifest.pipelineId(), step).aggregate(kind).mapName());
    }
}
