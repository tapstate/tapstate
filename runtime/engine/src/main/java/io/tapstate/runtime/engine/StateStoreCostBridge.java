package io.tapstate.runtime.engine;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.metrics.Metric;
import com.hazelcast.jet.core.metrics.Metrics;
import io.tapstate.runtime.engine.StateStoreCostMetricNames.Kind;
import io.tapstate.runtime.engine.join.JoinMaps;
import io.tapstate.runtime.engine.join.JoinStateMapStoreFactory;
import io.tapstate.runtime.engine.nest.NestStateMapStoreFactory;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Installs one member-local job metric handle per cold-store cost without a resident polling processor. */
final class StateStoreCostBridge extends AbstractProcessor {

    private static final Logger LOG = Logger.getLogger(StateStoreCostBridge.class.getName());

    private final List<String> namespaces;
    private transient StateStoreCostStats stats;
    private transient Map<String, StateStoreCostStats.Baseline> baselines;
    private transient int clusterSize;

    private StateStoreCostBridge(List<String> namespaces) {
        this.namespaces = namespaces;
    }

    static ProcessorMetaSupplier metaSupplier(Set<String> namespaces) {
        List<String> names = new ArrayList<>(Objects.requireNonNull(namespaces, "namespaces"));
        names.sort(String::compareTo);
        return ProcessorMetaSupplier.of(ProcessorSupplier.of(() -> new StateStoreCostBridge(names)));
    }

    @Override
    protected void init(Context context) {
        HazelcastInstance member = context.hazelcastInstance();
        clusterSize = member.getCluster().getMembers().size();
        stats = StateStoreCostStats.of(member);
        baselines = new LinkedHashMap<>();
        for (String namespace : namespaces) {
            try {
                if (backedOn(member, namespace)) {
                    baselines.put(namespace, stats.baseline(namespace));
                }
            } catch (RuntimeException unavailable) {
                LOG.log(Level.WARNING, "Could not prepare state-store cost measurement", unavailable);
            }
        }
    }

    @Override
    public boolean complete() {
        for (Map.Entry<String, StateStoreCostStats.Baseline> entry : baselines.entrySet()) {
            try {
                Map<Kind, Metric> handles = new EnumMap<>(Kind.class);
                for (Kind kind : Kind.values()) {
                    handles.put(kind, Metrics.threadSafeMetric(
                            StateStoreCostMetricNames.nameOf(kind, entry.getKey())));
                }
                if (stats.attach(entry.getValue(), handles)) {
                    handles.get(Kind.CLUSTER_SIZE).set(clusterSize);
                }
            } catch (RuntimeException unavailable) {
                // Monitoring may become absent; it cannot stop the job that owns the state.
                LOG.log(Level.WARNING, "Could not attach state-store cost metrics", unavailable);
            }
        }
        return true;
    }

    private static boolean backedOn(HazelcastInstance member, String namespace) {
        boolean bound = namespace.startsWith(JoinMaps.NAMESPACE_PREFIX)
                ? member.getUserContext().containsKey(JoinStateMapStoreFactory.USER_CONTEXT_KEY)
                : namespace.startsWith(NestStateMapStoreFactory.NAMESPACE_PREFIX)
                        && member.getUserContext().containsKey(NestStateMapStoreFactory.USER_CONTEXT_KEY);
        if (!bound) {
            return false;
        }
        MapConfig map = member.getConfig().findMapConfig(namespace);
        return map != null && map.getMapStoreConfig() != null && map.getMapStoreConfig().isEnabled();
    }
}
