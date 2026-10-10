package io.tapstate.app;

import com.hazelcast.jet.config.JetConfig;
import io.tapstate.adapters.mongostore.migration.MigrationRunner;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ClusterCapacityLimits;
import io.tapstate.core.lifecycle.ParallelismBudget;
import io.tapstate.spi.store.ExecutionProfile;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Builds the profile lazily, only when a network-discoverable member is admitted. */
final class ExecutionProfileFactory {
    private final HazelcastProperties hazelcast;
    private final ClusterProperties cluster;
    private final ExecutionProfileProperties properties;
    private final ParallelismBudget budget;
    private final ClusterCapacityLimits capacity;
    private final LongSupplier maxHeap;
    private final Supplier<InputStream> version;
    private final Supplier<InputStream> abi;

    ExecutionProfileFactory(HazelcastProperties hazelcast, ClusterProperties cluster,
            ExecutionProfileProperties properties, ParallelismBudget budget, ClusterCapacityLimits capacity) {
        this(hazelcast, cluster, properties, budget, capacity, () -> Runtime.getRuntime().maxMemory(),
                () -> ExecutionProfileFactory.class.getResourceAsStream("/tapstate-version.properties"),
                () -> ExecutionProfileFactory.class.getResourceAsStream("/tapstate-execution-abi.properties"));
    }

    ExecutionProfileFactory(HazelcastProperties hazelcast, ClusterProperties cluster,
            ExecutionProfileProperties properties, ParallelismBudget budget, ClusterCapacityLimits capacity,
            LongSupplier maxHeap, Supplier<InputStream> version, Supplier<InputStream> abi) {
        this.hazelcast = hazelcast;
        this.cluster = cluster;
        this.properties = properties;
        this.budget = budget;
        this.capacity = capacity;
        this.maxHeap = maxHeap;
        this.version = version;
        this.abi = abi;
    }

    ExecutionProfile create() {
        long heap = maxHeap.getAsLong();
        if (properties.getHeapTier() == null || properties.getHeapTier().isBlank()
                || properties.getMinimumHeapBytes() < 1 || heap < properties.getMinimumHeapBytes()) {
            throw invalid("the actual JVM heap does not satisfy its configured capacity tier", null);
        }
        Map<String, String> inputs = new TreeMap<>();
        inputs.put("productVersion", required(load(version, "product version"), "version"));
        Properties metadata = load(abi, "runtime and PDK ABI");
        for (String key : new String[] {"runtime-abi", "profile-protocol", "pdk-api", "pdk-entity",
                "pdk-errorcode", "pdk-runner", "pdk-common"}) {
            inputs.put(key, required(metadata, key));
        }
        inputs.put("systemDataVersion", String.valueOf(MigrationRunner.SUPPORTED_VERSION));
        inputs.put("heapTier", properties.getHeapTier());
        inputs.put("minimumHeapBytes", String.valueOf(properties.getMinimumHeapBytes()));
        inputs.put("maxHeapBytes", String.valueOf(heap));
        Integer configuredThreads = hazelcast.getJet().getCooperativeThreadCount();
        int threads = configuredThreads == null ? new JetConfig().getCooperativeThreadCount() : configuredThreads;
        if (threads < 1) {
            throw invalid("cooperative thread count must be positive", null);
        }
        inputs.put("cooperativeThreads", String.valueOf(threads));
        inputs.put("haProfile", cluster.getProfile().name());
        inputs.put("bootstrapMinMembers", String.valueOf(cluster.getBootstrapMinMembers()));
        inputs.put("maxLocalParallelism", String.valueOf(budget.maxLocalParallelism()));
        inputs.put("maxConnectorInstancesPerMember", String.valueOf(budget.maxConnectorInstancesPerMember()));
        inputs.put("maxBufferedRecordsPerMember", String.valueOf(budget.maxBufferedRecordsPerMember()));
        inputs.put("maxBlockingProcessorsPerMember", String.valueOf(budget.maxBlockingProcessorsPerMember()));
        inputs.put("capacityProcessors", String.valueOf(capacity.processors()));
        inputs.put("capacityBlockingProcessors", String.valueOf(capacity.blockingProcessors()));
        inputs.put("capacityWriters", String.valueOf(capacity.writers()));
        inputs.put("capacityConnectorInstances", String.valueOf(capacity.connectorInstances()));
        inputs.put("capacityBufferedRecords", String.valueOf(capacity.bufferedRecords()));
        inputs.put("capacityEdgeQueueRecords", String.valueOf(capacity.edgeQueueRecords()));
        return new ExecutionProfile(1, inputs);
    }

    private static Properties load(Supplier<InputStream> source, String name) {
        try (InputStream stream = source.get()) {
            if (stream == null) {
                throw invalid(name + " metadata is absent", null);
            }
            Properties properties = new Properties();
            properties.load(stream);
            return properties;
        } catch (IOException unreadable) {
            throw invalid(name + " metadata cannot be read", unreadable);
        }
    }

    private static String required(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.isBlank() || value.contains("${")) {
            throw invalid("immutable metadata is missing or unfiltered: " + key, null);
        }
        return value;
    }

    private static TapstateException invalid(String detail, Throwable cause) {
        return new TapstateException(BootError.EXECUTION_PROFILE_INVALID, Map.of("detail", detail), cause);
    }
}
