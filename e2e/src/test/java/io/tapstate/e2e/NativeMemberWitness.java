package io.tapstate.e2e;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.RoutingMode;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.tapstate.core.lifecycle.ExecutionPlan;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/** An owned client that reads native facts without creating a member or a data structure. */
final class NativeMemberWitness implements AutoCloseable {
    private final HazelcastInstance client;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExecutorService observations = Executors.newSingleThreadExecutor(task -> {
        Thread reader = new Thread(task, "native-fact-read");
        reader.setDaemon(true);
        return reader;
    });

    private NativeMemberWitness(HazelcastInstance client) {
        this.client = client;
    }

    /** Connects to this owned boot's real listener; reported member addresses still name its links. */
    static NativeMemberWitness connect(PartitionableCluster cluster, String liveNode) {
        return connect(cluster.clusterId(), cluster.processCarrying(liveNode));
    }

    /** Reads another existing harness's owned process through the same physical client binding. */
    static NativeMemberWitness connect(String clusterId, RealProcessServer server) {
        if (!server.isAlive()) {
            throw new AssertionError("the native membership witness requires a live owned boot");
        }
        return new NativeMemberWitness(HazelcastClient.newHazelcastClient(config(clusterId, server)));
    }

    /** A case-scoped client bound; the member and product settings stay unchanged. */
    static NativeMemberWitness connect(PartitionableCluster cluster, String liveNode, Duration connectionBound) {
        RealProcessServer server = cluster.processCarrying(liveNode);
        if (!server.isAlive()) {
            throw new AssertionError("the native fact reader requires a live owned boot");
        }
        long connectMillis = connectionBound.toMillis();
        if (connectMillis <= 0) {
            throw new IllegalArgumentException("the native client connection bound must be positive in milliseconds");
        }
        ClientConfig configured = config(cluster.clusterId(), server);
        configured.getNetworkConfig().setConnectionTimeout(Math.toIntExact(connectMillis));
        configured.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(connectMillis);
        return new NativeMemberWitness(HazelcastClient.newHazelcastClient(configured));
    }

    /** Reads actual submitted jobs, including retained completed normal jobs. */
    Set<String> submittedJobs(Duration bound) {
        requireRunning();
        long waitMillis = bound.toMillis();
        if (waitMillis <= 0) {
            throw new IllegalArgumentException("the native inventory read bound must be positive in milliseconds");
        }
        var pending = observations.submit(() -> {
            requireRunning();
            Set<String> ids = new TreeSet<>();
            client.getJet().getJobs().forEach(job -> ids.add(Long.toUnsignedString(job.getId())));
            requireRunning();
            return Set.copyOf(ids);
        });
        try {
            return pending.get(waitMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException unknown) {
            pending.cancel(true);
            throw failedObservation("the actual native submitted-job inventory did not answer within its bound", unknown);
        } catch (ExecutionException unknown) {
            throw failedObservation("the actual native submitted-job inventory could not be read", unknown.getCause());
        } catch (InterruptedException interrupted) {
            pending.cancel(true);
            Thread.currentThread().interrupt();
            throw failedObservation("interrupted while reading native submitted jobs", interrupted);
        }
    }

    private AssertionError failedObservation(String detail, Throwable cause) {
        AssertionError unknown = new AssertionError(detail, cause);
        try {
            close();
        } catch (RuntimeException | Error cleanup) {
            unknown.addSuppressed(cleanup);
        }
        return unknown;
    }

    /** Pure configuration inspection, separately compilable without opening a socket. */
    static ClientConfig config(String clusterId, RealProcessServer server) {
        String[] arguments = ProcessHandle.of(server.pid()).orElseThrow()
                .info().arguments().orElseThrow(() -> new AssertionError("the owned boot's arguments are unavailable"));
        if (!clusterId.equals(argument(arguments, "tapstate.cluster.id"))) {
            throw new AssertionError("the owned boot belongs to another cluster");
        }
        String host = argument(arguments, "tapstate.hz.bind-address");
        String port = argument(arguments, "tapstate.hz.member-port");
        String advertised = optionalArgument(arguments, "tapstate.hz.advertised-member-address");
        List<String> seeds = List.of(argument(arguments, "tapstate.hz.discovery.tcp-ip.seeds").split(","));
        if (advertised != null && !seeds.contains(advertised)) {
            throw new AssertionError("the owned boot's advertised link is absent from its actual seed set");
        }
        ClientConfig config = new ClientConfig().setClusterName(clusterId).setClassLoader(ExecutionPlan.class.getClassLoader());
        // Direct unisocket access keeps this observer out of the links' member-dialler accounting.
        config.getNetworkConfig().setAddresses(List.of(host + ":" + port))
                .setConnectionTimeout(Math.toIntExact(Duration.ofSeconds(10).toMillis()));
        config.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);
        config.getNetworkConfig().getAutoDetectionConfig().setEnabled(false);
        config.getConnectionStrategyConfig().setAsyncStart(false)
                .setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.OFF)
                .getConnectionRetryConfig().setClusterConnectTimeoutMillis(Duration.ofSeconds(10).toMillis());
        return config;
    }

    /** All actual members, including any lite member; no eligibility or placement filtering. */
    List<MemberFacts> members() {
        requireRunning();
        List<MemberFacts> observed = client.getCluster().getMembers().stream().map(member -> new MemberFacts(
                member.getUuid().toString(), member.getAddress().toString(), member.isLiteMember(),
                Map.copyOf(member.getAttributes()))).sorted(Comparator.comparing(MemberFacts::uuid)).toList();
        requireRunning();
        return observed;
    }

    /** Reads a submitted plan only from an already-present native map; never obtains a missing map. */
    ExecutionPlan publishedPlan(String pipeline) {
        requireRunning();
        var object = client.getDistributedObjects().stream()
                .filter(candidate -> candidate instanceof IMap<?, ?> && candidate.getName().equals("tapstate.execution-plans"))
                .findFirst().orElseThrow(() -> new AssertionError("the submitted execution has no published native plan map"));
        Object plan = ((IMap<?, ?>) object).get(pipeline);
        if (!(plan instanceof ExecutionPlan observed)) {
            throw new AssertionError("the submitted execution has no actual compiled plan");
        }
        requireRunning();
        return observed;
    }

    private void requireRunning() {
        if (closed.get() || !client.getLifecycleService().isRunning()) {
            throw new AssertionError("the native fact reader is not active");
        }
    }

    record MemberFacts(String uuid, String address, boolean lite, Map<String, String> attributes) {
        String nodeId() { return attributes.get("tapstate.node-id"); }
        String bootId() { return attributes.get("tapstate.boot-id"); }
        String controlUrl() { return attributes.get("tapstate.control-url"); }
        String profileGeneration() { return attributes.get("tapstate.profile-generation"); }
        String profileHash() { return attributes.get("tapstate.profile-hash"); }
    }

    private static String optionalArgument(String[] arguments, String key) {
        String prefix = "--" + key + "=";
        List<String> matches = Arrays.stream(arguments).filter(value -> value.startsWith(prefix))
                .map(value -> value.substring(prefix.length())).toList();
        if (matches.isEmpty()) { return null; }
        if (matches.size() != 1 || matches.getFirst().isBlank()) {
            throw new AssertionError("the owned boot must supply at most one nonblank value for " + key);
        }
        return matches.getFirst();
    }

    private static String argument(String[] arguments, String key) {
        String prefix = "--" + key + "=";
        List<String> matches = Arrays.stream(arguments).filter(value -> value.startsWith(prefix))
                .map(value -> value.substring(prefix.length())).toList();
        if (matches.size() != 1 || matches.getFirst().isBlank()) {
            throw new AssertionError("the owned boot must supply exactly one value for " + key);
        }
        return matches.getFirst();
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            client.getLifecycleService().shutdown();
        } finally {
            observations.shutdownNow();
            try {
                if (!observations.awaitTermination(2, TimeUnit.SECONDS)) {
                    throw new AssertionError("the owned native inventory reader did not stop after its client closed");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while closing the owned native inventory reader", interrupted);
            }
        }
    }
}
