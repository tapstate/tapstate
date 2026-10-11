package io.tapstate.e2e;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.RoutingMode;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.runtime.engine.NativeExecutionStartup;
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

    /** Bounded passive reads of known submitted jobs and an already-present initialization map. */
    NativeProjectionReading projectionReading(List<JobRequest> requested, Duration bound) {
        requireRunning();
        long waitMillis = bound.toMillis();
        if (waitMillis <= 0) { throw new IllegalArgumentException("the native projection read bound must be positive"); }
        List<JobRequest> exact = List.copyOf(requested);
        var pending = observations.submit(() -> {
            requireRunning();
            var startup = client.getDistributedObjects().stream()
                    .filter(candidate -> candidate instanceof IMap<?, ?> && candidate.getName().equals(NativeExecutionStartup.MAP_NAME))
                    .findFirst().orElse(null);
            List<NativeTuple> members = members().stream().map(member -> new NativeTuple(member.nodeId(), member.bootId(),
                    member.uuid(), member.lite(), member.profileGeneration(), member.profileHash())).toList();
            List<JobReading> jobs = new java.util.ArrayList<>();
            for (JobRequest request : exact) {
                var job = client.getJet().getJob(Long.parseUnsignedLong(request.jobId()));
                Object stored = startup == null ? null : ((IMap<?, ?>) startup).get(request.pipeline());
                var evidence = stored instanceof NativeExecutionStartup.Evidence value ? value : null;
                if (job == null) {
                    jobs.add(new JobReading(request, null, null, null, null, null, null, false, List.of(), evidence));
                    continue;
                }
                Long claim = argument(job, NativeExecutionStartup.CLAIM_ARGUMENT);
                Long execution = argument(job, NativeExecutionStartup.EXECUTION_ARGUMENT);
                Long profile = argument(job, NativeExecutionStartup.PROFILE_ARGUMENT);
                List<MetricReading> readings = new java.util.ArrayList<>();
                var metrics = job.getMetrics();
                for (String metric : new TreeSet<>(metrics.metrics())) {
                    for (var measurement : metrics.get(metric)) {
                        Map<String, String> tags = new java.util.LinkedHashMap<>();
                        for (String tag : List.of(com.hazelcast.jet.core.metrics.MetricTags.VERTEX,
                                com.hazelcast.jet.core.metrics.MetricTags.PROCESSOR,
                                com.hazelcast.jet.core.metrics.MetricTags.MEMBER,
                                com.hazelcast.jet.core.metrics.MetricTags.EXECUTION,
                                com.hazelcast.jet.core.metrics.MetricTags.PROCESSOR_TYPE)) {
                            String value = measurement.tag(tag);
                            if (value != null) { tags.put(tag, value); }
                        }
                        readings.add(new MetricReading(metric, measurement.value(), measurement.timestamp(), Map.copyOf(tags)));
                    }
                }
                jobs.add(new JobReading(request, Long.toUnsignedString(job.getId()), job.getName(), job.getStatus().name(),
                        claim, execution, profile, request.jobId().equals(Long.toUnsignedString(job.getId()))
                                && request.pipeline().equals(job.getName())
                                && java.util.Objects.equals(claim, request.claimGeneration())
                                && java.util.Objects.equals(execution, request.executionGeneration())
                                && java.util.Objects.equals(profile, request.profileGeneration()), List.copyOf(readings), evidence));
            }
            requireRunning();
            return new NativeProjectionReading(java.time.Instant.now(), members, startup != null, List.copyOf(jobs));
        });
        try { return pending.get(waitMillis, TimeUnit.MILLISECONDS); }
        catch (TimeoutException unavailable) {
            pending.cancel(true);
            throw new AssertionError("the passive native projection read did not answer within its bound", unavailable);
        } catch (ExecutionException unavailable) {
            throw new AssertionError("the passive native projection facts could not be read", unavailable.getCause());
        } catch (InterruptedException interrupted) {
            pending.cancel(true);
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while reading passive native projection facts", interrupted);
        }
    }

    private static Long argument(com.hazelcast.jet.Job job, String key) {
        Object value = job.getConfig().getArgument(key);
        return value instanceof Number number ? number.longValue() : null;
    }

    record JobRequest(String pipeline, String jobId, long claimGeneration, long executionGeneration, long profileGeneration) { }
    record NativeTuple(String nodeId, String bootId, String memberUuid, boolean lite, String profileGeneration, String profileHash) { }
    record MetricReading(String name, long value, long timestamp, Map<String, String> tags) { }
    record JobReading(JobRequest expected, String actualJobId, String actualName, String status,
            Long claimGeneration, Long executionGeneration, Long profileGeneration, boolean scopeMatches,
            List<MetricReading> metrics, NativeExecutionStartup.Evidence initialization) { }
    record NativeProjectionReading(java.time.Instant readAt, List<NativeTuple> members,
            boolean startupMapPresent, List<JobReading> jobs) { }

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
