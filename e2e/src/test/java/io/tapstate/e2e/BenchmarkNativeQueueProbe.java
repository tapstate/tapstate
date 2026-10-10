package io.tapstate.e2e;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.core.metrics.MetricTags;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.HashSet;
import java.util.Set;

/** Read-only native metrics from the owned single-member process, never a product pressure endpoint. */
final class BenchmarkNativeQueueProbe implements AutoCloseable {
    private final HazelcastInstance client;
    private final ControlPlane.BenchmarkMember owner;
    private final long maxMetricAgeMillis;
    private final int collectionFrequencySeconds;
    private final ControlPlane control;

    BenchmarkNativeQueueProbe(ControlPlane control, String expectedControlUrl, String nativeClusterName,
                              int collectionFrequencySeconds) {
        if (collectionFrequencySeconds < 1) { throw new AssertionError("actual metric collection cadence is required"); }
        this.collectionFrequencySeconds = collectionFrequencySeconds;
        this.control = control;
        maxMetricAgeMillis = Math.multiplyExact(collectionFrequencySeconds, 2_000L);
        try {
            var sdk = java.nio.file.Path.of(HazelcastClient.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            String sha = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256")
                    .digest(java.nio.file.Files.readAllBytes(sdk)));
            if (!sha.equals("241ec66cea8491787f68c3336f069ac463877af50bdb7676cd9d538f1940dc33")) {
                throw new AssertionError("native queue client SDK bytes differ from the qualified artifact");
            }
        } catch (java.io.IOException | java.net.URISyntaxException | java.security.NoSuchAlgorithmException failure) {
            throw new AssertionError("native queue client SDK provenance could not be verified", failure);
        }
        owner = control.benchmarkMember();
        if (owner.controlUrl() != null && !Objects.equals(owner.controlUrl(), expectedControlUrl)
                || !(owner.hzAddress().startsWith("127.0.0.1:") || owner.hzAddress().startsWith("[127.0.0.1]:"))) {
            throw new AssertionError("native benchmark probe has no exact owned loopback member");
        }
        if (nativeClusterName == null || nativeClusterName.isBlank()) { throw new AssertionError("native cluster configuration is required"); }
        var config = new ClientConfig().setClusterName(nativeClusterName);
        String address = owner.hzAddress().replace("[127.0.0.1]:", "127.0.0.1:");
        if (!address.matches("127\\.0\\.0\\.1:[0-9]{1,5}")) { throw new AssertionError("invalid owned native loopback address"); }
        config.getNetworkConfig().setSmartRouting(false).setConnectionTimeout(5_000).addAddress(address);
        config.getConnectionStrategyConfig().setAsyncStart(false)
                .setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.OFF);
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(5_000);
        config.setProperty("hazelcast.client.statistics.enabled", "false");
        config.setProperty("hazelcast.logging.type", "slf4j");
        client = HazelcastClient.newHazelcastClient(config);
        try { requireOwner(); }
        catch (RuntimeException | Error failure) {
            try { client.shutdown(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private void requireOwner() {
        var members = client.getCluster().getMembers();
        if (members.size() != 1 || !members.iterator().next().getUuid().toString().equals(owner.uuid())) {
            throw new AssertionError("native benchmark client reached another member identity");
        }
        String actual = members.iterator().next().getAddress().toString().replace("[127.0.0.1]:", "127.0.0.1:");
        if (!actual.equals(owner.hzAddress().replace("[127.0.0.1]:", "127.0.0.1:"))) {
            throw new AssertionError("native benchmark member address differs from its declared endpoint");
        }
    }

    /** One latest settled-row publication, with its timestamp treated only as an opaque cycle label. */
    BenchmarkNativeCounterBaseline.Snapshot delivery(String pipeline) {
        requireOwner();
        var job = client.getJet().getJob(pipeline);
        if (job == null || !pipeline.equals(job.getName()) || job.getStatus() != JobStatus.RUNNING) {
            throw new AssertionError("native delivery witness requires the named running job");
        }
        long jobId = job.getId();
        String jobIdString = job.getIdString();
        long started = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime();
        var metrics = job.getMetrics();
        Map<?, ?> topology = control.benchmarkPipelineTopology(pipeline);
        var current = client.getJet().getJob(pipeline);
        requireOwner();
        if (current == null || current.getId() != jobId || !pipeline.equals(current.getName())
                || current.getStatus() != JobStatus.RUNNING || job.getStatus() != JobStatus.RUNNING) {
            throw new AssertionError("native delivery job changed during its snapshot");
        }
        return deliverySnapshot(pipeline, jobIdString, owner.uuid(), metrics, topology, started, io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime());
    }

    static BenchmarkNativeCounterBaseline.Snapshot deliverySnapshot(String pipeline, String jobId,
            String memberUuid, JobMetrics metrics, Map<?, ?> topology, long started, long completed) {
        boundedDeliveryField(pipeline); boundedDeliveryField(jobId); boundedDeliveryField(memberUuid);
        if (metrics == null) { throw new AssertionError("native delivery metrics are missing"); }
        try {
            if (Math.subtractExact(completed, started) < 0) { throw new AssertionError("native delivery request clock moved backward"); }
        } catch (ArithmeticException overflow) {
            throw new AssertionError("native delivery request clock overflow", overflow);
        }
        TopologyRoster topologyRoster = topologyRoster(topology, memberUuid);
        if (topologyRoster.executions().size() != 1) {
            throw new AssertionError("native delivery topology has no unique execution");
        }
        String execution = topologyRoster.executions().iterator().next();
        boundedDeliveryField(execution);
        Set<String> expected = new HashSet<>();
        for (String identity : topologyRoster.identities()) {
            if (identity.startsWith(memberUuid + "/serve.") || identity.startsWith(memberUuid + "/view.")) {
                boundedDeliveryField(identity);
                expected.add(identity);
            }
        }
        if (expected.isEmpty() || expected.size() > 128) {
            throw new AssertionError("native delivery sink roster is missing or exceeds its bound");
        }
        Map<String, Long> counters = new java.util.LinkedHashMap<>();
        Set<String> represented = new HashSet<>();
        Long publication = null;
        for (String name : metrics.metrics()) {
            if (!name.startsWith("recordsOut.")) { continue; }
            boundedDeliveryField(name);
            String suffix = name.substring("recordsOut.".length());
            int separator = suffix.indexOf('.');
            if (separator < 1 || separator == suffix.length() - 1) {
                throw new AssertionError("native delivery metric has no operation and table");
            }
            for (var row : metrics.get(name)) {
                String member = row.tag(MetricTags.MEMBER), vertex = row.tag(MetricTags.VERTEX);
                String processor = row.tag(MetricTags.PROCESSOR), actualExecution = row.tag(MetricTags.EXECUTION);
                boundedDeliveryField(member); boundedDeliveryField(vertex); boundedDeliveryField(processor);
                boundedDeliveryField(actualExecution); boundedDeliveryField(row.tag(MetricTags.JOB));
                if (!processor.matches("0|[1-9][0-9]*")) { throw new AssertionError("native delivery processor is not an exact index"); }
                String identity = member + "/" + vertex + "/" + processor;
                boundedDeliveryField(identity);
                if (!name.equals(row.metric()) || !memberUuid.equals(member) || !jobId.equals(row.tag(MetricTags.JOB))
                        || !execution.equals(actualExecution) || !expected.contains(identity) || row.value() < 0) {
                    throw new AssertionError("native delivery row has another identity or an invalid counter");
                }
                if (publication == null) { publication = row.timestamp(); }
                if (publication != row.timestamp()) { throw new AssertionError("native delivery rows mix publication snapshots"); }
                String key = name + "|" + identity;
                if (counters.size() >= 4096 || counters.putIfAbsent(key, row.value()) != null) {
                    throw new AssertionError("native delivery counter is duplicate or exceeds its bound");
                }
                represented.add(identity);
            }
        }
        if (publication == null || !represented.equals(expected)) {
            throw new AssertionError("native delivery counter roster is incomplete");
        }
        return new BenchmarkNativeCounterBaseline.Snapshot(pipeline, jobId, memberUuid, execution, publication,
                started, completed, Set.copyOf(expected), Map.copyOf(counters));
    }

    private static void boundedDeliveryField(String value) {
        if (value == null || value.isBlank() || value.length() > 512 || value.indexOf('|') >= 0
                || value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 512) {
            throw new AssertionError("native delivery identity field is missing, ambiguous or exceeds its bound");
        }
    }

    Map<String, Object> read(String pipeline) {
        requireOwner();
        var job = client.getJet().getJob(pipeline);
        if (job == null) { return Map.of("state", "UNKNOWN", "reason", "NO_JOB"); }
        long started = io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime();
        long utcBefore = System.currentTimeMillis();
        long jobId = job.getId();
        if (!pipeline.equals(job.getName())) { throw new AssertionError("native queue job name differs"); }
        var metrics = job.getMetrics();
        var current = client.getJet().getJob(pipeline);
        requireOwner();
        if (current == null || current.getId() != jobId) { throw new AssertionError("native job changed during queue sample"); }
        var readings = new ArrayList<Map<String, Object>>();
        var identities = new HashSet<String>();
        var executions = new HashSet<String>();
        var timestamps = new HashSet<Long>();
        var capacities = new java.util.HashMap<String, com.hazelcast.jet.core.metrics.Measurement>();
        for (var capacity : metrics.get(MetricNames.QUEUES_CAPACITY)) {
            String member = capacity.tag(MetricTags.MEMBER), vertex = capacity.tag(MetricTags.VERTEX);
            String processor = capacity.tag(MetricTags.PROCESSOR), execution = capacity.tag(MetricTags.EXECUTION);
            if (!owner.uuid().equals(member) || vertex == null || processor == null || execution == null
                    || !job.getIdString().equals(capacity.tag(MetricTags.JOB)) || capacity.value() < 0) {
                throw new AssertionError("native capacity has an incomplete or different metric identity");
            }
            String key = member + "/" + vertex + "/" + processor;
            if (capacities.putIfAbsent(key, capacity) != null) { throw new AssertionError("duplicate native queue capacity identity"); }
        }
        long utcAfter = System.currentTimeMillis();
        for (var metric : metrics.get(MetricNames.QUEUES_SIZE)) {
            if (readings.size() >= 4096) { throw new AssertionError("native queue metric roster exceeded its bound"); }
            String member = metric.tag(MetricTags.MEMBER);
            String vertex = metric.tag(MetricTags.VERTEX);
            String processor = metric.tag(MetricTags.PROCESSOR);
            String execution = metric.tag(MetricTags.EXECUTION);
            if (member == null || vertex == null || processor == null || execution == null || metric.value() < 0) {
                throw new AssertionError("native queue metric has an incomplete typed identity");
            }
            if (!owner.uuid().equals(member) || !job.getIdString().equals(metric.tag(MetricTags.JOB))
                    || !identities.add(member + "/" + vertex + "/" + processor)) {
                throw new AssertionError("native queue metric has another job/member or a duplicate identity");
            }
            executions.add(execution); timestamps.add(metric.timestamp());
            var capacity = capacities.get(member + "/" + vertex + "/" + processor);
            if (capacity == null || capacity.timestamp() != metric.timestamp() || capacity.value() < metric.value()
                    || !execution.equals(capacity.tag(MetricTags.EXECUTION))
                    || !job.getIdString().equals(capacity.tag(MetricTags.JOB))) {
                throw new AssertionError("queue size has no matching capacity snapshot");
            }
            if (metric.timestamp() > utcAfter || utcBefore - metric.timestamp() > maxMetricAgeMillis) {
                throw new AssertionError("native queue metric is stale or from a future collection");
            }
            readings.add(Map.of("member", member, "vertex", vertex, "processor", processor,
                    "execution", execution, "sampledAtEpochMillis", metric.timestamp(), "queueItems", metric.value(),
                    "queueCapacity", capacity.value()));
        }
        if (executions.size() > 1 || timestamps.size() > 1) { throw new AssertionError("native queue rows mix metric snapshots"); }
        if (!capacities.keySet().equals(identities)) { throw new AssertionError("native queue capacity roster differs"); }
        var result = new java.util.LinkedHashMap<String, Object>();
        result.putAll(Map.of("state", readings.isEmpty() ? "UNKNOWN" : "RECORDED", "pipeline", pipeline,
                "jobId", jobId, "memberUuid", owner.uuid(), "readStartedAtNanos", started,
                "readCompletedAtNanos", io.tapstate.adapters.pdk.PdkBenchmarkClock.nanoTime(), "scope", "NATIVE_JOB_INPUT_QUEUE_METRICS",
                "readings", List.copyOf(readings), "readUtcMillis", List.of(utcBefore, utcAfter)));
        result.put("declaredCollectionFrequencySeconds", collectionFrequencySeconds);
        Map<?, ?> topology = control.benchmarkPipelineTopology(pipeline);
        var declared = topologyRoster(topology, owner.uuid());
        Set<String> expected = declared.identities();
        Set<String> topologyExecutions = declared.executions();
        boolean complete = !expected.isEmpty() && expected.equals(identities) && topologyExecutions.equals(executions);
        result.put("completeRosterQualified", complete);
        result.put("expectedProcessorIdentities", List.copyOf(expected));
        if (!complete) { result.put("state", "UNKNOWN"); }
        var sinkCounts = new ArrayList<Map<String, Object>>();
        for (var reading : metrics.get(MetricNames.RECEIVED_COUNT)) {
            String vertex = reading.tag(MetricTags.VERTEX);
            if (vertex == null || !(vertex.startsWith("serve.") || vertex.startsWith("view."))) { continue; }
            String member = reading.tag(MetricTags.MEMBER), processor = reading.tag(MetricTags.PROCESSOR);
            if (!owner.uuid().equals(member) || processor == null || !job.getIdString().equals(reading.tag(MetricTags.JOB))
                    || !executions.contains(reading.tag(MetricTags.EXECUTION)) || !timestamps.contains(reading.timestamp())
                    || reading.value() < 0) { throw new AssertionError("sink accepted-word metric differs from the queue snapshot"); }
            sinkCounts.add(Map.of("vertex", vertex, "processor", processor,
                    "acceptedNativeWords", reading.value(), "sampledAtEpochMillis", reading.timestamp()));
        }
        var settled = new ArrayList<Map<String, Object>>();
        for (String name : metrics.metrics()) {
            if (!name.startsWith("recordsOut.")) { continue; }
            for (var reading : metrics.get(name)) {
                if (!owner.uuid().equals(reading.tag(MetricTags.MEMBER)) || !job.getIdString().equals(reading.tag(MetricTags.JOB))
                        || !executions.contains(reading.tag(MetricTags.EXECUTION)) || !timestamps.contains(reading.timestamp())
                        || reading.value() < 0) { throw new AssertionError("settled-data metric differs from native queue snapshot"); }
                settled.add(Map.of("metric", name, "settledDataRows", reading.value(),
                        "sampledAtEpochMillis", reading.timestamp()));
            }
        }
        result.put("sinkAcceptedNativeWordCounts", List.copyOf(sinkCounts));
        result.put("sinkSettledDataRowCounts", List.copyOf(settled));
        result.put("sinkCountDifferenceScope", "INCLUDES_CONTROL_WORDS_AND_REPLAY_NOT_PROVEN_DATA_IN_FLIGHT");
        return Map.copyOf(result);
    }

    record TopologyRoster(Set<String> identities, Set<String> executions) {
        TopologyRoster { identities = Set.copyOf(identities); executions = Set.copyOf(executions); }
    }

    static TopologyRoster topologyRoster(Map<?, ?> topology, String memberUuid) {
        Set<String> expected = new HashSet<>();
        Set<String> topologyExecutions = new HashSet<>();
        if (topology.get("vertices") instanceof List<?> vertices) {
            if (vertices.size() > 4096) { throw new AssertionError("native topology vertex roster exceeded its bound"); }
            for (Object raw : vertices) {
                if (!(raw instanceof Map<?, ?> vertex) || !(vertex.get("name") instanceof String name)
                        || !(vertex.get("executionId") instanceof String execution)
                        || !(vertex.get("processors") instanceof List<?> processors)) {
                    throw new AssertionError("native topology vertex has incomplete execution identity");
                }
                topologyExecutions.add(execution);
                for (Object entry : processors) {
                    if (!(entry instanceof Map<?, ?> processor) || !(processor.get("memberUuid") instanceof String member)
                            || !(processor.get("index") instanceof Number index)) { throw new AssertionError("invalid native topology processor"); }
                    int exact;
                    try { exact = new java.math.BigDecimal(index.toString()).intValueExact(); }
                    catch (NumberFormatException | ArithmeticException invalid) { throw new AssertionError("native topology processor index is not an exact integer"); }
                    if (exact < 0 || !memberUuid.equals(member) || !expected.add(member + "/" + name + "/" + exact)
                            || expected.size() > 4096) { throw new AssertionError("native topology has a duplicate or different processor identity"); }
                }
            }
        } else { throw new AssertionError("native topology has no declared vertex roster"); }
        return new TopologyRoster(expected, topologyExecutions);
    }

    @Override public void close() { client.shutdown(); }
}
