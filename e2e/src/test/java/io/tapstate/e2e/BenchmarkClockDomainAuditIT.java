package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.testsupport.DockerGate;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** One explicitly requested cold fixture records clocks only; disagreements are diagnostic facts. */
@RequiresDocker
@Isolated("owns one disposable clock-only Mongo fixture")
class BenchmarkClockDomainAuditIT {
    private static final String OUTPUT = "tapstate.e2e.benchmark.clock-domain-output";

    @Test void coldHostAndGuestClockPointsAreRetainedWithoutQualification() throws Exception {
        String configured = System.getProperty(OUTPUT);
        Assumptions.assumeTrue(configured != null && !configured.isBlank(), "clock audit requires an explicit output path");
        Path output = Path.of(configured).toAbsolutePath();
        if (Files.exists(output)) { throw new AssertionError("clock audit output already exists"); }
        var receipt = new LinkedHashMap<>(BenchmarkClockDomainAudit.facts());
        List<BenchmarkClockDomainAudit.Exchange> rows = new ArrayList<>();
        var readFailure = new AtomicReference<Throwable>();
        var owner = new BenchmarkClockDomainAudit.Owner(ProcessHandle.current().pid(), ManagementFactory.getRuntimeMXBean().getStartTime());
        receipt.put("root", java.util.Map.of("pid", owner.pid(), "jvmStartTimeMillis", owner.jvmStartTimeMillis()));
        Throwable failure = null;
        try {
            DockerGate.require();
            try (var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))) {
                var arguments = new ArrayList<>(Arrays.asList(mongo.getCommandParts()));
                arguments.add("--wiredTigerCacheSizeGB=0.5"); mongo.withCommand(arguments.toArray(String[]::new));
                mongo.start();
                try (var client = MongoClients.create(mongo.getReplicaSetUrl("clock_domain_audit"))) {
                    var admin = client.getDatabase("admin").withTimeout(1, TimeUnit.SECONDS);
                    Document build = admin.runCommand(new Document("buildInfo", 1));
                    var info = mongo.getContainerInfo();
                    Number pid = info.getState().getPid();
                    if (mongo.getContainerId() == null || info.getImageId() == null || pid == null || pid.longValue() <= 0
                            || build.getString("version") == null || build.getString("gitVersion") == null) {
                        throw new AssertionError("clock audit actual fixture identity is incomplete");
                    }
                    receipt.put("fixture", java.util.Map.of("containerId", mongo.getContainerId(), "imageId", info.getImageId(),
                            "imageTag", mongo.getDockerImageName(), "daemonReportedContainerPid", pid.longValue(),
                            "version", build.getString("version"), "gitVersion", build.getString("gitVersion")));
                    var options = admin.runCommand(new Document("getCmdLineOpts", 1)).get("parsed", Document.class);
                    Number cache = options.get("storage", Document.class).get("wiredTiger", Document.class)
                            .get("engineConfig", Document.class).get("cacheSizeGB", Number.class);
                    if (cache == null || new java.math.BigDecimal(cache.toString()).compareTo(new java.math.BigDecimal("0.5")) != 0) {
                        throw new AssertionError("clock audit actual cache budget differs");
                    }
                    receipt.put("actualCacheSizeGB", cache); admin.runCommand(new Document("ping", 1));
                    try (var sampler = new BenchmarkTargetClockSampler(() -> read(admin, rows, owner, readFailure), () -> { })) {
                        Await.until("151 actual cold hello readings", Duration.ofSeconds(40), () -> {
                            if (readFailure.get() != null) { throw new AssertionError("clock audit actual hello read failed", readFailure.get()); }
                            return sampler.samplesRecorded() >= BenchmarkClockDomainAudit.TARGET_READS;
                        }, () -> "actualHelloReadings=" + sampler.samplesRecorded());
                        sampler.close();
                        var actual = sampler.readings();
                        var retained = snapshot(rows);
                        if (!actual.equals(retained.stream().map(BenchmarkClockDomainAudit.Exchange::guest).toList())) {
                            throw new AssertionError("clock audit sampler and retained hello rosters differ");
                        }
                        receipt.putAll(BenchmarkClockDomainAudit.evidence(retained));
                    }
                }
            }
        } catch (Exception | Error problem) {
            failure = problem; receipt.put("state", "UNKNOWN");
            receipt.put("failureType", problem.getClass().getName()); receipt.put("reason", String.valueOf(problem.getMessage()));
        } finally {
            receipt.put("readings", BenchmarkClockDomainAudit.raw(snapshot(rows)));
            receipt.put("sampleIntervalMillis", BenchmarkTargetClock.INTERIOR_SAMPLE_INTERVAL_MILLIS);
            receipt.put("targetHelloReads", BenchmarkClockDomainAudit.TARGET_READS);
            try {
                Files.createDirectories(output.getParent());
                Files.writeString(output, JsonWriter.write(receipt), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
            } catch (Exception writing) { if (failure == null) { throw writing; } failure.addSuppressed(writing); }
        }
        if (failure instanceof Exception problem) { throw problem; }
        if (failure instanceof Error problem) { throw problem; }
    }

    private static BenchmarkTargetClock.Reading read(MongoDatabase admin, List<BenchmarkClockDomainAudit.Exchange> rows,
            BenchmarkClockDomainAudit.Owner owner, AtomicReference<Throwable> failure) {
        synchronized (rows) {
            if (rows.size() >= BenchmarkClockDomainAudit.MAX_READS) {
                var capacity = new AssertionError("clock audit actual read capacity exceeded"); failure.compareAndSet(null, capacity); throw capacity;
            }
        }
        var before = BenchmarkClockDomainAudit.WallPoint.read();
        BenchmarkTargetClock.Reading guest = null;
        try { guest = BenchmarkTargetClock.read(admin); return guest; }
        catch (RuntimeException | Error problem) { failure.compareAndSet(null, problem); throw problem; }
        finally {
            var after = BenchmarkClockDomainAudit.WallPoint.read();
            synchronized (rows) { rows.add(new BenchmarkClockDomainAudit.Exchange(rows.size(), owner, Thread.currentThread().threadId(), before, guest, after)); }
        }
    }
    private static List<BenchmarkClockDomainAudit.Exchange> snapshot(List<BenchmarkClockDomainAudit.Exchange> rows) {
        synchronized (rows) { return List.copyOf(rows); }
    }
}
