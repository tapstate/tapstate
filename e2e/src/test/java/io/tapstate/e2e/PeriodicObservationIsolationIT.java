package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.sun.jdi.*;
import com.sun.jdi.event.*;
import com.sun.jdi.request.BreakpointRequest;
import com.sun.jdi.request.EventRequest;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import java.nio.file.*;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Pattern;

@RequiresDocker
class PeriodicObservationIsolationIT {
    private static final String A = "probe_a", B = "probe_b";
    private static final String ENGINE = "io.tapstate.runtime.engine.Engine";
    private static final String PROXY = "com.hazelcast.jet.impl.JobProxy";
    private static final String CACHE = "io.tapstate.adapters.mongostore.MongoCacheDomain";
    private static final String WRITER = "io.tapstate.app.ProcessObservationWriter";
    private static final String BULK_OBSERVATION_WRITE = "(Ljava/util/List;)Ljava/util/List;";
    private static final String DRIVER = "io.tapstate.app.ConvergenceDriver";
    private static final String CLAIMS = "io.tapstate.app.ClusterWorkloadClaims";
    private static final Duration START = Duration.ofSeconds(120), DELIVERY = Duration.ofSeconds(12);
    private enum Mode { EXECUTION_ADVANCE, NATIVE_METRICS, OBSERVATION_WRITE }
    private record Reading(long amount, long recordsOut, Instant observedAt) {
        boolean follows(Reading prior, long value) {
            return amount == value && recordsOut >= value + 1 && recordsOut > prior.recordsOut
                    && observedAt.isAfter(prior.observedAt);
        }
    }
    private record Held(EventSet set, String stack, AtomicBoolean draining, Thread pendingEvents,
            BreakpointRequest batchProbe, Set<Instant> laterBatchTimes, AtomicReference<Throwable> probeFailure) implements AutoCloseable {
        boolean includesPersistedBatch(Instant observedAt) {
            if (probeFailure.get() != null) throw new AssertionError("Could not read the later observation batch", probeFailure.get());
            return batchProbe == null || laterBatchTimes.contains(observedAt);
        }

        @Override public void close() throws InterruptedException {
            try {
                if (batchProbe != null) batchProbe.virtualMachine().eventRequestManager().deleteEventRequest(batchProbe);
            } finally {
                set.resume();
                draining.set(false);
            }
            pendingEvents.join(2000);
            if (pendingEvents.isAlive()) throw new AssertionError("Owned debugger event drain did not stop");
        }
    }

    private static Held holding(VirtualMachine vm, EventSet set, String stack, BreakpointRequest batchProbe) {
        // Deleting a request does not resume events already queued for another event thread.
        var draining = new AtomicBoolean(true);
        Set<Instant> laterBatchTimes = ConcurrentHashMap.newKeySet();
        var probeFailure = new AtomicReference<Throwable>();
        var pending = Thread.startVirtualThread(() -> {
            for (;;) {
                try {
                    var next = vm.eventQueue().remove(100);
                    if (next == null) {
                        if (!draining.get()) return;
                        continue;
                    }
                    try {
                        for (var event : next) {
                            if (event instanceof LocatableEvent location) {
                                var frames = location.thread().frames();
                                if (batchProbe != null && batchProbe.equals(event.request())) {
                                    recordLaterObservationBatch(frames, laterBatchTimes);
                                    continue;
                                }
                                var collector = frames.stream().filter(frame -> frameIs(frame, ENGINE, "nestStateReadings"))
                                        .findFirst().orElse(null);
                                String pipeline = collector != null && collector.getArgumentValues().getFirst() instanceof StringReference id
                                        ? id.value() : "<other>";
                                System.out.println("JDI_PENDING_RESUMED pipeline=" + pipeline + " thread=" + location.thread().name()
                                        + " stack=" + frames.stream().limit(8).map(frame -> frame.location().toString()).toList());
                            }
                        }
                    } catch (IncompatibleThreadStateException | RuntimeException | AssertionError failure) {
                        probeFailure.compareAndSet(null, failure);
                    } finally { next.resume(); }
                } catch (InterruptedException stopped) {
                    Thread.currentThread().interrupt(); return;
                } catch (VMDisconnectedException closed) { return; }
            }
        });
        return new Held(set, stack, draining, pending, batchProbe, laterBatchTimes, probeFailure);
    }

    @org.junit.jupiter.api.BeforeAll static void requireConnectors() {
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test void twoPipelinesProgressDuringTwoIndependentHolds() throws Exception {
        final String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        var sa = SharedMySql.settings("a604_" + suffix);
        var sb = SharedMySql.settings("b604_" + suffix);
        seed(sa); seed(sb);
        String ta = SharedMongo.replicaSetUrl("ta604_" + suffix);
        String tb = SharedMongo.replicaSetUrl("tb604_" + suffix);
        List<Mode> failures = new ArrayList<>();
        try (var storage = PreparedNativeStore.create("probe604", "probe604-pass");
             var server = RealProcessServer.startPreparedInJvm(storage.configurationUri(),
                     List.of("-Xmx1024m", "-XX:ActiveProcessorCount=2",
                             "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=127.0.0.1:0"),
                     port -> List.of("--tapstate.cluster.id=" + storage.clusterId(), "--tapstate.hz.member-port=0",
                             "--tapstate.hz.jet.cooperative-thread-count=2"));
             var ca = MongoClients.create(ta);
             var cb = MongoClients.create(tb)) {
            System.out.println("probe server PID=" + server.pid() + " log=" + server.output());
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            for (String id : List.of("mysql", "mongodb")) control.registerConnector(id, ConnectorJars.bytesFor(id));
            Map<String, String> all = new LinkedHashMap<>();
            resources(all, A, sa, ta);
            resources(all, B, sb, tb);
            control.apply(all);
            control.discoverSchema(A + "_source", "mysql", sa);
            control.discoverSchema(B + "_source", "mysql", sb);
            MongoDatabase dbA = ca.getDatabase(new ConnectionString(ta).getDatabase());
            MongoDatabase dbB = cb.getDatabase(new ConnectionString(tb).getDatabase());
            var vm = attach(server.output());
            try {
                try (var held = hold(vm, Mode.EXECUTION_ADVANCE, () -> control.lifecycle(A, LifecycleVerb.START))) {
                    control.lifecycle(B, LifecycleVerb.START);
                    Await.until("B delivers its snapshot while A's execution advance is held", DELIVERY,
                            () -> amount(dbB) == 0, () -> "B=" + amount(dbB) + "; A held at " + held.stack);
                }
                Await.until("both snapshot rows and published status", START,
                        () -> amount(dbA) == 0 && amount(dbB) == 0
                                && control.observedAt(A).isPresent() && control.observedAt(B).isPresent(),
                        () -> "A=" + amount(dbA) + ", B=" + amount(dbB));
                update(sa);
                Await.until("A CDC target arrives", START, () -> amount(dbA) == 1, () -> "A=" + amount(dbA));
                update(sb);
                Await.until("B delivers one real CDC row and publishes outbound facts before arming", START,
                        () -> amount(dbB) == 1 && outbound(control).orElse(0) >= 2,
                        () -> "B=" + amount(dbB) + " outbound=" + outbound(control));
                Supplier<Reading> read = () -> read(control, dbB);
                for (var mode : List.of(Mode.NATIVE_METRICS, Mode.OBSERVATION_WRITE, Mode.NATIVE_METRICS, Mode.OBSERVATION_WRITE)) {
                    Reading beforeControl = read.get();
                    update(sb);
                    var positive = awaitFresh(read, beforeControl, beforeControl.amount + 1, "unblocked control");
                    System.out.println("CONTROL " + mode + " before=" + beforeControl + " fresh=" + positive);
                    Reading beforeHold;
                    Reading endHold;
                    try (var held = hold(vm, mode, () -> {})) {
                        beforeHold = read.get();
                        System.out.println("HOLD " + mode + " baseline=" + beforeHold + " stack=" + held.stack);
                        update(sb);
                        long expect = beforeHold.amount + 1;
                        Await.until("B target advances while A held", DELIVERY, () -> amount(dbB) == expect,
                                () -> "B target=" + amount(dbB) + " expected=" + expect);
                        var atArrival = read.get();
                        // Native metrics collect every five seconds by default; use the same delivery budget for both faces.
                        var until = System.nanoTime() + DELIVERY.toNanos();
                        int polls = 0;
                        do {
                            Await.pauseBeforeNextPoll(Duration.ofMillis(350));
                            endHold = read.get();
                            ++polls;
                        } while (System.nanoTime() < until && (!endHold.follows(beforeHold, expect)
                                || !held.includesPersistedBatch(endHold.observedAt)));
                        System.out.println("HELD " + mode + " arrival=" + atArrival + " end=" + endHold + " polls=" + polls
                                + " observedAtDeltaMs=" + Duration.between(beforeHold.observedAt, endHold.observedAt).toMillis()
                                + " laterBatchTimes=" + held.laterBatchTimes);
                        if (!endHold.follows(beforeHold, expect) || !held.includesPersistedBatch(endHold.observedAt)) failures.add(mode);
                    }
                    var recovered = awaitFresh(read, beforeHold, beforeHold.amount + 1, "after release");
                    System.out.println("RELEASE " + mode + " recovered=" + recovered);
                }
            } finally { vm.dispose(); }
        }
        if (!failures.isEmpty()) throw new AssertionError("B's real target advanced but its observation did not follow during: " + failures);
    }

    private static Reading awaitFresh(Supplier<Reading> read, Reading prior, long expect, String phase) {
        Await.until("B target and observation move " + phase, DELIVERY, () -> read.get().follows(prior, expect),
                () -> "prior=" + prior + " actual=" + read.get());
        return read.get();
    }

    private static Reading read(ControlPlane control, MongoDatabase database) {
        Instant at = control.observedAt(B).orElseThrow();
        return new Reading(amount(database), outbound(control).orElseThrow(), at);
    }

    private static OptionalLong outbound(ControlPlane control) {
        var json = (Map<?, ?>) JsonReader.parse(control.metricsJson(B));
        var metrics = (Map<?, ?>) json.get("metrics");
        Object count = metrics.get("records.out");
        return count instanceof Number n ? OptionalLong.of(n.longValue()) : OptionalLong.empty();
    }

    private static VirtualMachine attach(Path log) throws Exception {
        var port = Pattern.compile("Listening for transport dt_socket at address: (\\d+)").matcher(Files.readString(log));
        if (!port.find()) throw new AssertionError("No owned JDWP startup in " + log);
        var connector = Bootstrap.virtualMachineManager().attachingConnectors().stream()
                .filter(c -> c.name().equals("com.sun.jdi.SocketAttach")).findFirst().orElseThrow();
        var args = connector.defaultArguments();
        args.get("hostname").setValue("127.0.0.1");
        args.get("port").setValue(port.group(1));
        args.get("timeout").setValue("5000");
        return connector.attach(args);
    }

    private static Held hold(VirtualMachine vm, Mode mode, Runnable trigger) throws Exception {
        String name = switch (mode) {
            case EXECUTION_ADVANCE -> CLAIMS;
            case NATIVE_METRICS -> PROXY;
            case OBSERVATION_WRITE -> CACHE;
        };
        var type = vm.classesByName(name);
        if (type.size() != 1) throw new AssertionError("Unexpected class identity " + name + ": " + type.size());
        Location site = switch (mode) {
            case EXECUTION_ADVANCE -> type.getFirst().methodsByName("advanceExecution").getFirst().location();
            case NATIVE_METRICS -> type.getFirst().methodsByName("getMetrics", "()Lcom/hazelcast/jet/core/metrics/JobMetrics;").getFirst().location();
            case OBSERVATION_WRITE -> type.getFirst().methodsByName("writeObservations", BULK_OBSERVATION_WRITE).getFirst().location();
        };
        var manager = vm.eventRequestManager();
        var request = manager.createBreakpointRequest(site);
        request.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
        // Both Pipeline measurements can reach the same timer tick. Hold B's producer only until a batch
        // containing A without B starts, so the later CDC assertion measures an independent batch.
        var producerRequest = mode == Mode.OBSERVATION_WRITE
                ? manager.createBreakpointRequest(vm.classesByName(PROXY).getFirst()
                        .methodsByName("getMetrics", "()Lcom/hazelcast/jet/core/metrics/JobMetrics;").getFirst().location())
                : null;
        if (producerRequest != null) {
            producerRequest.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
            producerRequest.enable();
        }
        request.enable();
        long deadline = System.nanoTime() + Duration.ofSeconds(14).toNanos();
        EventSet heldProducer = null;
        boolean keepBatchProbe = false;
        try {
            trigger.run();
            while (System.nanoTime() < deadline) {
                var set = vm.eventQueue().remove(1000);
                if (set == null) continue;
                boolean accepted = false;
                try {
                    for (var event : set) {
                        if (!(event instanceof BreakpointEvent bp)) continue;
                        if (producerRequest != null && producerRequest.equals(bp.request())) {
                            var frames = bp.thread().frames();
                            boolean periodic = frames.stream().anyMatch(f -> frameIs(f, DRIVER, "publishPeriodicObservation"));
                            if (heldProducer == null && periodic && frames.stream().anyMatch(f -> frameIs(f, ENGINE, "nestStateReadings")
                                    && f.getArgumentValues().getFirst() instanceof StringReference id && id.value().equals(B))) {
                                producerRequest.disable();
                                heldProducer = set;
                                accepted = true;
                                break;
                            }
                            continue;
                        }
                        if (!request.equals(bp.request())) continue;
                        var frames = bp.thread().frames();
                        String batchDetail = "";
                        if (mode == Mode.OBSERVATION_WRITE) {
                            if (!isObservationWriter(frames)) continue;
                            var pipelines = batchObservations(frames).stream().map(PeriodicObservationIsolationIT::pipeline).toList();
                            if (!pipelines.contains(A) || pipelines.contains(B)) continue;
                            batchDetail = " batch=" + pipelines;
                        } else {
                            if (frames.stream().noneMatch(f -> frameIs(f, DRIVER, "reconcile")
                                    || frameIs(f, DRIVER, "reconcilePipeline")
                                    || frameIs(f, DRIVER, "publishPeriodicObservation"))) continue;
                            var scope = frames.stream().filter(f -> mode == Mode.EXECUTION_ADVANCE
                                    ? frameIs(f, CLAIMS, "advanceExecution") : frameIs(f, ENGINE, "nestStateReadings"))
                                    .findFirst().orElse(null);
                            if (scope == null) continue;
                            Value arg = scope.getArgumentValues().getFirst();
                            if (mode == Mode.EXECUTION_ADVANCE) {
                                var claim = (ObjectReference) arg;
                                var key = (ObjectReference) claim.getValue(claim.referenceType().fieldByName("key"));
                                arg = key.getValue(key.referenceType().fieldByName("resourceId"));
                            }
                            if (!(arg instanceof StringReference id) || !id.value().equals(A)) continue;
                        }
                        String stack = frames.stream().limit(24).map(f -> f.location().declaringType().name()
                                + "." + f.location().method().name() + ":" + f.location().lineNumber())
                                .reduce((x,y) -> x + " <- " + y).orElseThrow();
                        keepBatchProbe = mode == Mode.OBSERVATION_WRITE;
                        if (!keepBatchProbe) request.disable();
                        accepted = true;
                        return holding(vm, set, bp.thread().name() + batchDetail + " " + stack, keepBatchProbe ? request : null);
                    }
                } finally { if (!accepted) set.resume(); }
            }
            throw new AssertionError("Never reached owned " + mode + " path for A");
        } finally {
            if (heldProducer != null) heldProducer.resume();
            if (producerRequest != null) manager.deleteEventRequest(producerRequest);
            if (!keepBatchProbe) manager.deleteEventRequest(request);
        }
    }

    private static boolean isObservationWriter(List<StackFrame> frames) {
        return frames.stream().anyMatch(f -> f.location().declaringType().name().equals(WRITER));
    }

    private static void recordLaterObservationBatch(List<StackFrame> frames, Set<Instant> laterBatchTimes) {
        if (!isObservationWriter(frames)) return;
        var observations = batchObservations(frames);
        if (observations.stream().anyMatch(observation -> pipeline(observation).equals(A))) return;
        for (var observation : observations) {
            if (!pipeline(observation).equals(B)) continue;
            var timestamp = (ObjectReference) field(observation, "observedAt");
            long seconds = ((LongValue) field(timestamp, "seconds")).value();
            int nanos = ((IntegerValue) field(timestamp, "nanos")).value();
            laterBatchTimes.add(Instant.ofEpochSecond(seconds, nanos).truncatedTo(ChronoUnit.MILLIS));
        }
    }

    private static List<ObjectReference> batchObservations(List<StackFrame> frames) {
        // Read the actual argument without invoking methods on a suspended application thread.
        var batch = (ObjectReference) frames.getFirst().getArgumentValues().getFirst();
        List<Value> writes = switch (batch.referenceType().name()) {
            case "java.util.ImmutableCollections$ListN" -> ((ArrayReference) field(batch, "elements")).getValues();
            case "java.util.ImmutableCollections$List12" -> {
                var first = field(batch, "e0");
                var second = (ObjectReference) field(batch, "e1");
                yield second.referenceType().name().equals("java.lang.Object") ? List.of(first) : List.of(first, second);
            }
            case "java.util.ArrayList" -> ((ArrayReference) field(batch, "elementData"))
                    .getValues(0, ((IntegerValue) field(batch, "size")).value());
            default -> throw new AssertionError("Unsupported observation batch list " + batch.referenceType().name());
        };
        return writes.stream().map(write -> (ObjectReference) field((ObjectReference) write, "observation")).toList();
    }

    private static String pipeline(ObjectReference observation) {
        return ((StringReference) field(observation, "pipelineId")).value();
    }

    private static Value field(ObjectReference object, String name) {
        return object.getValue(object.referenceType().fieldByName(name));
    }

    private static boolean frameIs(StackFrame frame, String type, String method) {
        return frame.location().declaringType().name().equals(type) && frame.location().method().name().equals(method);
    }

    private static long amount(MongoDatabase database) {
        var row = database.getCollection("orders").find(new Document("id", 1L)).first();
        return row == null ? Long.MIN_VALUE : ((Number) row.get("amount")).longValue();
    }

    private static void seed(Map<String, Object> source) throws Exception {
        try (var c = SharedMySql.connect(source); var s = c.createStatement()) {
            s.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, amount BIGINT NOT NULL)");
            s.execute("INSERT INTO orders VALUES (1, 0)");
        }
    }
    private static void update(Map<String, Object> source) throws Exception {
        try (var c = SharedMySql.connect(source); var s = c.createStatement()) {
            if (s.executeUpdate("UPDATE orders SET amount=amount+1 WHERE id=1") != 1) {
                throw new AssertionError("The expected one source row did not change");
            }
        }
    }
    private static void resources(Map<String, String> result, String id, Map<String,Object> mysql, String mongo) {
        result.put(id + "_source.tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s_source
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ orders ]
                """.formatted(id, mysql.get("host"), mysql.get("port"), mysql.get("database"),
                mysql.get("username"), mysql.get("password")));
        result.put(id + "_target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s_target
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(id, mongo));
        result.put(id + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s_source
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: orders
                  sync:
                    - source: %s_target
                """.formatted(id,id,id));
    }
}
