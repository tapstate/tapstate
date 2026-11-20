package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.jdi.Bootstrap;
import com.sun.jdi.ClassNotLoadedException;
import com.sun.jdi.IncompatibleThreadStateException;
import com.sun.jdi.InvalidTypeException;
import com.sun.jdi.InvocationException;
import com.sun.jdi.LongValue;
import com.sun.jdi.ObjectReference;
import com.sun.jdi.ReferenceType;
import com.sun.jdi.StringReference;
import com.sun.jdi.ThreadReference;
import com.sun.jdi.event.BreakpointEvent;
import com.sun.jdi.event.ClassPrepareEvent;
import com.sun.jdi.event.EventSet;
import com.sun.jdi.request.BreakpointRequest;
import com.sun.jdi.request.EventRequest;
import com.sun.jdi.request.EventRequestManager;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.runtime.engine.OperatorMapCoordinates;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A pointed-at row's first filing publishes each completed document once.
 *
 * <p>The two source tables travel on independent chains. An order registers the customer it names at
 * the lookup while its other copy travels toward the assembler; the customer is then filed before its
 * wake travels toward that same assembler. When the root crosses between the filing and the wake, it
 * reads the filed customer and publishes the complete document. The later wake must settle the
 * customer's position without publishing the identical document again.
 *
 * <p><b>Why the target contents do not prove this.</b> Mongo upserts both copies under the order's key,
 * so one write and two identical writes leave exactly the same collection. The target is still read to
 * prove every document completed. The discriminating reading is taken at the unchanged real connector's
 * write callback: the observer records every physical record before delegating, and the run is stopped
 * before its ledger is read.
 *
 * <p>Enough independent pairs are loaded to put both paths in flight together. One pair would leave the
 * relative scheduling to chance; a load wider than the connector's batch keeps registrations, filings,
 * roots and wakes crossing while every key remains independent and has exactly one legitimate output.
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=ANestFirstFilingPublishesEachDocumentOnceIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class ANestFirstFilingPublishesEachDocumentOnceIT {

    private static final int ROWS = 2_000;
    private static final String ORDERS = "orders";
    private static final String CUSTOMERS = "customers";
    private static final String PIPELINE_ID = "first_filing_once";

    @TempDir
    Path directory;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void customersReadFirstFromOneSourceStillReachOrdersAfterCdcAndRestart(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        Map<String, Object> mysql = SharedMySql.settings("nest_customer_first_" + suffix);
        seed(mysql);
        String pipelineId = "customer_first_" + suffix;
        String targetUri = SharedMongo.replicaSetUrl("nest_customer_first_target_" + suffix);
        try (PreparedNativeStore storage = PreparedNativeStore.create();
                ServerHandle server = tier.launch(storage); MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("src_all.tap.yml", sourceYaml("src_all", CUSTOMERS + ", " + ORDERS, mysql));
            resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
            resources.put("pipeline.tap.yml", pipelineYaml(pipelineId, "src_all", "snapshot_and_cdc"));
            control.apply(resources);
            control.discoverSchema("src_all", "mysql", mysql);
            control.lifecycle(pipelineId, LifecycleVerb.START);
            EndpointAddress target = EndpointAddress.uri(targetUri);
            Await.until("all orders after the customer-only snapshot prefix",
                    () -> mongo.count(target, ORDERS) == ROWS,
                    () -> mongo.count(target, ORDERS) + " orders");
            assertCompleted(mongo, target, 1);
            assertCompleted(mongo, target, ROWS);
            assertCustomerSnapshotWasBatched(control, pipelineId, storage);

            updateCustomer(mysql, "after-snapshot");
            awaitCustomerName(mongo, target, "after-snapshot");
            control.stop(pipelineId, false);
            Await.until("the pipeline to stop before its state is reused",
                    () -> control.state(pipelineId).filter(PipelineState.STOPPED::equals).isPresent(),
                    () -> String.valueOf(control.state(pipelineId)));
            control.lifecycle(pipelineId, LifecycleVerb.START);
            Await.until("the pipeline to resume with its retained references",
                    () -> control.state(pipelineId).filter(PipelineState.RUNNING::equals).isPresent(),
                    () -> String.valueOf(control.state(pipelineId)));
            updateCustomer(mysql, "after-restart");
            awaitCustomerName(mongo, target, "after-restart");
            assertThat(mongo.count(target, ORDERS)).isEqualTo(ROWS);
        }
    }

    private static void updateCustomer(Map<String, Object> mysql, String name) throws Exception {
        try (Connection connection = SharedMySql.connect(mysql);
                PreparedStatement update = connection.prepareStatement("UPDATE customers SET name = ? WHERE id = 1")) {
            update.setString(1, name);
            assertThat(update.executeUpdate()).isEqualTo(1);
        }
    }

    private static void assertCustomerSnapshotWasBatched(ControlPlane control, String pipelineId,
            PreparedNativeStore storage) {
        String namespace = countedCustomerReferences(storage, pipelineId);
        long[] settled = {-1L};
        long[] unchangedSince = {System.nanoTime()};
        Await.until("the completed snapshot cost reading to catch up and settle",
                () -> {
                    long current = control.metricTotal(pipelineId,
                            "nestStateAccesses." + namespace + ".run.").orElse(0L);
                    if (current != settled[0]) {
                        settled[0] = current;
                        unchangedSince[0] = System.nanoTime();
                        return false;
                    }
                    return current >= ROWS && System.nanoTime() - unchangedSince[0] >= Duration.ofSeconds(8).toNanos();
                }, () -> "reference accesses " + settled[0]);
        long accesses = settled[0];
        // Registrations still cost one access each. Customer wake reads must fold across rows; the
        // old row-at-a-time path costs another ROWS accesses while producing identical target data.
        assertThat(accesses).as("reference accesses after the customer-first snapshot")
                .isGreaterThanOrEqualTo(ROWS).isLessThanOrEqualTo(ROWS + ROWS / 2);
    }

    private static String countedCustomerReferences(PreparedNativeStore storage, String pipelineId) {
        try (var configuration = MongoConfigurationDomain.openExisting(
                new MongoConnectionSettings(storage.configurationUri(), null, Duration.ofSeconds(5)));
                var cache = MongoCacheDomain.connect(new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                        storage.cacheUri(), storage.cacheDatabase(), null), Duration.ofSeconds(5), "nest-cost-reader")) {
            var scope = configuration.preparations().read(pipelineId).orElseThrow().scope();
            var lookup = OperatorMapCoordinates.nest(scope, storage.cacheDatabase(), "order_doc", List.of("customer"),
                    OperatorMapCoordinates.NestMap.LOOKUP, "c");
            var references = OperatorMapCoordinates.nest(scope, storage.cacheDatabase(), "order_doc", List.of("customer"),
                    OperatorMapCoordinates.NestMap.REFERENCES, "c");
            var customers = cache.operatorState(lookup);
            var registrations = cache.operatorState(references);
            Await.until("every customer and reverse registration to be durably counted",
                    () -> customers.count(lookup.mapName()) == ROWS && registrations.count(references.mapName()) == ROWS,
                    () -> "customers=" + customers.count(lookup.mapName()) + ", references=" + registrations.count(references.mapName()));
            return references.mapName();
        }
    }

    private static void awaitCustomerName(MongoEndpoints mongo, EndpointAddress target, String name) {
        Await.until("the customer update " + name + " to reach its order",
                () -> mongo.fetch(target, ORDERS, Map.of("id", 1))
                        .map(order -> order.get("customer") instanceof Document customer
                                && name.equals(customer.getString("name"))).orElse(false),
                () -> String.valueOf(mongo.fetch(target, ORDERS, Map.of("id", 1))));
    }

    @Test
    void aFiledCustomerAlreadyReadByItsOrderDoesNotPublishThatOrderAgain() throws Exception {
        checkFirstFilingWrites(Tiers.IN_PROCESS);
    }

    @Test
    void aFiledCustomerAlreadyReadByItsOrderDoesNotPublishThatOrderAgainInTheBootJar() throws Exception {
        checkFirstFilingWrites(Tiers.REAL_PROCESS);
    }

    private void checkFirstFilingWrites(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        Map<String, Object> mysql = SharedMySql.settings("nest_first_filing_" + suffix);
        seed(mysql);

        String pipelineId = PIPELINE_ID + "_" + suffix;
        String targetUri = SharedMongo.replicaSetUrl("nest_first_filing_target_" + suffix);
        Path witness = directory.resolve("writes-" + suffix + ".tsv");

        try (PreparedNativeStore storage = PreparedNativeStore.create();
                ServerHandle server = launchFirstFiling(tier, storage);
                MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb",
                    ObservedMongoConnectorJar.build(ConnectorJars.bytesFor("mongodb"), witness));

            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("src_orders.tap.yml", sourceYaml("src_orders", ORDERS, mysql));
            resources.put("src_customers.tap.yml", sourceYaml("src_customers", CUSTOMERS, mysql));
            resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
            resources.put("pipeline.tap.yml", pipelineYaml(pipelineId));
            control.apply(resources);
            control.discoverSchema("src_orders", "mysql", mysql);
            control.discoverSchema("src_customers", "mysql", mysql);
            if (server instanceof RealProcessServer process) {
                finishJetBetweenStatusReads(process, control, pipelineId);
            } else {
                control.lifecycle(pipelineId, LifecycleVerb.START);
            }

            EndpointAddress target = EndpointAddress.uri(targetUri);
            Await.until("every completed order to reach the target",
                    () -> mongo.count(target, ORDERS) == ROWS,
                    () -> mongo.count(target, ORDERS) + " orders");
            assertCompleted(mongo, target, 1);
            assertCompleted(mongo, target, ROWS);

            var liveMetrics = tier == Tiers.REAL_PROCESS ? control.metricsNamed(pipelineId, "") : Map.of();
            Await.until("the bounded nest run to complete before its write ledger is read",
                    () -> control.state(pipelineId).filter(PipelineState.COMPLETED::equals).isPresent(),
                    () -> String.valueOf(control.state(pipelineId)));

            WriteLedger ledger = readWriteLedger(witness);
            if (server instanceof RealProcessServer process) {
                var metrics = control.metricsNamed(pipelineId, "");
                try (var log = Files.lines(process.output())) {
                    var executions = log.filter(line -> line.contains("Execution plan for jobId=")
                                    && line.contains("jobName='" + pipelineId + "'"))
                            .map(line -> line.substring(line.indexOf("Execution plan for jobId=")))
                            .toList();
                    System.out.printf("Mongo write callbacks=%d, records offered=%d, sink items=%s/%s, "
                                    + "sink records delivered=%s/%s (at target count / at completion), executions=%s%n",
                            ledger.calls(), ledger.records(), liveMetrics.get("recordCount"), metrics.get("recordCount"),
                            liveMetrics.get("records.out"), metrics.get("records.out"), executions);
                }
            }
            assertThat(ledger.records())
                    .as("physical Mongo records written for %d independently completed orders", ROWS)
                    .isEqualTo(ROWS);
        }
    }

    private static ServerHandle launchFirstFiling(Tiers tier, PreparedNativeStore storage) {
        if (tier == Tiers.IN_PROCESS) return tier.launch(storage);
        return RealProcessServer.startPreparedInJvm(storage.configurationUri(),
                List.of("-Xmx768m", "-XX:ActiveProcessorCount=2",
                        "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=127.0.0.1:0"),
                port -> List.of("--tapstate.cluster.id=" + storage.clusterId(),
                        "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2"));
    }

    private static void finishJetBetweenStatusReads(RealProcessServer process, ControlPlane control, String pipelineId)
            throws Exception {
        String listener = "Listening for transport dt_socket at address: ";
        String port = Files.readAllLines(process.output()).stream().filter(line -> line.startsWith(listener))
                .findFirst().orElseThrow().substring(listener.length());
        var connector = Bootstrap.virtualMachineManager().attachingConnectors().stream()
                .filter(candidate -> candidate.name().equals("com.sun.jdi.SocketAttach")).findFirst().orElseThrow();
        var arguments = connector.defaultArguments();
        arguments.get("hostname").setValue("127.0.0.1");
        arguments.get("port").setValue(port);
        var vm = connector.attach(arguments);
        try {
            var converger = vm.classesByName("io.tapstate.runtime.scheduler.PipelineConverger").getFirst();
            var running = converger.methodsByName("convergeRunning").getFirst();
            var requests = vm.eventRequestManager();
            var convergenceBreakpoints = new ArrayList<BreakpointRequest>();
            for (var location : running.allLineLocations()) {
                var breakpoint = requests.createBreakpointRequest(location);
                breakpoint.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
                convergenceBreakpoints.add(breakpoint);
            }
            String jetContext = "com.hazelcast.jet.impl.MasterJobContext";
            var preparing = requests.createClassPrepareRequest();
            preparing.addClassFilter(jetContext);
            preparing.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
            preparing.enable();
            var finishing = vm.classesByName(jetContext).stream().filter(ReferenceType::isPrepared).findFirst()
                    .map(type -> jetFinalizerBreakpoint(requests, type)).orElse(null);
            if (finishing != null) preparing.disable();
            var started = CompletableFuture.runAsync(() -> control.lifecycle(pipelineId, LifecycleVerb.START));

            // Keep convergence running while Jet processes the load. Once Jet is ready to finalize,
            // hold it until a convergence pass reads an empty completion for this job.
            EventSet jetPaused = null;
            LongValue finalizingJob = null;
            EventSet convergencePaused = null;
            ThreadReference convergenceThread = null;
            ObjectReference run = null;
            Duration waitBound = Duration.ofSeconds(60);
            long deadline = System.nanoTime() + waitBound.toNanos();
            while ((jetPaused == null || convergencePaused == null) && System.nanoTime() < deadline) {
                if (started.isCompletedExceptionally()) started.join();
                var events = vm.eventQueue().remove(250);
                if (events == null) continue;
                try {
                    for (var event : events) {
                        if (event instanceof ClassPrepareEvent prepared && finishing == null) {
                            finishing = jetFinalizerBreakpoint(requests, prepared.referenceType());
                            preparing.disable();
                        } else if (event instanceof BreakpointEvent stopped) {
                            if (stopped.request() == finishing) {
                                var jobId = finalizingJobId(stopped, pipelineId);
                                if (jobId != null) {
                                    finalizingJob = jobId;
                                    jetPaused = events;
                                    requests.deleteEventRequest(finishing);
                                    convergenceBreakpoints.forEach(BreakpointRequest::enable);
                                    deadline = System.nanoTime() + waitBound.toNanos();
                                }
                                continue;
                            }
                            var observed = originalRunAfterIncompleteRead(stopped, pipelineId);
                            if (observed == null) continue;
                            run = observed;
                            convergenceThread = stopped.thread();
                            convergencePaused = events;
                            // Start the completion budget at the empty read.
                            deadline = System.nanoTime() + waitBound.toNanos();
                            requests.deleteEventRequests(convergenceBreakpoints);
                        }
                    }
                } finally {
                    if (events != jetPaused && events != convergencePaused) events.resume();
                }
            }
            if (jetPaused == null || convergencePaused == null) {
                throw new AssertionError("timed out arranging Jet completion between convergence reads: "
                        + "Jet at finalizer=" + (jetPaused != null)
                        + ", convergence read empty=" + (convergencePaused != null) + ", server log=" + process.output());
            }
            var originalRun = run;
            var stoppedThread = convergenceThread;
            try {
                var originalJob = (LongValue) originalRun.invokeMethod(stoppedThread,
                        originalRun.referenceType().methodsByName("jobId").getFirst(),
                        List.of(), ObjectReference.INVOKE_SINGLE_THREADED);
                assertThat(finalizingJob.longValue()).as("Jet job held before convergence checks for a live run")
                        .isEqualTo(originalJob.longValue());
                jetPaused.resume();
                // Jet can log success before publishing its terminal job status.
                var remaining = Duration.ofNanos(Math.max(0, deadline - System.nanoTime()));
                Await.until("the original Jet execution to report COMPLETED while convergence is paused",
                        remaining, () -> jetCompleted(originalRun, stoppedThread), () -> process.output().toString());
            } finally {
                convergencePaused.resume();
            }
            started.join();
        } finally {
            vm.dispose();
        }
    }

    private static BreakpointRequest jetFinalizerBreakpoint(EventRequestManager requests, ReferenceType context) {
        var location = context.methodsByName("completeVertices").getFirst().location();
        var breakpoint = requests.createBreakpointRequest(location);
        breakpoint.setSuspendPolicy(EventRequest.SUSPEND_EVENT_THREAD);
        breakpoint.enable();
        return breakpoint;
    }

    private static LongValue finalizingJobId(BreakpointEvent stopped, String pipelineId) throws Exception {
        var finalizer = stopped.thread().frame(0).thisObject();
        var context = (ObjectReference) finalizer.getValue(finalizer.referenceType().fieldByName("mc"));
        var contextType = context.referenceType();
        var jobName = (StringReference) context.getValue(contextType.fieldByName("jobName"));
        return jobName != null && pipelineId.equals(jobName.value())
                ? (LongValue) context.getValue(contextType.fieldByName("jobId")) : null;
    }

    private static ObjectReference originalRunAfterIncompleteRead(BreakpointEvent stopped, String pipelineId)
            throws Exception {
        var frame = stopped.thread().frame(0);
        var observedPipeline = (StringReference) frame.getArgumentValues().getFirst();
        if (!pipelineId.equals(observedPipeline.value())) return null;
        var completion = frame.visibleVariableByName("completion");
        if (completion == null || !(frame.getValue(completion) instanceof ObjectReference result)
                || result.getValue(result.referenceType().fieldByName("value")) != null) return null;
        var converger = frame.thisObject();
        var actuator = (ObjectReference) converger.getValue(converger.referenceType().fieldByName("actuator"));
        return (ObjectReference) actuator.getValue(actuator.referenceType().fieldByName("run"));
    }

    private static boolean jetCompleted(ObjectReference run, ThreadReference thread) {
        try {
            var status = (ObjectReference) run.invokeMethod(thread, run.referenceType().methodsByName("status").getFirst(),
                    List.of(), ObjectReference.INVOKE_SINGLE_THREADED);
            var statusType = status.referenceType();
            return status.equals(statusType.getValue(statusType.fieldByName("COMPLETED")));
        } catch (InvalidTypeException | ClassNotLoadedException | IncompatibleThreadStateException
                | InvocationException failure) {
            throw new AssertionError("could not read the original Jet job status", failure);
        }
    }

    private static void assertCompleted(MongoEndpoints mongo, EndpointAddress target, int id) {
        Map<String, Object> order = mongo.fetch(target, ORDERS, Map.of("id", id)).orElseThrow();
        assertThat(order.get("customer"))
                .as("the customer embedded in order %d", id)
                .isInstanceOfSatisfying(Document.class, customer -> assertThat(customer)
                        .containsEntry("id", (long) id)
                        .containsEntry("name", "customer-" + id));
    }

    private static WriteLedger readWriteLedger(Path witness) throws Exception {
        assertThat(witness).as("the real Mongo connector's write ledger").isRegularFile();
        List<String> calls = Files.readAllLines(witness);
        long records = 0;
        for (String call : calls) {
            String[] fields = call.split("\\t", -1);
            assertThat(fields).as("one observed Mongo write callback").hasSize(3);
            assertThat(fields[0]).as("the table handed to the Mongo connector").isEqualTo(ORDERS);
            records += Long.parseLong(fields[1]);
        }
        return new WriteLedger(calls.size(), records);
    }

    private record WriteLedger(int calls, long records) {}

    private static void seed(Map<String, Object> settings) throws Exception {
        try (Connection connection = SharedMySql.connect(settings)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS " + ORDERS);
                statement.execute("DROP TABLE IF EXISTS " + CUSTOMERS);
                statement.execute("CREATE TABLE " + ORDERS
                        + " (id INT PRIMARY KEY, customer_id INT NOT NULL)");
                statement.execute("CREATE TABLE " + CUSTOMERS
                        + " (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL)");
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + ORDERS + " (id, customer_id) VALUES (?, ?)")) {
                for (int id = 1; id <= ROWS; id++) {
                    insert.setInt(1, id);
                    insert.setInt(2, id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + CUSTOMERS + " (id, name) VALUES (?, ?)")) {
                for (int id = 1; id <= ROWS; id++) {
                    insert.setInt(1, id);
                    insert.setString(2, "customer-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }

    private static String sourceYaml(
            String sourceId, String table, Map<String, Object> settings) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(sourceId, settings.get("host"), settings.get("port"),
                        settings.get("database"), settings.get("username"), settings.get("password"),
                        table);
    }

    private static String targetYaml(String targetUri) {
        return """
                version: tapstate/v1
                kind: source
                id: tgt_mongo
                connector: mongodb
                config: { uri: "%s" }
                """
                .formatted(targetUri);
    }

    private static String pipelineYaml(String pipelineId) {
        return pipelineYaml(pipelineId, "src_orders, src_customers", "snapshot_only");
    }

    private static String pipelineYaml(String pipelineId, String sources, String readMode) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ %s ]
                settings: { read_mode: %s }
                transforms:
                  - id: order_doc
                    type: nest
                    from: { o: orders, c: customers }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: c, on: { id: customer_id }, as: object, path: customer }
                serve:
                  from: order_doc
                  sync:
                    - source: tgt_mongo
                """
                .formatted(pipelineId, sources, readMode);
    }
}
