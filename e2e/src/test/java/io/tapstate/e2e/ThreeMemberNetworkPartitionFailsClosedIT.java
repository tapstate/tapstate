package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The network is cut between members while every process stays alive and every store stays reachable.
 *
 * <p>Killing a process is the easy half of high availability and the product already has a case for
 * it. This is the half that kills products: the member carrying the work is still running, still
 * holding its job, still able to reach the coordination store -- it has merely stopped being able to
 * see the others. Nothing about it looks broken from inside it. If the rule that decides who may act
 * is anything other than a strict majority of the committed set, both sides carry on and the same
 * pipeline runs twice against the same source, which is the one outcome no amount of idempotent
 * writing at the target can undo.
 *
 * <p>So the two halves of the rule are read separately, because a single-sided rule passes one of them
 * and a fixed threshold passes the other:
 *
 * <ul>
 *   <li>Cut one member off three. The lone side must stop even though nothing it can see has failed,
 *       and the pair must take the work over without anybody asking.</li>
 *   <li>Cut four members into two and two. Now neither side is a majority, so <em>neither</em> may
 *       act. A threshold pinned at two -- the obvious reading of "we run a cluster of at least two" --
 *       admits both sides here, and admitting both is exactly the double run.</li>
 *   <li>Then let the links carry again. One cluster, and one execution: the number the majority took.
 *       A side that acted while it could not see a majority left a second one behind, and the member
 *       rejoining is precisely the one that would be carrying it.</li>
 * </ul>
 *
 * <p>What makes the cut real rather than decorative is written up in {@link CuttableLink}: each member
 * reports the address of a link in front of it and dials from ports of its own, so a link can tell its
 * callers apart and refuse one of them. Both directions of every severed pair are refused, and the
 * fixture's own count of connections it could not attribute is asserted to be nought -- a partition
 * that holds only because the members were never talking is indistinguishable from one that holds.
 */
class ThreeMemberNetworkPartitionFailsClosedIT {

    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 3;

    private static final String SOURCE_ID = "partition_src";
    private static final String TARGET_ID = "partition_tgt";
    private static final String PIPELINE = "partition_pipe";

    /** Bound on the takeover: the cluster has to notice, then wait out a lease nobody released. */
    private static final Duration TAKEOVER = Duration.ofMinutes(3);

    /**
     * Bound on the precondition: rows crossing before anything is cut.
     *
     * <p>Longer than a single member needs, because this is three or four server processes and a
     * container sharing one machine. Nothing is discriminated here -- the case has not done anything
     * yet -- so the only thing a tighter bound buys is a red that means the machine was busy.
     */
    private static final Duration SEEDING = Duration.ofMinutes(3);

    /** Bound on the membership converging after a cut. Both sides re-dial throughout; neither gets in. */
    private static final Duration SEPARATION = Duration.ofSeconds(120);

    /**
     * Bound on the cluster coming back together once the links carry again.
     *
     * <p>Generous because rejoining two clusters into one is the library's own timer, not this
     * product's: the halves have to notice each other again before anything here can be read.
     */
    private static final Duration REUNION = Duration.ofMinutes(6);

    /**
     * How long the even split is watched for a side that acts anyway.
     *
     * <p>Comfortably past the claim lease, so a side that was going to take over has had its chance:
     * the one-sided cut above proves that a side which may act takes this long at most.
     */
    private static final Duration WATCHED_FOR_ACTION = Duration.ofSeconds(90);

    /** How long the stranded side is given to stand still before this reads as a side still working. */
    private static final Duration STOP_BUDGET = Duration.ofMinutes(2);

    /** The gap between samples, wide enough that a metric merely lagging cannot look like a stop. */
    private static final Duration SAMPLE_GAP = Duration.ofSeconds(10);

    /** How many identical samples in a row count as standing still. */
    private static final int SAMPLES = 3;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    /**
     * One member of three is cut off; it stops, and the two that can still see each other carry on.
     *
     * <p>The member that is cut off is the one driving the pipeline, read off the cluster rather than
     * assumed -- cutting off a member that was doing nothing would prove nothing about stopping.
     */
    @Test
    void theSideThatCannotSeeAMajorityStopsAndThePairTakesOver(@TempDir Path directory) throws IOException {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        Path source = Files.createDirectories(directory.resolve("src"));
        Path target = Files.createDirectories(directory.resolve("tgt"));
        List<String> nodes = List.of("node-a", "node-b", "node-c");

        try (FileEndpoints files = new FileEndpoints()) {
            EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
            EndpointAddress targetAddress = EndpointAddress.uri(target.toString());
            files.seed(sourceAddress, TABLE, SeedRows.generated(SEEDED_ROWS));

            String store = SharedMongo.replicaSetUrl("e2e_partition_three");
            try (PartitionableCluster cluster =
                         PartitionableCluster.start(store, "e2e-partition-three", nodes)) {
                cluster.awaitMembers("node-a", 3);
                ControlPlane control = cluster.member("node-a");
                start(control, connector, source, target);
                Await.until("the seeded rows to cross before the network is cut", SEEDING,
                        () -> files.count(targetAddress, TABLE) >= SEEDED_ROWS,
                        () -> seeding(control, files.count(targetAddress, TABLE)));

                String driver = Await.answered("the cluster to name the member driving the pipeline",
                        () -> control.pipelineControllerOf(PIPELINE));
                long generationBefore = control.executionGenerationOf(PIPELINE).orElseThrow();
                List<String> majority = nodes.stream().filter(node -> !node.equals(driver)).toList();

                cluster.separate(List.of(driver));

                assertThat(cluster.awaitExactly(driver, 1, SEPARATION))
                        .describedAs("the member that was cut off sees only itself. It is still running "
                                + "and its store is still reachable, so nothing it can see has failed")
                        .containsExactly(driver);
                for (String node : majority) {
                    assertThat(cluster.awaitExactly(node, 2, SEPARATION))
                            .describedAs("%s still sees the other side of the cut and nothing else", node)
                            .containsExactlyInAnyOrderElementsOf(majority);
                }
                assertThat(cluster.unattributedConnections())
                        .describedAs("every connection through every link was attributable to a member, "
                                + "so the cut reached all of them. Any that were not would have survived "
                                + "it, and this separation would be a fiction")
                        .isZero();

                // Nothing is asked of the product from here until the assertions below.
                ControlPlane survivor = cluster.member(majority.getFirst());

                // The takeover is waited for before anything is changed at the source, and that order is
                // the whole point. A cut is not a kill: the stranded member's process is alive and its
                // store is reachable, so its run stays authorized until the lease it proved runs out --
                // and the pair cannot take the pipeline over until that same lease expires, because that
                // is what frees the claim. In the stretch between, a change made at the source is
                // carried across by the side that is about to stop. Made before the takeover, the change
                // that is supposed to be the proof of one is the one thing that cannot be.
                //
                // Measured on a shard: the pair took over 26 seconds after the run died, and a change
                // made at the cut had already reached the target well before that, so the reading below
                // was taken while the handover was still owed. The product was doing what it promises --
                // a budget, not an instant. On the kill lane there is no such stretch, because a killed
                // process carries nothing across, which is why only this case needed the order fixed.
                // What is waited for is the execution generation moving, not the state reading RUNNING.
                // The state is what the pipeline was last recorded as, and right after a cut that is
                // still RUNNING -- the run is dead and nobody has written it down yet -- so waiting on it
                // returns at once and witnesses nothing. Only a generation nobody can lower says a new
                // run was taken under this claim.
                try {
                    Await.until("the pair to take the pipeline over, which waits out the lease the stranded "
                                    + "holder never released", TAKEOVER,
                            () -> survivor.executionGenerationOf(PIPELINE)
                                    .filter(taken -> taken == generationBefore + 1).isPresent()
                                    && survivor.state(PIPELINE)
                                    .filter(PipelineState.RUNNING::equals).isPresent(),
                            () -> "the majority reports " + survivor.state(PIPELINE) + " at execution "
                                    + survivor.executionGenerationOf(PIPELINE) + " (was " + generationBefore
                                    + " before the cut), captures owned by "
                                    + survivor.captureOwnersOf(PIPELINE).values());
                } catch (AssertionError failure) {
                    try {
                        retainTakeoverFailure(failure, cluster, store, driver, majority, generationBefore);
                    } catch (RuntimeException | Error unavailable) {
                        failure.addSuppressed(new AssertionError("Passive takeover diagnosis was unavailable: "
                                + unavailable.getClass().getName()));
                    }
                    throw failure;
                }

                long rowsAtTheTakeover = files.count(targetAddress, TABLE);
                files.cdc(sourceAddress, TABLE, CdcOp.INSERT, 1);

                Await.until("a change made after the takeover to reach the target, which no reading of "
                                + "the job's state can stand in for", TAKEOVER,
                        () -> files.count(targetAddress, TABLE) > rowsAtTheTakeover,
                        () -> "rows at target = " + files.count(targetAddress, TABLE)
                                + ", was " + rowsAtTheTakeover + " when the pair took over; the majority "
                                + "reports " + survivor.state(PIPELINE) + " and captures owned by "
                                + survivor.captureOwnersOf(PIPELINE).values());

                assertThat(survivor.state(PIPELINE))
                        .describedAs("the pipeline is still running on the side that kept a majority, "
                                + "and nobody asked it to be")
                        .contains(PipelineState.RUNNING);
                assertThat(survivor.captureOwnersOf(PIPELINE).values())
                        .describedAs("and the source is read from that side. A job back at RUNNING over "
                                + "a source nobody reads is the failure this rules out")
                        .isSubsetOf(majority);
                assertThat(survivor.executionGenerationOf(PIPELINE))
                        .describedAs("exactly one new execution replaced the one stranded on the other "
                                + "side, told apart from it by a number that only goes up")
                        .contains(generationBefore + 1);

                // Fails by timing out if it never stands still: the stranded side's process is alive
                // and its store is reachable, so a rule that asked either of those would have let it
                // carry on running the same pipeline as the majority -- the double run, which no
                // idempotent write at the target would show.
                awaitTheStrandedSideStandingStill(cluster.member(driver));

                cluster.reunite();

                for (String node : nodes) {
                    assertThat(cluster.awaitExactly(node, 3, REUNION))
                            .describedAs("%s is back in one cluster of three once the links carry again",
                                    node)
                            .containsExactlyInAnyOrderElementsOf(nodes);
                }
                assertThat(cluster.member(driver).executionGenerationOf(PIPELINE))
                        .describedAs("and one execution survived the whole thing, the one the majority "
                                + "took. A side that acted while it could not see a majority would have "
                                + "left a second behind, and the member that rejoins would be the one "
                                + "reporting it")
                        .contains(generationBefore + 1);
            }
        }
    }

    /**
     * Four members are cut two against two, and neither side may act.
     *
     * <p>This is the half a threshold cannot express. Two is a perfectly sensible-looking minimum for
     * a cluster of four, and it admits both sides of this cut; what the rule has to ask is whether a
     * side holds a strict majority of the set that was committed, which two of four never does.
     */
    @Test
    void neitherSideOfAnEvenSplitMayAct(@TempDir Path directory) throws IOException {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        Path source = Files.createDirectories(directory.resolve("src"));
        Path target = Files.createDirectories(directory.resolve("tgt"));
        List<String> nodes = List.of("node-a", "node-b", "node-c", "node-d");

        try (FileEndpoints files = new FileEndpoints()) {
            EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
            EndpointAddress targetAddress = EndpointAddress.uri(target.toString());
            files.seed(sourceAddress, TABLE, SeedRows.generated(SEEDED_ROWS));

            String store = SharedMongo.replicaSetUrl("e2e_partition_four");
            try (PartitionableCluster cluster =
                         PartitionableCluster.start(store, "e2e-partition-four", nodes)) {
                cluster.awaitMembers("node-a", 4);
                ControlPlane control = cluster.member("node-a");
                start(control, connector, source, target);
                Await.until("the seeded rows to cross before the network is cut", SEEDING,
                        () -> files.count(targetAddress, TABLE) >= SEEDED_ROWS,
                        () -> seeding(control, files.count(targetAddress, TABLE)));
                long generationBefore = control.executionGenerationOf(PIPELINE).orElseThrow();

                List<String> left = nodes.subList(0, 2);
                List<String> right = nodes.subList(2, 4);
                cluster.separate(left);

                for (String node : nodes) {
                    List<String> side = left.contains(node) ? left : right;
                    assertThat(cluster.awaitExactly(node, 2, SEPARATION))
                            .describedAs("%s sees its own side and nothing of the other", node)
                            .containsExactlyInAnyOrderElementsOf(side);
                }
                assertThat(cluster.unattributedConnections())
                        .describedAs("every connection was attributable, so both cuts reached what they "
                                + "were aimed at")
                        .isZero();

                long rowsAtTheCut = files.count(targetAddress, TABLE);
                files.cdc(sourceAddress, TABLE, CdcOp.INSERT, 1);

                assertThatThrownBy(() -> Await.until(
                        "a change made after the even split to reach the target", WATCHED_FOR_ACTION,
                        () -> files.count(targetAddress, TABLE) > rowsAtTheCut,
                        () -> "rows at target = " + files.count(targetAddress, TABLE)))
                        .describedAs("neither half of four is a majority of four, so the change made "
                                + "after the cut reaches nobody -- given the same budget in which the "
                                + "one-sided cut above delivered one. A minimum pinned at two would "
                                + "admit both sides here and this would arrive, with each side running "
                                + "the same pipeline against the same source")
                        .isInstanceOf(AssertionError.class)
                        .hasMessageContaining("timed out");
                for (String node : nodes) {
                    assertThat(cluster.member(node).executionGenerationOf(PIPELINE))
                            .describedAs("%s took no new execution either; a side that cannot act must "
                                    + "not leave a generation behind that a later one has to outrank",
                                    node)
                            .isIn(Optional.of(generationBefore), Optional.<Long>empty());
                }
            }
        }
    }

    /** Failure-only observations remain bounded and never replace the assertion that called them. */
    private static void retainTakeoverFailure(AssertionError failure, PartitionableCluster cluster, String storeUri,
            String stranded, List<String> survivors, long originalExecutionGeneration) {
        Duration readBound = Duration.ofSeconds(2);
        Map<String, Object> facts = new LinkedHashMap<>();
        facts.put("capturedAt", Instant.now());
        facts.put("clusterId", cluster.clusterId());
        facts.put("pipelineId", PIPELINE);
        facts.put("strandedNode", stranded);
        facts.put("survivingNodes", survivors);
        facts.put("originalExecutionGeneration", originalExecutionGeneration);
        for (String node : survivors) {
            facts.put("typed:" + node, passiveRead(() -> cluster.member(node).clusterStatus(readBound)));
        }
        facts.put("typed:stranded", passiveRead(() -> cluster.member(stranded).clusterStatus(readBound)));
        List<NativeMemberWitness.JobRequest> jobs = new ArrayList<>();
        try {
            ConnectionString connection = new ConnectionString(storeUri);
            MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connection)
                    .timeout(2, TimeUnit.SECONDS)
                    .applyToClusterSettings(value -> value.serverSelectionTimeout(1, TimeUnit.SECONDS))
                    .applyToSocketSettings(value -> value.connectTimeout(1, TimeUnit.SECONDS).readTimeout(2, TimeUnit.SECONDS))
                    .build();
            try (var mongo = MongoClients.create(settings)) {
                MongoDatabase store = mongo.getDatabase(connection.getDatabase()).withTimeout(2, TimeUnit.SECONDS);
                Document clusterScope = new Document("clusterId", cluster.clusterId());
                Document pipelineScope = new Document("_id", PIPELINE);
                facts.put("membership", passiveRows(store, SystemCollections.CLUSTER_MEMBERSHIP,
                        new Document("_id", cluster.clusterId()), null));
                facts.put("profile", passiveRows(store, SystemCollections.CLUSTER_EXECUTION_PROFILES,
                        new Document("_id", cluster.clusterId()), null));
                facts.put("nodeRegistry", passiveRows(store, SystemCollections.CLUSTER_NODE_REGISTRY, clusterScope, null));
                facts.put("claims", passiveRows(store, SystemCollections.WORKLOAD_CLAIMS, clusterScope, null));
                facts.put("desired", passiveRows(store, SystemCollections.PIPELINE_DESIRED, pipelineScope, null));
                facts.put("checkpoint", passiveRows(store, SystemCollections.PIPELINE_STATE, pipelineScope, null));
                facts.put("observation", passiveRows(store, SystemCollections.PIPELINE_OBSERVATION, pipelineScope, null));
                facts.put("artifactIdentities", passiveRows(store, SystemCollections.ARTIFACTS,
                        new Document("_id", new Document("$in", List.of(PIPELINE, SOURCE_ID, TARGET_ID))),
                        new Document("_id", 1).append("kind", 1).append("incarnation", 1).append("contentHash", 1)));
                facts.put("queue", passiveRows(store, SystemCollections.CLUSTER_RECOVERY_QUEUE,
                        new Document(clusterScope).append("pipelineId", PIPELINE), null));
                Document capacity = passiveRows(store, SystemCollections.CLUSTER_CAPACITY_OCCUPANCY,
                        new Document(clusterScope).append("pipelineId", PIPELINE), null);
                facts.put("capacity", capacity);
                if (capacity.get("documents") instanceof List<?> receipts) {
                    for (Object receipt : receipts) {
                        if (!(receipt instanceof Document row)) {
                            facts.put("nativeRequestUnavailable", "The capacity reading contained an unknown receipt type");
                            continue;
                        }
                        if (row.get("nativeJobId") == null) { continue; }
                        try {
                            Document claim = row.get("pipelineClaim", Document.class);
                            long cg = positive(claim, "claimGeneration");
                            long eg = positive(claim, "executionGeneration");
                            long pg = positive(claim, "profileGeneration");
                            String job = row.getString("nativeJobId");
                            if (!PIPELINE.equals(row.getString("pipelineId")) || job == null || job.isBlank()
                                    || positive(row, "executionGeneration") != eg
                                    || positive(row.get("profile", Document.class), "generation") != pg) {
                                throw new AssertionError("The submitted capacity receipt has inconsistent execution identity");
                            }
                            jobs.add(new NativeMemberWitness.JobRequest(PIPELINE, job, cg, eg, pg));
                        } catch (RuntimeException | AssertionError unavailable) {
                            facts.put("nativeRequestUnavailable:" + row.get("_id"), unavailable.getClass().getName());
                        }
                    }
                } else {
                    facts.put("nativeRequestUnavailable", "The submitted capacity receipts could not be read");
                }
            }
        } catch (RuntimeException | AssertionError unavailable) {
            facts.put("mongoUnavailable", unavailable.getClass().getName());
        }
        facts.put("nativeRequests", jobs);
        for (String node : survivors) {
            try (NativeMemberWitness nativeReader = NativeMemberWitness.connect(cluster, node, readBound)) {
                facts.put("nativeSubmittedJobs:" + node, passiveRead(() -> nativeReader.submittedJobs(readBound)));
                facts.put("nativeProjection:" + node, passiveRead(() -> nativeReader.projectionReading(jobs, readBound)));
            } catch (RuntimeException | AssertionError unavailable) {
                facts.put("nativeUnavailable:" + node, unavailable.getClass().getName());
            }
        }
        try {
            Path scene = Path.of(System.getProperty("tapstate.e2e.build-directory", "target"))
                    .resolve("failure-scenes").resolve("partition-takeover").resolve(cluster.clusterId());
            Files.createDirectories(scene);
            var json = new tools.jackson.databind.ObjectMapper();
            String snapshot = json.writeValueAsString(redactPassive(json.convertValue(facts, Object.class)))
                    .replaceAll("[a-zA-Z][a-zA-Z0-9+.-]*://[^\\s\"']+", "<address elided>");
            Files.writeString(scene.resolve("takeover-failure.json"), snapshot);
            failure.addSuppressed(new AssertionError("Passive takeover facts retained at " + scene));
        } catch (IOException | RuntimeException unavailable) {
            failure.addSuppressed(new AssertionError("Passive takeover facts could not be retained: "
                    + unavailable.getClass().getName()));
        }
    }

    private static Object passiveRead(Supplier<?> reading) {
        try { return reading.get(); }
        catch (RuntimeException | AssertionError unavailable) {
            return Map.of("readUnavailable", unavailable.getClass().getName());
        }
    }

    private static Document passiveRows(MongoDatabase store, SystemCollections collection, Document scope, Document fields) {
        Document reading = new Document("collection", collection.collectionName()).append("scope", scope);
        try {
            List<Document> stages = new ArrayList<>();
            stages.add(new Document("$match", scope));
            if (fields != null) { stages.add(new Document("$project", fields)); }
            stages.add(new Document("$addFields", new Document("diagnosticMongoNow", "$$NOW")
                    .append("leaseRemainingMillis", new Document("$subtract", List.of("$leaseUntil", "$$NOW")))
                    .append("retiredAuthorizationRemainingMillis", new Document("$subtract", List.of("$retiredAuthorizationUntil", "$$NOW")))
                    .append("deadlineRemainingMillis", new Document("$subtract", List.of("$deadline", "$$NOW")))));
            reading.append("documents", collection.on(store).aggregate(stages)
                    .maxTime(2, TimeUnit.SECONDS).into(new ArrayList<>()));
        } catch (RuntimeException unavailable) {
            reading.append("readUnavailable", unavailable.getClass().getName());
        }
        return reading;
    }

    private static long positive(Document document, String name) {
        if (document == null || !(document.get(name) instanceof Number number) || number.longValue() < 1) {
            throw new AssertionError("The observed receipt has no positive " + name);
        }
        return number.longValue();
    }

    private static Object redactPassive(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> redacted = new LinkedHashMap<>();
            map.forEach((key, entry) -> {
                String name = String.valueOf(key);
                String normalized = name.toLowerCase(java.util.Locale.ROOT).replaceAll("[^a-z]", "");
                redacted.put(name, normalized.contains("password") || normalized.contains("credential")
                        || normalized.contains("secret") || normalized.contains("config") || normalized.endsWith("uri")
                        || normalized.endsWith("url") || normalized.equals("authorization") || normalized.equals("apikey")
                        || normalized.equals("accesstoken") || normalized.equals("refreshtoken")
                                ? "<redacted>" : redactPassive(entry));
            });
            return redacted;
        }
        if (value instanceof List<?> list) { return list.stream().map(ThreeMemberNetworkPartitionFailsClosedIT::redactPassive).toList(); }
        return value;
    }

    /**
     * Waits for the stranded side's own count to stand still, and fails if it never does.
     *
     * <p>A wait rather than a fixed pause, and the difference is not stylistic. Read once across the
     * cut the figure means nothing: the engine publishes the last value it collected, so a count taken
     * just after the cut is still catching up with work done before it. What says the work stopped is
     * the value not moving, and expressed as a wait, a side that never stops reports that as a timeout
     * carrying every reading rather than as a pause that happened to end at a quiet moment.
     *
     * <p>The count is the one figure here the stranded member answers from its own run rather than from
     * the store both sides share, so it is the only one that can say anything about this side alone.
     * The state and the owners come from that shared store and are carried only for whoever reads a
     * failure.
     */
    private static void awaitTheStrandedSideStandingStill(ControlPlane stranded) {
        List<Long> recorded = new ArrayList<>();
        AtomicLong lastSampledAt = new AtomicLong(System.nanoTime() - SAMPLE_GAP.toNanos());
        Await.until("the stranded side's own count to stand still", STOP_BUDGET,
                () -> {
                    long now = System.nanoTime();
                    if (now - lastSampledAt.get() < SAMPLE_GAP.toNanos()) {
                        return false;
                    }
                    lastSampledAt.set(now);
                    // Nothing left to report is the strongest form of stopped, and is recorded as a
                    // value of its own so that a side which stops reporting halfway through does not
                    // read as a side that was standing still all along.
                    recorded.add(countOn(stranded).orElse(-1L));
                    return recorded.size() >= SAMPLES && recorded.subList(recorded.size() - SAMPLES,
                            recorded.size()).stream().distinct().count() == 1;
                },
                () -> "counts " + recorded + ", alongside " + stateOn(stranded) + " owned by "
                        + ownersOn(stranded));
    }

    private static String stateOn(ControlPlane plane) {
        try {
            return String.valueOf(plane.state(PIPELINE));
        } catch (RuntimeException | AssertionError refused) {
            // A diagnosis that throws replaces the failure it was written to explain. The read face
            // refuses with an assertion of its own, which is exactly the state this is called in.
            return "refused:" + refused.getClass().getSimpleName() + ": " + refused.getMessage();
        }
    }

    private static String ownersOn(ControlPlane plane) {
        try {
            return String.valueOf(plane.captureOwnersOf(PIPELINE).values());
        } catch (RuntimeException | AssertionError refused) {
            // A diagnosis that throws replaces the failure it was written to explain. The read face
            // refuses with an assertion of its own, which is exactly the state this is called in.
            return "refused:" + refused.getClass().getSimpleName() + ": " + refused.getMessage();
        }
    }

    private static Optional<Long> countOn(ControlPlane plane) {
        try {
            return plane.recordCount(PIPELINE);
        } catch (RuntimeException | AssertionError refused) {
            // A side that refuses to answer for the pipeline at all is not advancing it either, and a
            // refusal arrives from the read face as an assertion rather than as an exception.
            return Optional.empty();
        }
    }

    /**
     * What the precondition reports when nothing crosses, which is the reading that decides where to
     * look next.
     *
     * <p>"No rows" on its own sends the reader to the machine's load, and that is the wrong place:
     * this same shape has been a live product defect more than once, where the job is cancelled
     * seconds after it starts and nothing rebuilds it. The state, the failure code and who owns the
     * captures separate the two without anybody having to go and read four servers' logs.
     */
    private static String seeding(ControlPlane control, long rows) {
        return "rows at target = " + rows + "; the pipeline is " + stateOn(control)
                + ", its failure code is " + failureOn(control)
                + ", its captures are owned by " + ownersOn(control);
    }

    private static String failureOn(ControlPlane plane) {
        try {
            return String.valueOf(plane.failureCode(PIPELINE));
        } catch (RuntimeException | AssertionError refused) {
            // A diagnosis that throws replaces the failure it was written to explain. The read face
            // refuses with an assertion of its own, which is exactly the state this is called in.
            return "refused:" + refused.getClass().getSimpleName() + ": " + refused.getMessage();
        }
    }

    private static void start(ControlPlane control, byte[] connector, Path source, Path target) {
        control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
        control.discoverSchema(SOURCE_ID, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
        control.apply(resources(source, target));
        control.lifecycle(PIPELINE, LifecycleVerb.START);
        Await.until(PIPELINE + " to reach " + PipelineState.RUNNING, Duration.ofMinutes(1),
                () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> String.valueOf(control.state(PIPELINE)));
    }

    private static Map<String, String> resources(Path source, Path target) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE_ID + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE_ID, source));
        resources.put(TARGET_ID + ".tap.yml", Workspaces.targetYaml(TARGET_ID, target));
        resources.put(PIPELINE + ".tap.yml", Workspaces.pipelineYaml(PIPELINE, SOURCE_ID, TARGET_ID, TABLE));
        return resources;
    }
}
