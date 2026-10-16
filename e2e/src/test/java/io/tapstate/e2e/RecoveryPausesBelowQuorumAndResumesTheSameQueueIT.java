package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.tapstate.adapters.mongostore.MongoDerivedSchemaStore;
import io.tapstate.adapters.mongostore.MongoDesiredStore;
import io.tapstate.adapters.mongostore.MongoSchemaStore;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.ClusterClaimView;
import io.tapstate.control.core.ClusterMemberState;
import io.tapstate.control.core.ClusterMemberView;
import io.tapstate.control.core.ClusterPipelineView;
import io.tapstate.control.core.ClusterRecoveryItemView;
import io.tapstate.control.core.ClusterRecoveryView;
import io.tapstate.control.core.ClusterTopologyView;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.lifecycle.DesiredStateFingerprint;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.ProcessorRuntimeContext;
import io.tapstate.spi.store.DerivedSchema;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A real schema publication holds recovery before allocation while the actual member links lose quorum. */
class RecoveryPausesBelowQuorumAndResumesTheSameQueueIT {
    private static final List<String> NODES = List.of("node-a", "node-b", "node-c");
    private static final Duration SETUP = Duration.ofMinutes(3);
    private static final Duration RECOVERY = Duration.ofMinutes(4);
    private static final Duration WATCH = Duration.ofSeconds(3);
    private static final Duration CLEANUP_MARGIN = Duration.ofSeconds(8);
    private static final Duration READ_BOUND = Duration.ofSeconds(2);
    private static final Set<String> SOURCE_DATA_ENTRIES = Set.of("BATCH_READ", "STREAM_READ", "TIMESTAMP_OFFSET",
            "COUNT", "WRITE", "CLEAR", "TABLE_NAMES", "TABLE_INFO", "COMMAND", "DISCOVER",
            "CONNECTION_TEST", "TABLE_COUNT", "INIT", "LIGHT_INIT");
    private static final List<String> SUFFIXES = List.of("a", "b", "c");

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @Test
    void theUnallocatedStepWaitsForTheSameQuorumAndContinuesOnce(@TempDir Path directory) throws Exception {
        String run = UUID.randomUUID().toString().replace("-", "");
        String uri = SharedMongo.replicaSetUrl("e2e_quorum_pause_" + run);
        Path source = Files.createDirectories(directory.resolve("shared-source"));
        Path reads = Files.createDirectories(directory.resolve("shared-reads"));
        Path sourceEntries = directory.resolve("source-entries.tsv");
        Path writerEntries = directory.resolve("writer-entries.tsv");
        Path originalSdk = E2eConnectorJar.buildInto(Files.createDirectories(directory.resolve("connector")));
        byte[] connector = ObservedCsvConnectorJar.build(Files.readAllBytes(originalSdk), sourceEntries);
        byte[] targets = ObservedMongoConnectorJar.build(ConnectorJars.bytesFor("mongodb"), writerEntries);
        try (FileEndpoints files = new FileEndpoints(); MongoClient mongo = MongoClients.create(readSettings(uri));
                MongoClient holderClient = MongoClients.create(holderSettings(uri));
                PartitionableCluster cluster = PartitionableCluster.start(uri, "quorum-schema-pause", NODES)) {
            MongoDatabase store = mongo.getDatabase(new ConnectionString(uri).getDatabase());
            List<Lane> lanes = new ArrayList<>();
            for (String suffix : SUFFIXES) {
                String targetUri = SharedMongo.replicaSetUrl("e2e_quorum_target_" + run + "_" + suffix);
                lanes.add(new Lane("quorum_pause_" + suffix, "quorum_source_shared", "quorum_target_" + suffix,
                        "orders", source, targetUri, mongo.getDatabase(new ConnectionString(targetUri).getDatabase()), reads));
            }
            ControlPlane initial = cluster.member(NODES.getFirst());
            awaitActive(initial, NODES, SETUP);
            ClusterRecoveryItemView.Profile profile = initial.clusterProfile();
            assertThat(profile).isNotNull();
            initial.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
            initial.registerConnector("mongodb", targets);
            Map<String, String> resources = new LinkedHashMap<>();
            for (Lane lane : lanes) {
                publish(lane, false, 1);
                resources.put(lane.source() + ".tap.yml", Workspaces.cdcSourceYaml(lane.source(), lane.sourceDirectory(),
                        List.of(lane.table()), Map.of("read_witness", lane.reads().toString())));
                resources.put(lane.target() + ".tap.yml", """
                        version: tapstate/v1
                        kind: source
                        id: %s
                        connector: mongodb
                        config: { uri: "%s" }
                        """.formatted(lane.target(), lane.targetUri()));
                resources.put(lane.pipeline() + ".tap.yml",
                        Workspaces.pipelineYaml(lane.pipeline(), lane.source(), lane.target(), lane.table(), 3));
            }
            initial.apply(resources);
            Map<String, ClusterClaimView> oldClaims = new LinkedHashMap<>();
            Map<String, NativeIds> oldNative = new LinkedHashMap<>();
            Map<String, List<Authority>> oldAuthorities = new LinkedHashMap<>();
            Map<String, Truth> truth = new LinkedHashMap<>();
            Map<String, String> originalFloors = new LinkedHashMap<>();
            Map<String, Long> originalSnapshotReads = new LinkedHashMap<>();
            Map<String, DerivedSchema> originalSchemas = new LinkedHashMap<>();
            MongoDerivedSchemaStore derived = new MongoDerivedSchemaStore(SystemCollections.DERIVED_SCHEMAS.on(store));
            initial.discoverSchema(lanes.getFirst().source(), E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            for (Lane lane : lanes) {
                initial.lifecycle(lane.pipeline(), LifecycleVerb.START);
                Await.until(lane.pipeline() + " to run its complete original cohort", SETUP,
                        () -> initial.state(lane.pipeline()).filter(PipelineState.RUNNING::equals).isPresent()
                                && initial.membersCarryingPartOf(lane.pipeline()).equals(Set.copyOf(NODES))
                                && rowPresent(lane, 1), () -> String.valueOf(initial.clusterStatus()));
                Await.until(lane.pipeline() + " to finish its natural snapshot", SETUP,
                        () -> initial.snapshotTables(lane.pipeline()).containsKey(lane.table())
                                && initial.snapshotTables(lane.pipeline()).get(lane.table()).landed(),
                        () -> "snapshot=" + initial.snapshotTables(lane.pipeline()));
            }
            publish(lanes.getFirst(), false, 2);
            for (Lane lane : lanes) {
                Await.until(lane.pipeline() + " to acknowledge a real new CDC row", SETUP,
                        () -> rowPresent(lane, 2) && positionLines(lane).stream()
                                .anyMatch(line -> line.contains("\tDELIVERY\t" + lane.table() + "\t")
                                        && line.contains(lane.table() + "=2")),
                        () -> "positions=" + positionLines(lane) + "; writer entries=" + lines(writerEntries));
                originalFloors.put(lane.pipeline(), awaitConfirmedFloor(initial, store, lane));
                originalSnapshotReads.put(lane.pipeline(), snapshotReads(lane));
                ClusterClaimView original = claim(initial, lane);
                assertThat(original.executionContextCurrent()).isTrue();
                assertThat(original.executionClaimGeneration()).isEqualTo(original.claimGeneration());
                assertThat(original.executionMembers().stream().map(ClusterClaimView.Member::nodeId))
                        .containsExactlyInAnyOrderElementsOf(NODES);
                oldClaims.put(lane.pipeline(), original);
                oldNative.put(lane.pipeline(), awaitNative(initial, lane, original, NODES, SETUP));
                oldAuthorities.put(lane.pipeline(), authorities(store, cluster.clusterId(), original,
                        pipeline(initial, lane).captureClaims()));
                truth.put(lane.pipeline(), truth(store, lane));
                DerivedSchema schema = derived.pinned(lane.pipeline(), lane.step()).orElseThrow();
                assertThat(schema.version()).isZero();
                assertThat(schema.schema()).containsExactly(Map.entry("id", "STRING NULL"), Map.entry("seq", "STRING NULL"));
                originalSchemas.put(lane.pipeline(), schema);
            }
            assertThat(queue(store, cluster.clusterId())).isEmpty();
            String victim = initial.captureOwnersOf(lanes.getFirst().pipeline()).values().stream().findFirst().orElseThrow();
            Set<String> captures = new java.util.HashSet<>();
            for (Lane lane : lanes) {
                Map<String, String> owners = initial.captureOwnersOf(lane.pipeline());
                assertThat(owners).hasSize(1);
                assertThat(owners.values()).containsExactly(victim);
                captures.add(owners.keySet().iterator().next());
                assertThat(oldClaims.get(lane.pipeline()).executionMembers().stream().map(ClusterClaimView.Member::nodeId))
                        .contains(victim);
            }
            assertThat(captures).as("all pipelines share the same actual physical capture").hasSize(1);
            List<String> survivors = NODES.stream().filter(node -> !node.equals(victim)).toList();
            ControlPlane discoveryMember = cluster.member(survivors.getFirst());
            ClusterMemberView discoveryBoot = member(discoveryMember, survivors.getFirst());
            assertJoined(store, cluster.clusterId(), discoveryBoot, profile);
            Map<String, Map<String, String>> discoveredColumns = new LinkedHashMap<>();
            publish(lanes.getFirst(), true, 2);
            discoveryMember.discoverSchema(lanes.getFirst().source(), E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            for (Lane lane : lanes) {
                assertThat(discoveryMember.sourceSchemaFields(lane.source(), lane.table())).containsExactly("id", "seq", "note");
                var model = new MongoSchemaStore(SystemCollections.SOURCE_SCHEMAS.on(store)).get(lane.source()).orElseThrow();
                assertThat(model.connectorId()).isEqualTo(E2eConnectorJar.CONNECTOR_ID);
                SourceTable table = model.model().tables().stream().filter(value -> lane.table().equals(value.name())).findFirst().orElseThrow();
                assertThat(table.fields()).allSatisfy(field -> {
                    assertThat(field.dataType()).isEqualTo("string");
                    assertThat(field.type()).isEqualTo(TapstateType.STRING);
                });
                Map<String, String> columns = new LinkedHashMap<>();
                table.fields().forEach(field -> columns.put(field.name(), "STRING NULL"));
                assertThat(columns).containsEntry("note", "STRING NULL");
                discoveredColumns.put(lane.pipeline(), Map.copyOf(columns));
                assertThat(derived.pinned(lane.pipeline(), lane.step())).contains(originalSchemas.get(lane.pipeline()));
                assertThat(truth(store, lane)).isEqualTo(truth.get(lane.pipeline()));
            }
            Set<String> pipelineIds = lanes.stream().map(Lane::pipeline).collect(Collectors.toSet());
            QueueTrace trace = new QueueTrace(pipelineIds);
            Map<String, ClusterMemberView> survivorBoots = survivors.stream().collect(Collectors.toMap(
                    node -> node, node -> member(cluster.member(node), node)));
            Map<String, ClusterMemberView> originalBoots = initial.clusterStatus().members().stream()
                    .filter(value -> NODES.contains(value.nodeId())).collect(Collectors.toMap(ClusterMemberView::nodeId, value -> value));
            assertThat(originalBoots).hasSize(3);
            assertThat(sourceDataEntries(sourceEntries)).isNotEmpty();
            assertThat(lines(writerEntries)).isNotEmpty();
            long writesBeforeLoss = writerRows(writerEntries);
            int writerCallsBeforeLoss = lines(writerEntries).size();
            long batchCallsBeforeLoss = operationCount(sourceEntries, "BATCH_READ");
            try (NativeReaders nativeReaders = new NativeReaders(cluster, survivors, originalBoots, profile, store);
                    MongoChangeStreamCursor<ChangeStreamDocument<Document>> changes =
                    SystemCollections.CLUSTER_RECOVERY_QUEUE.on(store).watch().maxAwaitTime(100, TimeUnit.MILLISECONDS).cursor()) {
                Set<String> originalJobs = oldNative.values().stream().map(NativeIds::job).collect(Collectors.toSet());
                nativeReaders.assertInventory(originalJobs, null);
                nativeReaders.assertMembers(Set.copyOf(NODES));
                trace.drain(changes);
                cluster.kill(victim);
                assertThat(cluster.isAlive(victim)).isFalse();
                Await.until("the independently observed surviving pair", SETUP,
                        () -> nativeReaders.sameMembers(Set.copyOf(survivors)), nativeReaders::describe);
                Await.until("the three original executions to enter their durable queue", SETUP,
                        () -> { trace.drain(changes); return trace.inserted.keySet().equals(pipelineIds); },
                        () -> trace.describe());
                assertThat(queue(store, cluster.clusterId())).hasSize(3);
                awaitLateArm(store, cluster.clusterId(), lanes, oldClaims, trace, changes);
                Number lifetime = mongo.getDatabase("admin").runCommand(
                        new Document("getParameter", 1).append("transactionLifetimeLimitSeconds", 1))
                        .get("transactionLifetimeLimitSeconds", Number.class);
                assertThat(lifetime).isNotNull();
                try (SchemaHold hold = new SchemaHold(holderClient, store, lanes, Duration.ofSeconds(lifetime.longValue()))) {
                    try {
                        Blocked blocked = awaitBlocked(mongo, store, cluster.clusterId(), survivorBoots, profile, lanes,
                                oldClaims, originalSchemas, discoveredColumns, trace, changes, hold);
                        nativeReaders.assertInventory(originalJobs, hold);
                        Map<String, Step> paused = steps(trace.latest);
                        assertThat(paused.values()).allSatisfy(step -> assertThat(step.attempt()).isZero());
                        assertThat(trace.latest.values().stream().filter(row -> row.get("permit") != null).count()).isEqualTo(1);
                        Document firstPermit = trace.latest.get(blocked.pipeline()).get("permit", Document.class);
                        assertThat(number(firstPermit, "transferredExecutionGeneration")).isZero();
                        assertThat(trace.latest.get(blocked.pipeline()).get("successor")).isNull();
                        trace.freeze(paused);
                        String stranded = survivors.stream().filter(node -> !node.equals(blocked.member().nodeId())).findFirst().orElseThrow();
                        cluster.separate(List.of(stranded));
                        awaitPhase("both actual native survivors to become singleton views", Duration.ofSeconds(5), hold,
                                () -> nativeReaders.singletons(), nativeReaders::describe);
                        assertThat(cluster.unattributedConnections()).isZero();
                        ControlPlane alone = cluster.member(blocked.member().nodeId());
                        awaitPhase("the real survivor read face to report below quorum", Duration.ofSeconds(4), hold,
                                () -> Boolean.FALSE.equals(alone.clusterRecovery(READ_BOUND).quorumReady()), () -> "quorum is not yet false");
                        assertCommitted(store, cluster.clusterId(), profile);
                        Duration retirement = retirementBudget(store, oldAuthorities.values().stream().flatMap(List::stream).distinct().toList(), hold);
                        awaitPhase("only the exact original effect authorities to retire", retirement, hold,
                                () -> {
                                    trace.drain(changes);
                                    return authorityBounds(store, oldAuthorities.values().stream().flatMap(List::stream).distinct().toList())
                                            .retired();
                                },
                                () -> "original authorities still hold a store promise");
                        checked(hold, () -> { trace.drain(changes); return true; });
                        trace.freezePermitPayloads(checked(hold, () -> queue(store, cluster.clusterId())));
                        Map<String, List<String>> readsBefore = readSnapshot(lanes);
                        List<String> sourceCallsBefore = sourceDataEntries(sourceEntries);
                        List<String> writerCallsBefore = lines(writerEntries);
                        publish(lanes.getFirst(), true, 3);
                        watchBelowQuorum(store, cluster, survivors, alone, profile, lanes, oldClaims, oldNative,
                                trace, changes, readsBefore, sourceEntries, writerEntries, sourceCallsBefore, writerCallsBefore,
                                nativeReaders, originalJobs, hold);
                        hold.remaining(Duration.ofSeconds(3));
                        hold.abort();
                        hold.remaining(Duration.ZERO);
                        watchBelowQuorum(store, cluster, survivors, alone, profile, lanes, oldClaims, oldNative,
                                trace, changes, readsBefore, sourceEntries, writerEntries, sourceCallsBefore, writerCallsBefore,
                                nativeReaders, originalJobs, hold);
                        trace.thaw();
                        cluster.reunite();
                        for (String survivor : survivors) {
                            awaitActive(cluster.member(survivor), survivors, RECOVERY);
                        }
                        nativeReaders.requalifyAfterHeal(cluster, store, survivors);
                        Await.until("both actual native views to match the current qualified surviving cohort", RECOVERY,
                                () -> nativeReaders.sameMembers(Set.copyOf(survivors)), nativeReaders::describe);
                        ControlPlane restored = cluster.member(survivors.getFirst());
                        Await.until("each same item to finish one actual successor", RECOVERY, () -> {
                            trace.drain(changes);
                            return trace.latest.values().stream().allMatch(row -> "RECOVERED".equals(row.getString("status")))
                                    && lanes.stream().allMatch(lane -> rowPresent(lane, 3));
                        }, () -> trace.describe() + "; recovery=" + restored.clusterRecovery());
                        trace.drain(changes);
                        NativeProjectionEvidence nativeEvidence = new NativeProjectionEvidence(cluster.clusterId(), pipelineIds,
                                nativeReaders, restored, trace.latest);
                        nativeEvidence.retain("before-native-await", null);
                        try {
                            for (Lane lane : lanes) {
                                Document completed = trace.latest.get(lane.pipeline());
                                Step before = paused.get(lane.pipeline());
                                assertThat(completed.get("_id")).isEqualTo(before.id());
                                assertThat(number(completed, "enqueueSequence")).isEqualTo(before.sequence());
                                assertThat(number(completed, "attempt")).isEqualTo(before.attempt() + 1);
                                ClusterClaimView successor = claim(restored, lane);
                                assertThat(successor.executionGeneration()).isEqualTo(oldClaims.get(lane.pipeline()).executionGeneration() + 1);
                                ClusterRecoveryItemView item = restored.clusterRecovery().items().stream()
                                        .filter(value -> lane.pipeline().equals(value.pipelineId())).findFirst().orElseThrow();
                                assertThat(item.persistedStatus()).isEqualTo("RECOVERED");
                                assertThat(item.successor().sourceRequirementsRecorded()).isTrue();
                                assertThat(item.successor().requiredSourceIds()).containsExactly(lane.source());
                                assertThat(item.successor().requestedPositions()).containsOnlyKeys(lane.source());
                                assertThat(item.successor().acceptedPositions()).containsOnlyKeys(lane.source());
                                assertThat(item.originalPositions().get(lane.source()).token()).isEqualTo(originalFloors.get(lane.pipeline()));
                                assertThat(item.successor().nativeInitializedAt()).isNotNull();
                                assertThat(item.successor().sourcesAcceptedAt()).isNotNull();
                                NativeIds actual = awaitNative(restored, lane, successor, survivors, RECOVERY, nativeEvidence::typed);
                                assertThat(actual.job()).isEqualTo(item.successor().nativeJobId()).isNotEqualTo(oldNative.get(lane.pipeline()).job());
                                DerivedSchema advanced = derived.pinned(lane.pipeline(), lane.step()).orElseThrow();
                                assertThat(advanced.version()).isEqualTo(1);
                                assertThat(advanced.schema()).isEqualTo(discoveredColumns.get(lane.pipeline()));
                                assertThat(advanced.derivedFrom()).isNotEqualTo(originalSchemas.get(lane.pipeline()).derivedFrom());
                                assertThat(snapshotReads(lane)).as("completed source tables are not loaded again")
                                        .isEqualTo(originalSnapshotReads.get(lane.pipeline()));
                                assertThat(truth(store, lane)).isEqualTo(truth.get(lane.pipeline()));
                            }
                        } catch (RuntimeException | Error failure) {
                            nativeEvidence.retain("native-await-failure", failure);
                            throw failure;
                        }
                        nativeEvidence.retain("native-await-success", null);
                        Map<String, Long> stableGenerations = lanes.stream().collect(Collectors.toMap(Lane::pipeline,
                                lane -> claim(restored, lane).executionGeneration()));
                        Map<String, Long> stableRevisions = trace.latest.entrySet().stream().collect(Collectors.toMap(
                                Map.Entry::getKey, entry -> number(entry.getValue(), "itemRevision")));
                        long stableUntil = System.nanoTime() + WATCH.toNanos();
                        while (System.nanoTime() < stableUntil) {
                            trace.drain(changes);
                            for (Lane lane : lanes) {
                                assertThat(claim(restored, lane).executionGeneration()).isEqualTo(stableGenerations.get(lane.pipeline()));
                                assertThat(number(trace.latest.get(lane.pipeline()), "itemRevision")).isEqualTo(stableRevisions.get(lane.pipeline()));
                            }
                            Await.pauseBeforeNextPoll(Duration.ofMillis(100));
                        }
                        for (Lane lane : lanes) {
                            restored.stop(lane.pipeline(), false);
                            Await.until(lane.pipeline() + " to close its actual writers", SETUP,
                                    () -> restored.state(lane.pipeline()).filter(PipelineState.STOPPED::equals).isPresent(),
                                    () -> String.valueOf(restored.state(lane.pipeline())));
                            assertThat(lane.targetDatabase().getCollection(lane.table()).countDocuments()).isEqualTo(3);
                        }
                        assertThat(writerRows(writerEntries) - writesBeforeLoss)
                                .as("three source sentinels require exactly three physical delegate row attempts").isEqualTo(3);
                        assertThat(lines(writerEntries)).hasSize(writerCallsBeforeLoss + 3);
                        assertThat(operationCount(sourceEntries, "BATCH_READ")).isEqualTo(batchCallsBeforeLoss);
                        Set<String> finalJobs = new java.util.HashSet<>(originalJobs);
                        lanes.forEach(lane -> finalJobs.add(trace.latest.get(lane.pipeline()).get("successor", Document.class)
                                .getString("nativeJobId")));
                        assertThat(finalJobs).hasSize(6);
                        nativeReaders.assertInventory(finalJobs, null);
                        trace.drain(changes);
                        assertThat(trace.inserted).hasSize(3);
                        assertThat(trace.peakPermits).isEqualTo(1);
                    } finally {
                        try { hold.abort(); }
                        finally { cluster.reunite(); }
                    }
                }
            }
        }
    }

    private static void publish(Lane lane, boolean note, int through) {
        StringBuilder bytes = new StringBuilder(note ? "id,seq,note\n" : "id,seq\n");
        for (int id = 1; id <= through; id++) bytes.append(id).append(',').append(id).append('\n');
        FileEndpoints.replaceTable(lane.sourceDirectory().resolve(lane.table() + ".csv"), bytes.toString());
    }

    private static boolean rowPresent(Lane lane, int id) {
        return lane.targetDatabase().getCollection(lane.table()).find(new Document("id", Integer.toString(id))
                .append("seq", Integer.toString(id))).first() != null;
    }

    private static MongoClientSettings readSettings(String uri) {
        return MongoClientSettings.builder().applyConnectionString(new ConnectionString(uri))
                .timeout(2, TimeUnit.SECONDS)
                .applyToClusterSettings(settings -> settings.serverSelectionTimeout(1, TimeUnit.SECONDS))
                .applyToSocketSettings(settings -> settings.connectTimeout(1, TimeUnit.SECONDS).readTimeout(2, TimeUnit.SECONDS))
                .build();
    }

    private static MongoClientSettings holderSettings(String uri) {
        return MongoClientSettings.builder().applyConnectionString(new ConnectionString(uri))
                .applyToClusterSettings(settings -> settings.serverSelectionTimeout(1, TimeUnit.SECONDS))
                .applyToSocketSettings(settings -> settings.connectTimeout(1, TimeUnit.SECONDS).readTimeout(3, TimeUnit.SECONDS))
                .build();
    }

    private static List<String> lines(Path path) {
        if (!Files.exists(path)) return List.of();
        try { return Files.readAllLines(path); }
        catch (java.io.IOException unavailable) { throw new java.io.UncheckedIOException(unavailable); }
    }

    private static List<String> sourceDataEntries(Path path) {
        return lines(path).stream().filter(line -> {
            String[] fields = line.split("\t", -1);
            assertThat(fields).hasSize(4);
            return SOURCE_DATA_ENTRIES.contains(fields[1]);
        }).toList();
    }

    private static long writerRows(Path path) {
        return lines(path).stream().mapToLong(line -> {
            String[] fields = line.split("\t", -1);
            assertThat(fields).hasSize(3);
            return Long.parseLong(fields[1]);
        }).sum();
    }

    private static long operationCount(Path path, String operation) {
        return lines(path).stream().filter(line -> {
            String[] fields = line.split("\t", -1);
            assertThat(fields).hasSize(4);
            return operation.equals(fields[1]);
        }).count();
    }

    private static List<String> positionLines(Lane lane) {
        try (Stream<Path> files = Files.list(lane.reads())) {
            List<String> lines = new ArrayList<>();
            for (Path file : files.filter(path -> path.getFileName().toString().startsWith("positions-")).sorted().toList())
                lines.addAll(Files.readAllLines(file));
            return List.copyOf(lines);
        } catch (java.io.IOException invalid) { throw new java.io.UncheckedIOException(invalid); }
    }

    private static Map<String, List<String>> readSnapshot(List<Lane> lanes) {
        return lanes.stream().collect(Collectors.toMap(Lane::pipeline, RecoveryPausesBelowQuorumAndResumesTheSameQueueIT::readerLines));
    }

    private static List<String> readerLines(Lane lane) {
        try (Stream<Path> files = Files.list(lane.reads())) {
            List<String> lines = new ArrayList<>();
            for (Path file : files.filter(path -> {
                String name = path.getFileName().toString();
                return name.startsWith("reads-") || name.startsWith("positions-");
            }).sorted().toList())
                Files.readAllLines(file).forEach(line -> lines.add(file.getFileName() + "\t" + line));
            return List.copyOf(lines);
        } catch (java.io.IOException invalid) { throw new java.io.UncheckedIOException(invalid); }
    }

    private static long snapshotReads(Lane lane) {
        return readerLines(lane).stream().map(line -> line.split("\t", -1))
                .filter(cells -> cells.length == 5 && "snapshot".equals(cells[2]) && lane.table().equals(cells[3]))
                .mapToLong(cells -> Long.parseLong(cells[4])).sum();
    }

    private static String awaitConfirmedFloor(ControlPlane control, MongoDatabase store, Lane lane) {
        return Await.answered("the actual source and sink to retain the same confirmed point for " + lane.pipeline(), SETUP, () -> {
            Document consumer = SystemCollections.SRS_CONSUMER_OFFSETS.on(store).find(
                    new Document("pipelineId", SrsConsumerId.of(lane.pipeline(), lane.source()).value())).first();
            if (consumer == null || !(consumer.get("sinkAckedByTable") instanceof Document tables)
                    || !(tables.get(lane.table()) instanceof Document point) || point.getString("sinkAckedSrcpos") == null)
                return Optional.empty();
            String token = point.getString("sinkAckedSrcpos");
            if (!control.durablePosition(lane.pipeline(), lane.table()).filter(token::equals).isPresent())
                return Optional.empty();
            Document root = SystemCollections.SRS_META.on(store).find(new Document("_id", consumer.getString("miningChainId"))).first();
            if (root == null || !Boolean.TRUE.equals(root.getBoolean("sourceReadDurable"))
                    || !token.equals(root.getString("sourceReadOffset"))) return Optional.empty();
            return Optional.of(token);
        });
    }


    private static void awaitActive(ControlPlane control, List<String> expected, Duration bound) {
        Await.until("the complete live active cohort " + expected, bound,
                () -> control.clusterStatus().members().stream()
                        .filter(member -> member.state() == ClusterMemberState.ACTIVE && Boolean.TRUE.equals(member.live()))
                        .map(ClusterMemberView::nodeId).collect(Collectors.toSet()).equals(Set.copyOf(expected)),
                () -> String.valueOf(control.clusterStatus().members()));
    }

    private static ClusterMemberView member(ControlPlane control, String node) {
        return control.clusterStatus().members().stream().filter(value -> node.equals(value.nodeId())).findFirst().orElseThrow();
    }

    private static ClusterPipelineView pipeline(ControlPlane control, Lane lane) {
        return control.clusterStatus().pipelines().stream().filter(value -> lane.pipeline().equals(value.pipelineId())).findFirst().orElseThrow();
    }

    private static ClusterClaimView claim(ControlPlane control, Lane lane) {
        ClusterClaimView claim = pipeline(control, lane).controllerClaim();
        assertThat(claim).isNotNull();
        return claim;
    }

    private static List<ProcessorRuntimeContext> contexts(ControlPlane control, Lane lane) {
        return pipeline(control, lane).vertices().stream().flatMap(vertex -> vertex.processors().stream())
                .map(value -> value.context()).filter(Objects::nonNull).toList();
    }

    private static NativeIds awaitNative(ControlPlane control, Lane lane, ClusterClaimView claim, List<String> members, Duration bound) {
        return awaitNative(control, lane, claim, members, bound, null);
    }

    private static NativeIds awaitNative(ControlPlane control, Lane lane, ClusterClaimView claim, List<String> members,
            Duration bound, java.util.function.Consumer<ClusterPipelineView> observation) {
        return Await.answered("the actual initialized native context for " + lane.pipeline(), bound, () -> {
            ClusterPipelineView reading = control.clusterStatus().pipelines().stream().filter(value -> lane.pipeline().equals(value.pipelineId())).findFirst().orElseThrow();
            if (observation != null) { observation.accept(reading); }
            List<ProcessorRuntimeContext> contexts = reading.vertices().stream().flatMap(vertex -> vertex.processors().stream())
                    .map(value -> value.context()).filter(Objects::nonNull).toList();
            if (contexts.isEmpty() || !contexts.stream().map(ProcessorRuntimeContext::nodeId).collect(Collectors.toSet()).equals(Set.copyOf(members)))
                return Optional.empty();
            assertThat(contexts).allSatisfy(context -> {
                assertThat(context.claimGeneration()).isEqualTo(claim.executionClaimGeneration());
                assertThat(context.executionGeneration()).isEqualTo(claim.executionGeneration());
                assertThat(context.profileGeneration()).isEqualTo(claim.profileGeneration());
                assertThat(context.memberCount()).isEqualTo(members.size());
            });
            Set<String> jobs = contexts.stream().map(ProcessorRuntimeContext::jobId).collect(Collectors.toSet());
            Set<String> executions = contexts.stream().map(ProcessorRuntimeContext::runtimeExecutionId).collect(Collectors.toSet());
            assertThat(jobs).hasSize(1);
            assertThat(executions).hasSize(1);
            return Optional.of(new NativeIds(jobs.iterator().next(), executions.iterator().next()));
        });
    }

    private static Truth truth(MongoDatabase store, Lane lane) {
        Document artifact = SystemCollections.ARTIFACTS.on(store).find(new Document("_id", lane.pipeline())).first();
        assertThat(artifact).isNotNull();
        var desired = new MongoDesiredStore(SystemCollections.PIPELINE_DESIRED.on(store)).read(lane.pipeline()).orElseThrow();
        return new Truth(artifact.getString("incarnation"), artifact.getString("contentHash"), DesiredStateFingerprint.of(desired));
    }

    private static List<Authority> authorities(MongoDatabase store, String cluster, ClusterClaimView pipeline, List<ClusterClaimView> captures) {
        List<Authority> authorities = new ArrayList<>();
        authorities.add(authority(cluster, WorkloadClaimType.PIPELINE_ACTUATION, pipeline));
        captures.forEach(capture -> authorities.add(authority(cluster, WorkloadClaimType.CAPTURE, capture)));
        assertThat(authorities).allSatisfy(authority -> assertThat(SystemCollections.WORKLOAD_CLAIMS.on(store)
                .find(authority.exact()).first()).isNotNull());
        return List.copyOf(authorities);
    }

    private static Authority authority(String cluster, WorkloadClaimType type, ClusterClaimView claim) {
        assertThat(claim.profileGeneration()).isPositive();
        return new Authority(cluster, type.name(), claim.resourceId(), claim.ownerNodeId(), claim.ownerBootId(),
                claim.claimGeneration(), claim.executionGeneration(), claim.profileGeneration());
    }

    private static AuthorityBounds authorityBounds(MongoDatabase store, List<Authority> originals) {
        List<Document> rows = SystemCollections.WORKLOAD_CLAIMS.on(store).aggregate(List.of(
                new Document("$match", new Document("$or", originals.stream().map(Authority::key).toList())),
                new Document("$set", new Document("observedAt", "$$NOW")))).into(new ArrayList<>());
        assertThat(rows).hasSize(originals.size());
        boolean retired = true;
        Instant now = rows.getFirst().getDate("observedAt").toInstant();
        Instant next = now;
        for (Authority original : originals) {
            Document row = rows.stream().filter(value -> original.key().entrySet().stream()
                    .allMatch(field -> Objects.equals(field.getValue(), value.get(field.getKey())))).findFirst().orElseThrow();
            Date deadline = row.getDate("retiredAuthorizationUntil");
            assertThat(deadline).as("the old authority has a retained conservative promise").isNotNull();
            boolean sameTuple = original.tuple().entrySet().stream()
                    .allMatch(field -> sameScalar(field.getValue(), row.get(field.getKey())));
            Date lease = row.getDate("leaseUntil");
            assertThat(lease).isNotNull();
            boolean live = sameTuple && lease.toInstant().isAfter(now);
            retired &= !live && !deadline.toInstant().isAfter(now);
            if (deadline.toInstant().isAfter(next)) next = deadline.toInstant();
            if (live && lease.toInstant().isAfter(next)) next = lease.toInstant();
        }
        return new AuthorityBounds(now, next, retired);
    }

    private static Duration retirementBudget(MongoDatabase store, List<Authority> originals, SchemaHold hold) {
        AuthorityBounds bounds = checked(hold, () -> authorityBounds(store, originals));
        Duration required = Duration.between(bounds.now(), bounds.next()).plusSeconds(3);
        hold.remaining(required.plus(WATCH.multipliedBy(2)).plus(READ_BOUND.multipliedBy(2)));
        return required;
    }

    private static boolean sameScalar(Object expected, Object actual) {
        if (expected instanceof Number one && actual instanceof Number two) {
            assertThat(actual).isInstanceOfAny(Long.class, Integer.class);
            return one.longValue() == two.longValue();
        }
        return Objects.equals(expected, actual);
    }

    private static <T> T checked(SchemaHold hold, java.util.function.Supplier<T> observation) {
        if (hold != null) hold.remaining(READ_BOUND);
        T result = observation.get();
        if (hold != null) hold.remaining(Duration.ZERO);
        return result;
    }

    private static void awaitPhase(String what, Duration bound, SchemaHold hold,
            java.util.function.BooleanSupplier observation, java.util.function.Supplier<String> last) {
        hold.remaining(bound);
        Await.until(what, bound, () -> {
            hold.remaining(READ_BOUND);
            boolean result = observation.getAsBoolean();
            hold.remaining(Duration.ZERO);
            return result;
        }, last);
        hold.remaining(Duration.ZERO);
    }

    private static Instant now(MongoDatabase store) {
        return store.runCommand(new Document("hello", 1)).getDate("localTime").toInstant();
    }

    private static void awaitLateArm(MongoDatabase store, String cluster, List<Lane> lanes, Map<String, ClusterClaimView> originals,
            QueueTrace trace, MongoChangeStreamCursor<ChangeStreamDocument<Document>> changes) {
        Await.until("a late arm before the first actual allocator becomes eligible", SETUP, () -> {
            trace.drain(changes);
            assertThat(trace.latest.values()).allSatisfy(row -> {
                Document permit = row.get("permit", Document.class);
                if (permit != null) assertThat(number(permit, "transferredExecutionGeneration")).isZero();
                assertThat(row.get("successor")).isNull();
                assertThat(number(row, "attempt")).isZero();
            });
            long permits = trace.latest.values().stream().filter(row -> row.get("permit") != null).count();
            assertThat(permits).isLessThanOrEqualTo(1);
            // A reservation is acquired before the schema publication and allocator. It is already time
            // to arm the real write barrier; waiting on a later control tenure's promise misses that stage.
            if (permits == 1) {
                assertUnallocated(store, cluster, lanes, originals, null);
                return true;
            }
            boolean ready = false;
            for (Lane lane : lanes) {
                ClusterClaimView original = originals.get(lane.pipeline());
                Document queued = SystemCollections.CLUSTER_RECOVERY_QUEUE.on(store).find(
                        new Document("_id", trace.latest.get(lane.pipeline()).get("_id"))).first();
                assertThat(queued).isNotNull();
                Document claim = SystemCollections.WORKLOAD_CLAIMS.on(store).aggregate(List.of(
                        new Document("$match", new Document("clusterId", cluster)
                                .append("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())
                                .append("resourceId", lane.pipeline())),
                        new Document("$set", new Document("observedAt", "$$NOW")))).first();
                assertThat(claim).isNotNull();
                assertThat(number(claim, "executionGeneration")).isEqualTo(original.executionGeneration());
                assertThat(number(claim, "contextExecutionGeneration")).isEqualTo(original.executionGeneration());
                assertThat(number(claim, "executionClaimGeneration")).isEqualTo(original.executionClaimGeneration());
                assertThat(number(claim, "profileGeneration")).isEqualTo(original.profileGeneration());
                Document executionProfile = claim.get("executionProfile", Document.class);
                assertThat(executionProfile).isNotNull();
                assertThat(number(executionProfile, "generation")).isEqualTo(original.executionProfileGeneration());
                assertThat(executionProfile.getString("hash")).isEqualTo(original.executionProfileHash());
                Instant server = claim.getDate("observedAt").toInstant();
                Date lease = claim.getDate("leaseUntil");
                assertThat(lease).isNotNull();
                if (number(claim, "claimGeneration") <= original.executionClaimGeneration()
                        || !lease.toInstant().isAfter(server)) continue;
                Date promise = claim.getDate("retiredAuthorizationUntil");
                assertThat(promise).isNotNull();
                Date eligible = queued.getDate("nextEligibleAt");
                Instant lead = server.plusMillis(1_000);
                // A naturally acquired original context must be near its retained promise and queue eligibility.
                // The store still decides admission; this observation only times the owned schema barrier.
                ready |= !promise.toInstant().isAfter(lead) && (eligible == null || !eligible.toInstant().isAfter(lead));
            }
            return ready;
        }, trace::describe);
    }

    private static Blocked awaitBlocked(MongoClient client, MongoDatabase store, String clusterId,
            Map<String, ClusterMemberView> survivors, ClusterRecoveryItemView.Profile profile, List<Lane> lanes,
            Map<String, ClusterClaimView> originalClaims, Map<String, DerivedSchema> originals,
            Map<String, Map<String, String>> columns, QueueTrace trace,
            MongoChangeStreamCursor<ChangeStreamDocument<Document>> changes, SchemaHold hold) throws Exception {
        Object stableId = null;
        long stableSince = 0;
        long phaseDeadline = System.nanoTime() + hold.remaining(Duration.ofSeconds(10)).toNanos();
        while (System.nanoTime() < phaseDeadline) {
            trace.drain(changes);
            assertUnallocated(store, clusterId, lanes, originalClaims, hold);
            List<Document> operations = checked(hold, () -> client.getDatabase("admin").aggregate(List.of(
                    new Document("$currentOp", new Document("allUsers", true).append("idleConnections", false)),
                    new Document("$match", new Document("active", true).append("ns",
                            store.getName() + "." + SystemCollections.DERIVED_SCHEMAS.on(store).getNamespace().getCollectionName()))))
                    .into(new ArrayList<>()));
            for (ClusterMemberView member : survivors.values()) {
                for (Lane lane : lanes) {
                    Document operation = operations.stream().filter(value -> {
                        String app = value.getString("appName");
                        return app != null && (app.equals("tapstate/" + member.bootId()) || app.endsWith(" tapstate/" + member.bootId()))
                                && genuineUpdate(value.get("command", Document.class), lane, originals.get(lane.pipeline()), columns.get(lane.pipeline()));
                    }).findFirst().orElse(null);
                    if (operation == null || operation.get("opid") == null) continue;
                    if (!operation.get("opid").equals(stableId)) { stableId = operation.get("opid"); stableSince = System.nanoTime(); }
                    else if (System.nanoTime() - stableSince >= Duration.ofMillis(200).toNanos()) {
                        assertThat(System.nanoTime()).as("the physical schema-write phase was observed within its budget")
                                .isLessThan(phaseDeadline);
                        checked(hold, () -> { assertJoined(store, clusterId, member, profile); return true; });
                        hold.assertOriginalOutside();
                        assertThat(trace.latest.get(lane.pipeline()).get("permit")).isNotNull();
                        assertThat(trace.latest.values().stream().filter(row -> row.get("permit") != null).count()).isEqualTo(1);
                        return new Blocked(lane.pipeline(), member, operation.get("opid"));
                    }
                }
            }
            Await.pauseBeforeNextPoll(Duration.ofMillis(25));
        }
        throw new AssertionError("No actual boot's schema-delta write remained blocked before allocation");
    }

    private static boolean genuineUpdate(Document command, Lane lane, DerivedSchema original, Map<String, String> columns) {
        if (command == null) return false;
        if (new Document("_id", lane.key()).equals(command.get("q"))) return genuineMutation(command.get("u", Document.class), original, columns);
        if (!(command.get("updates") instanceof List<?> updates)) return false;
        return updates.stream().filter(Document.class::isInstance).map(Document.class::cast)
                .anyMatch(update -> new Document("_id", lane.key()).equals(update.get("q"))
                        && genuineMutation(update.get("u", Document.class), original, columns));
    }

    private static boolean genuineMutation(Document update, DerivedSchema original, Map<String, String> columns) {
        if (update == null || !(update.get("$set") instanceof Document fields)
                || !(fields.get("versions") instanceof List<?> versions) || versions.size() != 2
                || !(versions.getLast() instanceof Document version) || number(version, "version") != 1
                || !original.statement().equals(version.getString("statement"))
                || !original.derivedBy().equals(version.getString("derivedBy"))
                || version.getString("derivedFrom") == null || original.derivedFrom().equals(version.getString("derivedFrom"))) return false;
        Map<String, String> actual = new LinkedHashMap<>();
        for (Document field : version.getList("columns", Document.class)) {
            if (actual.put(field.getString("name"), field.getString("type")) != null) return false;
        }
        return actual.equals(columns) && "STRING NULL".equals(actual.get("note"));
    }

    private static Document assertJoined(MongoDatabase store, String cluster, ClusterMemberView member, ClusterRecoveryItemView.Profile profile) {
        assertThat(member.profileGeneration()).isEqualTo(profile.generation());
        assertThat(member.profileHash()).isEqualTo(profile.hash());
        assertThat(member.joined()).isTrue();
        assertThat(member.memberUuid()).isEqualTo(member.joinedMemberUuid());
        Document registry = SystemCollections.CLUSTER_NODE_REGISTRY.on(store).find(new Document("clusterId", cluster)
                .append("nodeId", member.nodeId()).append("bootId", member.bootId()).append("profileGeneration", profile.generation())
                .append("joined", true).append("hash", profile.hash())
                .append("memberUuid", member.memberUuid()).append("memberAddress", member.joinedMemberAddress())).first();
        assertThat(registry).isNotNull();
        Document session = SystemCollections.WORKLOAD_CLAIMS.on(store).find(new Document("clusterId", cluster)
                .append("resourceType", WorkloadClaimType.NODE_SESSION.name()).append("resourceId", member.nodeId())
                .append("ownerNodeId", member.nodeId()).append("ownerBootId", member.bootId())
                .append("profileGeneration", profile.generation())
                .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")))).first();
        assertThat(session).isNotNull();
        return session;
    }

    private static void assertCommitted(MongoDatabase store, String cluster, ClusterRecoveryItemView.Profile profile) {
        Document membership = SystemCollections.CLUSTER_MEMBERSHIP.on(store).find(new Document("_id", cluster)).first();
        assertThat(membership).isNotNull();
        assertThat(membership.getList("activeNodeIds", String.class)).containsExactlyInAnyOrderElementsOf(NODES);
        assertThat(number(membership, "profileGeneration")).isEqualTo(profile.generation());
        Document installed = SystemCollections.CLUSTER_EXECUTION_PROFILES.on(store).find(new Document("_id", cluster)).first();
        assertThat(installed).isNotNull();
        assertThat(number(installed, "generation")).isEqualTo(profile.generation());
        assertThat(installed.getString("hash")).isEqualTo(profile.hash());
    }

    private static void assertUnallocated(MongoDatabase store, String cluster, List<Lane> lanes,
            Map<String, ClusterClaimView> originals, SchemaHold hold) {
        List<String> ids = lanes.stream().map(Lane::pipeline).toList();
        List<Document> claims = checked(hold, () -> SystemCollections.WORKLOAD_CLAIMS.on(store).find(
                new Document("clusterId", cluster).append("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())
                        .append("resourceId", new Document("$in", ids))).into(new ArrayList<>()));
        assertThat(claims).hasSize(lanes.size());
        List<Document> occupancy = checked(hold, () -> SystemCollections.CLUSTER_CAPACITY_OCCUPANCY.on(store)
                .find(new Document("clusterId", cluster).append("pipelineId", new Document("$in", ids))).into(new ArrayList<>()));
        for (Lane lane : lanes) {
            Document claim = claims.stream().filter(row -> lane.pipeline().equals(row.getString("resourceId"))).findFirst().orElseThrow();
            assertThat(claim).isNotNull();
            assertThat(number(claim, "executionGeneration")).isEqualTo(originals.get(lane.pipeline()).executionGeneration());
            for (Document row : occupancy.stream().filter(value -> lane.pipeline().equals(value.getString("pipelineId"))).toList()) {
                Object execution = row.get("executionGeneration");
                if (execution != null) assertThat(((Number) execution).longValue()).isLessThanOrEqualTo(originals.get(lane.pipeline()).executionGeneration());
            }
        }
    }

    private static void watchBelowQuorum(MongoDatabase store, PartitionableCluster cluster, List<String> survivors,
            ControlPlane alone, ClusterRecoveryItemView.Profile profile, List<Lane> lanes,
            Map<String, ClusterClaimView> originals, Map<String, NativeIds> originalNative, QueueTrace trace,
            MongoChangeStreamCursor<ChangeStreamDocument<Document>> changes,
            Map<String, List<String>> reads, Path sourceEntries, Path writerEntries,
            List<String> sourceCalls, List<String> writerCalls, NativeReaders nativeReaders,
            Set<String> originalJobs, SchemaHold hold) throws Exception {
        long finish = System.nanoTime() + WATCH.toNanos();
        while (System.nanoTime() < finish) {
            hold.remaining(WATCH);
            checked(hold, () -> { trace.drain(changes); return true; });
            checked(hold, () -> { assertCommitted(store, cluster.clusterId(), profile); return true; });
            ClusterRecoveryView recovery = checked(hold, () -> alone.clusterRecovery(READ_BOUND));
            assertThat(recovery.quorumReady()).isFalse();
            assertThat(recovery.items()).extracting(ClusterRecoveryItemView::pipelineId)
                    .containsExactlyInAnyOrderElementsOf(lanes.stream().map(Lane::pipeline).toList());
            assertThat(recovery.items()).hasSize(lanes.size()).allSatisfy(item -> {
                assertThat(item.persistedStatus()).isNotIn("REBUILD_FAILED", "RECOVERED", "CANCELLED");
                assertThat(item.status()).as("nonterminal recovery item %s reports its below-quorum wait", item.pipelineId())
                        .isEqualTo("WAITING_QUORUM");
            });
            nativeReaders.assertInventory(originalJobs, hold);
            assertThat(nativeReaders.singletons()).isTrue();
            assertUnallocated(store, cluster.clusterId(), lanes, originals, hold);
            assertThat(sourceDataEntries(sourceEntries)).isEqualTo(sourceCalls);
            assertThat(lines(writerEntries)).isEqualTo(writerCalls);
            for (Lane lane : lanes) {
                assertThat(readerLines(lane)).isEqualTo(reads.get(lane.pipeline()));
                assertThat(checked(hold, () -> rowPresent(lane, 3))).isFalse();
            }
            Await.pauseBeforeNextPoll(Duration.ofMillis(100));
        }
    }

    private static List<Document> queue(MongoDatabase store, String cluster) {
        return SystemCollections.CLUSTER_RECOVERY_QUEUE.on(store).find(new Document("clusterId", cluster)).into(new ArrayList<>());
    }

    private static long number(Document document, String name) {
        assertThat(document.get(name)).as("the persisted " + name + " is known").isInstanceOf(Number.class);
        return ((Number) document.get(name)).longValue();
    }

    private static Map<String, Step> steps(Map<String, Document> rows) {
        Map<String, Step> steps = new LinkedHashMap<>();
        rows.forEach((pipeline, row) -> {
            Document permit = row.get("permit", Document.class);
            steps.put(pipeline, new Step(row.get("_id"), number(row, "enqueueSequence"), number(row, "attempt"),
                    number(row.get("event", Document.class), "originalExecutionGeneration"),
                    permit == null ? null : permit.getString("reservationId")));
        });
        return Map.copyOf(steps);
    }

    private static final class QueueTrace {
        private final Set<String> pipelines;
        private final Map<String, Long> inserted = new LinkedHashMap<>();
        private final Map<String, Document> latest = new LinkedHashMap<>();
        private Map<String, Step> frozen;
        private Map<String, Document> frozenPermitPayloads;
        private Map<String, Long> permitBaselineRevisions;
        private final Set<String> removedPermits = new java.util.HashSet<>();
        private int peakPermits;
        private QueueTrace(Set<String> pipelines) { this.pipelines = pipelines; }
        private void freeze(Map<String, Step> steps) { frozen = steps; }
        private void freezePermitPayloads(List<Document> rows) {
            Map<String, Document> payloads = new LinkedHashMap<>();
            Map<String, Long> revisions = new LinkedHashMap<>();
            for (Document row : rows) {
                String pipeline = row.getString("pipelineId");
                assertThat(pipelines).contains(pipeline);
                assertThat(revisions.put(pipeline, number(row, "itemRevision"))).isNull();
                Document permit = row.get("permit", Document.class);
                if (permit != null) {
                    payloads.put(pipeline, Document.parse(permit.toJson(org.bson.json.JsonWriterSettings.builder()
                            .outputMode(org.bson.json.JsonMode.EXTENDED).build())));
                }
            }
            assertThat(revisions.keySet()).containsExactlyInAnyOrderElementsOf(pipelines);
            frozenPermitPayloads = Map.copyOf(payloads);
            permitBaselineRevisions = Map.copyOf(revisions);
            removedPermits.clear();
        }
        private void assertPermitPayload(String pipeline, Document row) {
            if (frozenPermitPayloads == null || number(row, "itemRevision") < permitBaselineRevisions.get(pipeline)) return;
            Document permit = row.get("permit", Document.class);
            if (permit == null) {
                if (frozenPermitPayloads.containsKey(pipeline)) removedPermits.add(pipeline);
                return;
            }
            assertThat(removedPermits).as("a removed permit cannot be added again below quorum").doesNotContain(pipeline);
            assertThat(permit).as("the complete original permit cannot renew or change authority below quorum")
                    .isEqualTo(frozenPermitPayloads.get(pipeline));
        }
        private void thaw() {
            frozen = null;
            frozenPermitPayloads = null;
            permitBaselineRevisions = null;
            removedPermits.clear();
        }
        private void drain(MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor) {
            for (ChangeStreamDocument<Document> event; (event = cursor.tryNext()) != null;) {
                Document row = event.getFullDocument();
                if (row == null) {
                    assertThat(event.getOperationType()).as("the owned queue items cannot disappear during recovery")
                            .isNotEqualTo(OperationType.DELETE);
                    continue;
                }
                if (!pipelines.contains(row.getString("pipelineId"))) continue;
                assertThat(event.getOperationType()).isIn(OperationType.INSERT, OperationType.REPLACE);
                String pipeline = row.getString("pipelineId");
                if (event.getOperationType() == OperationType.INSERT)
                    assertThat(inserted.putIfAbsent(pipeline, number(row, "enqueueSequence"))).isNull();
                else assertThat(number(row, "enqueueSequence")).isEqualTo(inserted.get(pipeline));
                Document previous = latest.put(pipeline, row);
                if (previous != null) assertThat(number(row, "itemRevision")).isGreaterThan(number(previous, "itemRevision"));
                int permits = (int) latest.values().stream().filter(value -> value.get("permit") != null).count();
                peakPermits = Math.max(peakPermits, permits);
                assertThat(permits).isLessThanOrEqualTo(1);
                assertPermitPayload(pipeline, row);
                if (frozen != null) {
                    Step expected = frozen.get(pipeline);
                    assertThat(row.get("_id")).isEqualTo(expected.id());
                    assertThat(number(row, "enqueueSequence")).isEqualTo(expected.sequence());
                    assertThat(number(row, "attempt")).isEqualTo(expected.attempt());
                    assertThat(number(row.get("event", Document.class), "originalExecutionGeneration")).isEqualTo(expected.frontier());
                    Document successor = row.get("successor", Document.class);
                    assertThat(successor).isNull();
                    Document permit = row.get("permit", Document.class);
                    if (permit != null) {
                        assertThat(permit.getString("reservationId")).isEqualTo(expected.reservation());
                        assertThat(number(permit, "transferredExecutionGeneration")).isZero();
                    }
                }
            }
        }
        private String describe() { return "inserted=" + inserted + "; peak permits=" + peakPermits + "; latest=" + latest; }
    }

    private static final class SchemaHold implements AutoCloseable {
        private final ClientSession session;
        private final MongoDatabase store;
        private final MongoDatabase writerStore;
        private final Map<String, Document> original = new LinkedHashMap<>();
        private final long started = System.nanoTime();
        private final Duration lifetime;
        private SchemaHold(MongoClient client, MongoDatabase store, List<Lane> lanes, Duration lifetime) {
            this.store = store;
            this.writerStore = client.getDatabase(store.getName());
            this.lifetime = lifetime;
            assertThat(lifetime).isGreaterThan(CLEANUP_MARGIN.plusSeconds(10));
            this.session = client.startSession();
            try {
                session.startTransaction(TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
                        .writeConcern(WriteConcern.MAJORITY).build());
                for (Lane lane : lanes) {
                    Document before = SystemCollections.DERIVED_SCHEMAS.on(store).find(new Document("_id", lane.key())).first();
                    assertThat(before).isNotNull();
                    assertThat(number(before, "pin")).isZero();
                    List<Document> versions = before.getList("versions", Document.class);
                    assertThat(versions).as("the source schema delta is still unpublished before the write barrier").hasSize(1);
                    assertThat(number(versions.getFirst(), "version")).isZero();
                    original.put(lane.key(), before);
                    assertThat(SystemCollections.DERIVED_SCHEMAS.on(writerStore).updateOne(session, new Document("_id", lane.key()),
                            new Document("$inc", new Document("pin", 1L))).getModifiedCount()).isEqualTo(1);
                }
                assertOriginalOutside();
            } catch (RuntimeException | Error failure) {
                try { close(); }
                catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }
        private Duration remaining(Duration requested) {
            Duration left = lifetime.minusNanos(System.nanoTime() - started).minus(CLEANUP_MARGIN);
            assertThat(left).as("the actual transaction lifetime still covers the required phase").isGreaterThan(requested);
            return requested;
        }
        private void assertOriginalOutside() {
            original.forEach((key, document) -> assertThat(checked(this, () -> SystemCollections.DERIVED_SCHEMAS.on(store)
                    .find(new Document("_id", key)).first())).isEqualTo(document));
        }
        private void abort() { if (session.hasActiveTransaction()) session.abortTransaction(); }
        @Override public void close() {
            try { abort(); }
            finally { session.close(); }
        }
    }

    private record Lane(String pipeline, String source, String target, String table, Path sourceDirectory,
            String targetUri, MongoDatabase targetDatabase, Path reads) {
        private String step() { return source + "." + table; }
        private String key() { return pipeline + "." + step(); }
    }
    private record NativeIds(String job, String execution) {}
    private record Truth(String incarnation, String revision, String fingerprint) {}
    private record Step(Object id, long sequence, long attempt, long frontier, String reservation) {}
    private record Blocked(String pipeline, ClusterMemberView member, Object opid) {}
    private record AuthorityBounds(Instant now, Instant next, boolean retired) {}

    /** Retains observations only; neither missing native facts nor a UUID reset is an acceptance condition. */
    private static final class NativeProjectionEvidence {
        private static final tools.jackson.databind.ObjectMapper JSON = new tools.jackson.databind.ObjectMapper();
        private final Path directory;
        private final NativeReaders readers;
        private final List<NativeMemberWitness.JobRequest> requested;
        private final Map<String, ClusterPipelineView> typed = new LinkedHashMap<>();
        private final Map<String, String> unavailable = new LinkedHashMap<>();
        private NativeProjectionEvidence(String cluster, Set<String> pipelines, NativeReaders readers, ControlPlane restored,
                Map<String, Document> completed) {
            this.directory = Path.of(System.getProperty("tapstate.e2e.build-directory", "target"))
                    .resolve("failure-scenes").resolve("quorum-native-projection").resolve(cluster + "-" + UUID.randomUUID());
            this.readers = readers;
            List<NativeMemberWitness.JobRequest> jobs = new ArrayList<>();
            for (String pipeline : new java.util.TreeSet<>(pipelines)) {
                try {
                    Document successor = completed.get(pipeline).get("successor", Document.class);
                    Document claim = successor.get("pipelineClaim", Document.class);
                    jobs.add(new NativeMemberWitness.JobRequest(pipeline, successor.getString("nativeJobId"),
                            number(claim, "claimGeneration"), number(claim, "executionGeneration"), number(claim, "profileGeneration")));
                } catch (RuntimeException | Error missing) {
                    unavailable.put("requested:" + pipeline, missing.getClass().getName());
                }
            }
            this.requested = List.copyOf(jobs);
            try {
                restored.clusterStatus(READ_BOUND).pipelines().stream()
                        .filter(value -> pipelines.contains(value.pipelineId())).forEach(this::typed);
            } catch (RuntimeException | Error missing) { unavailable.put("initialTyped", missing.getClass().getName()); }
        }
        private void typed(ClusterPipelineView reading) { typed.put(reading.pipelineId(), reading); }
        private void retain(String phase, Throwable originalFailure) {
            Map<String, Object> nativeReadings = new LinkedHashMap<>();
            readers.readers.forEach((node, reader) -> {
                try { nativeReadings.put(node, reader.projectionReading(requested, READ_BOUND)); }
                catch (RuntimeException | Error unavailable) {
                    nativeReadings.put(node, Map.of("unavailable", unavailable.getClass().getName()));
                }
            });
            try {
                Files.createDirectories(directory);
                Map<String, Object> snapshot = new LinkedHashMap<>();
                snapshot.put("phase", phase);
                snapshot.put("capturedAt", Instant.now());
                snapshot.put("requested", requested);
                snapshot.put("unavailable", unavailable);
                snapshot.put("lastTyped", typed);
                snapshot.put("nativeReadings", nativeReadings);
                Files.writeString(directory.resolve(phase + ".json"), JSON.writeValueAsString(scrub(JSON.convertValue(snapshot, Map.class))));
            } catch (RuntimeException | java.io.IOException unavailable) {
                if (originalFailure != null) { originalFailure.addSuppressed(unavailable); }
                else { System.err.println("Passive native projection evidence could not be retained: " + unavailable.getClass().getName()); }
            }
        }
        private static Object scrub(Object value) {
            if (value instanceof Map<?, ?> map) {
                Map<String, Object> redacted = new LinkedHashMap<>();
                map.forEach((key, entry) -> {
                    String name = String.valueOf(key);
                    String normalized = name.toLowerCase(java.util.Locale.ROOT);
                    redacted.put(name, normalized.contains("address") || normalized.contains("url") || normalized.contains("uri")
                            || normalized.contains("credential") || normalized.contains("password") || normalized.equals("config")
                                    ? "<redacted>" : scrub(entry));
                });
                return redacted;
            }
            if (value instanceof List<?> list) { return list.stream().map(NativeProjectionEvidence::scrub).toList(); }
            return value;
        }
    }

    private static final class NativeReaders implements AutoCloseable {
        private final Map<String, NativeMemberWitness> readers = new LinkedHashMap<>();
        private final Map<String, ClusterMemberView> boots;
        private final ClusterRecoveryItemView.Profile profile;
        private final Map<String, Long> sessionGenerations = new LinkedHashMap<>();
        private boolean requalified;
        private NativeReaders(PartitionableCluster cluster, List<String> survivors,
                Map<String, ClusterMemberView> boots, ClusterRecoveryItemView.Profile profile, MongoDatabase store) {
            this.boots = new LinkedHashMap<>(boots);
            this.profile = profile;
            try {
                for (String node : survivors) {
                    Document session = assertJoined(store, cluster.clusterId(), boots.get(node), profile);
                    sessionGenerations.put(node, number(session, "claimGeneration"));
                    readers.put(node, NativeMemberWitness.connect(cluster, node, READ_BOUND));
                }
            } catch (RuntimeException | Error failure) {
                try { close(); }
                catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }
        private void requalifyAfterHeal(PartitionableCluster cluster, MongoDatabase store, List<String> survivors) {
            Map<String, ClusterMemberView> current = new LinkedHashMap<>();
            for (String node : survivors) {
                ClusterMemberView now = member(cluster.member(node), node);
                ClusterMemberView original = boots.get(node);
                assertThat(now.state()).isEqualTo(ClusterMemberState.ACTIVE);
                assertThat(now.live()).isTrue();
                assertThat(now.sessionLeased()).isTrue();
                assertThat(now.bootId()).isEqualTo(original.bootId()).isEqualTo(now.sessionBootId());
                assertThat(now.profileGeneration()).isEqualTo(profile.generation());
                assertThat(now.profileHash()).isEqualTo(profile.hash());
                assertThat(now.memberUuid()).isEqualTo(now.joinedMemberUuid());
                assertThat(now.hzAddress()).isEqualTo(now.joinedMemberAddress());
                Document session = assertJoined(store, cluster.clusterId(), now, profile);
                assertThat(number(session, "claimGeneration")).isEqualTo(sessionGenerations.get(node));
                current.put(node, now);
            }
            // A merge can close an observational client while retaining the same admitted process boot.
            // New owned readers qualify the current native identity; original execution facts stay frozen.
            close();
            readers.clear();
            boots.putAll(current);
            requalified = true;
            try {
                for (String node : survivors) readers.put(node, NativeMemberWitness.connect(cluster, node, READ_BOUND));
            } catch (RuntimeException | Error failure) {
                try { close(); }
                catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }
        private boolean sameMembers(Set<String> expected) {
            return readers.values().stream().allMatch(reader -> {
                var members = reader.members();
                members.forEach(this::assertIdentity);
                return members.size() == expected.size() && members.stream().noneMatch(NativeMemberWitness.MemberFacts::lite)
                        && members.stream().map(NativeMemberWitness.MemberFacts::nodeId).collect(Collectors.toSet()).equals(expected);
            });
        }
        private void assertMembers(Set<String> expected) { assertThat(sameMembers(expected)).isTrue(); }
        private boolean singletons() {
            return readers.entrySet().stream().allMatch(entry -> {
                var members = entry.getValue().members();
                members.forEach(this::assertIdentity);
                return members.stream().map(NativeMemberWitness.MemberFacts::nodeId).toList().equals(List.of(entry.getKey()));
            });
        }
        private void assertIdentity(NativeMemberWitness.MemberFacts member) {
            ClusterMemberView boot = boots.get(member.nodeId());
            assertThat(boot).isNotNull();
            assertThat(member.lite()).isFalse();
            assertThat(member.uuid()).isEqualTo(boot.memberUuid());
            assertThat(member.bootId()).isEqualTo(boot.bootId());
            assertThat(member.profileGeneration()).isEqualTo(Long.toString(profile.generation()));
            assertThat(member.profileHash()).isEqualTo(profile.hash());
            if (requalified) {
                assertThat(member.address()).isEqualTo(boot.hzAddress()).isEqualTo(boot.joinedMemberAddress());
            }
        }
        private void assertInventory(Set<String> expected, SchemaHold hold) {
            readers.forEach((node, reader) -> assertThat(checked(hold, () -> reader.submittedJobs(READ_BOUND)))
                    .as("the complete submitted and retained normal-job inventory on %s", node)
                    .containsExactlyInAnyOrderElementsOf(expected));
        }
        private String describe() { return readers.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                entry -> entry.getValue().members())).toString(); }
        @Override public void close() {
            Throwable first = null;
            for (NativeMemberWitness reader : readers.values()) {
                try { reader.close(); }
                catch (RuntimeException | Error failure) {
                    if (first == null) first = failure; else first.addSuppressed(failure);
                }
            }
            if (first instanceof RuntimeException failure) throw failure;
            if (first instanceof Error failure) throw failure;
        }
    }
    private record Authority(String cluster, String type, String resource, String node, String boot, long claim, long execution, long profile) {
        private Document key() { return new Document("clusterId", cluster).append("resourceType", type).append("resourceId", resource); }
        private Document tuple() { return new Document("ownerNodeId", node).append("ownerBootId", boot)
                .append("claimGeneration", claim).append("executionGeneration", execution).append("profileGeneration", profile); }
        private Document exact() { return key().append("ownerNodeId", node).append("ownerBootId", boot)
                .append("claimGeneration", claim).append("executionGeneration", execution).append("profileGeneration", profile); }
    }
}
