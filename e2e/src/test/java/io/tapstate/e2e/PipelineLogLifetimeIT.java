package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RequiresDocker
class PipelineLogLifetimeIT {
    private static final String PIPELINE = "recreated_log_probe";
    private static final String CONTROL = "new_id_log_control";
    private static final String OLD = "password authentication failed: no password was given";
    private static final String NEW = "the change stream stopped and will not resume";
    private static final Duration WAIT = Duration.ofSeconds(100);
    private record Tail(List<String> messages) {
        long count(String text) {
            return messages.stream().filter(m -> m.contains(text)).count();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void anAppliedPipelineNeverReadsItsPredecessorsLog(int trial, @TempDir Path fixture) throws Exception {
        Path oldSource = Files.createDirectory(fixture.resolve("old-source"));
        Path newSource = Files.createDirectory(fixture.resolve("new-source"));
        Path target = Files.createDirectory(fixture.resolve("target"));
        Path jars = Files.createDirectory(fixture.resolve("jars"));
        FileEndpoints.replaceTable(oldSource.resolve("orders.csv"), "id,name\n1,old\n");
        FileEndpoints.replaceTable(newSource.resolve("orders.csv"), "id,name\n2,new\n");

        try (var storage = PreparedNativeStore.create("probe-admin", "probe-admin-pass");
             var server = RealProcessServer.startPrepared(storage.configurationUri(), "127.0.0.1",
                     port -> List.of("--tapstate.cluster.id=" + storage.clusterId(),
                             "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2"));
             var mongo = MongoClients.create(storage.configurationUri())) {
            var configDb = mongo.getDatabase(new ConnectionString(storage.configurationUri()).getDatabase());
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(jars)));

            var resources = new LinkedHashMap<String, String>();
            resources.put("old.tap.yml", source("probe_src_old", oldSource, ", require_password: true", true));
            resources.put("new.tap.yml", source("probe_src_new", newSource, "", true));
            resources.put("target.tap.yml", source("probe_target", target, "", false));
            resources.put("pipeline.tap.yml", pipeline(PIPELINE, "probe_src_old"));
            control.apply(resources);
            control.discoverSchema("probe_src_old", E2eConnectorJar.CONNECTOR_ID,
                    Map.of("uri", oldSource.toString(), "password", "fixture-source-pass", "require_password", true));
            control.discoverSchema("probe_src_new", E2eConnectorJar.CONNECTOR_ID, Map.of("uri", newSource.toString()));

            var original = stateMapping(configDb, PIPELINE);
            String oldHash = control.contentHash(PIPELINE);
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.until("old connector marker in its public logs", WAIT, () -> tail(control, PIPELINE).count(OLD) > 0,
                    () -> "old marker count=" + tail(control, PIPELINE).count(OLD));
            Tail before = tail(control, PIPELINE);
            require(before.count(NEW) == 0, "old pipeline already contained the new-source marker");

            control.stop(PIPELINE, true);
            Await.until("old pipeline has stopped", WAIT,
                    () -> control.state(PIPELINE).filter(PipelineState.STOPPED::equals).isPresent(),
                    () -> "state=" + control.state(PIPELINE));
            require(tail(control, PIPELINE).count(OLD) > 0, "old marker disappeared before deletion");
            Await.until("the old artifact's physical state rebuild has settled", WAIT,
                    () -> {
                        var mapping = stateMapping(configDb, PIPELINE);
                        return "ACTIVE".equals(mapping.getString("phase"))
                                && mapping.getLong("stateGeneration") > original.getLong("stateGeneration");
                    },
                    () -> "mapping phase=" + stateMapping(configDb, PIPELINE).getString("phase"));
            deleteAtRest(control, PIPELINE, oldHash);
            require(control.artifact(PIPELINE).isEmpty(), "deletion did not remove the pipeline artifact");
            var retired = stateMapping(configDb, PIPELINE);
            require("RETIRED".equals(retired.getString("phase")), "delete did not retire its state mapping");

            control.apply(Map.of("replacement.tap.yml", pipeline(PIPELINE, "probe_src_new"),
                    "control.tap.yml", pipeline(CONTROL, "probe_src_new"),
                    "new.tap.yml", resources.get("new.tap.yml"), "target.tap.yml", resources.get("target.tap.yml")));
            var successor = stateMapping(configDb, PIPELINE);
            String newHash = control.contentHash(PIPELINE);
            require("ACTIVE".equals(successor.getString("phase"))
                    && successor.getLong("stateGeneration") > retired.getLong("stateGeneration")
                    && !newHash.equals(oldHash), "recreation did not publish a new active identity and artifact");
            Tail notStarted = tail(control, PIPELINE);
            Tail differentId = tail(control, CONTROL);
            require(differentId.count(OLD) == 0 && differentId.count(NEW) == 0,
                    "fresh different-ID control already contained a connector marker");

            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Tail after = Await.answered("replacement connector marker in its public logs", WAIT, () -> {
                Tail current = tail(control, PIPELINE);
                require(current.count(OLD) == 0, "A recreated Pipeline exposed the deleted generation's log over HTTP");
                return current.count(NEW) > 0 ? java.util.Optional.of(current) : java.util.Optional.empty();
            });
            System.out.printf("RECREATE_PROOF trial=%d pid=%d deleted=true oldGeneration=%d retiredGeneration=%d successorGeneration=%d "
                            + "oldBefore=%d oldInFreshDifferentId=%d oldAfterApply=%d newAfterApply=%d oldAfterNewStart=%d newAfterNewStart=%d%n",
                    trial, server.pid(), original.getLong("stateGeneration"), retired.getLong("stateGeneration"),
                    successor.getLong("stateGeneration"), before.count(OLD), differentId.count(OLD), notStarted.count(OLD),
                    notStarted.count(NEW), after.count(OLD), after.count(NEW));
            // Retain the actual public response privately. The issue needs only its fixture marker rows.
            Files.writeString(fixture.resolve("after-recreate.txt"), control.logs(PIPELINE) + "\n");
            require(notStarted.count(OLD) == 0 && after.count(OLD) == 0,
                    "A recreated Pipeline exposed the deleted generation's log over HTTP");
        }
    }

    static void deleteAtRest(ControlPlane control, String id, String hash) {
        Await.until("delete after acquiring its existing workload claim", WAIT, () -> {
            if (control.deleteArtifactWhenClaimAvailable(id, hash)) return true;
            require(control.artifact(id).isPresent() && control.contentHash(id).equals(hash),
                    "retry refused because the original artifact is no longer the same one");
            System.out.println("DELETE_PRECONDITION_RETRY " + id + " code=" + IoError.WORKLOAD_CLAIM_FENCED.code());
            return false;
        }, () -> "artifact=" + id + " still fenced");
    }

    private static Document stateMapping(MongoDatabase config, String id) {
        Document row = SystemCollections.OPERATOR_GENERATIONS.on(config).find(new Document("_id", id)).first();
        if (row == null) throw new AssertionError("No state generation for " + id);
        return row;
    }

    private static Tail tail(ControlPlane control, String id) {
        String response = control.logs(id);
        require(response.startsWith("200 "), "log read did not return HTTP 200 for " + id);
        var json = (Map<?, ?>) JsonReader.parse(response.substring(4));
        var lines = (List<?>) json.get("lines");
        return new Tail(lines.stream().map(raw -> (String) ((Map<?, ?>) raw).get("message")).toList());
    }

    private static String source(String id, Path path, String settings, boolean cdc) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s"%s }
                %s
                """.formatted(id, E2eConnectorJar.CONNECTOR_ID, path, settings, cdc ? "mode: cdc\ntables: [ orders ]" : "");
    }

    private static String pipeline(String id, String source) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: orders
                  sync:
                    - source: probe_target
                """.formatted(id, source.equals("probe_src_new")
                        ? "{ id: probe_src_new, nodeConfig: { fail_cdc: true } }" : source);
    }

    private static void require(boolean ok, String reason) {
        if (!ok) {
            throw new AssertionError(reason);
        }
    }
}
