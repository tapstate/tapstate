package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

/** Failed real admission closes only the new owner; two healthy JVMs keep the original storage and data flow. */
@RequiresDocker
class AFailedJoiningCandidateLeavesItsHealthyPeersRunningIT {
    private static final String JOINING="node-c-failed",PIPELINE="healthy_peer_pipeline",SOURCE="healthy_peer_source",TARGET="healthy_peer_target",TABLE="orders";
    private static final Duration BOUND=Duration.ofSeconds(90);

    @Test void aCandidateThatFailsAfterJoiningCannotCloseItsPeersOrResetTheirSharedFacts(@TempDir Path work) throws Exception {
        Path source=Files.createDirectory(work.resolve("source")),target=Files.createDirectory(work.resolve("target"));
        FileEndpoints.replaceTable(source.resolve(TABLE+".csv"),"id,priority\n1,initial\n");
        byte[] connector=CdcRecoveryFixture.connectorJar(E2eConnectorJar.buildInto(Files.createDirectory(work.resolve("connector"))));
        try (var storage=PreparedNativeStore.create(); var documents=new NativeSrsDocuments(storage);
             var observer=MongoClients.create(storage.configurationUri()); var cluster=TwoMemberCluster.startPrepared(storage)) {
            var control=cluster.first(); control.registerConnector(E2eConnectorJar.CONNECTOR_ID,connector);
            control.apply(resources(source,target)); control.discoverSchema(SOURCE,E2eConnectorJar.CONNECTOR_ID,Map.of("uri",source.toString()));
            control.lifecycle(PIPELINE,LifecycleVerb.START); awaitValue(target,"1","initial");
            CdcRecoveryFixture.change(source,1,TABLE,"1","initial","before-join"); awaitValue(target,"1","before-join");
            Await.until("the healthy run's first CDC change to be durably acknowledged",BOUND,() -> {
                var offset=offset(documents); if (offset==null) return false;
                var progress=offset.get("progress",Document.class); var ack=progress.get("sinkAcked",Document.class);
                return ack!=null && ack.getString("token")!=null && !ack.getString("token").equals(progress.getString("cdcStartPosition"));
            },() -> String.valueOf(offset(documents)));
            String originalToken=offset(documents).get("progress",Document.class).get("sinkAcked",Document.class).getString("token");
            var cache=observer.getDatabase(storage.cacheDatabase()); var config=observer.getDatabase(storage.configurationDatabase());
            var protectedFacts=fingerprints(config); var originalNodes=new TreeMap<String,Document>();
            for (String node:List.of(TwoMemberCluster.NODE_A,TwoMemberCluster.NODE_B)) {
                var row=SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cache).find(new Document("resourceType","NODE_SESSION").append("resourceId",node)).first();
                assertThat(row).isNotNull(); originalNodes.put(node,row);
            }
            int httpPort=RealProcessServer.reservePort(),memberPort=RealProcessServer.reservePort();
            Path staging=ServerHandle.privateStagingDirectory(),output=work.resolve("failed-join.log");
            var arguments=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-Xmx512m","-XX:ActiveProcessorCount=2",
                    "-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),
                    io.tapstate.app.FailingNativeJoinProcess.class.getName(),storage.configurationUri(),staging.toString()));
            arguments.addAll(cluster.joiningArguments(JOINING,httpPort,memberPort)); arguments.add("--server.port="+httpPort);
            Process joining=null;
            try {
                RealProcessServer.releasePort(httpPort); RealProcessServer.releasePort(memberPort);
                joining=new ProcessBuilder(arguments).directory(work.toFile()).redirectErrorStream(true).redirectOutput(output.toFile()).start();
                Process child=joining;
                Await.until("the candidate to start its actual native member",BOUND,() -> text(output).contains("JOIN_CANDIDATE_HELD "),() -> tail(output));
                assertThat(child.isAlive()).isTrue();
                String message=text(output).lines().filter(line -> line.startsWith("JOIN_CANDIDATE_HELD ")).findFirst().orElseThrow();
                var held=(Map<?,?>)JsonReader.parse(message.substring("JOIN_CANDIDATE_HELD ".length()));
                assertThat(held.get("nodeId")).isEqualTo(JOINING);
                assertThat(cluster.awaitMembers(3)).containsExactly(TwoMemberCluster.NODE_A,TwoMemberCluster.NODE_B,JOINING);
                Await.until("the shared membership to actually commit the joining node",BOUND,() -> {
                    var row=SystemCollections.CACHE_CLUSTER_MEMBERSHIP.on(cache).find().first();
                    return row!=null && new HashSet<>(row.getList("activeNodeIds",String.class)).equals(Set.of(TwoMemberCluster.NODE_A,TwoMemberCluster.NODE_B,JOINING));
                },() -> String.valueOf(SystemCollections.CACHE_CLUSTER_MEMBERSHIP.on(cache).find().first()));
                child.getOutputStream().write('\n'); child.getOutputStream().flush();
                Await.until("the failed candidate's exact resources to be closed",BOUND,() -> text(output).contains("JOIN_FAILED_CLEANED"),() -> tail(output));
                assertThat(child.isAlive()).as("its retained control process survives the isolated business failure").isTrue();
                Await.until("the healthy members to observe the failed joiner's departure",BOUND,
                        () -> control.clusterMemberNodeIds().equals(List.of(TwoMemberCluster.NODE_A,TwoMemberCluster.NODE_B)),() -> control.clusterReading());
                assertThat(SystemCollections.CACHE_CLUSTER_MEMBERSHIP.on(cache).find().first().getList("activeNodeIds",String.class))
                        .containsExactly(TwoMemberCluster.NODE_A,TwoMemberCluster.NODE_B,JOINING);
                for (var original:originalNodes.entrySet()) {
                    var live=node(cache,original.getKey()); assertThat(live).containsEntry("leased",true);
                    for (String field:List.of("_id","ownerNodeId","ownerBootId","claimGeneration")) assertThat(live.get(field)).isEqualTo(original.getValue().get(field));
                    Await.until("the healthy peer to actually renew after candidate cleanup",Duration.ofSeconds(20),() -> {
                        var renewed=node(cache,original.getKey());
                        return renewed!=null && Boolean.TRUE.equals(renewed.get("leased")) && renewed.getDate("leaseUntil").after(live.getDate("leaseUntil"))
                                && List.of("_id","ownerNodeId","ownerBootId","claimGeneration").stream().allMatch(field -> Objects.equals(renewed.get(field),live.get(field)));
                    },() -> String.valueOf(node(cache,original.getKey())));
                }
                var failed=node(cache,JOINING);
                assertThat(failed).containsEntry("ownerBootId",held.get("bootId")).containsEntry("claimGeneration",((Number)held.get("claimGeneration")).longValue());
                assertThat(failed).containsEntry("leased",false);
                assertThat(fingerprints(config)).isEqualTo(protectedFacts);
                CdcRecoveryFixture.change(source,2,TABLE,"2","","after-failed-join"); awaitValue(target,"2","after-failed-join");
                assertThat(value(target,"1")).isEqualTo("before-join");
                Await.until("the surviving native run to remain continuous",BOUND,() -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                        () -> control.state(PIPELINE)+" "+control.failureCode(PIPELINE));
                assertThat(cluster.second().state(PIPELINE)).contains(PipelineState.RUNNING);
                assertThat(documents.pipelineMembership(PIPELINE,SOURCE)).containsEntry("stateGeneration",1L);
                Await.until("the healthy peers to actually confirm their post-failure target write",BOUND,() -> {
                    var cursor=offset(documents); if (cursor==null) return false;
                    var progress=cursor.get("progress",Document.class); var ack=progress.get("sinkAcked",Document.class);
                    return ack!=null && ack.getString("token")!=null && !ack.getString("token").equals(originalToken)
                            && !ack.getString("token").equals(progress.getString("cdcStartPosition"));
                },() -> String.valueOf(offset(documents)));
                assertThat(fingerprints(config)).isEqualTo(protectedFacts);
            } finally {
                try {
                    if (joining!=null) {
                        try { joining.getOutputStream().close(); } finally {
                            try {
                                if (!joining.waitFor(10,TimeUnit.SECONDS)) {
                                    joining.destroy();
                                    if (!joining.waitFor(5,TimeUnit.SECONDS)) { joining.destroyForcibly(); joining.waitFor(5,TimeUnit.SECONDS); }
                                }
                            } finally { if (joining.isAlive()) joining.destroyForcibly(); }
                        }
                    }
                } finally {
                    try { RealProcessServer.releasePort(httpPort); } finally {
                        try { RealProcessServer.releasePort(memberPort); } finally {
                            assertThat(joining==null || !joining.isAlive())
                                    .as("the owned helper must stop before its staging directory can be discarded").isTrue();
                            ServerHandle.discardStagingDirectory(staging);
                        }
                    }
                }
            }
        }
    }
    private static String text(Path file) { try { return Files.exists(file) ? Files.readString(file) : ""; } catch (java.io.IOException failure) { throw new java.io.UncheckedIOException(failure); } }
    private static Document offset(NativeSrsDocuments documents) {
        var member=documents.pipelineMembership(PIPELINE,SOURCE); if (member==null) return null;
        return documents.consumerOffset(member.get("_id",Document.class).getString("chain"),io.tapstate.spi.store.SrsConsumerId.of(PIPELINE,SOURCE).value());
    }
    private static Document node(com.mongodb.client.MongoDatabase cache,String node) {
        return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cache).aggregate(List.of(
                new Document("$match",new Document("resourceType","NODE_SESSION").append("resourceId",node)),
                new Document("$set",new Document("leased",new Document("$gt",List.of("$leaseUntil","$$NOW")))))).first();
    }
    private static String tail(Path file) { String value=text(file); return value.substring(Math.max(0,value.length()-5000)); }
    private static Map<String,String> fingerprints(com.mongodb.client.MongoDatabase database) {
        var result=new TreeMap<String,String>();
        for (var table:List.of(SystemCollections.CLUSTER_KEYS,SystemCollections.STORAGE_BINDING,SystemCollections.STORAGE_SETUP,SystemCollections.OP_BOOTSTRAP,SystemCollections.SOURCES))
            result.put(table.collectionName(),CanonicalHash.ofText(table.on(database).find().sort(new Document("_id",1)).into(new ArrayList<>()).stream().map(Document::toJson).toList().toString()));
        return result;
    }
    private static String value(Path target,String id) {
        var lines=CdcRecoveryFixture.lines(target.resolve(TABLE+".csv")); if (lines.isEmpty()) return null;
        var columns=List.of(lines.getFirst().split(",",-1)); int key=columns.indexOf("id"),value=columns.indexOf("priority");
        for (String line:lines.subList(1,lines.size())) { var cells=line.split(",",-1); if (cells.length>Math.max(key,value) && id.equals(cells[key])) return cells[value]; } return null;
    }
    private static void awaitValue(Path target,String id,String expected) {
        Await.until("the real target row "+id+" to equal "+expected,BOUND,() -> expected.equals(value(target,id)),
                () -> CdcRecoveryFixture.lines(target.resolve(TABLE+".csv")).toString());
    }
    private static Map<String,String> resources(Path source,Path target) {
        return Map.of(SOURCE+".tap.yml","""
                version: tapstate/v1
                kind: source
                id: %s
                connector: e2e_file
                config: {uri: "%s"}
                mode: cdc
                tables: [orders]
                """.formatted(SOURCE,source),TARGET+".tap.yml",Workspaces.targetYaml(TARGET,target),PIPELINE+".tap.yml","""
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [{id: %s, srs: true}]
                settings: {read_mode: snapshot_and_cdc}
                serve: {from: orders, sync: [{source: %s}]}
                """.formatted(PIPELINE,SOURCE,TARGET));
    }
}
