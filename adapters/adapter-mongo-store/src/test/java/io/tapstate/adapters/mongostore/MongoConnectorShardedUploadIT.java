package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Files and chunks on different shards exercise every participant of the recovery transaction. */
@RequiresDocker
class MongoConnectorShardedUploadIT {
    private static final DockerImageName IMAGE = DockerImageName.parse("mongo:7.0");

    @Test
    void lateFileAndChunkCommitsCannotOutliveRecoveryAcrossShardParticipants() throws Exception {
        try (Network network = Network.newNetwork();
             GenericContainer<?> config = member(network, "config", true);
             GenericContainer<?> first = member(network, "first", false);
             GenericContainer<?> second = member(network, "second", false);
             GenericContainer<?> router = new GenericContainer<>(IMAGE).withNetwork(network).withExposedPorts(27017)
                     .withCommand("mongos", "--configdb", "config/config:27017", "--bind_ip_all",
                             "--setParameter", "enableTestCommands=1")
                     .withCreateContainerCmdModifier(command -> command.getHostConfig().withMemory(512L * 1024 * 1024))) {
            initiate(config, "config", true);
            initiate(first, "first", false);
            initiate(second, "second", false);
            router.start();
            String uri = "mongodb://" + router.getHost() + ":" + router.getMappedPort(27017) + "/";
            try (MongoClient client = MongoClients.create(uri)) {
                for (String shard : new String[]{"first", "second"}) {
                    client.getDatabase("admin").runCommand(new Document("addShard", shard + "/" + shard + ":27017"));
                }
            }
            byte[] chunks = new byte[17 * 255 * 1024 + 1];
            Arrays.fill(chunks, (byte) 59);
            for (byte[] bytes : new byte[][]{"delayed-file-commit".getBytes(java.nio.charset.StandardCharsets.UTF_8), chunks}) {
                MongoConnectorRegistryIT.assertLateCommitIsFenced(uri, false, 2, bytes, database -> {
                    try (MongoClient client = MongoClients.create(uri)) {
                        var admin = client.getDatabase("admin");
                        admin.runCommand(new Document("enableSharding", database.getName()));
                        String primary = client.getDatabase("config").getCollection("databases")
                                .find(new Document("_id", database.getName())).first().getString("primary");
                        String target = primary.equals("first") ? "second" : "first";
                        String namespace = database.getName() + ".connector_artifacts.chunks";
                        admin.runCommand(new Document("shardCollection", namespace)
                                .append("key", new Document("files_id", 1).append("n", 1)));
                        admin.runCommand(new Document("moveChunk", namespace)
                                .append("find", new Document("files_id", new ObjectId()).append("n", 0))
                                .append("to", target).append("_waitForDelete", true));
                        var configDatabase = client.getDatabase("config");
                        var collection = configDatabase.getCollection("collections", org.bson.BsonDocument.class)
                                .find(new org.bson.BsonDocument("_id", new org.bson.BsonString(namespace))).first();
                        var placement = configDatabase.getCollection("chunks", org.bson.BsonDocument.class)
                                .find(new org.bson.BsonDocument("uuid", collection.get("uuid"))
                                        .append("shard", new org.bson.BsonString(target))).first();
                        assertThat(placement).as("chunks moved away from the files collection's primary shard").isNotNull();
                    }
                });
            }
        }
    }

    private static GenericContainer<?> member(Network network, String alias, boolean config) {
        return new GenericContainer<>(IMAGE).withNetwork(network).withNetworkAliases(alias).withExposedPorts(27017)
                .withCommand("mongod", config ? "--configsvr" : "--shardsvr", "--replSet", alias,
                        "--port", "27017", "--bind_ip_all", "--wiredTigerCacheSizeGB", "0.25")
                .withCreateContainerCmdModifier(command -> command.getHostConfig().withMemory(512L * 1024 * 1024));
    }

    private static void initiate(GenericContainer<?> member, String alias, boolean config) throws Exception {
        member.start();
        String script = "const r=rs.initiate({_id:'" + alias + "',"
                + (config ? "configsvr:true," : "") + "members:[{_id:0,host:'" + alias + ":27017'}]});"
                + "if(r.ok!==1) throw Error(JSON.stringify(r));"
                + "for(let i=0;i<100;i++){if(db.hello().isWritablePrimary)quit(0);sleep(200);}quit(1);";
        var result = member.execInContainer("mongosh", "--quiet", "--eval", script);
        assertThat(result.getExitCode()).as(result.getStdout() + result.getStderr()).isZero();
    }
}
