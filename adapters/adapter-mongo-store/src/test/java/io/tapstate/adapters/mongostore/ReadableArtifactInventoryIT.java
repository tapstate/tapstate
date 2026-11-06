package io.tapstate.adapters.mongostore;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.mongodb.client.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/** Batch inventory preserves fault isolation and collection-local collation. */
@io.tapstate.testsupport.RequiresDocker
class ReadableArtifactInventoryIT {
    static MongoDBContainer mongo;
    static MongoClient client;
    static final DslParser parser=new DslParser();
    @BeforeAll static void start() {
        mongo=new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withTmpFs(Map.of("/data/db","rw","/data/configdb","rw"))
            .withCreateContainerCmdModifier(cmd->cmd.getHostConfig().withMemory(768L*1024*1024).withNanoCPUs(1_000_000_000L))
            .withCommand("--replSet","docker-rs","--bind_ip_all","--wiredTigerCacheSizeGB","0.25");
        mongo.start();client=MongoClients.create(mongo.getReplicaSetUrl());
    }
    @AfterAll static void stop() {if(client!=null)client.close();if(mongo!=null)mongo.close();}
    Resource serve(String id) {return parser.parse("version: tapstate/v1\nkind: serve\nid: "+id+"\n");}
    StoredArtifactRecord readable(String id) {return StoredArtifactRecord.of(serve(id));}
    @Test void defaultRetainsUnreadableDisappearanceAndReadTimeCorruptionBehavior() {
        ArtifactStore store=mock(ArtifactStore.class,CALLS_REAL_METHODS);
        Resource normal=serve("normal");
        when(store.listStored()).thenReturn(List.of(StoredArtifactRecord.of(normal),new StoredArtifactRecord("bad","pipeline",null,null,false),readable("gone"),readable("changed")));
        when(store.get("normal")).thenReturn(Optional.of(normal));when(store.get("gone")).thenReturn(Optional.empty());
        when(store.get("changed")).thenThrow(new TapstateException(IoError.DOCUMENT_UNREADABLE,Map.of("id","changed","field","body"),null));
        assertThat(store.listReadable()).containsExactly(normal);
        verify(store,never()).get("bad");
    }
    @Test void defaultPropagatesOtherCodedFailures() {
        ArtifactStore store=mock(ArtifactStore.class,CALLS_REAL_METHODS);
        var failure=new TapstateException(IoError.SOURCE_IDENTITY_UNAVAILABLE,Map.of("id","failed"),null);
        when(store.listStored()).thenReturn(List.of(readable("failed")));when(store.get("failed")).thenThrow(failure);
        assertThatThrownBy(store::listReadable).isSameAs(failure);
    }
    @Test void defaultPropagatesProgrammerFailures() {
        ArtifactStore store=mock(ArtifactStore.class,CALLS_REAL_METHODS);var failure=new IllegalStateException("fixture invariant");
        when(store.listStored()).thenReturn(List.of(readable("failed")));when(store.get("failed")).thenThrow(failure);
        assertThatThrownBy(store::listReadable).isSameAs(failure);
    }
    @Test void mongoDecodesEncryptedSourceAndSkipsOnlyCorruptBodies() {
        withStore((store,db,codec)->{
            Resource source=new SourceResource("src",null,"mongodb",Map.of("uri","mongodb://fixture:secret@localhost/db"),null,null,null,null);
            Resource normal=serve("normal");
            var encrypted=codec.encode(source);assertThat(encrypted.toJson()).doesNotContain("fixture:secret");
            SystemCollections.SOURCES.on(db).insertOne(encrypted);
            SystemCollections.SERVE_DEFINITIONS.on(db).insertMany(List.of(codec.encode(normal),new Document("_id","bad").append("kind","serve").append("body","corrupt")));
            assertThat(store.listReadable()).containsExactlyInAnyOrder(source,normal);
            assertThat(store.listReadable().stream().map(CanonicalHash::of).toList()).containsExactlyInAnyOrder(CanonicalHash.of(source),CanonicalHash.of(normal));
            assertThatThrownBy(store::list).isInstanceOfSatisfying(TapstateException.class,e->assertThat(e.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(store.listStored()).filteredOn(row->!row.readable()).extracting(StoredArtifactRecord::id).containsExactly("bad");
        });
    }
    @Test void mongoPropagatesOtherCodedDecodeFailures() {
        withStore((store,db,codec)->{
            Resource source=new SourceResource("src",null,"mongodb",Map.of("uri","mongodb://localhost/db"),null,null,null,null);
            SystemCollections.SOURCES.on(db).insertOne(codec.encode(source));
            var provider=mock(SourceConfigCipherProvider.class);
            var failure=new TapstateException(IoError.SOURCE_IDENTITY_UNAVAILABLE,Map.of("id","src"),null);
            when(provider.current()).thenThrow(failure);
            var failing=new MongoConfigurationArtifactStore(client,db,provider);
            assertThatThrownBy(failing::listReadable).isSameAs(failure);
        });
    }
    @Test void mongoRetainsExistingCollectionLocalCollationResults() throws Exception {
        MongoDatabase db=client.getDatabase("readable_collation_"+UUID.randomUUID().toString().replace("-",""));
        try {
            db.createCollection(SystemCollections.VIEW_DEFINITIONS.collectionName(),new com.mongodb.client.model.CreateCollectionOptions()
                .collation(com.mongodb.client.model.Collation.builder().locale("en")
                    .collationStrength(com.mongodb.client.model.CollationStrength.SECONDARY).build()));
            var cipher=new SourceConfigCipher(new byte[32]);var codec=new EncryptedArtifactCodec(cipher);
            Resource view=parser.parse("version: tapstate/v1\nkind: view\nid: Alias\nprimary_key: id\n");
            Resource serve=serve("alias");
            SystemCollections.VIEW_DEFINITIONS.on(db).insertOne(codec.encode(view));
            SystemCollections.SERVE_DEFINITIONS.on(db).insertOne(codec.encode(serve));
            var store=new MongoConfigurationArtifactStore(client,db,cipher);
            var existing=baseline(store);
            assertThat(existing).as("unchanged helper behavior under collection-local collation").containsExactly(view);
            assertThat(store.listReadable()).as("optimized typed list must preserve existing collation results").containsExactlyElementsOf(existing);
        } finally {db.drop();}
    }
    @Test void mongoRetainsNumericCollectionCollation() throws Exception {
        MongoDatabase db=client.getDatabase("readable_numeric_"+UUID.randomUUID().toString().replace("-",""));
        try {
            db.createCollection(SystemCollections.VIEW_DEFINITIONS.collectionName(),new com.mongodb.client.model.CreateCollectionOptions()
                .collation(com.mongodb.client.model.Collation.builder().locale("en").numericOrdering(true).build()));
            var cipher=new SourceConfigCipher(new byte[32]);var codec=new EncryptedArtifactCodec(cipher);
            Resource view=parser.parse("version: tapstate/v1\nkind: view\nid: item02\nprimary_key: id\n");
            SystemCollections.VIEW_DEFINITIONS.on(db).insertOne(codec.encode(view));
            SystemCollections.SERVE_DEFINITIONS.on(db).insertOne(codec.encode(serve("item2")));
            var store=new MongoConfigurationArtifactStore(client,db,cipher);
            var existing=baseline(store);
            assertThat(existing).containsExactly(view);
            assertThat(store.listReadable()).containsExactlyElementsOf(existing);
        } finally {db.drop();}
    }
    @Test void mongoUsesHistoricalCollationAfterConcurrentCollectionRecreation() {
        withStore((unused,db,codec)->{
            db.createCollection(SystemCollections.SOURCES.collectionName());
            db.createCollection(SystemCollections.VIEW_DEFINITIONS.collectionName(),new com.mongodb.client.model.CreateCollectionOptions()
                .collation(com.mongodb.client.model.Collation.builder().locale("en")
                    .collationStrength(com.mongodb.client.model.CollationStrength.SECONDARY).build()));
            Resource view=parser.parse("version: tapstate/v1\nkind: view\nid: Alias\nprimary_key: id\n");
            var original=codec.encode(view);
            SystemCollections.VIEW_DEFINITIONS.on(db).insertOne(original);
            SystemCollections.SERVE_DEFINITIONS.on(db).insertOne(codec.encode(serve("alias")));
            var request=new java.util.concurrent.atomic.AtomicInteger(-1);var armed=new java.util.concurrent.atomic.AtomicBoolean(true);
            var listener=new com.mongodb.event.CommandListener(){
                public void commandStarted(com.mongodb.event.CommandStartedEvent e){
                    if(e.getDatabaseName().equals(db.getName()) && e.getCommandName().equals("find")
                        && e.getCommand().getString("find").getValue().equals(SystemCollections.SOURCES.collectionName()) && armed.compareAndSet(true,false))request.set(e.getRequestId());
                }
                public void commandSucceeded(com.mongodb.event.CommandSucceededEvent e){
                    if(e.getRequestId()!=request.get())return;
                    SystemCollections.VIEW_DEFINITIONS.on(db).drop();
                    db.createCollection(SystemCollections.VIEW_DEFINITIONS.collectionName());
                    SystemCollections.VIEW_DEFINITIONS.on(db).insertOne(original);
                }
            };
            var settings=com.mongodb.MongoClientSettings.builder().applyConnectionString(new com.mongodb.ConnectionString(mongo.getReplicaSetUrl())).addCommandListener(listener).build();
            try(var reader=MongoClients.create(settings)){
                var store=new MongoConfigurationArtifactStore(reader,reader.getDatabase(db.getName()),new SourceConfigCipher(new byte[32]));
                assertThat(store.listReadable()).containsExactly(view);
            }
            assertThat(armed.get()).isFalse();
            assertThat(SystemCollections.VIEW_DEFINITIONS.on(db).find(new Document("_id","alias")).first()).isNull();
        });
    }
    @Test void mongoRejectsDuplicateIdentityBeforeToleratingAnyBody() {
        withStore((store,db,codec)->{
            SystemCollections.SERVE_DEFINITIONS.on(db).insertOne(codec.encode(serve("same")));
            SystemCollections.VIEW_DEFINITIONS.on(db).insertOne(new Document("_id","same").append("kind","view").append("body","corrupt"));
            assertThatThrownBy(store::listReadable).isInstanceOfSatisfying(TapstateException.class,e->{assertThat(e.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);assertThat(e.args().get("field")).isEqualTo("identity");});
        });
    }
    @Test void mongoRejectsWrongPhysicalKind() {
        withStore((store,db,codec)->{
            SystemCollections.SERVE_DEFINITIONS.on(db).insertOne(new Document("_id","bad").append("kind","pipeline").append("body","corrupt"));
            assertThatThrownBy(store::listReadable).isInstanceOfSatisfying(TapstateException.class,e->{assertThat(e.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);assertThat(e.args().get("field")).isEqualTo("kind");});
        });
    }
    private static List<Resource> baseline(ArtifactStore store) {
        var result = new ArrayList<Resource>();
        for (var row : store.listStored()) if (row.readable()) {
            try { store.get(row.id()).ifPresent(result::add); }
            catch (TapstateException failure) { if (failure.code() != IoError.DOCUMENT_UNREADABLE) throw failure; }
        }
        return List.copyOf(result);
    }
    void withStore(Scenario scenario) {
        MongoDatabase db=client.getDatabase("readable_contract_"+UUID.randomUUID().toString().replace("-",""));
        try {var cipher=new SourceConfigCipher(new byte[32]);scenario.run(new MongoConfigurationArtifactStore(client,db,cipher),db,new EncryptedArtifactCodec(cipher));}
        finally {db.drop();}
    }
    interface Scenario {void run(MongoConfigurationArtifactStore store,MongoDatabase db,EncryptedArtifactCodec codec);}
}
