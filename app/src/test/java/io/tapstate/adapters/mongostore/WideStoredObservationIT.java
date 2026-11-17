package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.*;
import io.tapstate.core.lifecycle.*;
import io.tapstate.runtime.scheduler.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.*;
import org.bson.codecs.*;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@RequiresDocker
class WideStoredObservationIT {
    @Container private static final MongoDBContainer MONGO =
            new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final String ID = "wide_probe";
    private static final Instant START = Instant.parse("2026-11-17T04:00:00Z");
    private static final List<String> OPS = List.of("r","i","u","d","ddl");
    @org.junit.jupiter.api.io.TempDir Path output;
    private final List<Map<String,Object>> EVIDENCE = new ArrayList<>();

    private record Inputs(CaptureReading capture, DeliveryReading delivery) {}
    private static Inputs inputs(int tables) {
        Map<String,Map<String,Long>> rows = new TreeMap<>();
        Map<String,Long> bytes = new TreeMap<>(), recent = new TreeMap<>();
        Map<String,HistogramValue> histograms = new TreeMap<>();
        var buckets = new ArrayList<>(Collections.nCopies(HistogramBounds.RECORD_DELIVERY_DURATION.buckets(),0L));
        buckets.set(4,5L);
        var distribution = HistogramBounds.RECORD_DELIVERY_DURATION.value(5,1.0,buckets);
        for(int i=0;i<tables;i++){
            String table = "table_" + String.format(Locale.ROOT,"%06d",i);
            Map<String,Long> ops = new TreeMap<>();
            for(String op:OPS) ops.put(op,1L);
            rows.put(table,ops);
            bytes.put(table,500L);
            recent.put(table,START.toEpochMilli());
            histograms.put(table,distribution);
        }
        return new Inputs(new CaptureReading(rows,bytes,START), new DeliveryReading(rows,bytes,recent,START,histograms));
    }

    @Test void widePublishedFramesExceedTheStoredBudgetAndEventuallyTheBsonLimit() throws Exception {
        String database = "wide_observation_" + UUID.randomUUID().toString().replace("-","");
        var recording = new Commands(database);
        try(var client=MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(recording).build())){
            var db=client.getDatabase(database);
            int maxBson=db.runCommand(new Document("hello",1)).getInteger("maxBsonObjectSize");
            db.createCollection(SystemCollections.CACHE_PIPELINE_OBSERVATION.collectionName());
            var collection=SystemCollections.CACHE_PIPELINE_OBSERVATION.on(db);
            var scope=new PipelineIntentScope("fixture-cluster",1,"fixture-cache",ID,1);
            var claim=new WorkloadClaimFence(new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,ID),
                    new WorkloadOwner("fixture-node","fixture-boot"),1,1,1);
            // Exercise the exact current native save path. This isolated fixture supplies no runtime/data-plane claims.
            var destination=new MongoCachePipelineObservationStore(new MongoCachePipelineAccess(client,db,scope,claim,()->{}));
            var checkpoint=new CheckpointDoc(ID,StateJson.of(PipelineState.RUNNING),7,START);
            StateStore state=mock(StateStore.class);
            when(state.read(ID)).thenReturn(Optional.of(checkpoint));
            var sampled=new AtomicReference<Observation>();
            ObservationStore capture=new ObservationStore(){
                public void save(Observation observation){throw new AssertionError("scoped epoch required");}
                public void save(Observation observation,long epoch){
                    sampled.set(observation);
                    destination.save(observation,epoch);
                }
                public Optional<Observation> read(String id){throw new AssertionError("running publication must not load old observation");}
                public void delete(String id){throw new AssertionError("not a deletion fixture");}
            };
            var reading=new AtomicReference<>(inputs(1));
            var at=new AtomicReference<>(START.plusSeconds(1));
            var clock=new Clock(){
                public ZoneId getZone(){return ZoneOffset.UTC;}
                public Clock withZone(ZoneId ignored){return this;}
                public Instant instant(){return at.get();}
            };
            var publisher=new ObservationPublisher(state,capture,id->OptionalLong.empty(),id->Map.of(),
                    id->SnapshotReading.NONE,id->Map.of(),id->Map.of(),
                    new NestColdLayerWatch(NestColdLayerPressure.DEFAULT,NestColdLayerAlert.NONE),
                    id->Map.of(),new FrontierStallWatch(FrontierStallPressure.DEFAULT,FrontierStallAlert.NONE),
                    id->Map.of(),id->Map.of(),id->Map.of(),id->reading.get().capture,id->reading.get().delivery,clock);
            Document key=null;
            for(int n:List.of(1,10,50,200,1000,2000,5000,6000)){
                reading.set(inputs(n));
                for(int repeat=1;repeat<=3;repeat++){
                    at.set(at.get().plusSeconds(1));
                    Document before=collection.find().first();
                    recording.begin();
                    String refusal="NONE";
                    Observation generated = null;
                    try{
                        generated = publisher.publishScoped(scope,state,capture,null).orElseThrow();
                    }catch(TapstateException tooLarge){
                        refusal=tooLarge.code().code();
                    }finally{
                        recording.armed.set(false);
                    }
                    assertThat(refusal).isEqualTo("NONE");
                    Observation raw=generated;
                    Observation stored=sampled.get();
                    verifyFold(raw.facts(),stored.facts());
                    assertThat(stored.metrics()).isEqualTo(raw.metrics());
                    assertThat(stored.snapshot()).isEqualTo(raw.snapshot());
                    assertThat(stored.positions()).isEqualTo(raw.positions());
                    assertThat(stored.initialLoad()).isEqualTo(raw.initialLoad());
                    assertThat(stored.failure()).isEqualTo(raw.failure());
                    assertThat(stored.observedAt()).isEqualTo(raw.observedAt());
                    assertThat(stored.state()).isEqualTo(raw.state());
                    int savedPoints=stored.facts().stream().mapToInt(f->f.points().size()).sum();
                    long savedOverflow=stored.facts().stream().flatMap(f->f.points().stream())
                            .filter(p->p.attributes().containsKey(MetricAttributes.OVERFLOW)).count();
                    assertThat(savedPoints).isEqualTo(14*Math.min(n,1001));
                    assertThat(savedOverflow).isEqualTo(n>1000?14:0);
                    int points=raw.facts().stream().mapToInt(f->f.points().size()).sum();
                    long overflow=raw.facts().stream().flatMap(f->f.points().stream())
                            .filter(p->p.attributes().containsKey(MetricAttributes.OVERFLOW)).count();
                    assertThat(points).isEqualTo(14*n);
                    assertThat(raw.facts()).hasSize(4);
                    assertThat(overflow).isZero();
                    assertThat(raw.metrics().get("records.out")).isEqualTo(5L*n);
                    assertThat(raw.metrics().get("records.in")).isEqualTo(5L*n);
                    Document after=collection.find().first();
                    if(key==null) key=(Document)after.get("_id");
                    var proposed=MongoObservationStore.toDocument(stored).append("_id",key).append("pipelineId",ID);
                    int rawBytes=size(MongoObservationStore.toDocument(raw).append("_id",key).append("pipelineId",ID));
                    int nativeBytes=size(proposed);
                    assertThat(recording.commands.getOrDefault("update",0)).isLessThanOrEqualTo(1);
                    long persistedBytes=collection.aggregate(List.of(new Document("$project",new Document("bytes",new Document("$bsonSize","$$ROOT")))))
                            .first().getInteger("bytes").longValue();
                    if(refusal.equals("NONE")){
                        assertThat(nativeBytes).isEqualTo(persistedBytes);
                        var decoded=MongoObservationStore.toObservation(new Document(after).append("_id",ID));
                        assertThat(decoded).isEqualTo(stored);
                        assertThat(recording.commands.getOrDefault("update",0)).isEqualTo(1);
                    }else{
                        assertThat(nativeBytes).isGreaterThan(maxBson);
                        assertThat(refusal).isEqualTo(IoError.DOCUMENT_TOO_LARGE.code());
                        assertThat(after).isEqualTo(before);
                    }
                    var folded=CardinalityBudget.folder();
                    List<MetricFact> facts=raw.facts().stream().map(folded::fold).toList();
                    verifyFold(raw.facts(),facts);
                    var diagnostic=new Observation(ID,raw.state(),raw.metrics(),raw.snapshot(),raw.positions(),
                            raw.failure(),raw.observedAt(),facts);
                    int diagnosticBytes=size(MongoObservationStore.toDocument(diagnostic).append("_id",key).append("pipelineId",ID));
                    var shell=new LinkedHashMap<String,Integer>();
                    for(String name:List.of("metrics","facts","positions","snapshot")){
                        shell.put(name,size(new Document(name,proposed.get(name))));
                    }
                    Map<String,Object> result=new LinkedHashMap<>();
                    result.put("tables",n);result.put("repeat",repeat);result.put("factsPoints",points);
                    result.put("overflowPoints",savedOverflow);result.put("storedFactsPoints",savedPoints);
                    result.put("rawBsonBytes",rawBytes);result.put("nativeBsonBytes",nativeBytes);
                    result.put("mongoPersistedBytes",persistedBytes);result.put("maxBsonObjectSize",maxBson);
                    result.put("outcome",refusal);result.put("commands",new TreeMap<>(recording.commands));
                    result.put("commandBsonBytes",recording.commandBytes);
                    result.put("factsFoldedOnlyPoints",facts.stream().mapToInt(f->f.points().size()).sum());
                    result.put("factsFoldedOnlyBsonBytes",diagnosticBytes);result.put("fields",shell);
                    EVIDENCE.add(result);
                    System.out.println("WIDE_WRITE "+JsonWriter.write(result));
                }
            }
            assertThat(EVIDENCE).hasSize(24);
            assertThat(EVIDENCE.stream().filter(e->!e.get("outcome").equals("NONE")).count()).isZero();
            assertThat(collection.countDocuments()).isEqualTo(1);
            Document retained=collection.find().first();
            long rawPoints=((List<Document>)retained.get("facts")).stream().mapToLong(f->f.getList("points",Document.class).size()).sum();
            assertThat(rawPoints).isEqualTo(14_014);
            var projectionFailure="";
            var candidate=reading.get();
            // The current flat reduction does not define a key when a folded LAG loses table identity.
            try{
                FlatMetricProjection.of(List.of(CardinalityBudget.folder().fold(rawLag(candidate,at.get()))),
                        Map.of("tapstate.pipeline.lag",attrs -> attrs.containsKey(MetricAttributes.TABLE_ID)
                                ? "lag."+attrs.get(MetricAttributes.TABLE_ID):null));
            }catch(IllegalArgumentException expected){
                projectionFailure=expected.getClass().getSimpleName();
            }
            assertThat(projectionFailure).isEqualTo("IllegalArgumentException");
            Map<String,Object> report=Map.of("purpose","stored-observation-cardinality",
                    "method","actual publisher with supplied four-family facts -> current native CACHE save -> real Mongo BSON",
                    "dataPlaneThroughputMeasured",false,"flatOverflowRuleFailure",projectionFailure,"rows",EVIDENCE);
            Files.writeString(output.resolve("measurements.json"),JsonWriter.write(report)+"\n");
            db.drop();
        }
    }

    private static MetricFact rawLag(Inputs data,Instant at){
        return new MetricFact("tapstate.pipeline.lag",MetricType.GAUGE,"ms",
                data.delivery.newestEventTimeByTable().keySet().stream().map(t->
                        MetricPoint.reading(Map.of(MetricAttributes.PIPELINE_ID,ID,MetricAttributes.TABLE_ID,t),at,100L)).toList());
    }

    private static void verifyFold(List<MetricFact> before,List<MetricFact> after){
        for(int i=0;i<before.size();i++){
            var a=before.get(i);var b=after.get(i);
            if(a.type()==MetricType.HISTOGRAM){
                assertThat(b.points().stream().mapToDouble(p->p.histogram().sum()).sum())
                        .isEqualTo(a.points().stream().mapToDouble(p->p.histogram().sum()).sum());
                assertThat(b.points().stream().mapToLong(p->p.histogram().count()).sum())
                        .isEqualTo(a.points().stream().mapToLong(p->p.histogram().count()).sum());
                for(int bucket=0;bucket<HistogramBounds.RECORD_DELIVERY_DURATION.buckets();bucket++){
                    int k=bucket;
                    assertThat(b.points().stream().mapToLong(p->p.histogram().bucketCounts().get(k)).sum())
                            .isEqualTo(a.points().stream().mapToLong(p->p.histogram().bucketCounts().get(k)).sum());
                }
            }else if(a.name().equals("tapstate.pipeline.lag")){
                assertThat(b.points().stream().mapToLong(MetricPoint::value).max())
                        .isEqualTo(a.points().stream().mapToLong(MetricPoint::value).max());
            }else{
                assertThat(b.points().stream().mapToLong(MetricPoint::value).sum())
                        .isEqualTo(a.points().stream().mapToLong(MetricPoint::value).sum());
            }
        }
    }

    private static int size(Document document){
        try(var out=new BasicOutputBuffer();var writer=new BsonBinaryWriter(new BsonWriterSettings(),
                new BsonBinaryWriterSettings(Integer.MAX_VALUE),out)){
            new DocumentCodec(MongoClientSettings.getDefaultCodecRegistry()).encode(writer,document,
                    EncoderContext.builder().isEncodingCollectibleDocument(true).build());
            return out.getSize();
        }
    }

    private static int size(BsonDocument document){
        try(var out=new BasicOutputBuffer();var writer=new BsonBinaryWriter(new BsonWriterSettings(),
                new BsonBinaryWriterSettings(Integer.MAX_VALUE),out)){
            new BsonDocumentCodec().encode(writer,document,EncoderContext.builder().build());
            return out.getSize();
        }
    }

    private static final class Commands implements CommandListener{
        private final String database;
        final AtomicBoolean armed=new AtomicBoolean();
        final Map<String,Integer> commands=new LinkedHashMap<>();
        long commandBytes;
        Commands(String database){this.database=database;}
        void begin(){commands.clear();commandBytes=0;armed.set(true);}
        @Override public void commandStarted(CommandStartedEvent event){
            if(armed.get()&&database.equals(event.getDatabaseName())){
                commands.merge(event.getCommandName(),1,Integer::sum);
                commandBytes+=size(event.getCommand());
            }
        }
    }
}
