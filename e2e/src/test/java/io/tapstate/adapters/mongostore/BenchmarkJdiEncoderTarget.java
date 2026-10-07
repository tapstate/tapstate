package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.RateSample;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** A small synchronous workload loaded only beside the selected immutable artifact's libraries. */
public final class BenchmarkJdiEncoderTarget {

    private static final BufferedReader INPUT = new BufferedReader(
            new InputStreamReader(System.in, StandardCharsets.UTF_8));
    private static final String WIRE_DATABASE = "jdi_cost_witness";
    private static String phase = "ORIGINS";

    private BenchmarkJdiEncoderTarget() {
    }

    public static void main(String[] args) {
        try {
            run(args[0]);
        } catch (Throwable failure) {
            reportFailure(failure);
            System.exit(2);
        }
    }

    private static void run(String mode) throws Exception {
        origin(MongoObservationStore.class);
        origin(MongoRateHistoryStore.class);
        origin(DocumentCodec.class);
        origin(Observation.class);
        if (mode.startsWith("store-raw")) {
            runRawStore(mode);
            return;
        }
        if (mode.startsWith("wire")) {
            runWire(mode);
            return;
        }
        phase("ENCODER_SETUP");
        DocumentCodec codec = new DocumentCodec();
        EncoderContext context = EncoderContext.builder().build();
        ready();
        if (mode.equals("disconnect")) {
            System.exit(0);
        } else if (mode.equals("duplicate")) {
            phase("DUPLICATE_LOADER");
            try (URLClassLoader duplicate = new URLClassLoader(
                    new java.net.URL[]{DocumentCodec.class.getProtectionDomain().getCodeSource().getLocation()},
                    ClassLoader.getPlatformClassLoader())) {
                Class.forName("org.bson.codecs.DocumentCodec", true, duplicate);
            }
        } else {
            phase("OBSERVATION_BUILD");
            Observation observation = observation();
            for (int i = 0; i < 3; i++) {
                if (MongoObservationStore.toDocument(observation).isEmpty()) {
                    throw new AssertionError("observation representation is empty");
                }
            }
            phase("RATE_BUILD");
            RateSample sample = new RateSample("proof", Instant.ofEpochSecond(1_000),
                    Map.of("records.out", 7L), Map.of(), Instant.ofEpochSecond(900));
            for (int i = 0; i < 2; i++) {
                if (MongoRateHistoryStore.toDocument(sample).isEmpty()) {
                    throw new AssertionError("rate representation is empty");
                }
            }
            // A flat document makes one typed codec invocation equal one binary encoding here.
            phase("BSON_ENCODING");
            Document document = new Document("id", 1L).append("value", "proof");
            for (int i = 0; i < (mode.equals("extra") ? 6 : 5); i++) {
                try (BasicOutputBuffer output = new BasicOutputBuffer();
                        BsonBinaryWriter writer = new BsonBinaryWriter(output)) {
                    codec.encode(writer, document, context);
                    if (output.getSize() <= 0) {
                        throw new AssertionError("binary encoding produced no bytes");
                    }
                }
            }
            phase("UNRELATED_METHODS");
            for (int i = 0; i < 2_000; i++) {
                if (codec.getEncoderClass() != Document.class) {
                    throw new AssertionError("wrong encoder type");
                }
            }
            if (mode.equals("exception")) {
                phase("FAILURE_ENCODING");
                try {
                    codec.encode(null, document, context);
                    throw new AssertionError("the failed encoding unexpectedly returned");
                } catch (NullPointerException expected) {
                    // The observer must reject the unmatched entry instead of reporting a zero.
                }
            }
        }
        if (mode.equals("stderr-oversized")) {
            // An unterminated line distinguishes incremental bounds from a readLine EOF check.
            System.err.print("x".repeat(4_097));
            System.err.flush();
        }
        done();
        if (mode.equals("post-done")) {
            System.out.println("POST_DONE");
        }
    }

    private static void runWire(String mode) throws Exception {
        phase("WIRE_SETUP");
        origin(Class.forName("com.mongodb.internal.connection.InternalStreamConnection"));
        String uri = System.getenv("TAPSTATE_JDI_WITNESS_MONGO_URI");
        if (uri == null || uri.isBlank()) {
            throw new AssertionError("wire witness requires its private connection input");
        }
        try (MongoClient client = MongoClients.create(uri)) {
            var database = client.getDatabase(WIRE_DATABASE);
            String collectionName = mode.equals("wire-unmapped") ? "unmapped_rows"
                    : mode.equals("wire-pipeline-state") ? "pipeline_state" : "pipeline_observation";
            var collection = database.getCollection(collectionName);
            database.runCommand(new Document("ping", 1));
            collection.drop();
            ready();
            phase("WIRE_COMMANDS");
            // Three documents fit one command. A row counter would incorrectly report three here.
            collection.insertMany(List.of(new Document("id", 1), new Document("id", 2),
                    new Document("id", 3)));
            database.runCommand(new Document("ping", 1));
            if (collection.countDocuments() != 3) {
                throw new AssertionError("wire witness lost a document");
            }
        }
        done();
    }

    private static void runRawStore(String mode) throws Exception {
        phase("WIRE_SETUP");
        String uri = System.getenv("TAPSTATE_JDI_WITNESS_MONGO_URI");
        if (uri == null || uri.isBlank()) { throw new AssertionError("raw store witness has no private connection input"); }
        try (MongoClient client = MongoClients.create(uri)) {
            var database = client.getDatabase(WIRE_DATABASE);
            var collection = database.getCollection("pipeline_rate_history");
            collection.drop();
            var store = new MongoRateHistoryStore(database, collection, java.time.Duration.ofDays(15));
            client.getDatabase("admin").runCommand(new Document("ping", 1));
            RateSample sample = new RateSample("proof", Instant.now(), Map.of("records.out", 7L),
                    Map.of("orders", 3L), Instant.now().minusSeconds(60));
            ready();
            phase("RATE_BUILD");
            store.append(sample);
            if (mode.equals("store-raw-conversion")) {
                try (var writer = new org.bson.BsonDocumentWriter(new org.bson.BsonDocument())) {
                    new DocumentCodec().encode(writer, new Document("redundant", 1L), EncoderContext.builder().build());
                }
            }
            if (mode.equals("store-raw-extra")) {
                Document document = new Document("redundant", 1L);
                try (BasicOutputBuffer output = new BasicOutputBuffer(); BsonBinaryWriter writer = new BsonBinaryWriter(output)) {
                    new DocumentCodec().encode(writer, document, EncoderContext.builder().build());
                    if (output.getSize() <= 0) { throw new AssertionError("redundant encoding produced no bytes"); }
                }
            }
            done();
            if (collection.countDocuments() != 1) { throw new AssertionError("raw publication did not persist exactly one sample"); }
        }
    }

    static Observation observation() {
        return new Observation("proof", PipelineState.RUNNING, Map.of("records.out", 7L),
                Map.of(), Map.of(), null, Instant.ofEpochSecond(1_000));
    }

    static void origin(Class<?> type) throws Exception {
        // Class literals can remain unprepared. Initialization is a causal barrier: the class
        // prepare event must resume after exact breakpoints are installed before READY can follow.
        if (Class.forName(type.getName(), true, type.getClassLoader()) != type) {
            throw new AssertionError("the initialized class changed its pinned loader identity");
        }
        Path location = Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI()).toRealPath();
        System.out.println("ORIGIN\t" + type.getName() + "\t" + location);
    }

    static void ready() throws Exception {
        phase("START_BARRIER");
        System.out.println("READY");
        if (!"START".equals(INPUT.readLine())) {
            throw new AssertionError("missing start barrier");
        }
    }

    static void done() throws Exception {
        phase("STOP_BARRIER");
        System.out.println("DONE");
        if (!"STOP".equals(INPUT.readLine())) {
            throw new AssertionError("missing stop barrier");
        }
    }

    static void phase(String value) {
        phase = value;
    }

    static void reportFailure(Throwable failure) {
        String exceptionClass = failure.getClass().getName();
        String linkageSymbol = "-";
        // Messages and causes can carry credentials. Only a whole linkage message that is a
        // bounded binary class symbol is admitted; method signatures and prose remain absent.
        if (failure instanceof LinkageError && failure.getMessage() != null) {
            String candidate = failure.getMessage().replace('/', '.');
            if (candidate.length() <= 240
                    && candidate.matches("[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)+")) {
                linkageSymbol = candidate;
            }
        }
        System.out.println("TARGET_FAILED\t" + phase + "\t" + exceptionClass + "\t" + linkageSymbol);
    }
}
