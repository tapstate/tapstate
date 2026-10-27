package io.tapstate.adapters.mongostore;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteError;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.model.GridFSFile;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.testsupport.JvmTestEnvironment;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** The actual registry and GridFS reader consume generated chunks, never a second full artifact. */
class MongoConnectorArtifactReadTest {
    private static final int LARGE_LENGTH = 203_082_147;
    private static final int CHUNK_SIZE = 255 * 1024;
    private static final String LARGE_HASH = "b2d7cfb51a863b51247e8af1cd51751049a63e360b1d51c5d5ec343a14350ca0";

    @Test
    void aLargeStoredArtifactFitsAHeapThatCanHoldOneArrayButNotGrowingCopies() throws Exception {
        assertThat(heapWitness("large", "320m"))
                .contains("artifact-length=" + LARGE_LENGTH, "artifact-sha256=" + LARGE_HASH);
    }

    @Test
    void anUntrustedLengthBeyondTheAvailableHeapIsRejectedWithACodedDiagnostic() throws Exception {
        assertThat(heapWitness("oversized", "64m")).contains("oversized-artifact-rejected");
    }

    private String heapWitness(String witness, String heap) throws Exception {
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        ProcessBuilder command = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx" + heap, "-XX:+UseSerialGC", "-cp", classpath, getClass().getName(), witness);
        JvmTestEnvironment.configure(command, Map.of());
        command.redirectErrorStream(true);
        Process child = command.start();
        boolean completed = child.waitFor(30, TimeUnit.SECONDS);
        if (!completed) {child.destroyForcibly(); child.waitFor();}
        String output = new String(child.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(completed).as("the isolated heap witness completes without a hung reader").isTrue();
        assertThat(child.exitValue()).withFailMessage("The isolated heap witness failed:\n%s", output).isZero();
        return output;
    }

    @Test
    void contentHashRacesDoNotDependOnDuplicateMessageFormatting() throws Exception {
        String namespace = "artifact_read_test.connector_artifacts.files";
        for (String message : List.of(
                "E11000 duplicate key error collection: " + namespace + " index: filename_idx dup key: { : \"" + LARGE_HASH + "\" }",
                "E11000 duplicate key error collection: " + namespace + " index: filename_idx dup key: {filename: \"" + LARGE_HASH + "\"}",
                "E11000 duplicate key error collection: " + namespace + " index: filename_idx duplicate value")) {
            assertThat(hashDuplicate(message, new BsonDocument())).as(message).isTrue();
        }
        BsonDocument details = BsonDocument.parse("{keyPattern: {filename: 1}, keyValue: {filename: '" + LARGE_HASH + "'}}");
        assertThat(hashDuplicate("structured duplicate without server prose", details)).isTrue();
    }

    @Test
    void anotherIndexNamespaceOrConflictingKeyDetailsCannotBecomeAnIdempotentRace() throws Exception {
        for (String message : List.of(
                "E11000 duplicate key error collection: artifact_read_test.connector_artifacts.files index: another_idx dup key: {}",
                "E11000 duplicate key error collection: another.files index: filename_idx dup key: {}",
                "E11000 duplicate key error collection: artifact_read_test.connector_artifacts.files index: filename_idx_extra dup key: {}")) {
            assertThat(hashDuplicate(message, new BsonDocument())).as(message).isFalse();
        }
        String exactMessage = "E11000 duplicate key error collection: artifact_read_test.connector_artifacts.files"
                + " index: filename_idx dup key: { filename: \"" + LARGE_HASH + "\" }";
        for (String details : List.of("{keyPattern: {other: 1}, keyValue: {other: 'value'}}",
                "{keyPattern: {filename: 1}, keyValue: {filename: 'different-hash'}}")) {
            assertThat(hashDuplicate(exactMessage, BsonDocument.parse(details))).isFalse();
        }
    }

    private boolean hashDuplicate(String message, BsonDocument details) throws Exception {
        MongoWriteException failure = new MongoWriteException(new WriteError(11000, message, details), new ServerAddress(), Set.of());
        var classifier = MongoConnectorRegistry.class.getDeclaredMethod("isContentHashDuplicate", MongoWriteException.class, String.class);
        classifier.setAccessible(true);
        return (boolean) classifier.invoke(new Fixture(0, 0).registry(), failure, LARGE_HASH);
    }

    @Test
    void generatedChunksReadBackWithAnIndependentFixedHashAndExactLength() {
        byte[] result = new Fixture(522_257, 522_257).registry().artifact(LARGE_HASH).orElseThrow();
        assertThat(result).hasSize(522_257);
        assertThat(ContentHash.of(result)).isEqualTo("6239ce19a2c3b439d215cc0d88187239663688c409357807ebe23db9d519c0c7");
    }

    @Test
    void emptyFilesReturnIndependentEmptyArrays() {
        byte[] first = new Fixture(0, 0).registry().artifact(LARGE_HASH).orElseThrow();
        byte[] second = new Fixture(0, 0).registry().artifact(LARGE_HASH).orElseThrow();
        assertThat(first).isEmpty(); assertThat(first).isNotSameAs(second);
        assertThat(ContentHash.of(first)).isEqualTo("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }

    @Test
    void absentAndDuplicateFilesAreNeverDownloadedOrChosenArbitrarily() {
        Fixture absent = new Fixture(1, 1); absent.fileCount = 0;
        assertThat(absent.registry().artifact(LARGE_HASH)).isEmpty(); assertThat(absent.downloads).isZero();
        Fixture duplicate = new Fixture(1, 1); duplicate.fileCount = 2;
        Throwable failure = catchThrowable(() -> duplicate.registry().artifact(LARGE_HASH));
        assertUnreadable(failure, "filename"); assertThat(duplicate.downloads).isZero();
    }

    @Test
    void negativeAndUnrepresentableLengthsRefuseBeforeDownloadingOrAllocating() {
        for (long length : new long[]{-1, Long.MIN_VALUE, Integer.MAX_VALUE - 7L, Integer.MAX_VALUE,
                Integer.MAX_VALUE + 1L, Long.MAX_VALUE}) {
            Fixture fixture = new Fixture(length, 0);
            assertUnreadable(catchThrowable(() -> fixture.registry().artifact(LARGE_HASH)));
            assertThat(fixture.downloads).isZero();
        }
    }

    @Test
    void aShortOrOverlongDownloadedStreamRemainsCodedWithItsContentHash() {
        for (long[] lengths : new long[][]{{5, 4}, {4, 5}, {0, 1}, {1, 0}}) {
            Fixture fixture = new Fixture(lengths[0], lengths[1]);
            assertUnreadable(catchThrowable(() -> fixture.registry().artifact(LARGE_HASH)));
            assertThat(fixture.downloads).isEqualTo(1);
        }
    }

    @Test
    void aDriverReadFailureKeepsTheExistingSafeStoreTranslation() {
        Fixture fixture = new Fixture(5, 5); fixture.driverFailure = true;
        Throwable failure = catchThrowable(() -> fixture.registry().artifact(LARGE_HASH));
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, coded -> {
            assertThat(coded.code()).isEqualTo(IoError.STORE_UNAVAILABLE);
            assertThat(coded.args()).containsEntry("detail", "MongoGridFSException");
            assertThat(coded.getCause()).isNull();
        });
        assertThat(failure.toString()).doesNotContain("driver-secret-sentinel");
    }

    @Test
    void streamRangeMistakesRemainBareAndSingleByteWritesUseTheSameExactBound() throws Exception {
        Class<?> type = Arrays.stream(MongoConnectorRegistry.class.getDeclaredClasses())
                .filter(candidate -> candidate.getSimpleName().equals("ExactArtifactBytes")).findFirst().orElseThrow();
        var constructor = type.getDeclaredConstructor(String.class, long.class); constructor.setAccessible(true);
        OutputStream sink = (OutputStream) constructor.newInstance(LARGE_HASH, 1L);
        for (int[] range : new int[][]{{-1, 1}, {0, -1}, {1, 1}, {Integer.MAX_VALUE, 1}})
            assertThatThrownBy(() -> sink.write(new byte[1], range[0], range[1])).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> sink.write(null, 0, 1)).isInstanceOf(NullPointerException.class);
        sink.write(new byte[0], 0, 0); sink.write(0x123);
        assertUnreadable(catchThrowable(() -> sink.write(0)));
    }

    private static void assertUnreadable(Throwable failure) {
        assertUnreadable(failure, "artifact");
    }

    private static void assertUnreadable(Throwable failure, String field) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, coded -> {
            assertThat(coded.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
            assertThat(coded.args()).containsEntry("id", LARGE_HASH).containsEntry("field", field);
            assertThat(coded.getCause()).isNull();
        });
    }

    public static void main(String[] args) {
        if (Arrays.equals(args, new String[]{"oversized"})) {
            assertUnreadable(catchThrowable(() -> new Fixture(1_500_000_000L, 1).registry().artifact(LARGE_HASH)));
            System.out.println("oversized-artifact-rejected");
            return;
        }
        if (!Arrays.equals(args, new String[]{"large"})) throw new IllegalArgumentException("unknown witness");
        byte[] bytes = new Fixture(LARGE_LENGTH, LARGE_LENGTH).registry().artifact(LARGE_HASH).orElseThrow();
        if (bytes.length != LARGE_LENGTH || !ContentHash.of(bytes).equals(LARGE_HASH)) throw new AssertionError("artifact bytes changed");
        System.out.println("artifact-length=" + bytes.length + " artifact-sha256=" + LARGE_HASH);
    }

    private static final class Fixture {
        private final long declaredLength;
        private final long streamedLength;
        private final ObjectId id = new ObjectId();
        private int fileCount = 1;
        private int downloads;
        private boolean driverFailure;

        private Fixture(long declaredLength, long streamedLength) {
            this.declaredLength = declaredLength;
            this.streamedLength = streamedLength;
        }

        private MongoConnectorRegistry registry() {
            MongoDatabase database = proxy(MongoDatabase.class, (self, method, args) -> switch (method.getName()) {
                case "getCollection" -> collection((String) args[0]);
                case "getCodecRegistry" -> MongoClientSettings.getDefaultCodecRegistry();
                default -> throw new AssertionError("unexpected database operation: " + method.getName());
            });
            return new MongoConnectorRegistry(proxy(com.mongodb.client.MongoClient.class,
                    (self, method, args) -> { throw new AssertionError(method.getName()); }), database);
        }

        private MongoCollection<?> collection(String name) {
            return proxy(MongoCollection.class, (self, method, args) -> switch (method.getName()) {
                case "withCodecRegistry", "withReadPreference", "withReadConcern", "withWriteConcern", "withTimeout" -> self;
                case "getNamespace" -> new MongoNamespace("artifact_read_test", name);
                case "getTimeout" -> null;
                case "getReadPreference" -> ReadPreference.primary();
                case "getReadConcern" -> ReadConcern.DEFAULT;
                case "getWriteConcern" -> WriteConcern.ACKNOWLEDGED;
                case "listIndexes" -> iterable(ListIndexesIterable.class, () -> List.of(new Document("name", "filename_idx")
                        .append("key", new Document("filename", 1)).append("unique", true)).iterator());
                case "find" -> {
                    if (name.endsWith(".chunks")) yield iterable(FindIterable.class, this::chunks);
                    BsonDocument[] filter = {args == null ? null : ((Bson) args[0]).toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry())};
                    yield iterable(FindIterable.class, () -> {
                        boolean lookup = filter[0] == null || filter[0].containsKey("filename");
                        long length = lookup ? declaredLength : streamedLength;
                        if (!lookup) downloads++;
                        GridFSFile file = new GridFSFile(new BsonObjectId(id), LARGE_HASH,
                                length, CHUNK_SIZE, new Date(0), new Document());
                        return (fileCount == 0 ? List.<GridFSFile>of() : fileCount == 2 ? List.of(file, file) : List.of(file)).iterator();
                    }, value -> filter[0] = value.toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry()));
                }
                default -> throw new AssertionError("unexpected collection operation: " + method.getName());
            });
        }

        private Iterator<BsonDocument> chunks() {
            if (driverFailure) throw new MongoException(123, "driver-secret-sentinel");
            return new Iterator<>() {
                private long offset;
                private int index;
                public boolean hasNext() {return offset < streamedLength;}
                public BsonDocument next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    byte[] chunk = new byte[(int) Math.min(CHUNK_SIZE, streamedLength - offset)];
                    Arrays.fill(chunk, (byte) 37);
                    offset += chunk.length;
                    return new BsonDocument("files_id", new BsonObjectId(id)).append("n", new BsonInt32(index++))
                            .append("data", new BsonBinary(chunk));
                }
            };
        }
    }

    private static Object iterable(Class<?> type, Supplier<? extends Iterator<?>> values) {
        return iterable(type, values, ignored -> {});
    }

    private static Object iterable(Class<?> type, Supplier<? extends Iterator<?>> values, Consumer<Bson> filter) {
        return proxy(type, (self, method, args) -> switch (method.getName()) {
            case "filter" -> {filter.accept((Bson) args[0]); yield self;}
            case "limit", "sort", "batchSize", "timeoutMode" -> self;
            case "first" -> {Iterator<?> iterator = values.get(); yield iterator.hasNext() ? iterator.next() : null;}
            case "iterator" -> cursor(values.get());
            case "into" -> {
                @SuppressWarnings("unchecked") Collection<Object> target = (Collection<Object>) args[0];
                values.get().forEachRemaining(target::add); yield target;
            }
            default -> throw new AssertionError("unexpected iterable operation: " + method.getName());
        });
    }

    private static Object cursor(Iterator<?> values) {
        return proxy(MongoCursor.class, (self, method, args) -> switch (method.getName()) {
            case "hasNext" -> values.hasNext();
            case "next" -> values.next();
            case "close" -> null;
            default -> throw new AssertionError("unexpected cursor operation: " + method.getName());
        });
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }
}
