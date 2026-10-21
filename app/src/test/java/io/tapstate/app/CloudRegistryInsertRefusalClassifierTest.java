package io.tapstate.app;

import com.mongodb.MongoCommandException;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class CloudRegistryInsertRefusalClassifierTest {
    private static final String QUOTA = "you are over your space quota, using 513 MB of 512 MB";
    private static final CloudRegistryInsertRefusalClassifier.Scope EXACT = scope(
            "mysql", true, true, "registry-register", "insert", MongoCommandException.class, 8000);

    @Test
    void aDisabledFlagNeverReadsEvenWithTheExactEntryAndScope() {
        assertThat(classifier(false).classify(EXACT, unreadable())).isEmpty();
    }

    @Test
    void theFlagCannotAuthorizeAnotherTestClassMethodOrMissingEntry() {
        for (var entry : new CloudRegistryInsertRefusalClassifier.Entry[] {null,
                new CloudRegistryInsertRefusalClassifier.Entry("ManagedAtlasCloudStoreIT",
                        CloudRegistryInsertRefusalClassifier.REAL_MANAGED_STARTUP.testMethod()),
                new CloudRegistryInsertRefusalClassifier.Entry("RealManagedAtlasStoreStartupIT", "otherMethod")}) {
            assertThat(new CloudRegistryInsertRefusalClassifier(true, entry).classify(EXACT, unreadable())).isEmpty();
        }
    }

    @Test
    void everyScopeMismatchAndASubclassLeaveTheOnceGuardAvailable() {
        var classifier = classifier(true);
        for (var scope : new CloudRegistryInsertRefusalClassifier.Scope[] {null,
                scope("postgres", true, true, "registry-register", "insert", MongoCommandException.class, 8000),
                scope("mysql", false, true, "registry-register", "insert", MongoCommandException.class, 8000),
                scope("mysql", true, false, "registry-register", "insert", MongoCommandException.class, 8000),
                scope("mysql", true, true, "spec-put", "insert", MongoCommandException.class, 8000),
                scope("mysql", true, true, "registry-register", "update", MongoCommandException.class, 8000),
                scope("mysql", true, true, "registry-register", "insert", DerivedCommandException.class, 8000),
                scope("mysql", true, true, "registry-register", "insert", IllegalStateException.class, 8000),
                scope("mysql", true, true, "registry-register", "insert", MongoCommandException.class, 13)}) {
            assertThat(classifier.classify(scope, unreadable())).isEmpty();
        }
        AtomicInteger reads = new AtomicInteger();
        assertThat(classifier.classify(EXACT, () -> { reads.incrementAndGet(); return QUOTA; }))
                .contains(CloudRegistryInsertRefusalClassifier.Category.SPACE_QUOTA);
        assertThat(classifier.classify(EXACT, unreadable())).isEmpty();
        assertThat(reads).hasValue(1);
    }

    @Test
    void onlyAnExactBoundedFullTemplateCanProduceThePositiveCategory() {
        for (String value : new String[] {"private-sentinel", "", "private-sentinel " + QUOTA,
                QUOTA + " private-sentinel", QUOTA + "\n", "x".repeat(161),
                "you are over your space quota, using 9999999999999 MB of 512 MB"}) {
            var result = classifier(true).classify(EXACT, () -> value);
            assertThat(result).contains(CloudRegistryInsertRefusalClassifier.Category.UNCLASSIFIED);
            assertThat(result.toString()).doesNotContain("private-sentinel", "513", "512", "space quota");
        }
        assertThat(classifier(true).classify(EXACT, () -> QUOTA))
                .contains(CloudRegistryInsertRefusalClassifier.Category.SPACE_QUOTA);
    }

    @Test
    void anUnavailableFieldConsumesOnlyTheOnePermittedAttempt() {
        for (Supplier<String> field : java.util.Arrays.<Supplier<String>>asList(null, () -> null,
                () -> { throw new IllegalArgumentException("private-sentinel"); })) {
            var classifier = classifier(true);
            assertThat(classifier.classify(EXACT, field))
                    .contains(CloudRegistryInsertRefusalClassifier.Category.UNCLASSIFIED);
            assertThat(classifier.classify(EXACT, unreadable())).isEmpty();
        }
    }

    @Test
    void simultaneousMatchingCallbacksReadAndEmitAtMostOnce() throws Exception {
        var classifier = classifier(true);
        AtomicInteger reads = new AtomicInteger();
        CountDownLatch ready = new CountDownLatch(4);
        CountDownLatch release = new CountDownLatch(1);
        try (var pool = Executors.newFixedThreadPool(4)) {
            List<java.util.concurrent.Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < 4; i++) results.add(pool.submit(() -> {
                ready.countDown();
                assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                return classifier.classify(EXACT, () -> { reads.incrementAndGet(); return QUOTA; }).isPresent();
            }));
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            release.countDown();
            int emitted = 0;
            for (var result : results) if (result.get(5, TimeUnit.SECONDS)) emitted++;
            assertThat(emitted).isEqualTo(1);
            assertThat(reads).hasValue(1);
        } finally {
            release.countDown();
        }
    }

    @Test
    void theNewClassificationDoesNotExpandTheOldSpecPutGate() {
        assertThat(CloudSpecPutRefusalClassifier.classify(true, true, "registry-register", "insert", 8000,
                unreadable())).isEqualTo(CloudSpecPutRefusalClassifier.Category.UNCLASSIFIED);
        assertThat(classifier(true).classify(EXACT, () -> QUOTA))
                .contains(CloudRegistryInsertRefusalClassifier.Category.SPACE_QUOTA);
        assertThat(CloudSpecPutRefusalClassifier.classify(true, true, "spec-put", "update", 8000, () -> QUOTA))
                .isEqualTo(CloudSpecPutRefusalClassifier.Category.SPACE_QUOTA);
    }

    private static CloudRegistryInsertRefusalClassifier classifier(boolean enabled) {
        return new CloudRegistryInsertRefusalClassifier(enabled, CloudRegistryInsertRefusalClassifier.REAL_MANAGED_STARTUP);
    }

    private static CloudRegistryInsertRefusalClassifier.Scope scope(String connector, boolean seed, boolean path,
            String operation, String command, Class<?> type, int code) {
        return new CloudRegistryInsertRefusalClassifier.Scope(connector, seed, path, operation, command, type, code);
    }

    private static Supplier<String> unreadable() {
        return () -> { throw new AssertionError("an out-of-scope diagnostic must not read"); };
    }

    private static final class DerivedCommandException extends MongoCommandException {
        private DerivedCommandException() { super(new BsonDocument(), null); }
    }
}
