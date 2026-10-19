package io.tapstate.app;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class CloudSpecPutRefusalClassifierTest {
    private static final String QUOTA = "you are over your space quota, using 513 MB of 512 MB";

    @Test
    void disabledClassificationNeverReadsTheField() {
        assertThat(CloudSpecPutRefusalClassifier.classify(false, true, "spec-put", "update", 8000,
                () -> { throw new AssertionError("disabled classification must not read"); }))
                .isEqualTo(CloudSpecPutRefusalClassifier.Category.NOT_ENABLED);
    }

    @Test
    void scopeOperationCommandAndCodeMustAllMatchBeforeReading() {
        AtomicInteger reads = new AtomicInteger();
        Supplier<String> field = () -> { reads.incrementAndGet(); return QUOTA; };
        assertThat(CloudSpecPutRefusalClassifier.classify(true, false, "spec-put", "update", 8000, field))
                .isEqualTo(CloudSpecPutRefusalClassifier.Category.UNCLASSIFIED);
        assertThat(CloudSpecPutRefusalClassifier.classify(true, true, "spec-has", "update", 8000, field))
                .isEqualTo(CloudSpecPutRefusalClassifier.Category.UNCLASSIFIED);
        assertThat(CloudSpecPutRefusalClassifier.classify(true, true, "spec-put", "find", 8000, field))
                .isEqualTo(CloudSpecPutRefusalClassifier.Category.UNCLASSIFIED);
        assertThat(CloudSpecPutRefusalClassifier.classify(true, true, "spec-put", "update", 13, field))
                .isEqualTo(CloudSpecPutRefusalClassifier.Category.UNCLASSIFIED);
        assertThat(reads).hasValue(0);
    }

    @Test
    void theExactTemplateReadsOnceAndReturnsOnlyTheFixedCategory() {
        AtomicInteger reads = new AtomicInteger();
        assertThat(CloudSpecPutRefusalClassifier.classify(true, true, "spec-put", "update", 8000,
                () -> { reads.incrementAndGet(); return QUOTA; }))
                .isEqualTo(CloudSpecPutRefusalClassifier.Category.SPACE_QUOTA);
        assertThat(reads).hasValue(1);
    }

    @Test
    void unknownOrDecoratedTextNeverLeaksIntoTheCategory() {
        for (String value : new String[] {"private-sentinel", "private-sentinel " + QUOTA,
                QUOTA + " private-sentinel", QUOTA + "\n", QUOTA + "\nprivate-sentinel", "",
                "you are over your space quota, using unknown MB of 512 MB",
                "you are over your space quota, using 9999999999999 MB of 512 MB"}) {
            assertThat(CloudSpecPutRefusalClassifier.classify(true, true, "spec-put", "update", 8000, () -> value))
                    .isEqualTo(CloudSpecPutRefusalClassifier.Category.UNCLASSIFIED);
        }
    }

    @Test
    void unavailableFieldRemainsUnknownWithoutExposingTheFailure() {
        assertThat(CloudSpecPutRefusalClassifier.classify(true, true, "spec-put", "update", 8000, () -> null))
                .isEqualTo(CloudSpecPutRefusalClassifier.Category.UNCLASSIFIED);
        assertThat(CloudSpecPutRefusalClassifier.classify(true, true, "spec-put", "update", 8000,
                () -> { throw new IllegalArgumentException("private-sentinel"); }))
                .isEqualTo(CloudSpecPutRefusalClassifier.Category.UNCLASSIFIED);
    }
}
