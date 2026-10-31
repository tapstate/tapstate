package io.tapstate.core.validation;

import static io.tapstate.core.validation.ValidationRootHintBatch.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ValidationRootHintBatchTest {
    @Test
    void movedKeysIncludeEveryOldAndNewRootAndDoNotMutateInput() {
        var changes = new ArrayList<>(List.of(new Change(ChangeKind.UPDATE, key(1), key(2)),
                new Change(ChangeKind.UPDATE, key(2), key(3))));
        var batch = batch(changes);
        changes.clear();
        assertThat(batch.changes()).hasSize(2);
        assertThat(batch.oneToOneRoots()).extracting(ValidationRoot::rootKey).containsExactly(key(1), key(2), key(3));
        assertThat(batch.oneToOneRoots()).allSatisfy(root -> {
            assertThat(root.mappingId()).isEqualTo("mapping"); assertThat(root.outputId()).isEqualTo("output");
            assertThat(root.ancestors()).isEmpty();
        });
        assertThatThrownBy(() -> batch.changes().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void missingBeforeCannotBeReplacedByTheAfterKey() {
        assertThatThrownBy(() -> new Change(ChangeKind.UPDATE, null, key(2)))
                .isInstanceOf(TapstateException.class).hasMessageContaining("hint.changeKeys");
        assertThatThrownBy(() -> new Change(ChangeKind.UPDATE, key(1), null)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new Change(ChangeKind.INSERT, key(1), key(2))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> new Change(ChangeKind.DELETE, key(1), key(2))).isInstanceOf(TapstateException.class);
        assertThat(batch(List.of(new Change(ChangeKind.UPDATE, key(1), key(1)))).oneToOneRoots()).hasSize(1);
    }

    @Test
    void declaredDomainsIncludeEveryComponentAndBinarySubtype() {
        var binary = new ValidationKey(List.of(ValidationKey.Part.binary(4, new byte[] {1, 2})));
        assertThatThrownBy(() -> batch(List.of(new Change(ChangeKind.INSERT, null, binary))))
                .isInstanceOf(TapstateException.class).hasMessageContaining("hint.keyDomain");
        assertThatThrownBy(() -> new Domain(ValidationKey.Kind.BINARY)).isInstanceOf(TapstateException.class);
        var domains = List.of(new Domain(ValidationKey.Kind.BINARY, 3));
        assertThatThrownBy(() -> new ValidationRootHintBatch(run(), sender(domains), 1,
                List.of(new Change(ChangeKind.INSERT, null, binary)))).isInstanceOf(TapstateException.class);
        var composite = new ValidationKey(List.of(ValidationKey.Part.number(1), ValidationKey.Part.text("1")));
        assertThatThrownBy(() -> batch(List.of(new Change(ChangeKind.INSERT, null, composite))))
                .isInstanceOf(TapstateException.class).hasMessageContaining("hint.keyDomain");
    }

    @Test
    void countAndByteBoundsApplyBeforeRetainingTheBatch() {
        var change = new Change(ChangeKind.UPDATE, key(1), key(2));
        assertThatThrownBy(() -> batch(Collections.nCopies(MAX_CHANGES + 1, change))).isInstanceOf(TapstateException.class);
        var part = ValidationKey.Part.text("x".repeat(8000));
        var large = new ValidationKey(List.of(part, part, part));
        var domains = Collections.nCopies(3, new Domain(ValidationKey.Kind.TEXT));
        assertThatThrownBy(() -> new ValidationRootHintBatch(run(), sender(domains), 1,
                Collections.nCopies(MAX_CHANGES, new Change(ChangeKind.UPDATE, large, large))))
                .isInstanceOf(TapstateException.class).hasMessageContaining("hint.bytes");
        var batch = batch(List.of(change));
        assertThat(batch.materializationReservationBytes()).isGreaterThan(batch.retainedBytes());
    }

    @Test
    void fingerprintsFrameTypesLengthsOrderAndEventKindInsteadOfUsingHashCode() {
        assertThat("Aa".hashCode()).isEqualTo("BB".hashCode());
        assertThat(fingerprint(ValidationKey.Part.text("Aa"))).isNotEqualTo(fingerprint(ValidationKey.Part.text("BB")));
        assertThat(fingerprint(ValidationKey.Part.number(1))).isNotEqualTo(fingerprint(ValidationKey.Part.text("1")));
        assertThat(batch(List.of(new Change(ChangeKind.INSERT, null, key(1)))).contentFingerprint())
                .isNotEqualTo(batch(List.of(new Change(ChangeKind.DELETE, key(1), null))).contentFingerprint());
        assertThat(batch(List.of(new Change(ChangeKind.UPDATE, key(1), key(2)))).contentFingerprint())
                .isNotEqualTo(batch(List.of(new Change(ChangeKind.UPDATE, key(2), key(1)))).contentFingerprint());
        var first = new ValidationKey(List.of(ValidationKey.Part.text("ab"), ValidationKey.Part.text("c")));
        var second = new ValidationKey(List.of(ValidationKey.Part.text("a"), ValidationKey.Part.text("bc")));
        var domains = Collections.nCopies(2, new Domain(ValidationKey.Kind.TEXT));
        assertThat(new ValidationRootHintBatch(run(), sender(domains), 1,
                List.of(new Change(ChangeKind.INSERT, null, first))).contentFingerprint())
                .isNotEqualTo(new ValidationRootHintBatch(run(), sender(domains), 1,
                        List.of(new Change(ChangeKind.INSERT, null, second))).contentFingerprint());
    }

    @Test
    void exactDecimalsAndExtremeScaleNeverExpandOrLosePrecision() {
        assertThat(fingerprint(ValidationKey.Part.number(new BigDecimal("1.00"))))
                .isEqualTo(fingerprint(ValidationKey.Part.number(1)));
        assertThat(fingerprint(ValidationKey.Part.number(9007199254740993L)))
                .isNotEqualTo(fingerprint(ValidationKey.Part.number(9007199254740992L)));
        assertThat(fingerprint(ValidationKey.Part.number(new BigDecimal(BigInteger.ONE, Integer.MIN_VALUE)))).hasSize(64);
        assertThat(fingerprint(ValidationKey.Part.number(new BigDecimal(BigInteger.ONE, Integer.MAX_VALUE)))).hasSize(64);
    }

    @Test
    void temporalAndBinaryComponentsRetainExactDomainAndOwnedContent() {
        byte[] bytes = {1, 2};
        var part = ValidationKey.Part.binary(4, bytes);
        String expected = fingerprint(part);
        bytes[0] = 9; ((byte[]) part.value())[0] = 8;
        assertThat(fingerprint(part)).isEqualTo(expected);
        assertThat(expected).isNotEqualTo(fingerprint(ValidationKey.Part.binary(3, new byte[] {1, 2})));
        var values = List.of(ValidationKey.Part.bool(true), ValidationKey.Part.bool(false),
                ValidationKey.Part.date(LocalDate.of(2026, 1, 1)), ValidationKey.Part.time(LocalTime.of(1, 2, 3, 4)),
                ValidationKey.Part.localDateTime(LocalDateTime.of(2026, 1, 1, 1, 2, 3, 4)),
                ValidationKey.Part.instant(Instant.ofEpochSecond(1, 2)), ValidationKey.Part.instant(Instant.ofEpochSecond(1, 3)));
        assertThat(values.stream().map(ValidationRootHintBatchTest::fingerprint).distinct()).hasSize(values.size());
    }

    private static String fingerprint(ValidationKey.Part part) {
        return new ValidationRootHintBatch(run(), sender(List.of(new Domain(part.kind(), part.binarySubtype()))), 1,
                List.of(new Change(ChangeKind.INSERT, null, new ValidationKey(List.of(part))))).contentFingerprint();
    }
    private static ValidationRootHintBatch batch(List<Change> changes) {
        return new ValidationRootHintBatch(run(), sender(List.of(new Domain(ValidationKey.Kind.EXACT_NUMBER))), 1, changes);
    }
    private static ValidationKey key(long n) { return new ValidationKey(List.of(ValidationKey.Part.number(n))); }
    private static Sender sender(List<Domain> domains) {
        return new Sender("sender", "execution", "source", "orders", "capture", "boot", 1, 2,
                "route", "key-contract", "mapping", "output", domains);
    }
    private static ValidationRunIdentity run() {
        return new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "owner", "owner-boot", 1, 1, 1),
                "task", "run", ValidationMode.INCREMENTAL, "spec", 1, "rules");
    }
}
