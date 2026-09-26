package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.HistoryRollupStore;
import io.tapstate.spi.store.HistoryRollupStore.Bucket;
import io.tapstate.spi.store.HistoryRollupStore.Fragment;
import io.tapstate.spi.store.HistoryRollupStore.Gap;
import io.tapstate.spi.store.HistoryRollupStore.GapReason;
import io.tapstate.spi.store.HistoryRollupStore.Key;
import io.tapstate.spi.store.HistoryRollupStore.Lag;
import io.tapstate.spi.store.HistoryRollupStore.Rate;
import io.tapstate.spi.store.HistoryRollupStore.Resolution;
import io.tapstate.spi.store.HistoryRollupStore.Scope;
import io.tapstate.spi.store.HistoryRollupStore.StartReason;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MongoHistoryRollupStoreTest {

    private static final Instant START = Instant.parse("2026-09-27T10:00:00Z");
    private static final Instant READ_AT = Instant.parse("2026-09-27T10:35:00Z");

    static Bucket bucket(Scope scope) {
        Key key = new Key("flow", scope, Resolution.PT30M, START);
        Fragment first = new Fragment(0, StartReason.WINDOW_START,
                START, START.plusSeconds(120),
                new Rate(new BigDecimal("12.5"), new BigDecimal("0.104166667"),
                        new BigDecimal("0.200000000")), null,
                List.of(new Lag("orders", START.plusSeconds(60), 3, 8)));
        Fragment second = new Fragment(1, StartReason.GAP,
                START.plusSeconds(300), START.plusSeconds(360),
                null, new Rate(new BigDecimal("6"), new BigDecimal("0.1"), new BigDecimal("0.1")),
                List.of());
        return new Bucket(key, READ_AT.plusSeconds(1), READ_AT,
                READ_AT.plus(Duration.ofMinutes(5)), false,
                List.of(first, second),
                List.of(new Gap(1, START.plusSeconds(120), START.plusSeconds(300), GapReason.SAMPLE_GAP)));
    }

    @Test
    void roundTripPreservesExactRatesSegmentsGapsAndOwner() {
        Bucket bucket = bucket(Scope.incarnation("inc-a"));

        Document stored = MongoHistoryRollupStore.toDocument(bucket);

        assertThat(stored.get("bucketStart")).isEqualTo(Date.from(START));
        assertThat(stored.getString("pipelineIncarnationId")).isEqualTo("inc-a");
        assertThat(((Document) stored.get("_id")).getString("scopeKey")).isEqualTo("incarnation:inc-a");
        assertThat(MongoHistoryRollupStore.toBucket(stored)).isEqualTo(bucket);
        assertThat(bucket.usableAt(READ_AT.plusSeconds(299))).isTrue();
        assertThat(bucket.usableAt(bucket.computedAt().minusMillis(1))).isFalse();
        assertThat(bucket.usableAt(bucket.validUntil())).isFalse();
    }

    @Test
    void legacyHasItsOwnReservedKeyAndNoIncarnationEnvelope() {
        Bucket legacy = bucket(Scope.legacy());
        Document stored = MongoHistoryRollupStore.toDocument(legacy);

        assertThat(stored).doesNotContainKey("pipelineIncarnationId");
        assertThat(stored.getString("scopeKey")).isEqualTo("legacy");
        assertThat(MongoHistoryRollupStore.toBucket(stored)).isEqualTo(legacy);
        assertThat(((Document) stored.get("_id")))
                .isNotEqualTo(MongoHistoryRollupStore.toDocument(bucket(Scope.incarnation("legacy"))).get("_id"));

        stored.put("pipelineIncarnationId", null);
        assertThatThrownBy(() -> MongoHistoryRollupStore.toBucket(stored))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    @Test
    void aCascadedDeadlineCannotBeExtendedByACompletedWrite() {
        Key key = new Key("flow", Scope.incarnation("inc-a"), Resolution.PT5M, START);

        assertThatThrownBy(() -> new Bucket(key, READ_AT.plusSeconds(1), READ_AT,
                READ_AT.plus(HistoryRollupStore.MAX_CACHE_AGE).plusMillis(1), false, List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Bucket(key, READ_AT.plusSeconds(1), READ_AT,
                READ_AT, false, List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(new Bucket(key, START.plusSeconds(300), START.plusSeconds(240),
                START.plusSeconds(360), false, List.of(), List.of()).usableAt(START.plusSeconds(300)))
                .isTrue();
    }

    @Test
    void aBucketCannotGrowUnboundedFragmentsOrRetainPartialContentBehindFallbackMarker() {
        Key key = new Key("flow", Scope.incarnation("inc-a"), Resolution.PT30M, START);
        Fragment fragment = bucket(key.scope()).fragments().getFirst();
        List<Fragment> tooMany = new ArrayList<>();
        for (int index = 0; index <= HistoryRollupStore.MAX_FRAGMENTS; index++) {
            tooMany.add(fragment);
        }

        assertThatThrownBy(() -> new Bucket(key, READ_AT.plusSeconds(1), READ_AT,
                READ_AT.plusSeconds(60), false,
                tooMany, List.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Bucket(key, READ_AT.plusSeconds(1), READ_AT,
                READ_AT.plusSeconds(60), true,
                List.of(fragment), List.of())).isInstanceOf(IllegalArgumentException.class);
        assertThat(new Bucket(key, READ_AT.plusSeconds(1), READ_AT,
                READ_AT.plusSeconds(60), true,
                List.of(), List.of()).usableAt(READ_AT)).isFalse();
    }

    @Test
    void rejectsUnalignedBucketKeysAndCorruptStoredDates() {
        assertThatThrownBy(() -> new Key("flow", Scope.legacy(), Resolution.PT30M,
                START.plusSeconds(60))).isInstanceOf(IllegalArgumentException.class);
        Document stored = MongoHistoryRollupStore.toDocument(bucket(Scope.incarnation("inc-a")));
        stored.put("validUntil", "2026-09-27T10:10:00Z");

        assertThatThrownBy(() -> MongoHistoryRollupStore.toBucket(stored))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    @Test
    void aHugeExponentIsRejectedBeforeItCanExpandIntoAnUnboundedBsonString() {
        assertThatThrownBy(() -> new Rate(new BigDecimal("1E+1000000000"),
                BigDecimal.ZERO, BigDecimal.ZERO)).isInstanceOf(IllegalArgumentException.class);
    }
}
