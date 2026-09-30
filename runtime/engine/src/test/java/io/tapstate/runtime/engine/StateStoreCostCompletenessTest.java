package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.core.metrics.MetricTags;
import io.tapstate.runtime.engine.StateStoreCostMetricNames.Kind;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** A live job's true member-local reading may not be represented as a whole-job cost. */
class StateStoreCostCompletenessTest {

    private static final Set<String> MEMBERS = Set.of("member-a", "member-b");
    private static final Instant STARTED = Instant.parse("2026-09-27T08:00:00Z");
    private static final String JOB = "job-a";
    private static final String EXECUTION = "exec-one";
    private static final String NAMESPACE = "join.orders.state";
    private static final long COLLECTED_A = 1_700_000_001_000L;
    private static final long COLLECTED_B = 1_700_000_001_005L;
    private static final long START_A = 1_700_000_000_000L;
    private static final long START_B = 1_700_000_000_050L;

    @Test
    void completeCostsUseTheLatestNativeMemberStartFromTheirOwnCollection() {
        var read = Engine.stateStoreCostReadingsIn(JobMetrics.of(fixture()), JOB, MEMBERS);

        assertThat(read).containsKey(NAMESPACE);
        assertThat(read.get(NAMESPACE).countingSince()).isEqualTo(Instant.ofEpochMilli(START_B));
        assertThat(read.get(NAMESPACE).operations().get("save").completed()).isEqualTo(3);
        assertThat(read.get(NAMESPACE).codecs().get("encode").count()).isEqualTo(3);
    }

    @Test
    void missingMixedOrTornNativeMemberCollectionCannotLookLikeCompleteCost() {
        Map<String, List<Measurement>> missing = fixture();
        missing.put(MetricNames.EXECUTION_START_TIME,
                List.of(missing.get(MetricNames.EXECUTION_START_TIME).getFirst()));
        assertThat(Engine.stateStoreCostReadingsIn(JobMetrics.of(missing), JOB, MEMBERS)).isEmpty();

        Map<String, List<Measurement>> mixed = fixture();
        List<Measurement> nativeStarts = new ArrayList<>(mixed.get(MetricNames.EXECUTION_START_TIME));
        nativeStarts.set(1, Measurement.of(MetricNames.EXECUTION_START_TIME, START_B,
                COLLECTED_B, nativeTags("member-b", "exec-two")));
        mixed.put(MetricNames.EXECUTION_START_TIME, nativeStarts);
        assertThat(Engine.stateStoreCostReadingsIn(JobMetrics.of(mixed), JOB, MEMBERS)).isEmpty();

        Map<String, List<Measurement>> torn = fixture();
        String save = StateStoreCostMetricNames.nameOf(Kind.SAVE_COMPLETED, NAMESPACE);
        List<Measurement> saves = new ArrayList<>(torn.get(save));
        saves.set(0, Measurement.of(save, 3, COLLECTED_A - 1,
                costTags("member-a", EXECUTION)));
        torn.put(save, saves);
        assertThat(Engine.stateStoreCostReadingsIn(JobMetrics.of(torn), JOB, MEMBERS)).isEmpty();

        assertThat(Engine.stateStoreCostReadingsIn(JobMetrics.of(fixture()), JOB,
                Set.of("member-a"))).as("membership drift cannot publish a partial total").isEmpty();
    }

    private static Map<String, List<Measurement>> fixture() {
        Map<String, List<Measurement>> metrics = new LinkedHashMap<>();
        for (Kind kind : Kind.values()) {
            String name = StateStoreCostMetricNames.nameOf(kind, NAMESPACE);
            long onA = switch (kind) {
                case READY -> 1;
                case CLUSTER_SIZE -> 2;
                case SAVE_COMPLETED, ENCODE_COMPLETED -> 3;
                default -> 0;
            };
            long onB = switch (kind) {
                case READY -> 1;
                case CLUSTER_SIZE -> 2;
                default -> 0;
            };
            metrics.put(name, List.of(
                    Measurement.of(name, onA, COLLECTED_A, costTags("member-a", EXECUTION)),
                    Measurement.of(name, onB, COLLECTED_B, costTags("member-b", EXECUTION))));
        }
        metrics.put(MetricNames.EXECUTION_START_TIME, List.of(
                Measurement.of(MetricNames.EXECUTION_START_TIME, START_A, COLLECTED_A,
                        nativeTags("member-a", EXECUTION)),
                Measurement.of(MetricNames.EXECUTION_START_TIME, START_B, COLLECTED_B,
                        nativeTags("member-b", EXECUTION))));
        return metrics;
    }

    private static Map<String, String> nativeTags(String member, String execution) {
        return Map.of(MetricTags.JOB, JOB, MetricTags.MEMBER, member,
                MetricTags.EXECUTION, execution);
    }

    private static Map<String, String> costTags(String member, String execution) {
        Map<String, String> tags = new LinkedHashMap<>(nativeTags(member, execution));
        tags.put(MetricTags.VERTEX, StateStoreCostMetricNames.VERTEX);
        return tags;
    }

    @Test
    void oneMissingMemberOrMixedExecutionSuppressesEveryCost() {
        Engine.CostMeasurements partial = new Engine.CostMeasurements();
        addMember(partial, "member-a", "exec-one", 1);
        assertThat(partial.complete(MEMBERS, STARTED))
                .as("one member's observed write cannot become the two-member pipeline total")
                .isEmpty();

        addMember(partial, "member-b", "exec-one", 0);
        assertThat(partial.complete(MEMBERS, STARTED)).isPresent()
                .get().satisfies(reading -> assertThat(reading.operations().get("save").completed())
                        .isEqualTo(1));

        Engine.CostMeasurements mixed = new Engine.CostMeasurements();
        addMember(mixed, "member-a", "exec-one", 1);
        addMember(mixed, "member-b", "exec-two", 0);
        assertThat(mixed.complete(MEMBERS, STARTED)).isEmpty();

        Engine.CostMeasurements changedMembership = new Engine.CostMeasurements();
        addMember(changedMembership, "member-a", "exec-one", 1);
        addMember(changedMembership, "member-b", "exec-one", 0);
        assertThat(changedMembership.complete(Set.of("member-a"), STARTED))
                .as("an old two-member execution is absent after membership shrinks")
                .isEmpty();
    }

    private static void addMember(Engine.CostMeasurements measured, String member,
            String execution, long saveCount) {
        for (Kind kind : Kind.values()) {
            long value = switch (kind) {
                case READY -> 1;
                case CLUSTER_SIZE -> 2;
                case SAVE_COMPLETED -> saveCount;
                default -> 0;
            };
            measured.add(kind, member, execution, value);
        }
    }
}
