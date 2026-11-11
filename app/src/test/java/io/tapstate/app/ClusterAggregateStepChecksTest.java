package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.control.core.AggregateStepChecks;
import io.tapstate.control.core.AuditGate;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.aggregate.AggregateError;
import io.tapstate.runtime.engine.aggregate.AggregateRequests;
import io.tapstate.spi.store.AuditRecord;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** A check or a repair reaches the member running the step, and comes back as the step answered it. */
class ClusterAggregateStepChecksTest {

    private static final Clock FIXED = Clock.fixed(Instant.parse("2026-11-11T00:00:00Z"), ZoneOffset.UTC);

    private final List<AuditRecord> audited = Collections.synchronizedList(new ArrayList<>());
    private final List<AggregateRequests.Request> asked = Collections.synchronizedList(new ArrayList<>());
    private HazelcastInstance member;
    private AggregateRequests.Inbox inbox;
    private Thread step;
    private volatile Function<AggregateRequests.Request, AggregateRequests.Report> answer;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setClusterName("aggregate-checks-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stopMember() {
        if (step != null) {
            step.interrupt();
        }
        member.shutdown();
    }

    /** Stands in for the running step: takes each request as the step would between drains. */
    private void runStep() {
        inbox = AggregateRequests.register(member, "sales", "by_region");
        step = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                AggregateRequests.Waiting waiting = inbox.poll();
                if (waiting == null) {
                    Thread.onSpinWait();
                    continue;
                }
                asked.add(waiting.request());
                waiting.answer().complete(answer.apply(waiting.request()));
            }
        });
        step.setDaemon(true);
        step.start();
    }

    private ClusterAggregateStepChecks checks() {
        return new ClusterAggregateStepChecks(member, new AuditGate(audited::add, FIXED));
    }

    @Test
    @DisplayName("a check walks the step's groups a page at a time and hands back where to go on from")
    void aCheckIsAnsweredByTheRunningStep() {
        runStep();
        Map<String, String> group = new LinkedHashMap<>();
        group.put("region", "EU");
        answer = request -> new AggregateRequests.Report(2,
                List.of(new AggregateRequests.Finding(group, "mismatch", List.of("revenue"), null, null)),
                new byte[] {1, 2, 3}, 4, 5);

        AggregateStepChecks.Report first = checks().check("sales", "by_region", new AggregateStepChecks.Request(null, null));

        assertThat(first.pipeline()).isEqualTo("sales");
        assertThat(first.step()).isEqualTo("by_region");
        assertThat(first.repaired()).isFalse();
        assertThat(first.groups()).isEqualTo(2);
        assertThat(first.conflicts()).isEqualTo(4);
        assertThat(first.withheld()).isEqualTo(5);
        assertThat(first.findings()).singleElement().satisfies(finding -> {
            assertThat(finding.group()).containsExactly(Map.entry("region", "EU"));
            assertThat(finding.verdict()).isEqualTo("mismatch");
            assertThat(finding.fields()).containsExactly("revenue");
        });
        assertThat(asked.getFirst().action()).isEqualTo(AggregateRequests.Action.CHECK);
        assertThat(asked.getFirst().group()).isNull();
        assertThat(asked.getFirst().after()).isNull();

        checks().check("sales", "by_region", new AggregateStepChecks.Request(null, first.next()));

        assertThat(asked.get(1).after()).as("the place the first answer gave").containsExactly(1, 2, 3);
        assertThat(audited).as("a check changes nothing and leaves no record").isEmpty();
    }

    @Test
    @DisplayName("a repair of one group is recorded against the caller, with the step and group it named")
    void aRepairIsAudited() {
        runStep();
        answer = request -> new AggregateRequests.Report(1, List.of(), null, 0, 0);

        AggregateStepChecks.Report repaired = checks().repair("operator", "sales", "by_region",
                new AggregateStepChecks.Request(Map.of("region", "EU"), null));

        assertThat(repaired.repaired()).isTrue();
        assertThat(repaired.next()).isNull();
        assertThat(asked.getFirst().action()).isEqualTo(AggregateRequests.Action.REPAIR);
        assertThat(asked.getFirst().group()).containsExactly(Map.entry("region", "EU"));
        assertThat(audited).singleElement().satisfies(record -> {
            assertThat(record.principal()).isEqualTo("operator");
            assertThat(record.operationId()).isEqualTo("pipeline.aggregate-repair");
            assertThat(record.resourceId()).isEqualTo("sales");
            assertThat(record.detail()).containsEntry("step", "by_region").containsEntry("group", "{region=EU}");
        });
    }

    @Test
    @DisplayName("a step no member runs is refused by name, and so is a node with no data plane")
    void aStepThatIsNotRunningIsRefused() {
        assertRefused(() -> checks().check("sales", "by_region", new AggregateStepChecks.Request(null, null)),
                AggregateError.STEP_NOT_RUNNING, Map.of("pipeline", "sales", "step", "by_region"));
        assertRefused(() -> new ClusterAggregateStepChecks(null, new AuditGate(audited::add, FIXED))
                        .check("sales", "by_region", new AggregateStepChecks.Request(null, null)),
                AggregateError.STEP_NOT_RUNNING, Map.of("pipeline", "sales", "step", "by_region"));
    }

    @Test
    @DisplayName("what the step refuses comes back under the step's own code")
    void theStepsRefusalKeepsItsCode() {
        runStep();
        answer = request -> AggregateRequests.Report.refused("aggregate.group-invalid",
                Map.of("step", "by_region", "reason", "`country` is not one of its group names [region]"));

        assertRefused(() -> checks().check("sales", "by_region",
                        new AggregateStepChecks.Request(Map.of("country", "EU"), null)),
                AggregateError.GROUP_INVALID,
                Map.of("step", "by_region", "reason", "`country` is not one of its group names [region]"));
    }

    @Test
    @DisplayName("a place to go on from that no answer gave is refused before any member is asked")
    void anUnreadableCursorIsRefused() {
        runStep();
        answer = request -> {
            throw new AssertionError("the step must not be asked");
        };

        assertThatThrownBy(() -> checks().check("sales", "by_region", new AggregateStepChecks.Request(null, "not base64 !")))
                .isInstanceOfSatisfying(TapstateException.class,
                        refused -> assertThat(refused.code()).isEqualTo(AggregateError.GROUP_INVALID));
        assertThat(asked).isEmpty();
    }

    private static void assertRefused(Runnable action, AggregateError code, Map<String, Object> args) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(TapstateException.class, refused -> {
            assertThat(refused.code()).isEqualTo(code);
            assertThat(refused.args()).containsExactlyInAnyOrderEntriesOf(args);
        });
    }
}
