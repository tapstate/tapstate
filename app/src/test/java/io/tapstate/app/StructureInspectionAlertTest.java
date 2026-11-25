package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoStructureInspection;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.alerting.*;
import io.tapstate.spi.store.AlertStore;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class StructureInspectionAlertTest {
    static final Instant T = AlertEvaluationHostTest.T;
    static final AlarmKey.Resource CLUSTER = new AlarmKey.Resource("cluster", "cluster", "installation");
    static AlertConfiguration.Rule rule(SystemCollections.Database domain, AlertChannelPolicy.Deployment deployment) {
        String type = domain == SystemCollections.Database.CONFIG ? "cluster.config-structure-drift" : "cluster.cache-structure-drift";
        var header = AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,
                AlertEvaluationHostTest.SCOPE, "builtin-" + type), deployment, T, "system");
        return new AlertConfiguration.Rule(header, "Structure changed", type, 1,
                new AlertConfiguration.Resources(Set.of("cluster"), true, Set.of()), Map.of(), null, true, null)
                .validated(BuiltinAlarmTypes.catalog());
    }
    static MongoStructureInspection.Difference missingIndex(SystemCollections.Database domain, String name, String command) {
        return new MongoStructureInspection.Difference(domain, "managed_db", "managed_collection", name,
                MongoStructureInspection.Problem.INDEX_MISSING, "required index", "missing", command);
    }
    static AlertInput.Conditions input(SystemCollections.Database domain, Instant at, String index) {
        return PipelineAlertRuntime.structureInspectionInput(at, domain,
                List.of(missingIndex(domain, index, "db.getCollection(\"managed_collection\").createIndex({key:1})")), true);
    }
    @Test void hourlyEvidenceRetainsFirstAndLatestDetailsAndUnknownCannotRecover() {
        var f = new AlertEvaluationHostTest();
        var domain = SystemCollections.Database.CONFIG;
        var binding = new AlertEvaluationHost.Binding(rule(domain, AlertChannelPolicy.Deployment.ON_PREM), CLUSTER,
                "cluster-structure-inspection", 0, () -> {});
        assertThat(f.host.accept(binding, 0, input(domain, T, "first_index"))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
        f.now.set(T.plusSeconds(3600));
        f.host.tick(binding);
        var first = f.rows.get(binding.key()).value();
        assertThat(first.snapshot().quality()).isEqualTo(AlertSnapshot.Quality.VALID);
        assertThat(first.snapshot().rule().maxGap()).isEqualTo(Duration.ofMinutes(65));
        assertThat(f.host.accept(binding, 1, input(domain, f.now.get(), "second_index"))).isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
        var history = f.rows.get(binding.key()).value().episode().diagnostics().history();
        assertThat(history.first().trace().text()).contains("first_index", "Manual repair:");
        assertThat(history.latest().trace().text()).contains("second_index");
        f.now.set(T.plusSeconds(7200));
        f.host.accept(binding, 2, PipelineAlertRuntime.structureInspectionInput(f.now.get(), domain, List.of(), false));
        assertThat(f.rows.get(binding.key()).value().snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
        assertThat(f.rows.get(binding.key()).value().snapshot().quality()).isEqualTo(AlertSnapshot.Quality.UNKNOWN);
        f.now.set(T.plusSeconds(10800));
        f.host.accept(binding, 3, PipelineAlertRuntime.structureInspectionInput(f.now.get(), domain, List.of(), true));
        assertThat(f.rows.get(binding.key()).value().snapshot().status()).isEqualTo(AlertSnapshot.Status.RESOLVED);
        assertThat(f.rows.get(binding.key()).value().episode().value().endReason()).isEqualTo(AlertSnapshot.EndReason.RECOVERED);
    }
    @Test void domainsRemainSeparateAndOldTypesRetainTheirShortFreshness() {
        var f = new AlertEvaluationHostTest();
        for(var domain : List.of(SystemCollections.Database.CONFIG, SystemCollections.Database.CACHE)) {
            var binding = new AlertEvaluationHost.Binding(rule(domain, AlertChannelPolicy.Deployment.ON_PREM), CLUSTER,
                    "cluster-structure-inspection", 0, () -> {});
            f.host.accept(binding, 0, input(domain, T, "missing"));
        }
        assertThat(f.rows).hasSize(2);
        assertThat(f.rows.keySet()).extracting(AlarmKey::typeId)
                .containsExactlyInAnyOrder("cluster.config-structure-drift", "cluster.cache-structure-drift");
        var existing = AlertEvaluationHostTest.binding(AlertEvaluationHostTest.definition("pipeline.failed"), "producer");
        f.host.accept(existing, 0, new AlertInput.Conditions(T, AlertSignal.Condition.BAD, null));
        assertThat(f.rows.get(existing.key()).value().snapshot().rule().maxAge()).isEqualTo(Duration.ofSeconds(15));
        assertThat(f.rows.get(existing.key()).value().snapshot().rule().maxGap()).isEqualTo(Duration.ofSeconds(5));
    }
    @Test void severeDamageAndOversizedCommandsGiveSupportGuidanceAndFindingsAreBounded() {
        var domain = SystemCollections.Database.CACHE;
        var findings = new ArrayList<MongoStructureInspection.Difference>();
        findings.add(missingIndex(domain, "unsafe", null));
        findings.add(missingIndex(domain, "too_long", "x".repeat(513)));
        for(int i=0;i<20;i++)findings.add(missingIndex(domain, "index_"+i, "short_command"));
        var trace = PipelineAlertRuntime.structureInspectionInput(T, domain, findings, false).trace();
        assertThat(trace.text()).contains("CACHE managed_db.managed_collection", "No safe manual repair command", "Contact support", "Additional differences omitted");
        assertThat(trace.text()).doesNotContain("x".repeat(513), "index_19", "Manual repair:");
        assertThat(PipelineAlertRuntime.structureInspectionInput(T, domain, List.of(missingIndex(domain, "single", "short_command")), false)
                .trace().text()).doesNotContain("Manual repair:");
        assertThat(PipelineAlertRuntime.structureInspectionInput(T, domain, List.of(missingIndex(domain, "single", "x".repeat(513))), true)
                .trace().text()).doesNotContain("Manual repair:");
        assertThat(trace.truncated()).isTrue();
        assertThat(trace.text().getBytes(java.nio.charset.StandardCharsets.UTF_8)).hasSizeLessThanOrEqualTo(AlarmDiagnostics.MAX_TEXT_BYTES);
        assertThatThrownBy(() -> PipelineAlertRuntime.structureInspectionInput(T, SystemCollections.Database.CONFIG, findings, true))
                .isInstanceOf(IllegalArgumentException.class);
    }
    @ParameterizedTest
    @EnumSource(value = AlertChannelPolicy.Deployment.class, names = {"ON_PREM", "CLOUD_MANAGED"})
    void structureDriftStaysInAppEvenWhenTheHostNormallyHasManagedOutbound(AlertChannelPolicy.Deployment deployment) {
        var f = new AlertEvaluationHostTest();
        var host = mock(AlertStore.Host.class);
        when(host.deployment()).thenReturn(deployment);
        // A managed host without any outbound binding would fail if this alert attempted external planning.
        when(f.store.planning()).thenReturn(new AlertStore.Planning(new AlertStore.PlanVersion(1,1), host,
                List.of(), List.of(), List.of(), Map.of(), Map.of(), Map.of(), Set.of()));
        var planner = new AlertNotificationPlanner(f.store, f.clock, "planner", null, () -> {});
        var evaluator = new AlertEvaluationHost(f.store, BuiltinAlarmTypes.catalog(), deployment, f.clock, "owner", () -> {}).withPlanner(planner);
        var binding = new AlertEvaluationHost.Binding(rule(SystemCollections.Database.CONFIG, deployment), CLUSTER,
                "cluster-structure-inspection", 0, () -> {});
        assertThat(evaluator.accept(binding, 0, input(SystemCollections.Database.CONFIG, T, "missing")))
                .isEqualTo(AlertEvaluationHost.Result.ACCEPTED);
        assertThat(f.rows.get(binding.key()).value().snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
        assertThat(f.commits).hasSize(1);
        assertThat(f.commits.getFirst().schedules()).isEmpty();
        assertThat(f.commits.getFirst().intents()).isEmpty();
    }
}
