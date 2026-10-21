package io.tapstate.core.alerting;

import io.tapstate.core.common.TapstateException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.core.alerting.AlarmTypeCatalog.*;

class AlarmTypeCatalogTest {
    @Test void registrationDoesNotInventAProviderOrHideAnUnsupportedVersion() {
        AlarmTypeCatalog catalog = BuiltinAlarmTypes.catalog();
        assertThat(catalog.types()).hasSize(5);
        assertThat(catalog.availability("source.connection-failed", 1, 1, Set.of(), Set.of("condition"))).isEqualTo(Availability.INPUT_UNAVAILABLE);
        assertThat(catalog.availability("source.connection-failed", 1, 1, Set.of("source-operational-health"), Set.of())).isEqualTo(Availability.EVALUATOR_UNAVAILABLE);
        assertThat(catalog.availability("source.connection-failed", 1, 1, Set.of("source-operational-health"), Set.of("condition"))).isEqualTo(Availability.AVAILABLE);
        assertThat(catalog.availability("source.connection-failed", 2, 1, Set.of(), Set.of())).isEqualTo(Availability.UNSUPPORTED_TYPE_VERSION);
        assertThat(catalog.availability("source.connection-failed", 1, 0, Set.of(), Set.of())).isEqualTo(Availability.UNSUPPORTED_RECORD_VERSION);
        assertThat(catalog.availability("future.type", 1, 1, Set.of(), Set.of())).isEqualTo(Availability.UNKNOWN_TYPE);
    }
    @Test void newMetricAndTypedProviderTypesRegisterWithoutLifecycleChanges() {
        Type metric = type("test.metric", Kind.STATEFUL, "metrics", "threshold", "test.metric.recovery");
        Type provider = type("test.provider", Kind.STATEFUL, "new-provider", "condition", "test.provider.recovery");
        AlarmTypeCatalog catalog = new AlarmTypeCatalog(List.of(metric, provider));
        assertThat(catalog.availability(metric.id(), 1, 1, Set.of("metrics"), Set.of("threshold"))).isEqualTo(Availability.AVAILABLE);
        assertThat(catalog.availability(provider.id(), 1, 1, Set.of("new-provider"), Set.of("condition"))).isEqualTo(Availability.AVAILABLE);
        assertThatThrownBy(() -> new AlarmTypeCatalog(List.of(metric, metric))).isInstanceOf(TapstateException.class);
    }
    @Test void statefulTypesRequireRecoveryAndEventsCannotPretendToHaveAnEpisode() {
        assertThatThrownBy(() -> type("test.state", Kind.STATEFUL, "provider", "condition", null)).isInstanceOf(TapstateException.class);
        Type event = type("test.event", Kind.EVENT, "provider", "event", null);
        assertThat(event.recoveryTextKey()).isNull();
        assertThatThrownBy(() -> type("test.event", Kind.EVENT, "provider", "event", "fake.recovery")).isInstanceOf(TapstateException.class);
    }
    @Test void parameterRangesRejectUnknownFractionalOrOutOfRangeValuesAsAWhole() {
        Type stale = BuiltinAlarmTypes.catalog().types().stream().filter(t -> t.id().equals("pipeline.observation-stale")).findFirst().orElseThrow();
        assertThat(stale.validatedParameters(Map.of()).get("max-age")).isEqualByComparingTo("120");
        assertThat(stale.validatedParameters(Map.of("max-age", BigDecimal.valueOf(5))).get("max-age")).isEqualByComparingTo("5");
        assertThat(stale.validatedParameters(Map.of("max-age", BigDecimal.valueOf(3600))).get("max-age")).isEqualByComparingTo("3600");
        for (BigDecimal invalid : List.of(BigDecimal.valueOf(4), BigDecimal.valueOf(3601), new BigDecimal("120.5"))) {
            assertThatThrownBy(() -> stale.validatedParameters(Map.of("max-age", invalid))).isInstanceOf(TapstateException.class);
        }
        assertThatThrownBy(() -> stale.validatedParameters(Map.of("made-up", BigDecimal.ONE))).isInstanceOf(TapstateException.class);
    }
    private static Type type(String id, Kind kind, String provider, String evaluator, String recovery) {
        return new Type(id, 1, kind, Set.of("pipeline"), provider, evaluator, List.of(), AlarmSeverity.MINOR, false,
                id + ".condition", recovery, id + ".guidance");
    }
}
