package io.tapstate.archtests;

import io.tapstate.core.alerting.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

/** Locks identity and the independent lifecycle dimensions without treating them as a storage codec. */
class AlertCoreContractTest {
    @Test void coreIdentityAndLifecycleDimensionsStayExplicit() throws IOException {
        String actual = "contract-version=" + AlarmKey.CONTRACT_VERSION + "\n"
                + fields("alarm-key", AlarmKey.class) + fields("scope", AlarmKey.Scope.class)
                + fields("resource", AlarmKey.Resource.class) + fields("rule", AlertRule.class)
                + fields("signal", AlertSignal.class) + fields("permit", DeliveryAttempt.Permit.class)
                + fields("group-member", FrozenGroupPolicy.Member.class) + fields("frozen-group", FrozenGroupPolicy.Batch.class)
                + fields("group-disposition-key", FrozenGroupPolicy.DispositionKey.class)
                + fields("target-limit-domain", AlertTargetLimit.Domain.class) + fields("target-limit", AlertTargetLimit.class)
                + fields("metric-input-budget", ObservationMetricInput.Budget.class)
                + fields("metric-publication", ObservationMetricInput.Publication.class)
                + fields("metric-reading", ObservationMetricInput.Reading.class)
                + "metric-input-version=" + ObservationMetricInput.CONTRACT_VERSION + "\n"
                + "metric-input-max-facts=" + ObservationMetricInput.MAX_FACTS + "\n"
                + "metric-input-max-points=" + ObservationMetricInput.MAX_POINTS + "\n"
                + "metric-input-status=" + names(ObservationMetricInput.Status.values()) + "\n"
                + fields("delivery-capability-header", DeliveryCapabilityPolicy.Header.class)
                + fields("delivery-capability-decision", DeliveryCapabilityPolicy.Decision.class)
                + "delivery-capability-version=" + DeliveryCapabilityPolicy.CONTRACT_VERSION + "\n"
                + "delivery-capability-max-channels=" + DeliveryCapabilityPolicy.MAX_CHANNELS + "\n"
                + "delivery-capability-disposition=" + names(DeliveryCapabilityPolicy.Disposition.values()) + "\n"
                + "delivery-capability-reason=" + names(DeliveryCapabilityPolicy.Reason.values()) + "\n"
                + "group-max-members=" + FrozenGroupPolicy.MAX_MEMBERS + "\n"
                + "severity=" + names(AlarmSeverity.values()) + "\n"
                + "condition=" + names(AlertSnapshot.Status.values()) + "\n"
                + "quality=" + names(AlertSnapshot.Quality.values()) + "\n"
                + "end-reason=" + names(AlertSnapshot.EndReason.values()) + "\n";
        try (var in = getClass().getResourceAsStream("/alert-core-contract.golden")) {
            assertThat(in).isNotNull();
            assertThat(actual).isEqualTo(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }
    private static String fields(String name, Class<?> type) {
        return name + "=" + Arrays.stream(type.getRecordComponents()).map(c -> c.getName())
                .collect(Collectors.joining(",")) + "\n";
    }
    private static String names(Enum<?>[] values) {
        return Arrays.stream(values).map(Enum::name).collect(Collectors.joining(","));
    }
}
