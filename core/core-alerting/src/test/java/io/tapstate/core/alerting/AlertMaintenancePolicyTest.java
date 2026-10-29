package io.tapstate.core.alerting;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class AlertMaintenancePolicyTest {
    @Test void pressureStopsOptionalWorkBeforeNewFaultAdmissionAndNeedsTwoLowSamplesToClear() {
        long maximum=AlertMaintenancePolicy.CAPACITY_BYTES;
        var value=AlertMaintenancePolicy.Capacity.initial().sample(maximum*8/10);
        assertThat(value.pressure()).isEqualTo(AlertMaintenancePolicy.Pressure.WARNING);
        assertThat(value.admitsTestOrReminder()).isTrue();
        value=value.sample(maximum*9/10);
        assertThat(value.admitsTestOrReminder()).isFalse();assertThat(value.admitsNewFact()).isTrue();
        value=value.sample(maximum);assertThat(value.admitsNewFact()).isFalse();
        value=value.sample(maximum*8/10-1);assertThat(value.admitsNewFact()).isFalse();
        value=value.sample(maximum*8/10);assertThat(value.admitsNewFact()).isFalse();
        value=value.sample(0);assertThat(value.admitsNewFact()).isFalse();
        assertThat(value.sample(0).pressure()).isEqualTo(AlertMaintenancePolicy.Pressure.NORMAL);
    }
}
