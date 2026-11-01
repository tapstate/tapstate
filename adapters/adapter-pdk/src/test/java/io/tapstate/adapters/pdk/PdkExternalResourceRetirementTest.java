package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.*;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapstate.core.common.TapstateException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PdkExternalResourceRetirementTest {
    @Test void absenceCountsTheClusterUniqueSlotNameWithoutExcludingPhysicalSlots() {
        assertThat(PdkExternalResourceRetirement.query("original's_slot")).isEqualTo(
                "SELECT COUNT(*) AS slot_count FROM pg_catalog.pg_replication_slots WHERE slot_name='original''s_slot'");
        assertThat(PdkExternalResourceRetirement.scalar(1,List.of(row(1L)))).isEqualTo(1);
        assertThat(PdkExternalResourceRetirement.scalar(1,List.of(row(0L)))).isZero();
    }
    @Test void missingOrAmbiguousScalarIsNeverEvidenceOfAbsence() {
        assertThatThrownBy(() -> PdkExternalResourceRetirement.scalar(0,List.of())).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PdkExternalResourceRetirement.scalar(1,List.of())).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PdkExternalResourceRetirement.scalar(2,List.of(row(0L)))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PdkExternalResourceRetirement.scalar(1,List.of(row(0L),row(0L)))).isInstanceOf(TapstateException.class);
        for(var invalid:List.of(0,0d,"0",-1L))
            assertThatThrownBy(() -> PdkExternalResourceRetirement.scalar(1,List.of(row(invalid)))).isInstanceOf(TapstateException.class);
    }
    private static TapEvent row(Object value){return TapInsertRecordEvent.create().table("slot_verification").after(Map.of("slot_count",value));}
}
