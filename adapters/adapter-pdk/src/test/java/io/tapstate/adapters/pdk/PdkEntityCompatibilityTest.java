package io.tapstate.adapters.pdk;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapstate.core.event.Op;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class PdkEntityCompatibilityTest {
    @Test
    void theHostCanLinkAndDecodeTheOracleConnectorsDdlWarningEvent() throws ReflectiveOperationException {
        // Connector APIs delegate entity types to this loader; a plugin cannot supply a missing host type.
        Class<?> warning = Class.forName("io.tapdata.entity.event.ddl.TapDDLWarningEvent", true,
                PdkConnector.class.getClassLoader());
        TapDDLEvent event = (TapDDLEvent) warning.getConstructor().newInstance();
        event.setTableId("orders");
        event.setReferenceTime(123L);
        event.setOriginDDL("ALTER TABLE orders ADD extra INT");
        var decoded = TapEventCodec.decodeChange(event, new TapCodecsRegistry(), Map.of());
        assertThat(decoded.op()).isEqualTo(Op.DDL);
        assertThat(decoded.src()).isEqualTo("orders");
        assertThat(decoded.ts()).isEqualTo(123L);
        assertThat(decoded.schema()).containsValue("ALTER TABLE orders ADD extra INT");
    }
}
