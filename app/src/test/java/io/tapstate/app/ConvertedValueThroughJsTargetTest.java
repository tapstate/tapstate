package io.tapstate.app;

import io.tapstate.adapters.pdk.TapEventCodec;
import io.tapstate.adapters.transform.StatelessTransforms;
import io.tapstate.core.event.Envelope;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.value.TapStringValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConvertedValueThroughJsTargetTest {

    private record DriverKey(String hex) {
    }

    @Test
    @DisplayName("a converted value untouched by javascript is restored for the target")
    void aConvertedValueUntouchedByJavascriptIsRestoredForTheTarget() {
        DriverKey key = new DriverKey("64f0c0de");
        TapCodecsRegistry codecs = new TapCodecsRegistry()
                .registerToTapValue(DriverKey.class, (value, tapType) ->
                        new TapStringValue(((DriverKey) value).hex()))
                .registerFromTapValue(TapStringValue.class, tapValue ->
                        "OBJECT_ID".equals(tapValue.getOriginType())
                                ? new DriverKey(tapValue.getValue())
                                : tapValue.getValue());
        TapInsertRecordEvent source = TapInsertRecordEvent.create()
                .table("orders")
                .referenceTime(1L)
                .after(new LinkedHashMap<>(Map.of("_id", key)));
        Envelope decoded = TapEventCodec.decodeChange(source, codecs, Map.of("_id", "OBJECT_ID"));

        Envelope transformed = StatelessTransforms.js(
                        "function process(r, ctx) { r.after.seen = (r.after._id === '64f0c0de'); return r; }")
                .transform(decoded)
                .get(0);
        TapInsertRecordEvent target = (TapInsertRecordEvent) TapEventCodec.encode(transformed, codecs);

        assertThat(target.getAfter().get("_id"))
                .as("the target must receive its driver key, not the portable text exposed to javascript")
                .isEqualTo(key)
                .isInstanceOf(DriverKey.class);
    }
}
