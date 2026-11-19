package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.spi.sink.TargetTable;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

/** Resolves decimal candidates separately for each row image on a flexible target. */
final class PdkDeferredDecimal {
    private PdkDeferredDecimal() {
    }

    static Map<String, TapField> fields(TargetTable target, Map<String, TapField> declared, Map<String, Object> row) {
        if (row == null) return declared;
        Map<String, TapField> resolved = null;
        for (var field : target.fields()) {
            if (!field.isDeferredDecimal()) continue;
            Object value = row.get(field.name());
            if (!(ConvertedValue.unwrap(value) instanceof BigDecimal)) continue;
            if (!(value instanceof ConvertedValue carried)
                    || !"TapNumberValue".equals(carried.valueKind()) || carried.typeDescription() == null
                    || !field.numericType().equals(PdkTypeMapping.numericType(
                            PdkJsonProvider.parser().fromJson(carried.typeDescription(), TapNumber.class)))) {
                throw new IllegalArgumentException("decimal field '" + field.name() + "' in table '"
                        + target.name() + "' requires a matching source numeric descriptor");
            }
            var descriptor = new TapField(field.name(), null);
            descriptor.tapType(PdkTypeMapping.targetType(TapstateType.DECIMAL, field.numericType(), null));
            if (resolved == null) resolved = new LinkedHashMap<>(declared);
            resolved.put(field.name(), descriptor);
        }
        return resolved == null ? declared : resolved;
    }
}
