package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.value.DateTime;
import java.time.Instant;

/** Keeps PDK date-time values inside the connector adapter boundary. */
public final class PdkPreviewDateTimes {

    private PdkPreviewDateTimes() {
    }

    public static boolean isDateTime(Object value) {
        return value instanceof DateTime;
    }

    public static String toIsoString(Object value) {
        return ((DateTime) value).toInstant().toString();
    }

    public static Object fromIsoString(String value) {
        return new DateTime(Instant.parse(value));
    }
}
