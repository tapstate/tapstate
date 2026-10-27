package io.tapstate.core.event;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;
import java.util.Set;

/** Diagnosable violations of the portable row-value contract. */
public enum RowValueError implements TapstateErrorCode {
    NON_STRING_MAP_KEY;

    @Override
    public String code() {
        return "core.row-map-key-unsupported";
    }

    @Override
    public Severity severity() {
        return Severity.ERROR;
    }

    @Override
    public Set<String> placeholders() {
        return Set.of("path", "keyType");
    }
}
