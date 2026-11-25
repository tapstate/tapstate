package io.tapstate.cli;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** The protected saved-Source settings read used only to build connection probes. */
sealed interface ConnectionSettingsOutcome {

    /** The existing Source API's secret-redacted settings, never a generic artifact export. */
    record Found(String connector, Map<String, Object> settings) implements ConnectionSettingsOutcome {
        public Found {
            settings = Collections.unmodifiableMap(new LinkedHashMap<>(settings));
        }

        @Override public String toString() {
            return "Found[connector=" + connector + ", settings=<withheld>]";
        }
    }

    record Absent() implements ConnectionSettingsOutcome { }
    record Rejected(String code, String message) implements ConnectionSettingsOutcome { }
    record Unreachable() implements ConnectionSettingsOutcome { }
}
