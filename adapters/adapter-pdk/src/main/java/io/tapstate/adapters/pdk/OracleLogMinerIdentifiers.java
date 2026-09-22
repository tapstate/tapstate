package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Refuses Oracle identifiers that LogMiner cannot represent before its change stream starts. */
final class OracleLogMinerIdentifiers {

    private static final String ORACLE = "oracle";
    private static final String LOG_MINER = "logMiner";
    private static final int MAX_IDENTIFIER_LENGTH = 30;

    private OracleLogMinerIdentifiers() {
    }

    /** Whether this capture uses the Oracle connector's default LogMiner mode. */
    static boolean appliesTo(CaptureConfig config) {
        if (!ORACLE.equals(config.connectorId())) {
            return false;
        }
        Object mode = config.settings().get("logPluginName");
        return mode == null || LOG_MINER.equals(mode);
    }

    /** Checks identifiers already present in the resolved connection and capture selection. */
    static void validateConfigured(CaptureConfig config) {
        if (!appliesTo(config)) {
            return;
        }
        Object schema = config.settings().get("schema");
        if (schema instanceof String identifier) {
            validate(config, "schema", identifier);
        }
        for (String table : config.streams()) {
            validate(config, "table", table);
        }
    }

    /** Checks identifiers that become known only when the connector discovers the selected schema. */
    static void validateDiscovered(CaptureConfig config, List<TapTable> tables) {
        if (!appliesTo(config)) {
            return;
        }
        for (TapTable table : tables) {
            validate(config, "table", table.getId());
            Map<String, TapField> fields = table.getNameFieldMap();
            if (fields == null) {
                continue;
            }
            fields.forEach((name, field) -> {
                validate(config, "column", name);
                if (field != null && !Objects.equals(name, field.getName())) {
                    validate(config, "column", field.getName());
                }
            });
        }
    }

    private static void validate(CaptureConfig config, String kind, String identifier) {
        if (identifier == null
                || identifier.codePointCount(0, identifier.length()) <= MAX_IDENTIFIER_LENGTH) {
            return;
        }
        throw new TapstateException(ConnectorError.LOGMINER_IDENTIFIER_TOO_LONG,
                Map.of("connector", config.connectorId(),
                        "kind", kind,
                        "identifier", identifier,
                        "limit", MAX_IDENTIFIER_LENGTH), null);
    }
}
