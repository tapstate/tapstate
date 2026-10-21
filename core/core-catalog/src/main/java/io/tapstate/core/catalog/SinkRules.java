package io.tapstate.core.catalog;

import java.util.ArrayList;
import java.util.List;

import io.tapstate.core.model.WriteMode;

/**
 * Derives a connector's {@link SinkCapability} from its derived write capability and its declared
 * DML policies. {@code write_record} makes it a sink; the {@code dml_insert_policy} alternatives and
 * the presence of a {@code dml_update_policy} tell apart upsert from append-only.
 */
public final class SinkRules {

    /** The insert disposition that updates a row when its key already exists, i.e. upsert. */
    private static final String UPDATE_ON_EXISTS = "update_on_exists";

    private SinkRules() {
    }

    /** Applies the running release's role restriction to a derived or previously stored sink. */
    static SinkCapability applySupportPolicy(String connectorId, SinkCapability sink) {
        return OfficialConnectors.isSourceOnly(connectorId) ? new SinkCapability(false, List.of()) : sink;
    }

    public static SinkCapability derive(boolean writeRecordCapable,
                                        List<String> dmlInsertAlternatives,
                                        boolean hasDmlUpdatePolicy) {
        if (!writeRecordCapable) {
            return new SinkCapability(false, List.of());
        }

        // Any sink can insert, so append is always available; upsert needs a keyed-update signal.
        // With no DML signal at all (~30% of connectors have no capabilities block) default to the
        // common superset rather than silently dropping upsert. Note this no-signal guess carries no
        // provenance flag (unlike a mode's ModeSource), so it is indistinguishable from a real DML
        // signal here; listing no-signal sinks for human review is the ingest report's job (a later slice).
        boolean hasInsertSignal = dmlInsertAlternatives != null && !dmlInsertAlternatives.isEmpty();
        boolean upsert = !hasInsertSignal
                || dmlInsertAlternatives.contains(UPDATE_ON_EXISTS)
                || hasDmlUpdatePolicy;

        List<WriteMode> semantics = new ArrayList<>();
        if (upsert) {
            semantics.add(WriteMode.UPSERT);
        }
        semantics.add(WriteMode.APPEND);
        return new SinkCapability(true, semantics);
    }
}
