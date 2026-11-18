package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class IoErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.IO);

    public static final ProductErrorDefinition STORE_UNAVAILABLE =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("detail"));
    public static final ProductErrorDefinition STORE_QUOTA_EXCEEDED =
            DEFINITIONS.define(2, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition STORE_UNAUTHORIZED =
            DEFINITIONS.define(3, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition DOCUMENT_UNREADABLE =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("id", "field"));
    public static final ProductErrorDefinition CONNECTOR_ARTIFACT_CLEANUP_UNCONFIRMED =
            DEFINITIONS.define(5, Severity.ERROR, Set.of("hash", "upload"));
    public static final ProductErrorDefinition DOCUMENT_INVALID =
            DEFINITIONS.define(6, Severity.ERROR, Set.of("id", "field"));
    public static final ProductErrorDefinition SCHEMA_READ_CONTENTION =
            DEFINITIONS.define(7, Severity.ERROR, Set.of("connectionId"));
    public static final ProductErrorDefinition SCHEMA_WRITE_CONTENTION =
            DEFINITIONS.define(8, Severity.ERROR, Set.of("connectionId"));
    public static final ProductErrorDefinition WORKLOAD_CLAIM_FENCED =
            DEFINITIONS.define(9, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition SINK_WRITER_PROGRESS_AMBIGUOUS =
            DEFINITIONS.define(10, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition NOTE_WRITE_CONFLICT =
            DEFINITIONS.define(11, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition REQUIRED_DEAD_LETTER_FENCED =
            DEFINITIONS.define(12, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition REQUIRED_DEAD_LETTER_CONFLICT =
            DEFINITIONS.define(13, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CACHE_AUTH_FENCED =
            DEFINITIONS.define(14, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition PIPELINE_INTENT_FENCED =
            DEFINITIONS.define(15, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition PIPELINE_STATE_REBUILD_REQUIRED =
            DEFINITIONS.define(16, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition CAPTURE_STATE_REBUILD_REQUIRED =
            DEFINITIONS.define(17, Severity.ERROR, Set.of("capture"));
    public static final ProductErrorDefinition CACHE_BUSINESS_FENCED =
            DEFINITIONS.define(18, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition SRS_EVENT_MODEL_UNAVAILABLE =
            DEFINITIONS.define(19, Severity.ERROR, Set.of("ring", "epoch", "version"));
    public static final ProductErrorDefinition SRS_CONSUMER_UNAVAILABLE =
            DEFINITIONS.define(20, Severity.ERROR, Set.of("chain"));
    public static final ProductErrorDefinition SRS_CHECKPOINT_UNPROVEN =
            DEFINITIONS.define(21, Severity.ERROR, Set.of("chain"));
    public static final ProductErrorDefinition SRS_MODEL_UNAVAILABLE =
            DEFINITIONS.define(22, Severity.ERROR, Set.of("chain", "table", "version"));
    public static final ProductErrorDefinition SRS_MODEL_CAPACITY =
            DEFINITIONS.define(23, Severity.ERROR, Set.of("chain", "bytes"));
    public static final ProductErrorDefinition SRS_FULL_PREPARATION_CAPACITY =
            DEFINITIONS.define(24, Severity.ERROR, Set.of("chain", "bytes"));
    public static final ProductErrorDefinition SRS_FULL_PREPARATION_UNPROVEN =
            DEFINITIONS.define(25, Severity.ERROR, Set.of("chain"));
    public static final ProductErrorDefinition SRS_POSITION_MODEL_UNPROVEN =
            DEFINITIONS.define(26, Severity.ERROR, Set.of("chain"));
    public static final ProductErrorDefinition SRS_MODEL_CONFLICT =
            DEFINITIONS.define(27, Severity.ERROR, Set.of("chain", "table", "version"));
    public static final ProductErrorDefinition NOTE_ORIGIN_MISMATCH =
            DEFINITIONS.define(28, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition NOTE_REFERENCED =
            DEFINITIONS.define(29, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition NOTE_SCOPE_UNAVAILABLE =
            DEFINITIONS.define(30, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition SRS_PROGRESS_UNPROVEN =
            DEFINITIONS.define(31, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition DIRECT_CHANNEL_UNAVAILABLE =
            DEFINITIONS.define(32, Severity.ERROR, Set.of("channel"));
    public static final ProductErrorDefinition PIPELINE_CAPTURE_IDENTITY_UNPROVEN =
            DEFINITIONS.define(33, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition PIPELINE_TABLE_SELECTION_REBUILD_REQUIRED =
            DEFINITIONS.define(34, Severity.ERROR, Set.of("pipeline", "tables"));
    public static final ProductErrorDefinition CONFIGURATION_IMPORT_REFUSED =
            DEFINITIONS.define(35, Severity.ERROR, Set.of("reason"));
    public static final ProductErrorDefinition CONFIGURATION_IMPORT_LIMIT =
            DEFINITIONS.define(36, Severity.ERROR, Set.of("limit"));
    public static final ProductErrorDefinition DOCUMENT_TOO_LARGE =
            DEFINITIONS.define(37, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition SINK_MODEL_CAPACITY =
            DEFINITIONS.define(38, Severity.ERROR, Set.of("pipeline", "target", "bytes", "limit"));
    public static final ProductErrorDefinition CONNECTOR_UPLOAD_UNSETTLED =
            DEFINITIONS.define(39, Severity.ERROR, Set.of("hash", "attempt"));
    public static final ProductErrorDefinition SOURCE_IDENTITY_UNAVAILABLE =
            DEFINITIONS.define(40, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition SOURCE_IDENTITY_EXHAUSTED =
            DEFINITIONS.define(41, Severity.ERROR, Set.of("id"));

    private IoErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
