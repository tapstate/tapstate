package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class IoErrorDefinitions {
    public static final ProductErrorDefinition STORE_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.IO, 1, Severity.ERROR, Set.of("detail"));
    public static final ProductErrorDefinition STORE_QUOTA_EXCEEDED =
            new ProductErrorDefinition(ProductErrorModule.IO, 2, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition STORE_UNAUTHORIZED =
            new ProductErrorDefinition(ProductErrorModule.IO, 3, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition DOCUMENT_UNREADABLE =
            new ProductErrorDefinition(ProductErrorModule.IO, 4, Severity.ERROR, Set.of("id", "field"));
    public static final ProductErrorDefinition CONNECTOR_ARTIFACT_CLEANUP_UNCONFIRMED =
            new ProductErrorDefinition(ProductErrorModule.IO, 5, Severity.ERROR, Set.of("hash", "upload"));
    public static final ProductErrorDefinition DOCUMENT_INVALID =
            new ProductErrorDefinition(ProductErrorModule.IO, 6, Severity.ERROR, Set.of("id", "field"));
    public static final ProductErrorDefinition SCHEMA_READ_CONTENTION =
            new ProductErrorDefinition(ProductErrorModule.IO, 7, Severity.ERROR, Set.of("connectionId"));
    public static final ProductErrorDefinition SCHEMA_WRITE_CONTENTION =
            new ProductErrorDefinition(ProductErrorModule.IO, 8, Severity.ERROR, Set.of("connectionId"));
    public static final ProductErrorDefinition WORKLOAD_CLAIM_FENCED =
            new ProductErrorDefinition(ProductErrorModule.IO, 9, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition SINK_WRITER_PROGRESS_AMBIGUOUS =
            new ProductErrorDefinition(ProductErrorModule.IO, 10, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition NOTE_WRITE_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.IO, 11, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition REQUIRED_DEAD_LETTER_FENCED =
            new ProductErrorDefinition(ProductErrorModule.IO, 12, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition REQUIRED_DEAD_LETTER_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.IO, 13, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CACHE_AUTH_FENCED =
            new ProductErrorDefinition(ProductErrorModule.IO, 14, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition PIPELINE_INTENT_FENCED =
            new ProductErrorDefinition(ProductErrorModule.IO, 15, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition PIPELINE_STATE_REBUILD_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.IO, 16, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition CAPTURE_STATE_REBUILD_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.IO, 17, Severity.ERROR, Set.of("capture"));
    public static final ProductErrorDefinition CACHE_BUSINESS_FENCED =
            new ProductErrorDefinition(ProductErrorModule.IO, 18, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition SRS_EVENT_MODEL_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.IO, 19, Severity.ERROR, Set.of("ring", "epoch", "version"));
    public static final ProductErrorDefinition SRS_CONSUMER_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.IO, 20, Severity.ERROR, Set.of("chain"));
    public static final ProductErrorDefinition SRS_CHECKPOINT_UNPROVEN =
            new ProductErrorDefinition(ProductErrorModule.IO, 21, Severity.ERROR, Set.of("chain"));
    public static final ProductErrorDefinition SRS_MODEL_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.IO, 22, Severity.ERROR, Set.of("chain", "table", "version"));
    public static final ProductErrorDefinition SRS_MODEL_CAPACITY =
            new ProductErrorDefinition(ProductErrorModule.IO, 23, Severity.ERROR, Set.of("chain", "bytes"));
    public static final ProductErrorDefinition SRS_FULL_PREPARATION_CAPACITY =
            new ProductErrorDefinition(ProductErrorModule.IO, 24, Severity.ERROR, Set.of("chain", "bytes"));
    public static final ProductErrorDefinition SRS_FULL_PREPARATION_UNPROVEN =
            new ProductErrorDefinition(ProductErrorModule.IO, 25, Severity.ERROR, Set.of("chain"));
    public static final ProductErrorDefinition SRS_POSITION_MODEL_UNPROVEN =
            new ProductErrorDefinition(ProductErrorModule.IO, 26, Severity.ERROR, Set.of("chain"));
    public static final ProductErrorDefinition SRS_MODEL_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.IO, 27, Severity.ERROR, Set.of("chain", "table", "version"));
    public static final ProductErrorDefinition NOTE_ORIGIN_MISMATCH =
            new ProductErrorDefinition(ProductErrorModule.IO, 28, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition NOTE_REFERENCED =
            new ProductErrorDefinition(ProductErrorModule.IO, 29, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition NOTE_SCOPE_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.IO, 30, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition SRS_PROGRESS_UNPROVEN =
            new ProductErrorDefinition(ProductErrorModule.IO, 31, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition DIRECT_CHANNEL_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.IO, 32, Severity.ERROR, Set.of("channel"));
    public static final ProductErrorDefinition PIPELINE_CAPTURE_IDENTITY_UNPROVEN =
            new ProductErrorDefinition(ProductErrorModule.IO, 33, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition PIPELINE_TABLE_SELECTION_REBUILD_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.IO, 34, Severity.ERROR, Set.of("pipeline", "tables"));
    public static final ProductErrorDefinition CONFIGURATION_IMPORT_REFUSED =
            new ProductErrorDefinition(ProductErrorModule.IO, 35, Severity.ERROR, Set.of("reason"));
    public static final ProductErrorDefinition CONFIGURATION_IMPORT_LIMIT =
            new ProductErrorDefinition(ProductErrorModule.IO, 36, Severity.ERROR, Set.of("limit"));
    public static final ProductErrorDefinition DOCUMENT_TOO_LARGE =
            new ProductErrorDefinition(ProductErrorModule.IO, 37, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition SINK_MODEL_CAPACITY =
            new ProductErrorDefinition(ProductErrorModule.IO, 38, Severity.ERROR, Set.of("pipeline", "target", "bytes", "limit"));
    public static final ProductErrorDefinition CONNECTOR_UPLOAD_UNSETTLED =
            new ProductErrorDefinition(ProductErrorModule.IO, 39, Severity.ERROR, Set.of("hash", "attempt"));
    public static final ProductErrorDefinition SOURCE_IDENTITY_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.IO, 40, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition SOURCE_IDENTITY_EXHAUSTED =
            new ProductErrorDefinition(ProductErrorModule.IO, 41, Severity.ERROR, Set.of("id"));

    private IoErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(STORE_UNAVAILABLE, STORE_QUOTA_EXCEEDED, STORE_UNAUTHORIZED, DOCUMENT_UNREADABLE, CONNECTOR_ARTIFACT_CLEANUP_UNCONFIRMED, DOCUMENT_INVALID, SCHEMA_READ_CONTENTION, SCHEMA_WRITE_CONTENTION, WORKLOAD_CLAIM_FENCED, SINK_WRITER_PROGRESS_AMBIGUOUS, NOTE_WRITE_CONFLICT, REQUIRED_DEAD_LETTER_FENCED, REQUIRED_DEAD_LETTER_CONFLICT, CACHE_AUTH_FENCED, PIPELINE_INTENT_FENCED, PIPELINE_STATE_REBUILD_REQUIRED, CAPTURE_STATE_REBUILD_REQUIRED, CACHE_BUSINESS_FENCED, SRS_EVENT_MODEL_UNAVAILABLE, SRS_CONSUMER_UNAVAILABLE, SRS_CHECKPOINT_UNPROVEN, SRS_MODEL_UNAVAILABLE, SRS_MODEL_CAPACITY, SRS_FULL_PREPARATION_CAPACITY, SRS_FULL_PREPARATION_UNPROVEN, SRS_POSITION_MODEL_UNPROVEN, SRS_MODEL_CONFLICT, NOTE_ORIGIN_MISMATCH, NOTE_REFERENCED, NOTE_SCOPE_UNAVAILABLE, SRS_PROGRESS_UNPROVEN, DIRECT_CHANNEL_UNAVAILABLE, PIPELINE_CAPTURE_IDENTITY_UNPROVEN, PIPELINE_TABLE_SELECTION_REBUILD_REQUIRED, CONFIGURATION_IMPORT_REFUSED, CONFIGURATION_IMPORT_LIMIT, DOCUMENT_TOO_LARGE, SINK_MODEL_CAPACITY, CONNECTOR_UPLOAD_UNSETTLED, SOURCE_IDENTITY_UNAVAILABLE, SOURCE_IDENTITY_EXHAUSTED);
    }
}
