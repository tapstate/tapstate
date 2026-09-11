package io.tapstate.spi.store;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One entry in a mining chain's schema history: a versioned snapshot of a table's field schema and the
 * ring sequence of the ddl event that introduced it. A version is added on a schema change and never
 * mutated in place, so a consumer can resolve the schema in force at any change it reads from the version
 * the change carries. Which versions are still there is the store's to bound — it keeps the newest ones
 * and drops the oldest, as {@link SrsMetaStore#appendSchemaVersion} states.
 *
 * <p>Fields — {@code version} (the monotonic schema version, non-negative), {@code schema} (the field
 * schema at this version, a shallow-unmodifiable defensive copy that preserves field order), and
 * {@code ddlSeq} (the ring sequence of the ddl event that introduced the version, non-negative).
 *
 * <p>A pure value over {@code java..} only (rule R2).
 */
public record SchemaVersion(long version, Map<String, Object> schema, long ddlSeq) {

    public SchemaVersion {
        if (version < 0) {
            throw new IllegalArgumentException("schema version must be non-negative");
        }
        if (ddlSeq < 0) {
            throw new IllegalArgumentException("schema ddlSeq must be non-negative");
        }
        if (schema == null) {
            throw new IllegalArgumentException("schema version schema must be set");
        }
        schema = Collections.unmodifiableMap(new LinkedHashMap<>(schema));
    }
}
