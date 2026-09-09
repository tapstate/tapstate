package io.tapstate.core.model;

/**
 * SRS configuration surface (§4) — only legal on {@code mode: cdc} sources
 * (validate-layer rule). {@code key} is the shared mining-chain assertion; physical SRS
 * internals belong to the storage layer, not this model.
 */
@Doc("Stream Replay Store configuration; only valid on sources running in cdc mode.")
public record Srs(
        @Doc("Shared mining-chain assertion key identifying the replay store.")
        String key,
        @Doc("How long captured change data is retained in the replay store.")
        String retention,
        @Doc("Schema-evolution policy applied as upstream table structures change.")
        SrsSchemaEvolution schemaEvolution,
        @Doc("Whether the replay store can be queried directly.")
        Boolean queryable,
        @Doc(value = "Whether the replay store is provisioned; false streams cdc straight to the "
                + "single consumer with no shared buffering.", def = "true")
        Boolean enabled) {
}
