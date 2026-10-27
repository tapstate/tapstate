package io.tapstate.runtime.srs;

import io.tapstate.spi.capture.CaptureConfig;

import java.util.Objects;

/**
 * The identity a mining chain or an independent direct channel is keyed by. Every per-table change ring and the
 * durable meta record live under this id, so resolving two cdc sources to the same id is what force-merges
 * them onto one shared change stream instead of mining the source log twice.
 *
 * <p>A shared chain has two derivations. By default it is the physical source coordinate: a content hash over the
 * connector and its settings, so the same database reached the same way is one chain — and, deliberately,
 * the table subset a source reads is <em>not</em> part of it, so two sources reading different tables of one
 * database share a chain and union their table sets. When config derivation cannot be trusted to coincide
 * (or must be forced to), an explicit {@code srs.key} overrides the hash and asserts the chain directly. The
 * two derivations live in separate namespaces, so a keyed id never collides with a derived one.
 *
 * <p>The config canonicalization is order-independent (map keys sort) and injective (each value is tagged by
 * type and every string is length-prefixed), so a re-ordered config resolves identically while two distinct
 * configs cannot be made to collide — a collision would silently merge unrelated sources onto one chain.
 *
 * <p>A direct channel has no shared log to replay, so its recovery record additionally names its pipeline
 * and source. Its namespace is separate from the physical shared chain and remains stable when the read
 * mode, initial start or table selection changes.
 */
public record MiningChainId(String value) {

    private static final String KEYED_PREFIX = "mck-";

    public MiningChainId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("mining chain id value must be non-blank");
        }
    }

    /** The chain a source reads by its physical coordinate: a content hash of the connector and settings. */
    public static MiningChainId of(CaptureConfig config) {
        Objects.requireNonNull(config, "config");
        return new MiningChainId(io.tapstate.core.model.canonical.SourceCaptureIdentity.physical(config.connectorId(),config.settings()));
    }

    /** The chain a manual {@code srs.key} asserts directly, overriding config derivation. */
    public static MiningChainId ofKey(String srsKey) {
        if (srsKey == null || srsKey.isBlank()) {
            throw new IllegalArgumentException("srs.key must be non-blank");
        }
        return new MiningChainId(KEYED_PREFIX + srsKey);
    }

    /** The explicit key when one is given (config hash skipped), else the derived config hash. */
    public static MiningChainId resolve(CaptureConfig config, String srsKey) {
        return srsKey != null && !srsKey.isBlank() ? ofKey(srsKey) : of(config);
    }

    /** The recovery record of one independent channel, isolated from every other reader of its source. */
    public static MiningChainId forChannel(
            CaptureConfig config, String srsKey, String pipelineId, String sourceId) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(sourceId, "sourceId");
        if (pipelineId.isBlank() || sourceId.isBlank()) {
            throw new IllegalArgumentException("a direct channel names a non-blank pipeline and source");
        }
        return new MiningChainId(io.tapstate.core.model.canonical.SourceCaptureIdentity.directChannel(
                config.connectorId(),config.settings(),srsKey,pipelineId,sourceId));
    }

}
