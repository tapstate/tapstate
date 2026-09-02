package io.tapstate.runtime.srs;

import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.capture.CaptureConfig;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The identity a mining chain — the shared cdc capture — is keyed by. Every per-table change ring and the
 * durable meta record live under this id, so resolving two cdc sources to the same id is what force-merges
 * them onto one shared change stream instead of mining the source log twice.
 *
 * <p>Identity has two derivations. By default it is the physical source coordinate: a content hash over the
 * connector and its settings, so the same database reached the same way is one chain — and, deliberately,
 * the table subset a source reads is <em>not</em> part of it, so two sources reading different tables of one
 * database share a chain and union their table sets. When config derivation cannot be trusted to coincide
 * (or must be forced to), an explicit {@code srs.key} overrides the hash and asserts the chain directly. The
 * two derivations live in separate namespaces, so a keyed id never collides with a derived one.
 *
 * <p>The config canonicalization is order-independent (map keys sort) and injective (each value is tagged by
 * type and every string is length-prefixed), so a re-ordered config resolves identically while two distinct
 * configs cannot be made to collide — a collision would silently merge unrelated sources onto one chain.
 */
public record MiningChainId(String value) {

    private static final String DERIVED_PREFIX = "mc-";
    private static final String KEYED_PREFIX = "mck-";

    public MiningChainId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("mining chain id value must be non-blank");
        }
    }

    /** The chain a source reads by its physical coordinate: a content hash of the connector and settings. */
    public static MiningChainId of(CaptureConfig config) {
        Objects.requireNonNull(config, "config");
        return new MiningChainId(DERIVED_PREFIX + CanonicalHash.ofText(canonicalConfig(config)));
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

    /** The connector plus its settings in an order-independent, injective form — the hash input. */
    private static String canonicalConfig(CaptureConfig config) {
        StringBuilder sb = new StringBuilder();
        token(sb, config.connectorId());
        canonicalValue(sb, config.settings());
        return sb.toString();
    }

    private static void canonicalValue(StringBuilder sb, Object value) {
        switch (value) {
            case null -> sb.append('~');
            case Map<?, ?> m -> {
                TreeMap<String, Object> sorted = new TreeMap<>();
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    sorted.put(String.valueOf(e.getKey()), e.getValue());
                }
                sb.append('M').append(sorted.size()).append('{');
                for (Map.Entry<String, Object> e : sorted.entrySet()) {
                    token(sb, e.getKey());
                    canonicalValue(sb, e.getValue());
                }
                sb.append('}');
            }
            case List<?> l -> {
                sb.append('L').append(l.size()).append('[');
                for (Object o : l) {
                    canonicalValue(sb, o);
                }
                sb.append(']');
            }
            case String s -> {
                sb.append('S');
                token(sb, s);
            }
            case Boolean b -> sb.append('B').append(b);
            default -> {
                // Number and any other scalar: tagged apart from a string so "1" and 1 never coincide.
                sb.append('N');
                token(sb, String.valueOf(value));
            }
        }
    }

    /** A length-prefixed string token — the prefix makes the encoding unspoofable by a crafted value. */
    private static void token(StringBuilder sb, String s) {
        sb.append(s.length()).append(':').append(s);
    }
}
