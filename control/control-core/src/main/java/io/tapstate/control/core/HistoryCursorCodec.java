package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.RateHistoryStore.Key;
import io.tapstate.spi.store.RateHistoryStore.Visibility;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Stateless HMAC-SHA256 cursors for the eventual history walk. */
public final class HistoryCursorCodec {

    public static final Duration DEFAULT_TTL = Duration.ofMinutes(10);

    private static final String ALGORITHM = "HmacSHA256";
    private static final byte[] CONTEXT = "history-cursor-v1\0".getBytes(StandardCharsets.UTF_8);
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    private final byte[] secret;
    private final Duration ttl;
    private final Clock clock;

    public HistoryCursorCodec(byte[] secret, Duration ttl, Clock clock) {
        Objects.requireNonNull(secret, "secret");
        if (secret.length == 0) {
            throw new IllegalArgumentException("a history cursor signing secret is not empty");
        }
        this.secret = secret.clone();
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("a history cursor lifetime is positive");
        }
    }

    public HistoryCursorCodec(byte[] secret, Clock clock) {
        this(secret, DEFAULT_TTL, clock);
    }

    /** Everything a cursor binds that a caller must repeat unchanged. */
    public record QueryBinding(String pipelineId, Instant from, Instant to,
            HistoryResolution resolution, int limit, List<String> tables, Visibility visibility) {
        public QueryBinding {
            Objects.requireNonNull(pipelineId, "pipelineId");
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(to, "to");
            Objects.requireNonNull(resolution, "resolution");
            tables = List.copyOf(Objects.requireNonNull(tables, "tables"));
            Objects.requireNonNull(visibility, "visibility");
        }

        public QueryBinding(String pipelineId, Instant from, Instant to,
                HistoryResolution resolution, int limit, List<String> tables) {
            this(pipelineId, from, to, resolution, limit, tables, new Visibility(null, true));
        }
    }

    /** Frozen bounds and the exact key/boundary at which projection resumes. */
    public record State(QueryBinding binding, Instant effectiveFrom, Instant effectiveTo,
            Instant retentionCutoff, Key afterKey, Instant resumeAt, Instant issuedAt, Instant expiresAt) {
        public State {
            Objects.requireNonNull(binding, "binding");
            Objects.requireNonNull(effectiveFrom, "effectiveFrom");
            Objects.requireNonNull(effectiveTo, "effectiveTo");
            Objects.requireNonNull(retentionCutoff, "retentionCutoff");
            Objects.requireNonNull(afterKey, "afterKey");
            Objects.requireNonNull(resumeAt, "resumeAt");
            Objects.requireNonNull(issuedAt, "issuedAt");
            Objects.requireNonNull(expiresAt, "expiresAt");
        }
    }

    /** Issues a cursor without retaining any server-side state. */
    public String issue(QueryBinding binding, Instant effectiveFrom, Instant effectiveTo,
            Instant retentionCutoff, Key afterKey, Instant resumeAt) {
        Instant issuedAt = clock.instant();
        State state = new State(binding, effectiveFrom, effectiveTo, retentionCutoff, afterKey, resumeAt,
                issuedAt, issuedAt.plus(ttl));
        byte[] claims = claims(state).getBytes(StandardCharsets.UTF_8);
        return ENCODER.encodeToString(claims) + "." + ENCODER.encodeToString(sign(claims));
    }

    /** Verifies, parses, expires and query-binds a cursor in that order. */
    public State read(String token, QueryBinding expected) {
        Objects.requireNonNull(expected, "expected");
        if (token == null || token.isBlank()) {
            throw invalid("MALFORMED", null);
        }
        String[] pieces = token.split("\\.", -1);
        if (pieces.length != 2 || pieces[0].isEmpty() || pieces[1].isEmpty()) {
            throw invalid("MALFORMED", null);
        }

        byte[] claims;
        byte[] signature;
        try {
            claims = DECODER.decode(pieces[0]);
            signature = DECODER.decode(pieces[1]);
        } catch (IllegalArgumentException malformed) {
            throw invalid("MALFORMED", malformed);
        }
        if (!java.security.MessageDigest.isEqual(sign(claims), signature)) {
            throw invalid("TAMPERED", null);
        }

        State state;
        try {
            state = parse(new String(claims, StandardCharsets.UTF_8));
        } catch (RuntimeException malformed) {
            if (malformed instanceof TapstateException coded) {
                throw coded;
            }
            throw invalid("MALFORMED", malformed);
        }
        if (!clock.instant().isBefore(state.expiresAt())) {
            throw new TapstateException(MonitorError.CURSOR_EXPIRED,
                    Map.of("operation", "pipeline.metrics.history"), null);
        }
        if (!state.binding().equals(expected)) {
            throw invalid("QUERY_MISMATCH", null);
        }
        return state;
    }

    private String claims(State state) {
        QueryBinding binding = state.binding();
        StringBuilder out = new StringBuilder();
        line(out, "v", "2");
        line(out, "op", "pipeline.metrics.history");
        line(out, "pipeline", text(binding.pipelineId()));
        line(out, "from", instant(binding.from()));
        line(out, "to", instant(binding.to()));
        line(out, "resolution", binding.resolution().name());
        line(out, "limit", Integer.toString(binding.limit()));
        line(out, "tables", Integer.toString(binding.tables().size()));
        for (String table : binding.tables()) {
            line(out, "table", text(table));
        }
        line(out, "incarnation", binding.visibility().incarnationId() == null
                ? "" : text(binding.visibility().incarnationId()));
        line(out, "includeLegacy", Boolean.toString(binding.visibility().includeLegacy()));
        line(out, "effectiveFrom", instant(state.effectiveFrom()));
        line(out, "effectiveTo", instant(state.effectiveTo()));
        line(out, "retentionCutoff", instant(state.retentionCutoff()));
        line(out, "afterAt", instant(state.afterKey().observedAt()));
        line(out, "afterKey", text(state.afterKey().internalKey()));
        line(out, "resumeAt", instant(state.resumeAt()));
        line(out, "issuedAt", instant(state.issuedAt()));
        line(out, "expiresAt", instant(state.expiresAt()));
        return out.toString();
    }

    private static State parse(String claims) {
        String[] lines = claims.split("\\n", -1);
        Map<String, List<String>> values = new LinkedHashMap<>();
        for (String line : lines) {
            if (line.isEmpty()) {
                continue;
            }
            int split = line.indexOf('=');
            if (split < 1) {
                throw new IllegalArgumentException("claim without a name");
            }
            values.computeIfAbsent(line.substring(0, split), ignored -> new ArrayList<>())
                    .add(line.substring(split + 1));
        }
        String version = one(values, "v");
        if (!("1".equals(version) || "2".equals(version))
                || !"pipeline.metrics.history".equals(one(values, "op"))) {
            throw new IllegalArgumentException("unsupported cursor claims");
        }
        int tableCount = Integer.parseInt(one(values, "tables"));
        List<String> encodedTables = values.getOrDefault("table", List.of());
        if (tableCount != encodedTables.size()) {
            throw new IllegalArgumentException("table count differs");
        }
        List<String> tables = encodedTables.stream().map(HistoryCursorCodec::plain).toList();
        Visibility visibility;
        if ("2".equals(version)) {
            String includeLegacy = one(values, "includeLegacy");
            if (!("true".equals(includeLegacy) || "false".equals(includeLegacy))) {
                throw new IllegalArgumentException("invalid history visibility flag");
            }
            String incarnation = one(values, "incarnation");
            visibility = new Visibility(incarnation.isEmpty() ? null : plain(incarnation),
                    Boolean.parseBoolean(includeLegacy));
        } else {
            visibility = new Visibility(null, true);
        }
        QueryBinding binding = new QueryBinding(
                plain(one(values, "pipeline")),
                parsedInstant(one(values, "from")),
                parsedInstant(one(values, "to")),
                HistoryResolution.valueOf(one(values, "resolution")),
                Integer.parseInt(one(values, "limit")),
                tables, visibility);
        Key key = new Key(parsedInstant(one(values, "afterAt")), plain(one(values, "afterKey")));
        State state = new State(binding,
                parsedInstant(one(values, "effectiveFrom")),
                parsedInstant(one(values, "effectiveTo")),
                parsedInstant(one(values, "retentionCutoff")),
                key,
                parsedInstant(one(values, "resumeAt")),
                parsedInstant(one(values, "issuedAt")),
                parsedInstant(one(values, "expiresAt")));
        List<String> expectedNames = List.of("v", "op", "pipeline", "from", "to", "resolution", "limit",
                "tables", "table", "incarnation", "includeLegacy", "effectiveFrom", "effectiveTo",
                "retentionCutoff", "afterAt", "afterKey", "resumeAt", "issuedAt", "expiresAt");
        if (values.keySet().stream().anyMatch(name -> !expectedNames.contains(name))) {
            throw new IllegalArgumentException("unknown cursor claim");
        }
        return state;
    }

    private byte[] sign(byte[] claims) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(secret, ALGORITHM));
            mac.update(CONTEXT);
            return mac.doFinal(claims);
        } catch (GeneralSecurityException unavailable) {
            throw new IllegalStateException("HMAC-SHA256 unavailable", unavailable);
        }
    }

    private static String one(Map<String, List<String>> values, String name) {
        List<String> matches = values.get(name);
        if (matches == null || matches.size() != 1) {
            throw new IllegalArgumentException("cursor claim is missing or repeated: " + name);
        }
        return matches.getFirst();
    }

    private static void line(StringBuilder out, String name, String value) {
        out.append(name).append('=').append(value).append('\n');
    }

    private static String text(String value) {
        return ENCODER.encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String plain(String value) {
        return new String(DECODER.decode(value), StandardCharsets.UTF_8);
    }

    private static String instant(Instant value) {
        return value.getEpochSecond() + ":" + value.getNano();
    }

    private static Instant parsedInstant(String value) {
        String[] pieces = value.split(":", -1);
        if (pieces.length != 2) {
            throw new IllegalArgumentException("instant claim has the wrong shape");
        }
        return Instant.ofEpochSecond(Long.parseLong(pieces[0]), Integer.parseInt(pieces[1]));
    }

    private static TapstateException invalid(String reason, Throwable cause) {
        return new TapstateException(MonitorError.INVALID_CURSOR,
                Map.of("operation", "pipeline.metrics.history", "reason", reason), cause);
    }
}
