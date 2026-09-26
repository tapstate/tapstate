package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.PipelineEventStore.Key;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;

/** Stateless, signed continuation for one eventually consistent event query. */
public final class EventsCursorCodec {

    public static final Duration DEFAULT_TTL = Duration.ofMinutes(10);

    private static final String OPERATION = "pipeline.events";
    private static final byte[] CONTEXT = "events-cursor-v1\0".getBytes(StandardCharsets.UTF_8);
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    public record QueryBinding(String pipelineId, String incarnationId, Instant from, Instant to, int limit) {
        public QueryBinding {
            Objects.requireNonNull(pipelineId, "pipelineId");
            Objects.requireNonNull(incarnationId, "incarnationId");
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(to, "to");
        }
    }

    public record State(QueryBinding binding, Instant effectiveFrom, Instant effectiveTo,
            Instant retentionCutoff, Key after, Instant issuedAt, Instant expiresAt) {
    }

    private final byte[] secret;
    private final Clock clock;
    private final Duration ttl;

    public EventsCursorCodec(byte[] secret, Clock clock) {
        this(secret, DEFAULT_TTL, clock);
    }

    public EventsCursorCodec(byte[] secret, Duration ttl, Clock clock) {
        Objects.requireNonNull(secret, "secret");
        if (secret.length == 0) {
            throw new IllegalArgumentException("an events cursor signing secret is not empty");
        }
        this.secret = secret.clone();
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("an events cursor lifetime is positive");
        }
    }

    public String issue(QueryBinding binding, Instant effectiveFrom, Instant effectiveTo,
            Instant retentionCutoff, Key after) {
        Instant issuedAt = clock.instant();
        State state = new State(binding, effectiveFrom, effectiveTo, retentionCutoff,
                after, issuedAt, issuedAt.plus(ttl));
        byte[] claims = encode(state);
        return ENCODER.encodeToString(claims) + "." + ENCODER.encodeToString(sign(claims));
    }

    public State read(String token, QueryBinding expected) {
        Objects.requireNonNull(expected, "expected");
        if (token == null || token.length() > 4096 || token.isBlank()) {
            throw invalid("MALFORMED", null);
        }
        String[] parts = token.split("\\.", -1);
        if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
            throw invalid("MALFORMED", null);
        }
        byte[] claims;
        byte[] signature;
        try {
            claims = DECODER.decode(parts[0]);
            signature = DECODER.decode(parts[1]);
        } catch (IllegalArgumentException malformed) {
            throw invalid("MALFORMED", malformed);
        }
        if (!MessageDigest.isEqual(sign(claims), signature)) {
            throw invalid("TAMPERED", null);
        }
        State state;
        try {
            state = decode(claims);
        } catch (IOException | RuntimeException malformed) {
            throw invalid("MALFORMED", malformed);
        }
        if (!clock.instant().isBefore(state.expiresAt())) {
            throw new TapstateException(MonitorError.CURSOR_EXPIRED, Map.of("operation", OPERATION), null);
        }
        if (!state.binding().equals(expected)) {
            throw invalid("QUERY_MISMATCH", null);
        }
        return state;
    }

    private static byte[] encode(State state) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeByte(1);
            out.writeUTF(OPERATION);
            out.writeUTF(state.binding().pipelineId());
            out.writeUTF(state.binding().incarnationId());
            instant(out, state.binding().from());
            instant(out, state.binding().to());
            out.writeInt(state.binding().limit());
            instant(out, state.effectiveFrom());
            instant(out, state.effectiveTo());
            instant(out, state.retentionCutoff());
            instant(out, state.after().occurredAt());
            out.writeUTF(state.after().id());
            instant(out, state.issuedAt());
            instant(out, state.expiresAt());
            return bytes.toByteArray();
        } catch (IOException malformed) {
            throw new IllegalArgumentException("events cursor values are too large", malformed);
        }
    }

    private static State decode(byte[] claims) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(claims));
        if (in.readUnsignedByte() != 1 || !OPERATION.equals(in.readUTF())) {
            throw new IllegalArgumentException("unsupported events cursor");
        }
        String pipeline = in.readUTF();
        String incarnation = in.readUTF();
        Instant from = instant(in);
        Instant to = instant(in);
        int limit = in.readInt();
        QueryBinding binding = new QueryBinding(pipeline, incarnation, from, to, limit);
        Instant effectiveFrom = instant(in);
        Instant effectiveTo = instant(in);
        Instant cutoff = instant(in);
        Key after = new Key(instant(in), in.readUTF());
        Instant issued = instant(in);
        Instant expires = instant(in);
        if (in.available() != 0 || !issued.isBefore(expires)
                || effectiveFrom.isAfter(effectiveTo) || after.occurredAt().isBefore(effectiveFrom)
                || !after.occurredAt().isBefore(effectiveTo)) {
            throw new IllegalArgumentException("invalid events cursor bounds");
        }
        return new State(binding, effectiveFrom, effectiveTo, cutoff, after, issued, expires);
    }

    private static void instant(DataOutputStream out, Instant value) throws IOException {
        out.writeLong(value.getEpochSecond());
        out.writeInt(value.getNano());
    }

    private static Instant instant(DataInputStream in) throws IOException {
        return Instant.ofEpochSecond(in.readLong(), in.readInt());
    }

    private byte[] sign(byte[] claims) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            mac.update(CONTEXT);
            return mac.doFinal(claims);
        } catch (GeneralSecurityException unavailable) {
            throw new IllegalStateException("HMAC-SHA256 unavailable", unavailable);
        }
    }

    private static TapstateException invalid(String reason, Throwable cause) {
        return new TapstateException(MonitorError.INVALID_CURSOR,
                Map.of("operation", OPERATION, "reason", reason), cause);
    }
}
