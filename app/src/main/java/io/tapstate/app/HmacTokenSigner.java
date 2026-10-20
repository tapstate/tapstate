package io.tapstate.app;

import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TokenSigner;
import io.tapstate.control.core.VerifiedToken;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * An HMAC-SHA256 session token signer. A token is three base64url segments joined by dots —
 * {@code header.claims.signature} — where the signature is {@code HMAC-SHA256(secret, header.claims)}.
 * The claims bind the subject and its capability grade to an issue time and a bounded expiry.
 *
 * <p>The token is signed, not encrypted: its content is readable, but it cannot be forged or altered
 * without the secret. {@link #verify} recomputes the signature and compares it in constant time
 * (never a short-circuiting equality, which would leak the signature byte by byte through timing),
 * then rejects an expired token against the injected clock. Any token that fails a check — bad
 * signature, expired, or malformed — yields an empty result rather than an exception, so a bad token
 * is simply unauthenticated.
 */
public final class HmacTokenSigner implements TokenSigner {

    /** The default token lifetime when a caller does not choose one: short, to bound replay. */
    public static final Duration DEFAULT_TTL = Duration.ofMinutes(15);

    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final String HEADER_JSON = "{\"alg\":\"HS256\",\"typ\":\"CYX\"}";
    private static final Base64.Encoder B64_URL = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder B64_URL_DEC = Base64.getUrlDecoder();

    private final byte[] secret;
    private final Duration ttl;
    private final Clock clock;
    private final io.tapstate.spi.store.CacheAuthScope authScope;

    public HmacTokenSigner(byte[] secret, Duration ttl, Clock clock) {
        this(secret, ttl, clock, null);
    }

    /** A formal OP issuer binds access tokens to its original durable authentication identity. */
    public HmacTokenSigner(byte[] secret, Duration ttl, Clock clock, io.tapstate.spi.store.CacheAuthScope authScope) {
        Objects.requireNonNull(secret, "secret");
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.authScope = authScope;
        if (authScope != null && authScope.mode() != io.tapstate.spi.store.StorageBinding.Mode.OP)
            throw new IllegalArgumentException("the local token issuer requires an OP authentication scope");
        if (secret.length == 0) {
            throw new IllegalArgumentException("token signing secret must not be empty");
        }
        this.secret = secret.clone();
    }

    @Override
    public String issue(String subject, Scope scope) {
        Objects.requireNonNull(subject, "subject");
        Objects.requireNonNull(scope, "scope");
        long iat = clock.instant().getEpochSecond();
        long exp = iat + ttl.getSeconds();
        String header = encode(HEADER_JSON);
        String claims = encode(claimsJson(subject, scope, iat, exp));
        String signingInput = header + "." + claims;
        String signature = B64_URL.encodeToString(hmac(signingInput));
        return signingInput + "." + signature;
    }

    @Override
    public Optional<VerifiedToken> verify(String token) {
        if (token == null) {
            return Optional.empty();
        }
        String[] parts = token.split("\\.", -1);
        if (parts.length != 3) {
            return Optional.empty();
        }
        String signingInput = parts[0] + "." + parts[1];
        byte[] expected = hmac(signingInput);
        byte[] presented;
        try {
            presented = B64_URL_DEC.decode(parts[2]);
        } catch (IllegalArgumentException malformedSignature) {
            return Optional.empty();
        }
        // Constant-time comparison: a byte-by-byte short-circuit would leak the correct signature
        // through timing. A mismatch means the token was forged or altered — reject it.
        if (!MessageDigest.isEqual(expected, presented)) {
            return Optional.empty();
        }
        // The signature is authentic, so the claims are the ones this signer wrote; parsing is still
        // guarded so a corrupt-but-somehow-authentic body cannot crash the caller.
        try {
            Map<String, String> claims = parseFlatObject(
                    new String(B64_URL_DEC.decode(parts[1]), StandardCharsets.UTF_8));
            String subject = claims.get("sub");
            String scopeName = claims.get("scope");
            String expText = claims.get("exp");
            if (!matchesScope(claims)) return Optional.empty();
            if (subject == null || scopeName == null || expText == null) {
                return Optional.empty();
            }
            long exp = Long.parseLong(expText);
            if (clock.instant().getEpochSecond() >= exp) {
                return Optional.empty();
            }
            return Optional.of(new VerifiedToken(subject, Scope.valueOf(scopeName)));
        } catch (RuntimeException malformedClaims) {
            return Optional.empty();
        }
    }

    private byte[] hmac(String data) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(secret, HMAC_ALGORITHM));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException unavailable) {
            // HMAC-SHA256 and a non-empty key are both guaranteed here; a failure is a broken JVM
            // invariant, not a user-facing condition, so it crashes bare rather than being coded.
            throw new IllegalStateException("HMAC-SHA256 unavailable", unavailable);
        }
    }

    private static String encode(String json) {
        return B64_URL.encodeToString(json.getBytes(StandardCharsets.UTF_8));
    }

    /** Builds the flat claims object; the subject is JSON-escaped, the rest are safe literals. */
    private String claimsJson(String subject, Scope scope, long iat, long exp) {
        return "{\"sub\":" + jsonString(subject)
                + ",\"scope\":" + jsonString(scope.name())
                + ",\"iat\":" + iat
                + ",\"exp\":" + exp + scopeClaims() + "}";
    }

    private String scopeClaims() {
        if (authScope == null) return "";
        return ",\"clusterId\":" + jsonString(authScope.clusterId())
                + ",\"bindingVersion\":" + authScope.bindingVersion()
                + ",\"cacheIncarnation\":" + jsonString(authScope.cacheIncarnation())
                + ",\"authGeneration\":" + authScope.authGeneration();
    }

    private boolean matchesScope(Map<String,String> claims) {
        if (authScope == null) return java.util.Collections.disjoint(claims.keySet(),
                java.util.Set.of("clusterId", "bindingVersion", "cacheIncarnation", "authGeneration"));
        return authScope.clusterId().equals(claims.get("clusterId"))
                && Long.toString(authScope.bindingVersion()).equals(claims.get("bindingVersion"))
                && authScope.cacheIncarnation().equals(claims.get("cacheIncarnation"))
                && Long.toString(authScope.authGeneration()).equals(claims.get("authGeneration"));
    }

    /** Writes a JSON string literal, escaping the quote, backslash and control characters. */
    private static String jsonString(String value) {
        StringBuilder sb = new StringBuilder(value.length() + 2).append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append("\\u").append(hex4(c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    private static String hex4(char c) {
        String h = Integer.toHexString(c);
        return "0000".substring(h.length()) + h;
    }

    /**
     * Parses a flat JSON object whose values are strings or numbers (the shape this signer emits) into
     * a name -> raw-text map: a string value is unescaped, a number value is kept as its digits. It
     * accepts no nesting because the claims carry none; anything malformed throws, and the caller
     * treats that as an unverifiable token.
     */
    private static Map<String, String> parseFlatObject(String json) {
        Map<String, String> out = new LinkedHashMap<>();
        Cursor c = new Cursor(json);
        c.skipWhitespace();
        c.expect('{');
        c.skipWhitespace();
        if (c.peek() == '}') {
            c.next();
            return out;
        }
        while (true) {
            c.skipWhitespace();
            String key = c.readString();
            c.skipWhitespace();
            c.expect(':');
            c.skipWhitespace();
            String value = c.peek() == '"' ? c.readString() : c.readNumber();
            out.put(key, value);
            c.skipWhitespace();
            char sep = c.next();
            if (sep == ',') {
                continue;
            }
            if (sep == '}') {
                return out;
            }
            throw new IllegalArgumentException("expected ',' or '}' in claims");
        }
    }

    /** A minimal forward cursor over the claims text, enough for a flat string/number object. */
    private static final class Cursor {
        private final String s;
        private int i;

        Cursor(String s) {
            this.s = s;
        }

        char peek() {
            if (i >= s.length()) {
                throw new IllegalArgumentException("unexpected end of claims");
            }
            return s.charAt(i);
        }

        char next() {
            char c = peek();
            i++;
            return c;
        }

        void expect(char expected) {
            if (next() != expected) {
                throw new IllegalArgumentException("expected '" + expected + "' in claims");
            }
        }

        void skipWhitespace() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                i++;
            }
        }

        String readString() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (true) {
                char c = next();
                if (c == '"') {
                    return sb.toString();
                }
                if (c == '\\') {
                    char esc = next();
                    switch (esc) {
                        case '"' -> sb.append('"');
                        case '\\' -> sb.append('\\');
                        case '/' -> sb.append('/');
                        case 'n' -> sb.append('\n');
                        case 'r' -> sb.append('\r');
                        case 't' -> sb.append('\t');
                        case 'b' -> sb.append('\b');
                        case 'f' -> sb.append('\f');
                        case 'u' -> {
                            String hex = s.substring(i, i + 4);
                            i += 4;
                            sb.append((char) Integer.parseInt(hex, 16));
                        }
                        default -> throw new IllegalArgumentException("invalid escape in claims");
                    }
                } else {
                    sb.append(c);
                }
            }
        }

        String readNumber() {
            int start = i;
            while (i < s.length() && "-+.0123456789eE".indexOf(s.charAt(i)) >= 0) {
                i++;
            }
            if (i == start) {
                throw new IllegalArgumentException("expected a number in claims");
            }
            return s.substring(start, i);
        }
    }
}
