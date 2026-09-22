package io.tapstate.messages;

import io.tapstate.core.common.TapstateErrorCode;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The error-code message catalog — the presentation-layer renderer. It turns a coded exception
 * (canonical code + named arguments) into a user-facing diagnostic by looking the code up in the
 * bundled {@code messages/<locale>.yml} catalog and substituting named {@code {name}} placeholders
 * from the arguments. A code with no catalog entry falls back to its bare canonical code, so a
 * missing entry degrades to the machine-stable identity rather than failing.
 *
 * <p>This is a shared module so every presentation face renders coded errors the same way: the
 * offline CLI and the service assembly root both depend on it. It depends only on the core ring
 * (the error-code contract) and carries no third-party dependency.
 *
 * <p>The catalog is read with a small purpose-built reader rather than a YAML library, so the
 * presentation faces that depend on it carry no YAML dependency, and the catalog is a controlled,
 * fixed-shape file we own. The reader accepts exactly two levels — a top-level {@code <code>:} key,
 * then {@code message} and optional {@code solution} double-quoted scalars — and fails loudly on
 * anything else, since a malformed catalog is a build asset defect, not a user input.
 */
public final class MessageCatalog {

    /** A rendered diagnostic: the primary user-facing message and an optional next-step hint. */
    public record Rendered(String message, String solution) {
    }

    /** A raw catalog entry: the message template and an optional solution template. */
    record Entry(String message, String solution) {
    }

    private final Map<String, Entry> entries;

    private MessageCatalog(Map<String, Entry> entries) {
        this.entries = entries;
    }

    /** Loads the bundled {@code en} catalog (the mandatory locale). */
    public static MessageCatalog bundled() {
        return fromResource("/messages/en.yml");
    }

    /** Loads another bundled catalog with the same strict two-level shape. */
    static MessageCatalog fromResource(String resource) {
        return new MessageCatalog(parse(read(resource)));
    }

    /** Renders the code's message and solution, substituting named placeholders from the args. */
    public Rendered render(TapstateErrorCode code, Map<String, Object> args) {
        return render(code.code(), args);
    }

    /**
     * Renders a canonical code in its String form — for a code that reached this process as data (read back
     * from a store, or carried over the wire) and so has no enum constant in hand. The catalog is keyed by
     * that same string, so this is a plain lookup: no reflection, and no enum is reconstructed. An unknown
     * code renders as itself rather than blank, so a message never silently disappears.
     */
    public Rendered render(String code, Map<String, Object> args) {
        Entry entry = entries.get(code);
        if (entry == null) {
            return new Rendered(code, null);
        }
        String solution = entry.solution() == null ? null : substitute(entry.solution(), args);
        return new Rendered(substitute(entry.message(), args), solution);
    }

    /** Replaces each {@code {name}} token with its argument; an unbound name is left verbatim. */
    static String substitute(String template, Map<String, Object> args) {
        StringBuilder sb = new StringBuilder(template.length() + 16);
        int i = 0;
        while (i < template.length()) {
            char c = template.charAt(i);
            if (c == '{') {
                int close = template.indexOf('}', i + 1);
                if (close > i) {
                    String name = template.substring(i + 1, close);
                    if (args.containsKey(name)) {
                        sb.append(String.valueOf(args.get(name)));
                    } else {
                        sb.append('{').append(name).append('}');
                    }
                    i = close + 1;
                    continue;
                }
            }
            sb.append(c);
            i++;
        }
        return sb.toString();
    }

    static Map<String, Entry> parse(String yaml) {
        Map<String, String> messages = new LinkedHashMap<>();
        Map<String, String> solutions = new LinkedHashMap<>();
        String code = null;
        int lineNo = 0;
        for (String raw : yaml.split("\n", -1)) {
            lineNo++;
            String line = raw.endsWith("\r") ? raw.substring(0, raw.length() - 1) : raw;
            String trimmed = line.strip();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                continue;
            }
            if (line.startsWith(" ")) {
                if (code == null) {
                    throw new IllegalStateException("catalog entry field before any code, line " + lineNo);
                }
                int colon = trimmed.indexOf(':');
                if (colon < 0) {
                    throw new IllegalStateException("expected '<field>: \"...\"', line " + lineNo);
                }
                String field = trimmed.substring(0, colon).strip();
                String value = unquote(trimmed.substring(colon + 1).strip(), lineNo);
                switch (field) {
                    case "message" -> messages.put(code, value);
                    case "solution" -> solutions.put(code, value);
                    default -> throw new IllegalStateException("unknown catalog field '" + field + "', line " + lineNo);
                }
            } else {
                if (!trimmed.endsWith(":")) {
                    throw new IllegalStateException("expected '<code>:', line " + lineNo);
                }
                code = trimmed.substring(0, trimmed.length() - 1).strip();
            }
        }
        Map<String, Entry> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> message : messages.entrySet()) {
            out.put(message.getKey(), new Entry(message.getValue(), solutions.get(message.getKey())));
        }
        return out;
    }

    /** Unwraps a double-quoted scalar, honouring {@code \"} and {@code \\} escapes. */
    private static String unquote(String token, int lineNo) {
        if (token.length() < 2 || token.charAt(0) != '"') {
            throw new IllegalStateException("expected a double-quoted value, line " + lineNo);
        }
        StringBuilder sb = new StringBuilder(token.length());
        boolean closed = false;
        for (int i = 1; i < token.length(); i++) {
            char c = token.charAt(i);
            if (c == '\\' && i + 1 < token.length()) {
                sb.append(token.charAt(++i));
            } else if (c == '"') {
                closed = true;
                break;
            } else {
                sb.append(c);
            }
        }
        if (!closed) {
            throw new IllegalStateException("unterminated quoted value, line " + lineNo);
        }
        return sb.toString();
    }

    private static String read(String resource) {
        try (InputStream in = MessageCatalog.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("bundled message catalog not found on the classpath: " + resource);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
