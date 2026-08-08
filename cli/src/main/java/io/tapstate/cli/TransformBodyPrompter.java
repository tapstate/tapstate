package io.tapstate.cli;

import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.TransformBody;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds a transform's type-specific body from interactive answers, shared by the inline pipeline
 * flow and the standalone {@code kind: transform} wizard so an inline definition and a standalone one
 * ask identically. The body is pure logic — no {@code from:} wiring (X19); the caller supplies wiring
 * for an inline step, while a standalone definition has none. Every question routes through a
 * {@link Prompter}, never a terminal directly.
 */
final class TransformBodyPrompter {

    /** The transform type names, in menu order: streaming bodies first, then the multi-input ones. */
    static final List<String> TYPES = List.of("filter", "map", "js", "union", "nest", "join");

    /** The embed-loop sentinel that ends the (possibly nested) child-adding loop. */
    private static final String DONE = "(done)";

    private final Prompter prompter;

    TransformBodyPrompter(Prompter prompter) {
        this.prompter = prompter;
    }

    /**
     * Collect the body for {@code type}. Returns {@code null} only for a {@code map} with no fields:
     * a fields-less map cannot round-trip, so the caller drops the transform (inline) or re-asks the
     * type (standalone). Every other type always yields a body.
     */
    TransformBody askBody(String type) {
        return switch (type) {
            case "filter" -> new TransformBody.Filter(prompter.ask("Filter expression (CEL)", null));
            case "map" -> askMap();
            case "js" -> new TransformBody.Js(blockText(prompter.lines("Script (JS)")));
            case "union" -> new TransformBody.Union();
            case "nest" -> askNest();
            case "join" -> askJoin();
            default -> throw new IllegalStateException("unhandled transform type: " + type);
        };
    }

    /** Collect map projection fields in declared (output) order; a field-less map is skipped (returns null). */
    private TransformBody askMap() {
        Map<String, FieldRule> fields = new LinkedHashMap<>();
        while (true) {
            String name = prompter.ask("Output field (blank to finish)", null);
            if (name == null || name.isBlank()) {
                return fields.isEmpty() ? null : new TransformBody.MapProjection(fields);
            }
            String rule = prompter.ask("Rule for '" + name + "' ($src, =expr, false, or a literal)", null);
            fields.put(name, toFieldRule(rule));
        }
    }

    private static FieldRule toFieldRule(String rule) {
        String r = rule == null ? "" : rule.trim();
        if (r.equals("false")) {
            return FieldRule.drop();
        }
        if (r.startsWith("$")) {
            return FieldRule.rename(r.substring(1));
        }
        if (r.startsWith("=")) {
            return FieldRule.computed(r.substring(1));
        }
        return FieldRule.literal(r);
    }

    /** A flat join over duckdb: an engine (default duckdb) and a multi-line SQL block. */
    private TransformBody askJoin() {
        String engine = prompter.ask("Join engine", "duckdb");
        engine = engine == null || engine.isBlank() ? "duckdb" : engine.trim();
        return new TransformBody.Join(engine, blockText(prompter.lines("Join SQL")));
    }

    /**
     * A nest tree: the root parent alias and upsert key, then a (possibly nested) list of embedded
     * children. Aliases name the use-site {@code from:} map and stay abstract in a definition (X19);
     * the second tier (root.mode / order / ignoreUpdates / trackKeyChanges) is authored by hand.
     */
    private TransformBody askNest() {
        String from = prompter.ask("Nest root: parent stream alias", null);
        List<String> key = askKeyList("Root upsert key (comma-separated, blank for none)");
        List<Embed> embed = askEmbeds();
        return new TransformBody.Nest(null, null, new NestRoot(from, key, null, null, embed));
    }

    /** Zero or more embedded children, each of which may carry its own nested children (recursive). */
    private List<Embed> askEmbeds() {
        List<Embed> embeds = new ArrayList<>();
        while (!DONE.equals(prompter.choose("Add a child embed?", List.of("embed", DONE)))) {
            embeds.add(askEmbed());
        }
        return embeds.isEmpty() ? null : embeds;
    }

    private Embed askEmbed() {
        String from = prompter.ask("Embed child alias", null);
        Map<String, String> on = askOn();
        EmbedAs as = askEmbedAs();
        String path = prompter.ask("Embed path (field under parent)", null);
        List<String> arrayKey = as == EmbedAs.ARRAY
                ? askKeyList("Array key (comma-separated, blank for none)")
                : null;
        return new Embed(from, on, as, path, arrayKey, null, null, askEmbeds());
    }

    /** The child-to-parent join field map; a blank child field ends the list. */
    private Map<String, String> askOn() {
        Map<String, String> on = new LinkedHashMap<>();
        while (true) {
            String child = prompter.ask("Embed join field — child side (blank to finish)", null);
            if (child == null || child.isBlank()) {
                return on;
            }
            String parent = prompter.ask("  matches parent field", null);
            on.put(child.trim(), parent == null ? "" : parent.trim());
        }
    }

    /** Array is the canonical default, listed last so an empty reply selects it. */
    private EmbedAs askEmbedAs() {
        String chosen = prompter.choose("Embed as?", List.of("object", "array"));
        for (EmbedAs a : EmbedAs.values()) {
            if (a.yaml().equals(chosen)) {
                return a;
            }
        }
        return EmbedAs.ARRAY; // unreachable: choose returns one of the offered options
    }

    /** A comma-separated key list; blank (or token-less) means none. */
    private List<String> askKeyList(String question) {
        String answer = prompter.ask(question, null);
        if (answer == null || answer.isBlank()) {
            return null;
        }
        List<String> keys = new ArrayList<>();
        for (String token : answer.split(",")) {
            String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                keys.add(trimmed);
            }
        }
        return keys.isEmpty() ? null : keys;
    }

    /** A clean {@code script: |} / {@code sql: |} block carries exactly one trailing newline. */
    private static String blockText(String answer) {
        String s = answer == null ? "" : answer;
        return s.endsWith("\n") ? s : s + "\n";
    }
}
