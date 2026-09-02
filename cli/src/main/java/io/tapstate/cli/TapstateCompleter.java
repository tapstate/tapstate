package io.tapstate.cli;

import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.OfficialConnectors;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.schema.SchemaNavigator;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.FileNameCompleter;
import picocli.CommandLine;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

/**
 * Tab completion for the offline REPL. The first word completes to a verb; after {@code explain} the
 * argument completes to grammar field paths (fed by the schema), and after {@code validate} to
 * filesystem paths. The {@code new} verb's catalog-backed options complete too: {@code --connector}
 * to catalog connector ids, {@code --mode} to the read modes the connector on the line allows (the
 * capability matrix), {@code --kind} to the scaffoldable kinds, and {@code --type} to the transform
 * types. Other verbs offer no argument completion in this build.
 *
 * <p>{@link #candidates} is the pure, testable seam; {@link #complete} adapts it to JLine and
 * delegates the {@code validate} path argument to JLine's filesystem completer.
 */
final class TapstateCompleter implements Completer {

    private final List<String> verbs;
    private final SchemaNavigator schema;
    private final TapstateCatalog catalog;
    private final FileNameCompleter files = new FileNameCompleter();

    TapstateCompleter(Collection<String> verbs, SchemaNavigator schema, TapstateCatalog catalog) {
        this.verbs = new TreeSet<>(verbs).stream().toList();   // sorted, de-duplicated
        this.schema = schema;
        this.catalog = catalog;
    }

    /** Builds the completer for a REPL command table: registered subcommands plus the REPL builtins. */
    static TapstateCompleter forRepl(CommandLine commandLine, SchemaNavigator schema) {
        return CommandRegistry.forCommandLine(commandLine, schema).completer();
    }

    /** Options that consume a following value token (so completion skips past the value). */
    private static final List<String> VALUE_OPTIONS = List.of("-o", "--output");

    /**
     * The completion strings for a word position, by context. Word 0 completes verbs (filtered by
     * what is typed); a {@code new} option value completes from the catalog; the first positional
     * argument after {@code explain} completes grammar field paths, regardless of any options typed
     * before it. Everything else returns nothing here ({@code validate}'s filesystem completion is
     * handled in {@link #complete}).
     */
    List<String> candidates(List<String> words, int wordIndex) {
        String current = wordIndex >= 0 && wordIndex < words.size() ? words.get(wordIndex) : "";
        if (wordIndex == 0) {
            return verbs.stream().filter(v -> v.startsWith(current)).toList();
        }
        String verb = words.isEmpty() ? "" : words.get(0);
        if ("new".equals(verb)) {
            List<String> values = newOptionValues(words, wordIndex);
            if (values != null) {
                return values.stream().filter(v -> v.startsWith(current)).toList();
            }
        }
        if ("context".equals(verb) && wordIndex == 1) {
            return List.of("list")
                    .stream().filter(value -> value.startsWith(current)).toList();
        }
        if ("explain".equals(verb) && isFirstPositional(words, wordIndex)) {
            return schema.complete(current);
        }
        return List.of();
    }

    /**
     * The candidate values for the {@code new} option immediately to the left of the word being
     * completed, or {@code null} when the slot is not one of {@code new}'s catalog-backed options.
     * {@code --mode} is narrowed to the modes allowed by the {@code --connector} already on the line.
     */
    private List<String> newOptionValues(List<String> words, int wordIndex) {
        if (wordIndex < 1 || wordIndex - 1 >= words.size()) {
            return null;
        }
        String prev = words.get(wordIndex - 1);
        if (isOpt(prev, "-c", "--connector")) {
            return OfficialConnectors.presentIn(catalog);
        }
        if (isOpt(prev, "-m", "--mode")) {
            return modeCandidates(words);
        }
        if (isOpt(prev, "--kind")) {
            return List.of("pipeline", "serve", "source", "transform", "view");
        }
        if (isOpt(prev, "--type")) {
            return TransformBodyPrompter.TYPES;
        }
        return null;
    }

    /** Read modes allowed for the connector on the line; every mode when no trustworthy one is named. */
    private List<String> modeCandidates(List<String> words) {
        String connector = optionValue(words, "-c", "--connector");
        if (connector != null && catalog.ids().contains(connector)) {
            ConnectorCatalogEntry entry = catalog.byId(connector);
            if (entry.modesAreTrustworthy() && !entry.modes().isEmpty()) {
                return entry.modes().stream().map(SourceMode::yaml).toList();
            }
        }
        return Arrays.stream(SourceMode.values()).map(SourceMode::yaml).toList();
    }

    private static boolean isOpt(String word, String... names) {
        for (String name : names) {
            if (name.equals(word)) {
                return true;
            }
        }
        return false;
    }

    /** The token following the first occurrence of any of {@code names} on the line, or null. */
    private static String optionValue(List<String> words, String... names) {
        for (int i = 1; i < words.size() - 1; i++) {
            if (isOpt(words.get(i), names)) {
                return words.get(i + 1);
            }
        }
        return null;
    }

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> out) {
        List<String> words = line.words();
        int wordIndex = line.wordIndex();
        String verb = words.isEmpty() ? "" : words.get(0);
        if ("validate".equals(verb) && isFirstPositional(words, wordIndex)) {
            files.complete(reader, line, out);
            return;
        }
        for (String value : candidates(words, wordIndex)) {
            out.add(new Candidate(value));
        }
    }

    /**
     * Whether the word at {@code wordIndex} is the verb's first positional argument — true only when
     * no positional word precedes it. Option flags (and the value token of a value-taking option)
     * before it are skipped; the current word is not a positional if it is an option flag itself or
     * the value of a preceding value-taking option.
     */
    private static boolean isFirstPositional(List<String> words, int wordIndex) {
        if (wordIndex <= 0) {
            return false;
        }
        String current = wordIndex < words.size() ? words.get(wordIndex) : "";
        if (current.startsWith("-")) {
            return false;   // completing an option flag, not a positional
        }
        if (VALUE_OPTIONS.contains(words.get(wordIndex - 1))) {
            return false;   // completing the value of a value-taking option
        }
        int positionalsBefore = 0;
        for (int i = 1; i < wordIndex; i++) {
            String word = words.get(i);
            if (word.startsWith("-")) {
                if (VALUE_OPTIONS.contains(word)) {
                    i++;    // skip the option's separate value token
                }
                continue;
            }
            positionalsBefore++;
        }
        return positionalsBefore == 0;
    }
}
