package io.tapstate.cli;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Pure keyboard reducer for the TUI command bar. Terminal escape sequences are decoded by the runtime,
 * while this class owns the state transitions that can be exercised without a terminal.
 */
final class TuiCommandBar {

    private static final Pattern ANSI_SEQUENCE = Pattern.compile(
            "\\u001B(?:\\[[0-?]*[ -/]*[@-~]|\\][^\\u0007]*(?:\\u0007|\\u001B\\\\))");

    static final int ENTER = 10;
    static final int CARRIAGE_RETURN = 13;
    static final int BACKSPACE = 8;
    static final int DELETE = 127;
    static final int ESCAPE = 27;
    static final int CTRL_C = 3;
    static final int CTRL_D = 4;
    static final int CTRL_P = 16;

    enum Event {
        NONE, SUBMIT, CANCEL, QUIT, PALETTE
    }

    record Update(String value, Event event) {
        Update {
            value = value == null ? "" : value;
            if (event == null) {
                event = Event.NONE;
            }
        }
    }

    record Palette(List<String> entries, int selectedIndex) {
        Palette {
            entries = normalizeEntries(entries);
            selectedIndex = clamp(selectedIndex, entries.size());
        }

        String selected() {
            return entries.isEmpty() ? "" : entries.get(selectedIndex);
        }

        Palette move(int delta) {
            return new Palette(entries, selectedIndex + delta);
        }

        Palette select(int index) {
            return new Palette(entries, index);
        }
    }

    record Completion(List<String> candidates, int selectedIndex) {
        Completion {
            candidates = normalizeEntries(candidates);
            selectedIndex = clamp(selectedIndex, candidates.size());
        }

        String selected() {
            return candidates.isEmpty() ? "" : candidates.get(selectedIndex);
        }

        Completion move(int delta) {
            return new Completion(candidates, selectedIndex + delta);
        }

        Completion select(int index) {
            return new Completion(candidates, index);
        }
    }

    record ResultPane(boolean keepRunning, int exitCode, boolean success, List<String> lines, String notice) {
        ResultPane {
            lines = List.copyOf(lines == null ? List.of() : lines);
            notice = notice == null ? "" : notice;
        }
    }

    static Palette palette(Collection<String> entries) {
        return new Palette(entries == null ? List.of() : new ArrayList<>(entries), 0);
    }

    /** Returns the commands exposed by the shared registry plus TUI-only commands. */
    static List<String> paletteCommands(CommandRegistry registry) {
        LinkedHashSet<String> commands = new LinkedHashSet<>();
        commands.addAll(List.of(":ctx", ":login", ":logout", ":help", ":quit"));
        if (registry != null) {
            commands.addAll(registry.commandLine().getSubcommands().keySet());
            commands.addAll(Repl.BUILTINS);
        }
        return normalizeEntries(commands);
    }

    /** Creates a safe operation model without retaining credentials in its description. */
    static TuiOperation operationFor(String line, long sequence) {
        String safeLine = safeDisplayText(line);
        String id = "tui-" + Math.max(1L, sequence);
        List<String> words = CommandInvocation.parse(line).words();
        String verb = words.isEmpty() ? "" : words.getFirst();
        if (isStream(words, verb)) {
            return TuiOperation.stream(id, safeLine);
        }
        if (isWrite(words, verb)) {
            return TuiOperation.write(id, safeLine);
        }
        return TuiOperation.command(id, safeLine.isEmpty() ? "command" : safeLine);
    }

    static Completion complete(TapstateCompleter completer, TuiCommandHistory history,
                               List<String> words, int wordIndex) {
        List<String> line = words == null ? List.of() : words;
        List<String> values = completer == null ? List.of() : completer.candidates(line, wordIndex);
        LinkedHashSet<String> merged = new LinkedHashSet<>(values == null ? List.of() : values);
        if (history != null && wordIndex == 0) {
            String prefix = wordIndex < line.size() ? line.get(wordIndex) : "";
            for (String entry : history.matches(prefix)) {
                int separator = entry.indexOf(' ');
                merged.add(separator < 0 ? entry : entry.substring(0, separator));
            }
        }
        return new Completion(List.copyOf(merged), 0);
    }

    /** Returns live suggestions for the current token, hiding the already-complete exact match. */
    static Completion suggestions(TapstateCompleter completer, TuiCommandHistory history,
                                  List<String> words, int wordIndex) {
        Completion completion = complete(completer, history, words, wordIndex);
        String current = words != null && wordIndex >= 0 && wordIndex < words.size()
                ? words.get(wordIndex) : "";
        return new Completion(completion.candidates().stream()
                .filter(candidate -> !candidate.equals(current)).toList(), 0);
    }

    static ResultPane project(CommandResult result, String output) {
        CommandResult safeResult = result == null ? new CommandResult(true, 0) : result;
        List<String> lines = safeDisplayLines(output);
        return new ResultPane(safeResult.keepRunning(), safeResult.exitCode(), safeResult.exitCode() == 0,
                lines, String.join(" ", lines));
    }

    static String safeDisplayText(String value) {
        return TuiActivity.result(stripTerminalControls(value));
    }

    private TuiCommandBar() {
    }

    static Update accept(String current, int code) {
        String value = current == null ? "" : current;
        if (code == CTRL_C) {
            return new Update(value, Event.CANCEL);
        }
        if (code == CTRL_D) {
            return new Update(value, value.isEmpty() ? Event.QUIT : Event.NONE);
        }
        if (code == CTRL_P) {
            return new Update(value, Event.PALETTE);
        }
        if (code == ENTER || code == CARRIAGE_RETURN) {
            return new Update(value, Event.SUBMIT);
        }
        if (code == ESCAPE) {
            return new Update("", Event.NONE);
        }
        if (code == BACKSPACE || code == DELETE) {
            if (value.isEmpty()) {
                return new Update("", Event.NONE);
            }
            int start = value.offsetByCodePoints(value.length(), -1);
            return new Update(value.substring(0, start), Event.NONE);
        }
        if (Character.isValidCodePoint(code) && code >= 32 && code != 127 && !Character.isISOControl(code)) {
            return new Update(value + new String(Character.toChars(code)), Event.NONE);
        }
        return new Update(value, Event.NONE);
    }

    private static List<String> safeDisplayLines(String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        List<String> lines = new ArrayList<>();
        for (String line : value.split("\\R")) {
            String safe = safeDisplayText(line);
            if (!safe.isEmpty()) {
                lines.add(safe);
            }
        }
        return List.copyOf(lines);
    }

    private static String stripTerminalControls(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        String stripped = ANSI_SEQUENCE.matcher(value).replaceAll("");
        StringBuilder safe = new StringBuilder(stripped.length());
        stripped.codePoints().forEach(codePoint -> {
            if (!Character.isISOControl(codePoint) || Character.isWhitespace(codePoint)) {
                safe.appendCodePoint(codePoint);
            } else {
                safe.append(' ');
            }
        });
        return safe.toString();
    }

    private static List<String> normalizeEntries(Collection<String> entries) {
        LinkedHashSet<String> normalized = new LinkedHashSet<>();
        if (entries != null) {
            for (String entry : entries) {
                if (entry != null && !entry.isBlank()) {
                    String value = entry.trim();
                    if (!value.isEmpty()) {
                        normalized.add(value);
                    }
                }
            }
        }
        return List.copyOf(normalized);
    }

    private static boolean isStream(List<String> words, String verb) {
        return Set.of("watch", "tail").contains(verb)
                || words.stream().skip(1).anyMatch(word -> "--watch".equals(word) || "--follow".equals(word));
    }

    private static boolean isWrite(List<String> words, String verb) {
        String action = verb;
        int separator = action.lastIndexOf('.');
        if (separator >= 0 && separator + 1 < action.length()) {
            action = action.substring(separator + 1);
        }
        if (Set.of("apply", "delete", "register", "start", "stop", "pause", "resume").contains(action)) {
            return true;
        }
        return "token".equals(verb) && words.stream().skip(1)
                .anyMatch(word -> "create".equals(word) || "revoke".equals(word));
    }

    private static int clamp(int index, int size) {
        if (size == 0) {
            return 0;
        }
        return Math.max(0, Math.min(index, size - 1));
    }
}
