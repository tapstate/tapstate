package io.tapstate.cli;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.messages.MessageCatalog;
import picocli.CommandLine.Help.Ansi;

import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Renders a coded diagnostic into the CLI's stable structured shape — the single source of truth for
 * the {@code {code, severity, message, solution?, source?, line?, column?, params?}} envelope the
 * offline verbs emit ({@code validate}, {@code desc}, {@code new}). The message and solution come from
 * the bundled message catalog; {@code params} carries the named arguments sorted for a stable machine
 * contract regardless of throw-site order, and the location fields appear only when known.
 */
final class Diagnostics {

    /** What the pair reads as with no session in hand -- every one-shot offline command is here. */
    static final String OFFLINE_VERSIONS = "cli " + Cli.VERSION_NUMBER + ", server not connected";

    private Diagnostics() {
    }

    /**
     * Renders a coded diagnostic to an error stream in the CLI's stable text shape — a bold-red
     * {@code error: <code>} header, then the catalog message, then the solution hint when the catalog
     * carries one. This is the one text renderer every face shares, so a coded diagnostic reads
     * identically whether an offline verb ({@code desc}, {@code connect}) or a connected online verb
     * raised it; the message and solution come from the same bundled catalog as the structured form.
     */
    static void printText(PrintWriter err, TapstateErrorCode code, Map<String, Object> args) {
        printText(err, code, args, OFFLINE_VERSIONS);
    }

    /**
     * The same, stamped with which two builds produced it. Every reported problem carries the pair so
     * that pasting the error is enough -- a reader does not have to know that the CLI and the server are
     * separate installs, let alone that they can differ, and the question "which version are you on" has
     * already been answered before anyone asks it. Uniform rather than judged per code: deciding which
     * diagnostics look version-related is a judgement that would have to be made again for every code
     * ever added, and the ones that get it wrong are exactly the surprising ones.
     */
    static void printText(
            PrintWriter err, TapstateErrorCode code, Map<String, Object> args, String versions) {
        MessageCatalog.Rendered rendered = MessageCatalog.bundled().render(code, args);
        err.println(Ansi.AUTO.string("@|bold,red error:|@") + " " + code.code());
        err.println("  " + rendered.message());
        if (rendered.solution() != null) {
            err.println("  " + rendered.solution());
        }
        err.println("  (" + versions + ")");
        err.flush();
    }

    /** One coded diagnostic as a stable, machine-readable map. */
    static Map<String, Object> map(TapstateErrorCode code, Map<String, Object> args, String source, int line, int column) {
        MessageCatalog.Rendered rendered = MessageCatalog.bundled().render(code, args);
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("code", code.code());
        d.put("severity", code.severity().name());
        d.put("message", rendered.message());
        if (rendered.solution() != null) {
            d.put("solution", rendered.solution());
        }
        if (source != null) {
            d.put("source", source);
        }
        if (line > 0) {
            d.put("line", line);
        }
        if (column > 0) {
            d.put("column", column);
        }
        if (args != null && !args.isEmpty()) {
            d.put("params", new TreeMap<>(args));   // sorted for a stable machine contract
        }
        return d;
    }
}
