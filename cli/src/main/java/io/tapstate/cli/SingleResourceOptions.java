package io.tapstate.cli;

import io.tapstate.core.model.SourceMode;
import picocli.CommandLine.Option;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Options shared by {@code add KIND} and the deprecated {@code new --kind KIND} compatibility path. */
abstract class SingleResourceOptions {

    @Option(names = {"-y", "--yes", "--non-interactive"},
            description = "Never prompt; take every answer from flags (scripting / AI).")
    boolean nonInteractive;

    @Option(names = "--type", paramLabel = "TYPE",
            description = "Transform type: filter, map, js, union, nest or join.")
    String type;

    @Option(names = {"-c", "--connector"}, paramLabel = "ID",
            description = "Connector id from the catalog (source kind).")
    String connector;

    @Option(names = "--id", paramLabel = "ID",
            description = "Top-level id of the artifact.")
    String id;

    @Option(names = {"-m", "--mode"}, paramLabel = "MODE",
            description = "Source read mode (cdc, snapshot, stream, file, api) — must suit the connector.")
    SourceMode mode;

    @Option(names = "--primary-key", paramLabel = "FIELD",
            description = "Field that uniquely identifies a record in a view - required for view kind.")
    String primaryKey;

    @Option(names = "--set", paramLabel = "KEY=VALUE",
            description = "A connector config entry (repeatable).")
    Map<String, String> config = new LinkedHashMap<>();

    @Option(names = "--source", paramLabel = "ID",
            description = "Source id the pipeline reads from (repeatable).")
    List<String> sources = new ArrayList<>();

    @Option(names = "--sync-to", paramLabel = "ID",
            description = "Target source id to sync the pipeline output to (repeatable).")
    List<String> syncTo = new ArrayList<>();

    @Option(names = "--out", paramLabel = "DIR",
            description = "Write the artifact flat into this exact directory, bypassing the workspace layout.")
    String out;

    @Option(names = "--force",
            description = "Overwrite an existing artifact at the target path.")
    boolean force;

    @Option(names = "--dry-run",
            description = "Preview the canonical artifact on stdout without writing any file.")
    boolean dryRun;

    @Option(names = {"-o", "--output"}, paramLabel = "FORMAT",
            description = "Output format for the result report: text, json or yaml (default: text).",
            defaultValue = "text", completionCandidates = OutputFormat.Candidates.class)
    OutputFormat output;
}
