package io.tapstate.cli;

import java.util.List;

/**
 * One derived step as read back from the server: which step, which table it writes into, whether that
 * table's own columns could be read at all, and the per-column comparison.
 *
 * <p>{@code targetKnown} is carried rather than inferred from an empty target column. A target nobody
 * has discovered and a target that happens to hold none of these columns render the same way otherwise,
 * and only one of them means "go and look" - which is why the flag is on the wire.
 *
 * <p>This mirrors the server's shape independently (rule R6: the CLI carries no shared control type).
 */
record RemoteDerivedStep(String step, String targetTable, boolean targetKnown,
        List<RemoteDerivedColumn> columns) {

    RemoteDerivedStep {
        columns = columns == null ? List.of() : List.copyOf(columns);
    }
}
