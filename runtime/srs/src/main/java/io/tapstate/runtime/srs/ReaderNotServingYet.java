package io.tapstate.runtime.srs;

import java.util.List;

/**
 * A pipeline arriving on a chain found its reader running without a table the pipeline reads. The request is
 * recorded and the reader takes the table on the next time it looks; until then the pipeline cannot start,
 * because a load of a table the reader is not subscribed to would miss every change made between the load
 * and the moment the reader took the table on. The caller gives the start back and tries again.
 *
 * <p>Uncoded: it is a wait, not a fault. The caller turns a wait that outlasts its bound into a coded refusal.
 */
public final class ReaderNotServingYet extends RuntimeException {

    private final String chainId;
    private final List<String> tables;

    public ReaderNotServingYet(String chainId, List<String> tables) {
        super("the reader of chain " + chainId + " does not read " + tables + " yet", null, false, false);
        this.chainId = chainId;
        this.tables = List.copyOf(tables);
    }

    public String chainId() {
        return chainId;
    }

    public List<String> tables() {
        return tables;
    }
}
