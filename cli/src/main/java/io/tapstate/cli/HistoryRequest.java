package io.tapstate.cli;

import java.util.List;

/** Arguments the CLI sends to one bounded history page. */
record HistoryRequest(
        String from,
        String to,
        String resolution,
        Integer limit,
        List<String> tables,
        String cursor) {

    HistoryRequest {
        tables = tables == null ? List.of() : List.copyOf(tables);
    }
}
