package io.tapstate.cli;

import java.util.List;

/** A parsed command line, independent of the terminal surface that supplied it. */
record CommandInvocation(List<String> words) {

    CommandInvocation {
        words = List.copyOf(words == null ? List.of() : words);
    }

    static CommandInvocation parse(String line) {
        return new CommandInvocation(Repl.tokenize(line == null ? "" : line));
    }
}
