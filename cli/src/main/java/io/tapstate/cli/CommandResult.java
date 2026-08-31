package io.tapstate.cli;

/** The surface-neutral outcome of one command invocation. */
record CommandResult(boolean keepRunning, int exitCode) {
}
