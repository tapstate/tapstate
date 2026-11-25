package io.tapstate.cli;

/** One answer to a start check's question: the finding's key and the id of the action chosen. */
record StartDecision(String finding, String action) {
}
