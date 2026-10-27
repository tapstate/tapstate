package io.tapstate.control.core;

/**
 * One answer to a start check's question: the finding's key, and the id of the action chosen from the
 * ones it offers.
 */
public record StartDecision(String finding, String action) {
}
