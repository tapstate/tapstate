package io.tapstate.cli;

/** Arguments for one bounded pipeline event page. */
record EventsRequest(String from, String to, Integer limit, String cursor) {
}
