package io.tapstate.control.restapi;

import io.tapstate.control.core.ListBounds;

import java.util.List;

/** Validates and slices an optional list window at the HTTP boundary. */
final class ListWindow {

    private ListWindow() {
    }

    static <T> List<T> page(List<T> items, Integer limit, Integer offset) {
        if (limit == null && offset == null) {
            return items;
        }
        int bound = limit == null ? ListBounds.DEFAULT_LIMIT : limit;
        int start = offset == null ? 0 : offset;
        if (bound < 1 || bound > ListBounds.MAX_LIMIT) {
            throw MalformedRequest.rejecting(
                    "limit must be between 1 and " + ListBounds.MAX_LIMIT, null);
        }
        if (start < 0) {
            throw MalformedRequest.rejecting("offset must be non-negative", null);
        }
        if (start >= items.size()) {
            return List.of();
        }
        int end = (int) Math.min((long) items.size(), (long) start + bound);
        return items.subList(start, end);
    }
}
