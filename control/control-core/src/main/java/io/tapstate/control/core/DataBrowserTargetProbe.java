package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;

import java.util.Objects;

/**
 * Looks at a target through the data browser's own reads, so a start check reaches a target exactly the
 * way a person browsing it does -- the same connection resolution, the same set of connectors it may
 * ask, the same refusals -- and adds no new way into a target of its own.
 *
 * <p>One row decides whether the target is empty. How many rows it holds comes from the same read when the
 * connector reports a total with it, otherwise from the collection's statistics, which are the store's
 * metadata and an estimate; a count that cannot be had is left out rather than guessed.
 */
public final class DataBrowserTargetProbe implements TargetProbe {

    private final DataBrowserService browser;

    public DataBrowserTargetProbe(DataBrowserService browser) {
        this.browser = Objects.requireNonNull(browser, "browser");
    }

    @Override
    public TargetRows rows(String connection, String table) {
        DataBrowserPreviewReport first;
        try {
            first = browser.find(connection, table, null, null, 1);
        } catch (TapstateException refused) {
            if (refused.code() == DataBrowserError.UNKNOWN_COLLECTION) {
                return TargetRows.EMPTY;
            }
            throw refused;
        }
        if (first.rows().isEmpty()) {
            return TargetRows.EMPTY;
        }
        Long count = first.approximateTotal();
        if (count == null || count < 1) {
            try {
                count = browser.stats(connection, table).numOfRows();
            } catch (TapstateException unknown) {
                count = null;
            }
        }
        return new TargetRows(false, count == null || count < 1 ? null : count, false);
    }
}
