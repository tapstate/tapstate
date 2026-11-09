package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;

import java.util.Objects;

/**
 * Samples through the data browser: the same whitelisted, read-only probes a person browsing a source uses,
 * so a source the browser cannot read is one these checks report as not sampled rather than one they reach
 * some other way.
 */
public final class DataBrowserNestSampler implements NestSampler {

    private final DataBrowserService browser;

    public DataBrowserNestSampler(DataBrowserService browser) {
        this.browser = Objects.requireNonNull(browser, "browser");
    }

    @Override
    public Sample sample(String source, String table, int rows) {
        DataBrowserPreviewReport found;
        try {
            found = browser.find(source, table, null, null, Math.min(rows, DataBrowserService.MAX_LIMIT));
        } catch (TapstateException refused) {
            return Sample.unavailable(refused.code().code());
        }
        Long total = found.approximateTotal() != null ? found.approximateTotal() : rowCount(source, table);
        return new Sample(total, found.rows(), null);
    }

    /** The table's row count from its stats, or null where its connector keeps none. */
    private Long rowCount(String source, String table) {
        try {
            return browser.stats(source, table).numOfRows();
        } catch (TapstateException noStats) {
            return null;
        }
    }
}
