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
        try {
            DataBrowserPreviewReport found = browser.find(source, table, null, null,
                    Math.min(rows, DataBrowserService.MAX_LIMIT));
            Long total = found.approximateTotal();
            if (total == null) {
                try {
                    total = browser.stats(source, table).numOfRows();
                } catch (TapstateException noStats) {
                    total = null;
                }
            }
            return new Sample(total, found.rows(), null);
        } catch (TapstateException refused) {
            return Sample.unavailable(refused.code().code());
        }
    }
}
