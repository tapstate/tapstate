package io.tapstate.app;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * How the per-pipeline history of movement samples is kept: how often a sample is taken off the
 * published observation, and how long the store keeps one before letting it expire.
 *
 * <p>The two are one decision, not two knobs: the history's size is the retention divided by the
 * interval, per key kept, per pipeline. The defaults are a minute and fifteen days — enough to draw a
 * fortnight of lines, and about twenty-two thousand documents per pipeline for it. The retention is
 * settable to the second on purpose; a bound nobody can shorten is a bound nothing can witness.
 */
@ConfigurationProperties("tapstate.metrics.history")
public class MetricsHistoryProperties {

    /** How often a sample is taken. */
    private Duration sampleInterval = Duration.ofSeconds(60);

    /** How long a sample is kept before the store lets it expire. */
    private Duration retention = Duration.ofDays(15);

    public Duration getSampleInterval() {
        return sampleInterval;
    }

    public void setSampleInterval(Duration sampleInterval) {
        this.sampleInterval = sampleInterval;
    }

    public Duration getRetention() {
        return retention;
    }

    public void setRetention(Duration retention) {
        this.retention = retention;
    }
}
