package io.tapstate.adapters.otel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class LinuxProcessRssTest {

    @TempDir
    Path directory;

    @Test
    void readsTheRollupRssAndExpiresTheCacheWithoutReadingOnScrape() throws Exception {
        Path rollup = directory.resolve("smaps_rollup");
        Files.writeString(rollup, "00400000-7fffffff ---p 00000000 00:00 0 [rollup]\n"
                + "Rss:                512 kB\nPss:                403 kB\n");
        AtomicLong now = new AtomicLong(1);
        Instant measuredAt = Instant.parse("2026-09-27T00:00:00Z");
        LinuxProcessRss rss = new LinuxProcessRss(rollup, now::get,
                Clock.fixed(measuredAt, ZoneOffset.UTC), false);
        try (rss) {
            assertThat(rss.reading()).isEmpty();
            rss.refresh();
            assertThat(rss.reading()).contains(new LinuxProcessRss.Sample(512L * 1_024, measuredAt));

            Files.delete(rollup);
            assertThat(rss.reading()).contains(new LinuxProcessRss.Sample(512L * 1_024, measuredAt));
            now.addAndGet(Duration.ofSeconds(31).toNanos());
            assertThat(rss.reading()).isEmpty();
            rss.refresh();
            assertThat(rss.reading()).isEmpty();
        }
        Files.writeString(rollup, "Rss: 1024 kB\n");
        rss.refresh();
        assertThat(rss.reading()).isEmpty();
    }

    @Test
    void malformedAndOverflowedRssAreAbsent() {
        assertThat(LinuxProcessRss.parse("Rss: 100 kB\n")).isEqualTo(102_400);
        assertThat(LinuxProcessRss.parse("Rss: -1 kB\n")).isEqualTo(-1);
        assertThat(LinuxProcessRss.parse("Rss: 100 MB\n")).isEqualTo(-1);
        assertThat(LinuxProcessRss.parse("Rss: 9223372036854775807 kB\n")).isEqualTo(-1);
        assertThat(LinuxProcessRss.parse("Pss: 100 kB\n")).isEqualTo(-1);
    }
}
