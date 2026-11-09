package io.tapstate.control.core;

import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.DataBrowserPreview;
import io.tapstate.spi.store.DataBrowserTableInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sampling a nest's tables goes through the data browser and nowhere else: its rows and row count are what
 * the browser reads, a count the read does not carry is asked of the table's stats, and a source the browser
 * refuses comes back as not sampled with the refusal's code rather than as an empty table.
 */
class DataBrowserNestSamplerTest {

    private static final SourceResource SHOP = new SourceResource("shop", null, "mongodb",
            Map.of("uri", "mongodb://db.local", "database", "shop"), null, null, null, null);
    private static final SourceResource FILES = new SourceResource("files", null, "e2e_file",
            Map.of(), null, null, null, null);

    private static final List<Map<String, Object>> ROWS = List.of(Map.of("id", 1), Map.of("id", 2));

    @Test
    void theRowsAndTheReadsOwnCountComeBackAndNoStatsAreAskedFor() {
        AtomicInteger asked = new AtomicInteger();
        NestSampler.Sample sample = sampler(new DataBrowserPreview(ROWS, 40L, true), asked, 99L)
                .sample("shop", "orders", 200);

        assertThat(sample).isEqualTo(new NestSampler.Sample(40L, ROWS, null));
        assertThat(asked).hasValue(0);
    }

    @Test
    void aReadThatCarriesNoCountTakesItFromTheTablesStats() {
        AtomicInteger asked = new AtomicInteger();
        NestSampler.Sample sample = sampler(new DataBrowserPreview(ROWS, null, true), asked, 99L)
                .sample("shop", "orders", 200);

        assertThat(sample.tableRows()).isEqualTo(99L);
        assertThat(asked).hasValue(1);
    }

    @Test
    void aSourceTheBrowserCannotReadIsNotSampledWithTheRefusalsCode() {
        NestSampler.Sample sample = sampler(new DataBrowserPreview(ROWS, 40L, true), new AtomicInteger(), 99L)
                .sample("files", "orders", 200);

        assertThat(sample.rows()).isEmpty();
        assertThat(sample.unavailable()).startsWith("data-browser.");
    }

    private static DataBrowserNestSampler sampler(DataBrowserPreview found, AtomicInteger statsAsked, Long rows) {
        return new DataBrowserNestSampler(new DataBrowserService(store(SHOP, FILES), new EmptySchemaStore(),
                config -> List.of("orders"),
                (config, collection) -> {
                    statsAsked.incrementAndGet();
                    return new DataBrowserTableInfo(rows, null, null);
                },
                (config, query) -> found,
                (config, request, listener) -> {
                    throw new AssertionError("sampling never follows a table");
                }));
    }

    private static ArtifactStore store(Resource... stored) {
        return new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> saved) {
            }

            @Override
            public Optional<Resource> get(String id) {
                return List.of(stored).stream().filter(r -> r.id().equals(id)).findFirst();
            }

            @Override
            public List<Resource> list() {
                return List.of(stored);
            }
        };
    }
}
