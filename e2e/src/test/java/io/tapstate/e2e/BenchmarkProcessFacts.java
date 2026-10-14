package io.tapstate.e2e;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Process facts read from the owned server's Prometheus scrape endpoint, the only face they have: a
 * process fact never acquires a pipeline label and has no REST read. Only {@code tapstate_process_}
 * series are kept. A series present in both reads is differenced when its exposition marks it as an
 * accumulation; everything else is reported as its later reading.
 */
final class BenchmarkProcessFacts {

    private static final String PREFIX = "tapstate_process_";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private BenchmarkProcessFacts() {
    }

    /** One scrape: series text (name plus its label set) to value, and the local instant it completed. */
    record Scrape(long readAtNanos, Map<String, Double> series) {
        Scrape {
            series = Map.copyOf(series);
        }
    }

    static Scrape scrape(int port) {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/metrics"))
                .timeout(TIMEOUT).GET().build();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(TIMEOUT).build()) {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new AssertionError("process fact scrape answered HTTP " + response.statusCode());
            }
            return new Scrape(BenchmarkReturnWorkBudget.rootNanoTime(), parse(response.body()));
        } catch (IOException failure) {
            throw new AssertionError("process fact scrape failed", failure);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("process fact scrape was interrupted", interrupted);
        }
    }

    static Map<String, Double> parse(String exposition) {
        Map<String, Double> series = new TreeMap<>();
        for (String line : exposition.split("\n")) {
            if (line.isBlank() || line.startsWith("#") || !line.startsWith(PREFIX)) { continue; }
            int split = line.lastIndexOf(' ');
            if (split <= 0) { throw new AssertionError("unreadable exposition line: " + line); }
            String key = line.substring(0, split).trim();
            double value = Double.parseDouble(line.substring(split + 1).trim());
            if (series.put(key, value) != null) { throw new AssertionError("exposition repeats series " + key); }
        }
        return series;
    }

    /** Accumulations by the exposition's own suffixes; a value that moved backward reports a restart. */
    static List<Map<String, Object>> changes(Scrape before, Scrape after) {
        if (after.readAtNanos() < before.readAtNanos()) { throw new AssertionError("process scrapes compared backward"); }
        List<Map<String, Object>> changes = new ArrayList<>();
        after.series().forEach((key, later) -> {
            String name = key.contains("{") ? key.substring(0, key.indexOf('{')) : key;
            boolean accumulates = name.endsWith("_total") || name.endsWith("_sum") || name.endsWith("_count")
                    || name.endsWith("_bucket");
            Map<String, Object> change = new LinkedHashMap<>();
            change.put("series", key);
            change.put("after", later);
            Double earlier = before.series().get(key);
            if (accumulates) {
                change.put("before", earlier);
                boolean restarted = earlier == null || later < earlier;
                change.put("accountRestarted", restarted);
                change.put("delta", restarted ? later : later - earlier);
            }
            changes.add(change);
        });
        return changes;
    }
}
