package io.tapstate.e2e;

import io.tapstate.e2e.connector.CsvConnector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Readers must see a complete generation while replay replaces the same target rows. */
class CsvAtomicPublicationTest {
    @Test
    void rewritingFiveRowsNeverPublishesAnEmptyOrPartialTable(@TempDir Path directory) throws Exception {
        var write = CsvConnector.class.getDeclaredMethod("write", Path.class, List.class, List.class);
        write.setAccessible(true);
        List<Map<String, Object>> rows = java.util.stream.LongStream.rangeClosed(1, 5)
                .mapToObj(id -> Map.<String, Object>of("id", id, "seq", id)).toList();
        Path table = directory.resolve("orders.csv");
        write.invoke(null, table, List.of("id", "seq"), rows);
        FileEndpoints endpoints = new FileEndpoints();
        EndpointAddress address = EndpointAddress.uri(directory.toString());
        assertThat(endpoints.count(address, "orders")).isEqualTo(5);
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor();
        var writer = executor.submit(() -> {
            try {
                start.await();
                for (int generation = 0; generation < 3000 && !Thread.currentThread().isInterrupted(); generation++) {
                    write.invoke(null, table, List.of("id", "seq"), rows);
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            } catch (Exception error) {
                throw new AssertionError("the target rewrite failed", error);
            }
        });
        try {
            Long partial = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            start.countDown();
            do {
                long count = endpoints.count(address, "orders");
                if (count != 5 && partial == null) {
                    partial = count;
                }
            } while (!writer.isDone() && System.nanoTime() < deadline);
            writer.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            assertThat(endpoints.count(address, "orders")).isEqualTo(5);
            assertThat(partial).as("no reader may observe a truncated or partially published five-row target").isNull();
        } finally {
            writer.cancel(true);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS))
                    .as("the writer must stop before the temporary directory is released").isTrue();
        }
    }
}
