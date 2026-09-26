package io.tapstate.e2e;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Holds on the harness connector's writes, placed and let go through the directory its {@code hold} setting names.
 *
 * <p>A hold {@link #before} a row keeps the batch carrying it from being written at all, which is a slow writer as
 * the product sees one; a hold {@link #after} a row lets the batch be written and keeps it from being reported done,
 * which is a write the target has and the product has not heard back about. A writer that is holding a row says so
 * with a file naming its process, which is how a case knows which member to kill to catch a write in that state.
 */
final class Holds {

    private final Path directory;

    Holds(Path directory) {
        this.directory = directory;
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException("creating the hold directory at " + directory, e);
        }
    }

    /** Where the connector is told to look for holds: the value of its {@code hold} setting. */
    Path directory() {
        return directory;
    }

    /** Holds {@code table}'s load before it is read, on a source that names this directory. */
    void read(String table) {
        try {
            Files.writeString(directory.resolve("read-" + table), "");
        } catch (IOException e) {
            throw new UncheckedIOException("placing a hold in " + directory, e);
        }
    }

    /** Lets go of the hold on reading {@code table}'s load. */
    void releaseRead(String table) {
        try {
            Files.deleteIfExists(directory.resolve("read-" + table));
        } catch (IOException e) {
            throw new UncheckedIOException("letting go of a hold in " + directory, e);
        }
    }

    /** The process holding {@code table}'s load before reading it, once one is. */
    Optional<Long> holderOfRead(String table) {
        return holderNamed("held-read-" + table + "-");
    }

    /** Holds any batch carrying {@code id} of {@code table} before it is written. */
    void before(String table, Object id) {
        place("before", table, id);
    }

    /** Holds any batch carrying {@code id} of {@code table} after it is written, before it is reported done. */
    void after(String table, Object id) {
        place("after", table, id);
    }

    /** Lets go of the hold before {@code id} of {@code table} is written. */
    void releaseBefore(String table, Object id) {
        release("before", table, id);
    }

    /** Lets go of the hold after {@code id} of {@code table} is written. */
    void releaseAfter(String table, Object id) {
        release("after", table, id);
    }

    /** The process holding {@code id} of {@code table} before its write, once one is. */
    Optional<Long> holderBefore(String table, Object id) {
        return holder("before", table, id);
    }

    /** The process holding {@code id} of {@code table} after its write, once one is. */
    Optional<Long> holderAfter(String table, Object id) {
        return holder("after", table, id);
    }

    private void place(String phase, String table, Object id) {
        try {
            Files.writeString(directory.resolve(phase + "-" + table + "-" + id), "");
        } catch (IOException e) {
            throw new UncheckedIOException("placing a hold in " + directory, e);
        }
    }

    private void release(String phase, String table, Object id) {
        try {
            Files.deleteIfExists(directory.resolve(phase + "-" + table + "-" + id));
        } catch (IOException e) {
            throw new UncheckedIOException("letting go of a hold in " + directory, e);
        }
    }

    private Optional<Long> holder(String phase, String table, Object id) {
        return holderNamed("held-" + phase + "-" + table + "-" + id + "-");
    }

    private Optional<Long> holderNamed(String prefix) {
        try (Stream<Path> files = Files.list(directory)) {
            return files.map(path -> path.getFileName().toString())
                    .filter(name -> name.startsWith(prefix))
                    .map(name -> Long.parseLong(name.substring(prefix.length())))
                    .findFirst();
        } catch (IOException e) {
            throw new UncheckedIOException("reading the hold directory at " + directory, e);
        }
    }
}
