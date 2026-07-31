package io.tapstate.e2e;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * The run's own record of which witnesses actually executed and passed, one line per witness.
 *
 * <p>This exists because the build's aggregate counts cannot be trusted with absence: a witness that
 * aborts before its first test reports {@code Tests run: 0, Skipped: 0}, and a parameterized case is
 * reported under an index that shifts when a sibling is added. The ledger inverts the burden - a line
 * is written only at the end of a run that held every assertion, so a witness that was skipped,
 * aborted, or never discovered simply is not there, and the release gate reads the absence.
 *
 * <p>The ledger is truncated once per JVM, before the first line is written, so a stale file from an
 * earlier build cannot vouch for a run that never happened.
 */
final class WitnessLedger {

    private static final Path LEDGER = Path.of("target", "witness-ledger.txt");
    private static boolean truncated;

    private WitnessLedger() {
    }

    /** Records one witness as executed and passed, in the form the manifest names it. */
    static synchronized void record(String example, Tiers tier) {
        try {
            Files.createDirectories(LEDGER.getParent());
            if (!truncated) {
                Files.deleteIfExists(LEDGER);
                truncated = true;
            }
            Files.writeString(LEDGER, example + " on " + tier.name() + System.lineSeparator(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot record a witness in " + LEDGER, e);
        }
    }
}
