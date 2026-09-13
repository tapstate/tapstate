package io.tapstate.tools.catalog.derive;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * The worklist loop of catalog-derive: for each manifest entry it resolves the connector id's staged jar and
 * probes the named connector class, collecting the registered capability ids keyed by connector id.
 * Two gaps are expected in a real refresh and survived rather than fatal: a connector with no built
 * jar (not part of the OSS dist build), and a connector whose jar cannot be opened or classloaded at
 * all. The second is not a platform or architecture matter: a connector module may encrypt its own
 * shaded jar as part of packaging, and the product of that step is not a zip, so opening it throws
 * before any class is looked up. Building such a module with the encrypting execution skipped yields
 * an ordinary jar the probe reads normally. Both gaps are recorded with a reason and left out of the
 * bitmap, so the refresh completes and the assembler can report the gap. Jar resolution and probing are injected so the loop is unit-tested
 * without a real checkout; the real run passes {@code JarResolver::find} and
 * {@code ConnectorCapabilityProbe::probe}.
 */
final class BitmapEmitter {

    private BitmapEmitter() {
    }

    static EmitOutcome emit(List<ManifestEntry> entries,
                            Function<String, Optional<Path>> jarResolver,
                            BiFunction<Path, String, Set<String>> prober) {
        Map<String, Set<String>> bitmap = new LinkedHashMap<>();
        Map<String, String> skipped = new LinkedHashMap<>();
        for (ManifestEntry entry : entries) {
            Optional<Path> jar = jarResolver.apply(entry.id());
            if (jar.isEmpty()) {
                skipped.put(entry.id(), "no built jar");
                continue;
            }
            try {
                bitmap.put(entry.id(), prober.apply(jar.get(), entry.connectorClass()));
            } catch (RuntimeException | LinkageError | AssertionError e) {
                // The jar is present but classloading/probing it failed (a jar the module encrypted
                // during packaging so it is not a zip, a linkage error, or a connector static
                // initializer throwing). Record and keep going —
                // one connector must not abort a 60-connector refresh. A VirtualMachineError (out of
                // memory, stack overflow) is left to propagate: the JVM is unusable, so aborting is right.
                skipped.put(entry.id(), "probe failed: " + rootCause(e));
            }
        }
        return new EmitOutcome(bitmap, skipped);
    }

    private static String rootCause(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return message == null ? root.getClass().getSimpleName()
                : root.getClass().getSimpleName() + ": " + message;
    }
}
