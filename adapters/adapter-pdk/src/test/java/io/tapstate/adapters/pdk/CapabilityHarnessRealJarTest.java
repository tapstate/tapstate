package io.tapstate.adapters.pdk;

import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.DerivedCapability;
import io.tapstate.core.catalog.ModeResolver;
import io.tapstate.core.catalog.ModeSource;
import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.model.SourceMode;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * A connector's capabilities are derived twice by two implementations that share no code: once
 * offline, by the tool that builds the bundled catalog, and once live, by the harness the server
 * runs when a connector is registered. They classload differently — the offline one resolves the PDK
 * contract parent-first from its own classpath, the live one runs the connector inside an isolated
 * loader — and everything downstream assumes the two answers are the same. Each has its own unit
 * tests and both are green; that the two agree on a real connector was never asked.
 *
 * <p>So this reconciles three answers about the same jar, and a disagreement between any two is a
 * failure: what the live harness reads back, what the offline derivation wrote into the capability
 * bitmap, and what the bundled catalog ships as that connector's row. Two implementations plus the
 * artifact they are supposed to have produced together.
 *
 * <p>The worklist is the refresh job's own three artifacts rather than connectors named here, so
 * widening the witness is a matter of building more jars and nothing in this file changes:
 *
 * <pre>
 *   mvn -pl adapters/adapter-pdk test \
 *     -Dtapstate.pdk.it.manifest=&lt;probe manifest, one id/module/class line per connector&gt; \
 *     -Dtapstate.pdk.it.dist=&lt;directory of built connector jars&gt; \
 *     -Dtapstate.pdk.it.bitmap=&lt;capability bitmap the offline derivation wrote for that same dist&gt;
 * </pre>
 *
 * <p>The bitmap has to come from a derivation over the same dist. Pointing this at an older one
 * compares the live harness against capabilities read out of jars that are no longer there, which
 * passes or fails for reasons that have nothing to do with either implementation.
 *
 * <p>Gated on those properties, so it skips in a normal build, which carries no connector jars.
 * A manifest row whose module has no jar in the dist is skipped the same way the offline derivation
 * skips it — not every connector in source is part of the dist build. A row that does have a jar and
 * is missing from the bitmap is a failure rather than a skip: that is the offline derivation having
 * dropped a connector it was handed.
 *
 * <p>The floor of {@link #MINIMUM_CONNECTORS} is what keeps a run that reconciled nothing from
 * reporting green — an empty dist, a manifest that resolved no jars, and a genuinely agreeing pair
 * otherwise produce the same silence.
 *
 * <p>Modes are only compared for a row whose modes rest on derivation alone. Where a mode was
 * written by hand, derivation cannot reproduce it and is not supposed to; the sink capability is
 * derived for every connector, so it is compared for every connector.
 *
 * <p>Supplying {@code tapstate.pdk.it.manifest} activates the {@code pdk-it} profile, which puts the
 * PDK runtime on the host test classpath — a connector's base class bootstraps the runtime through
 * the host class loader, so it must be host-reachable, not inside the isolated connector loader.
 * A connector dist jar is a thin plugin and does not always bundle its own driver libraries either;
 * {@code tapstate.pdk.it.runtimeCp} is an optional {@link File#pathSeparator}-joined list of those,
 * and it joins the isolated loader's classpath rather than the host's.
 *
 * <p>The deterministic mechanism is covered by {@link CapabilityHarnessTest} with synthetic jars;
 * this is the real-connector witness.
 */
class CapabilityHarnessRealJarTest {

    /**
     * Below this the witness is one connector shape rather than a cross-check: the connectors that
     * derive identically are the easy ones, and a single database proves nothing about the next.
     */
    private static final int MINIMUM_CONNECTORS = 3;

    @Test
    void bothDerivationPathsAndTheBundledCatalogAgreeOnEveryConnectorTheDistHolds() {
        String manifestPath = System.getProperty("tapstate.pdk.it.manifest");
        String distPath = System.getProperty("tapstate.pdk.it.dist");
        String bitmapPath = System.getProperty("tapstate.pdk.it.bitmap");
        assumeTrue(manifestPath != null && distPath != null && bitmapPath != null,
                "no -Dtapstate.pdk.it.{manifest,dist,bitmap} - not a real-connector run, skipping");

        Path dist = Path.of(distPath);
        Map<String, Set<String>> offline = readBitmap(Path.of(bitmapPath));
        List<Path> runtimeCp = classpathEntries(System.getProperty("tapstate.pdk.it.runtimeCp"));
        TapstateCatalog catalog = TapstateCatalog.load();

        List<String> reconciled = new ArrayList<>();
        for (ManifestRow row : readManifest(Path.of(manifestPath))) {
            Optional<Path> jar = distJar(dist, row.id());
            if (jar.isEmpty()) {
                continue;
            }
            assertThat(offline)
                    .as("%s was built into %s, so the offline derivation had it on its worklist", row.id(), dist)
                    .containsKey(row.id());

            Set<String> live = liveCapabilities(jar.get(), runtimeCp, row.connectorClass());
            assertThat(live)
                    .as("the live harness and the offline derivation disagree on what %s registers", row.id())
                    .containsExactlyInAnyOrderElementsOf(offline.get(row.id()));

            ConnectorCatalogEntry snapshot = catalog.byId(row.id());
            Set<DerivedCapability> derived = DerivedCapability.fromCapabilityIds(live);
            assertThat(derived.contains(DerivedCapability.WRITE_RECORD))
                    .as("live-derived sink capability for %s", row.id())
                    .isEqualTo(snapshot.sink().capable());

            if (carriesADeclaration(snapshot)) {
                continue;
            }
            Set<SourceMode> liveModes = ModeResolver.resolve(derived, null, null).modes();
            assertThat(liveModes)
                    .as("live-derived source modes for %s", row.id())
                    .containsExactlyInAnyOrderElementsOf(snapshot.modes());
            reconciled.add(row.id());
        }

        assertThat(reconciled)
                .as("connectors whose whole catalog row was reconciled against both derivation paths")
                .hasSizeGreaterThanOrEqualTo(MINIMUM_CONNECTORS);
    }

    private static Set<String> liveCapabilities(Path jar, List<Path> runtimeCp, String connectorClass) {
        List<Path> classpath = new ArrayList<>();
        classpath.add(jar);
        classpath.addAll(runtimeCp);
        try (ConnectorClassLoader loader = ConnectorClassLoader.open(classpath)) {
            return CapabilityHarness.deriveCapabilities(loader, connectorClass);
        }
    }

    /** Whether any of this row's modes was written by hand rather than derived from capabilities. */
    private static boolean carriesADeclaration(ConnectorCatalogEntry entry) {
        return entry.provenance().modeSource().values().stream().anyMatch(ModeSource::isDeclaration);
    }

    /**
     * The connector id's jar in the dist, or empty when it was not built - the same prefix rule and the
     * same treatment of an ambiguous match the offline derivation resolves jars by, so the two sides
     * cannot end up comparing answers read out of two different jars.
     */
    private static Optional<Path> distJar(Path dist, String connectorId) {
        String prefix = connectorId + "-connector-";
        try (Stream<Path> files = Files.list(dist)) {
            List<Path> matches = files
                    .filter(path -> {
                        String name = path.getFileName().toString();
                        return name.startsWith(prefix) && name.endsWith(".jar");
                    })
                    .sorted()
                    .toList();
            if (matches.size() > 1) {
                throw new IllegalStateException(
                        "ambiguous dist jars for connector " + connectorId + " under " + dist + ": " + matches);
            }
            return matches.stream().findFirst();
        } catch (IOException e) {
            throw new UncheckedIOException("listing dist dir " + dist, e);
        }
    }

    /** One connector on the probe worklist: the catalog id, the upstream module, the class to load. */
    private record ManifestRow(String id, String module, String connectorClass) {
    }

    private static List<ManifestRow> readManifest(Path manifest) {
        List<ManifestRow> rows = new ArrayList<>();
        for (String line : lines(manifest)) {
            String[] fields = line.split("\t");
            if (fields.length != 3) {
                throw new IllegalStateException("malformed manifest line: " + line);
            }
            rows.add(new ManifestRow(fields[0], fields[1], fields[2]));
        }
        return rows;
    }

    /** The bitmap as written by the offline derivation: one line per connector, its id then its capability ids. */
    private static Map<String, Set<String>> readBitmap(Path bitmap) {
        Map<String, Set<String>> byId = new LinkedHashMap<>();
        for (String line : lines(bitmap)) {
            String[] fields = line.split("\t");
            byId.put(fields[0], new TreeSet<>(Arrays.asList(fields).subList(1, fields.length)));
        }
        return byId;
    }

    private static List<String> lines(Path file) {
        try {
            return Files.readAllLines(file).stream().filter(line -> !line.isBlank()).toList();
        } catch (IOException e) {
            throw new UncheckedIOException("reading " + file, e);
        }
    }

    /** Splits a {@link File#pathSeparator}-joined classpath, or an empty list when none is supplied. */
    private static List<Path> classpathEntries(String joined) {
        List<Path> entries = new ArrayList<>();
        if (joined != null && !joined.isBlank()) {
            for (String entry : joined.split(File.pathSeparator)) {
                if (!entry.isBlank()) {
                    entries.add(Path.of(entry));
                }
            }
        }
        return entries;
    }
}
