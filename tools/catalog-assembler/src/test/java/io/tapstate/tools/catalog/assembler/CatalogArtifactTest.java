package io.tapstate.tools.catalog.assembler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.tapstate.core.catalog.TapstateCatalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Drives and locks the bundled connector catalog. Generation needs the connectors checkout and the
 * derived capability bitmap (produced by catalog-derive, which touches PDK), so the byte-lock runs
 * only in the connector-present refresh job and skips otherwise. The refresh is three steps, each a
 * property-gated run of this class or catalog-derive:
 *
 * <p>All three read the connectors checkout from {@code -Dtapstate.catalog.connectors=<path>}, falling
 * back to a sibling directory named {@code tapdata-connectors} when the property is absent.
 *
 * <ol>
 *   <li>{@code -Dtapstate.catalog.manifest=<path>} — walk the checkout and write the probe manifest;</li>
 *   <li>catalog-derive reads that manifest, probes, and writes the bitmap;</li>
 *   <li>{@code -Dtapstate.catalog.update -Dtapstate.catalog.bitmap=<path> -Dtapstate.catalog.sha=<sha>
 *       -Dtapstate.catalog.capability-sha=<sha>} — regenerate the checked-in catalog (index, per-connector
 *       entries), the ingest report and the bitmap itself.</li>
 * </ol>
 *
 * <p>Two revisions rather than one because the two faces are refreshed by different jobs: a spec-only
 * refresh reuses the checked-in bitmap, so its capability revision is whatever last derived one, and
 * stamping this run's revision on both would make the catalog claim capabilities nothing re-read.
 *
 * Without the update toggle the same step byte-compares the regenerated catalog to the checked-in
 * artifacts, so an upstream drift is caught. Catalog entries embed the connectors repo sha, so the
 * artifacts are stable for a fixed checkout.
 */
class CatalogArtifactTest {

    private static final String INDEX = "index.json";

    private static final boolean UPDATE = Boolean.getBoolean("tapstate.catalog.update");

    @Test
    void emitsTheProbeManifestWhenAskedTo() throws IOException {
        String manifestPath = System.getProperty("tapstate.catalog.manifest");
        assumeTrue(manifestPath != null, "no -Dtapstate.catalog.manifest — not a manifest-emit run, skipping");
        Optional<Path> checkout = connectorsRepo();
        assumeTrue(checkout.isPresent(), "connectors checkout absent — skipping");

        WalkResult walk = ConnectorWalker.walk(checkout.get());
        Files.writeString(Path.of(manifestPath), ManifestWriter.write(walk.sources()));
    }

    @Test
    void emitsTheDriftScanFetchListWhenAskedTo() throws IOException {
        String fetchList = System.getProperty("tapstate.catalog.fetch-list");
        assumeTrue(fetchList != null, "no -Dtapstate.catalog.fetch-list — not a drift-scan run, skipping");
        String upstreamPaths = System.getProperty("tapstate.catalog.upstream-paths");
        assumeTrue(upstreamPaths != null, "no -Dtapstate.catalog.upstream-paths — nothing to enumerate against, skipping");

        List<String> upstream = Files.readAllLines(Path.of(upstreamPaths)).stream()
                .map(String::strip)
                .filter(line -> !line.isEmpty())
                .toList();
        Files.writeString(Path.of(fetchList),
                String.join("\n", SpecPathEnumerator.specPathsToFetch(TapstateCatalog.load().all(), upstream)));
    }

    @Test
    void emitsTheDriftReportWhenAskedTo() throws IOException {
        String reportPath = System.getProperty("tapstate.catalog.drift-report");
        assumeTrue(reportPath != null, "no -Dtapstate.catalog.drift-report — not a drift-scan run, skipping");
        Path fetched = Path.of(requireProperty("tapstate.catalog.fetched"));
        int ageDays = Integer.parseInt(requireProperty("tapstate.catalog.catalog-age-days"));
        // Boolean.parseBoolean reads anything that is not "true" as false, and false is the value
        // that changes nothing - so a property name typed wrong upstream would leave the decision
        // exactly as it was before this input existed, which is the miswiring it was added to stop.
        String pullRequestAlreadyOpen = requireProperty("tapstate.catalog.drift-pr-open");
        if (!pullRequestAlreadyOpen.equals("true") && !pullRequestAlreadyOpen.equals("false")) {
            throw new IllegalStateException(
                    "-Dtapstate.catalog.drift-pr-open must be true or false, not " + pullRequestAlreadyOpen);
        }

        Map<String, String> fetchedByPath = new LinkedHashMap<>();
        for (String path : Files.readAllLines(Path.of(requireProperty("tapstate.catalog.fetch-list")))) {
            String relative = path.strip();
            if (relative.isEmpty()) {
                continue;
            }
            Path file = fetched.resolve(relative);
            // Absent is a finding, not an error: it is how a connector deleted or moved upstream
            // shows up. Leaving it out of the map is what lets the comparison say so.
            if (Files.isRegularFile(file)) {
                fetchedByPath.put(relative, Files.readString(file));
            }
        }

        SpecDrift.Report drift = SpecDrift.compare(TapstateCatalog.load().all(), fetchedByPath);
        DriftTriage.Decision decision =
                DriftTriage.decide(drift.allIds(), ageDays, pullRequestAlreadyOpen.equals("true"));
        Files.writeString(Path.of(reportPath), String.join("\n",
                "decision=" + decision,
                "changed=" + String.join(" ", drift.changedIds()),
                "vanished=" + String.join(" ", drift.vanishedIds()),
                "new_connectors=" + String.join(" ", drift.newConnectorIds()),
                "pr_already_open=" + pullRequestAlreadyOpen) + "\n");
    }

    /** A property this step cannot proceed without — absent means the caller is wired wrong. */
    private static String requireProperty(String name) {
        String value = System.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("missing -D" + name);
        }
        return value;
    }

    @Test
    void generatedCatalogMatchesTheCheckedInArtifacts() throws IOException {
        Optional<Path> checkout = connectorsRepo();
        String bitmapPath = System.getProperty("tapstate.catalog.bitmap");
        assumeTrue(checkout.isPresent(), "connectors checkout absent — skipping");
        assumeTrue(bitmapPath != null, "no -Dtapstate.catalog.bitmap (derive the bitmap first) — skipping");

        String bitmapTsv = Files.readString(Path.of(bitmapPath));
        Map<String, Set<String>> bitmap = BitmapReader.read(bitmapTsv);
        GeneratedCatalog catalog = CatalogGenerator.generate(
                checkout.get(), resolveSpecSha(), resolveCapabilitySha(), bitmap);

        if (UPDATE) {
            writeArtifacts(catalog, bitmapTsv);
            return;
        }
        assertCheckedIn(catalog, bitmapTsv);
    }

    @Test
    void catalogUpdateToggleIsOffDuringNormalRuns() {
        // The regenerate path rewrites the artifacts and skips the byte assertion. Were the toggle set
        // during a normal run, a real catalog regression would be silently rebaselined and pass. This
        // guard makes any run with the toggle RED, so regeneration is always deliberate.
        assertThat(UPDATE)
                .as("tapstate.catalog.update must not be set during a normal run — it rewrites the catalog")
                .isFalse();
    }

    private void writeArtifacts(GeneratedCatalog catalog, String bitmapTsv) throws IOException {
        Path catalogDir = catalogDir();
        Files.createDirectories(catalogDir);
        // Remove stale entries (a connector dropped upstream must not linger), then write fresh.
        for (Path json : jsonFiles()) {
            Files.delete(json);
        }
        Files.writeString(catalogDir.resolve(INDEX), catalog.index());
        for (Map.Entry<String, String> entry : catalog.entries().entrySet()) {
            Files.writeString(catalogDir.resolve(entry.getKey() + ".json"), entry.getValue());
        }
        Files.writeString(reportFile(), catalog.report());
        // The bitmap is checked in alongside what was generated from it, so the capability revision in
        // the index head has the thing it names sitting next to it rather than in a build that is gone.
        // It is also what a spec-only refresh merges: with it in the tree, refreshing the spec face
        // builds no jars at all, and the two jobs stop writing the same files.
        Files.writeString(bitmapFile(), bitmapTsv);
    }

    private void assertCheckedIn(GeneratedCatalog catalog, String bitmapTsv) throws IOException {
        Path catalogDir = catalogDir();
        assertThat(Files.exists(catalogDir.resolve(INDEX)))
                .as("catalog index missing — regenerate with -Dtapstate.catalog.update")
                .isTrue();
        assertThat(Files.readString(catalogDir.resolve(INDEX))).isEqualTo(catalog.index());
        for (Map.Entry<String, String> entry : catalog.entries().entrySet()) {
            Path file = catalogDir.resolve(entry.getKey() + ".json");
            assertThat(Files.exists(file)).as("catalog entry missing: " + file).isTrue();
            String checkedIn = Files.readString(file);
            String regenerated = entry.getValue();
            // The equality below is what decides red; the clause only says where to look first. It is
            // computed inside the mismatch case so the common path does not parse 77 entries twice.
            assertThat(checkedIn)
                    .as("catalog entry drift: " + file + CatalogEntryDrift.describe(checkedIn, regenerated))
                    .isEqualTo(regenerated);
        }
        // Orphan guard: no checked-in entry beyond the regenerated set (mirrors the golden orphan gate).
        Set<String> expected = new TreeSet<>();
        catalog.entries().keySet().forEach(id -> expected.add(id + ".json"));
        Set<String> actual = new TreeSet<>();
        for (Path json : jsonFiles()) {
            actual.add(json.getFileName().toString());
        }
        assertThat(actual).as("stale catalog entries not in the regenerated set").isEqualTo(expected);
        assertThat(Files.readString(reportFile())).as("ingest report drift").isEqualTo(catalog.report());
        // Locked for the reason the entries are: the checked-in bitmap is what a spec-only refresh
        // merges, so one that has drifted from the jars this run probed would keep producing rows from
        // capabilities nothing has held since - and every other artifact here would still match.
        assertThat(Files.exists(bitmapFile()))
                .as("capability bitmap missing - regenerate with -Dtapstate.catalog.update")
                .isTrue();
        assertThat(Files.readString(bitmapFile())).as("capability bitmap drift").isEqualTo(bitmapTsv);
    }

    /** The {@code <id>.json} entry files (the index is not an entry). */
    private List<Path> jsonFiles() throws IOException {
        Path catalogDir = catalogDir();
        if (!Files.isDirectory(catalogDir)) {
            return List.of();
        }
        List<Path> files = new ArrayList<>();
        try (Stream<Path> entries = Files.list(catalogDir)) {
            entries.filter(p -> p.getFileName().toString().endsWith(".json"))
                    .filter(p -> !p.getFileName().toString().equals(INDEX))
                    .forEach(files::add);
        }
        return files;
    }

    /** The connectors revision the spec files were read at; required when regenerating so the durable
     *  catalog is never poisoned with a sentinel. */
    private static String resolveSpecSha() {
        return requiredWhenRegenerating("tapstate.catalog.sha", "the connectors revision the specs were read at");
    }

    /**
     * The connectors revision the bitmap being merged was derived at. Passed rather than assumed equal
     * to the spec revision: a spec-only refresh reuses a bitmap derived at an older revision, and
     * stamping that run's revision on both faces would make the only provenance there is say the
     * capabilities are current when nothing re-derived them.
     */
    private static String resolveCapabilitySha() {
        return requiredWhenRegenerating("tapstate.catalog.capability-sha",
                "the connectors revision the bitmap was derived at");
    }

    private static String requiredWhenRegenerating(String property, String what) {
        String value = System.getProperty(property);
        if (UPDATE && (value == null || value.isBlank())) {
            throw new IllegalStateException("regeneration requires -D" + property + "=<sha> - " + what);
        }
        return value == null ? "unknown" : value;
    }

    /** The runtime bundles the catalog from core-catalog's resources, so the artifacts live there. */
    private static Path catalogDir() {
        return repoRoot().resolve("core").resolve("core-catalog")
                .resolve("src").resolve("main").resolve("resources").resolve("catalog");
    }

    /** The derived capability bitmap, checked in beside the tool that merges it. */
    static Path bitmapFile() {
        return repoRoot().resolve("tools").resolve("catalog-assembler").resolve("capability-bitmap.tsv");
    }

    /** The ingest report is a build audit (not bundled into the runtime), kept beside this tool. */
    private static Path reportFile() {
        return repoRoot().resolve("tools").resolve("catalog-assembler").resolve("ingest-report.md");
    }

    /** Walks up to the repo root (the directory holding the core/core-catalog module) so the artifact
     *  paths do not depend on the test's working directory. */
    private static Path repoRoot() {
        for (Path dir = Path.of("").toAbsolutePath(); dir != null; dir = dir.getParent()) {
            if (Files.isDirectory(dir.resolve("core").resolve("core-catalog"))) {
                return dir;
            }
        }
        throw new IllegalStateException("repo root with core/core-catalog not found above the working directory");
    }

    private static Optional<Path> connectorsRepo() {
        return ConnectorsCheckout.locate(
                System.getProperty(ConnectorsCheckout.PROPERTY), Path.of("").toAbsolutePath());
    }
}
