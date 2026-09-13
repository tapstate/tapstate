package io.tapstate.tools.catalog.assembler;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.tapstate.core.catalog.CatalogEntryWriter;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.ConnectorOverlay;

/**
 * Composes the whole PDK-free assembly path into the deterministic catalog artifacts: walk a
 * connectors checkout, assemble entries (merging the derived bitmap and declared modes through the
 * core rules), then serialize the index, the per-connector entries and the ingest report. Reads spec
 * files from the checkout; produces content only, leaving writing and byte-locking to the caller.
 */
final class CatalogGenerator {

    private CatalogGenerator() {
    }

    static GeneratedCatalog generate(Path connectorsRoot, String specSha, String capabilitySha,
                                     Map<String, Set<String>> bitmap) {
        return generate(List.of(connectorsRoot), specSha, capabilitySha, bitmap);
    }

    static GeneratedCatalog generate(List<Path> roots, String specSha, String capabilitySha,
                                     Map<String, Set<String>> bitmap) {
        List<Assembly> assemblies = new ArrayList<>();
        ConnectorOverlay overlay = ConnectorOverlay.load();
        for (Path root : roots) {
            assemblies.add(CatalogAssembler.assemble(ConnectorWalker.walk(root), specSha, capabilitySha,
                    bitmap, overlay, relativePath -> read(root.resolve(relativePath))));
        }
        List<ConnectorCatalogEntry> ordered = assemblies.stream().flatMap(value -> value.entries().stream())
                .sorted(java.util.Comparator.comparing(ConnectorCatalogEntry::id)).toList();

        JsonWriter writer = new JsonWriter();
        List<String> ids = new ArrayList<>();
        Map<String, String> seenLowercase = new LinkedHashMap<>();
        Map<String, String> entries = new LinkedHashMap<>();
        for (ConnectorCatalogEntry entry : ordered) {
            // Entry files are <id>.json, which collapse on a case-insensitive filesystem; reject ids
            // that differ only in case so one entry is never silently overwritten by another.
            String prior = seenLowercase.put(entry.id().toLowerCase(java.util.Locale.ROOT), entry.id());
            if (prior != null) {
                throw new IllegalStateException(
                        "connector ids collide case-insensitively: '" + prior + "' and '" + entry.id() + "'");
            }
            ids.add(entry.id());
            entries.put(entry.id(), writer.write(CatalogEntryWriter.toTree(entry)));
        }
        // The index carries the two revisions once for the whole catalog. Per entry they were the
        // same value copied once per connector, so a refresh that changed one connector rewrote every
        // file - and the spec-face refresh opens a pull request daily.
        String index = index(specSha, capabilitySha, ids);
        List<IngestReport> reports = assemblies.stream().map(Assembly::report).toList();
        IngestReport report = new IngestReport(specSha, capabilitySha,
                collect(reports, IngestReport::ingestedIds),
                collect(reports, IngestReport::unclassified),
                collect(reports, IngestReport::notDerived),
                collect(reports, IngestReport::notBuilt),
                collect(reports, IngestReport::unverifiedModes),
                collect(reports, IngestReport::overlayAlone),
                collect(reports, IngestReport::overlayDivergences),
                collect(reports, IngestReport::overlayNotDerivable),
                collect(reports, IngestReport::sinkDefaultedNoSignal),
                collect(reports, IngestReport::unknownTypeFields),
                collect(reports, IngestReport::unresolvedLabelRefs),
                collect(reports, IngestReport::exemptions));
        return new GeneratedCatalog(index, entries, ReportRenderer.render(report));
    }

    static String index(String specSha, String capabilitySha, List<String> ids) {
        Map<String, Object> head = new LinkedHashMap<>();
        head.put("specSha", specSha);
        head.put("capabilitySha", capabilitySha);
        head.put("entries", new ArrayList<Object>(ids));
        return new JsonWriter().write(head);
    }

    private static <T> List<T> collect(List<IngestReport> reports,
                                      java.util.function.Function<IngestReport, List<T>> values) {
        return reports.stream().flatMap(report -> values.apply(report).stream()).toList();
    }

    private static String read(Path file) {
        try {
            return Files.readString(file);
        } catch (IOException e) {
            throw new UncheckedIOException("reading " + file, e);
        }
    }
}
