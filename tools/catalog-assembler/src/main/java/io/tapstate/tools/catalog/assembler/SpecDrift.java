package io.tapstate.tools.catalog.assembler;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import io.tapstate.core.catalog.CatalogJson;
import io.tapstate.core.catalog.ConnectorCatalogEntry;

/**
 * Compares one fetch of the upstream specifications against what the catalog last recorded.
 *
 * <p>Three findings, kept apart because they are acted on differently: content that moved under a
 * connector the catalog already carries, a connector whose file is no longer where its row says it
 * is, and a connector upstream carries that no row mentions at all.
 */
final class SpecDrift {

    /**
     * The shape a connector id is allowed to have. Nothing upstream enforces this, and everything
     * this method returns is a string an unrelated project chose: it flows into the catalog, into a
     * scan report read as key=value, and from there into a pull request body. An id carrying a
     * newline would not be a connector nobody catalogued - it would be extra lines in whatever reads
     * the report next.
     */
    private static final Pattern CONNECTOR_ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._\\-]*");

    /**
     * @param changedIds      catalogued connectors whose specification content no longer matches the
     *                        fingerprint their row recorded
     * @param vanishedIds     catalogued connectors whose specification is not at the path their row
     *                        records — deleted upstream, or moved. A path-driven scan is the only
     *                        thing that sees this class at all: a scan that walks whatever upstream
     *                        happens to hold finds nothing missing by construction
     * @param newConnectorIds connectors upstream carries that the catalog has no row for
     */
    record Report(List<String> changedIds, List<String> vanishedIds, List<String> newConnectorIds) {

        /** Every connector this scan has something to say about — what triage weighs. */
        List<String> allIds() {
            TreeSet<String> ids = new TreeSet<>(changedIds);
            ids.addAll(vanishedIds);
            ids.addAll(newConnectorIds);
            return List.copyOf(ids);
        }
    }

    private SpecDrift() {
    }

    static Report compare(List<ConnectorCatalogEntry> snapshot, Map<String, String> fetchedByPath) {
        return compareRepositories(snapshot, Map.of(SpecRepository.OSS, fetchedByPath));
    }

    static Report compareRepositories(List<ConnectorCatalogEntry> snapshot,
                                     Map<SpecRepository, Map<String, String>> fetchedByRepository) {
        List<String> changed = new ArrayList<>();
        List<String> vanished = new ArrayList<>();
        TreeSet<String> catalogued = new TreeSet<>();
        Map<SpecRepository, Set<String>> claimedPaths = new EnumMap<>(SpecRepository.class);

        for (ConnectorCatalogEntry row : snapshot) {
            catalogued.add(row.id());
            String path = row.provenance() == null ? null : row.provenance().specPath();
            if (path == null || path.isBlank()) {
                continue; // registered at runtime; no upstream file stands behind it
            }
            SpecRepository repository = SpecRepository.of(row);
            Map<String, String> fetched = fetchedByRepository.get(repository);
            if (fetched == null) {
                continue; // an unvisited repository has no deletion authority
            }
            // Newly catalogued enterprise rows can be found in the scanned checkout without adding
            // repository fields to the catalog. Absence from both scanned repositories is drift.
            Map<String, String> enterprise = fetchedByRepository.get(SpecRepository.ENTERPRISE);
            if (repository == SpecRepository.OSS && !fetched.containsKey(path)
                    && enterprise != null && enterprise.containsKey(path)
                    && row.id().equals(connectorId(enterprise.get(path)))) {
                repository = SpecRepository.ENTERPRISE;
                fetched = enterprise;
            }
            claimedPaths.computeIfAbsent(repository, ignored -> new TreeSet<>()).add(path);
            String content = fetched.get(path);
            if (content == null) {
                vanished.add(row.id());
            } else if (!SpecHash.of(content).equals(row.provenance().specContentHash())) {
                changed.add(row.id());
            }
        }

        return new Report(List.copyOf(changed), List.copyOf(vanished),
                newConnectors(fetchedByRepository, claimedPaths, catalogued));
    }

    private static List<String> newConnectors(Map<SpecRepository, Map<String, String>> fetchedByRepository,
                                              Map<SpecRepository, Set<String>> claimedPaths,
                                              Set<String> catalogued) {
        TreeSet<String> discovered = new TreeSet<>();
        for (var repository : fetchedByRepository.entrySet()) {
            for (Map.Entry<String, String> fetched : repository.getValue().entrySet()) {
                if (claimedPaths.getOrDefault(repository.getKey(), Set.of()).contains(fetched.getKey())) {
                    continue;
                }
                // What makes a file a connector specification is the id inside it. The scan fetches on
                // shape and decides here, so a resource that merely sits where a specification would is
                // passed over rather than announced as a connector nobody catalogued.
                String id = connectorId(fetched.getValue());
                if (id != null && !catalogued.contains(id)) {
                    discovered.add(id);
                }
            }
        }
        return List.copyOf(discovered);
    }

    private static String connectorId(String specContent) {
        Object tree;
        try {
            tree = CatalogJson.parse(specContent);
        } catch (RuntimeException malformed) {
            return null; // not parseable, so not a specification this scan can speak for
        }
        if (tree instanceof Map<?, ?> root && root.get("properties") instanceof Map<?, ?> properties
                && properties.get("id") instanceof String id && CONNECTOR_ID.matcher(id).matches()) {
            return id;
        }
        return null;
    }
}
