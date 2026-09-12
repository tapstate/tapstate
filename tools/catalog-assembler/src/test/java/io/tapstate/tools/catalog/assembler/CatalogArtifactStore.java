package io.tapstate.tools.catalog.assembler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import io.tapstate.core.catalog.CatalogJson;

/** Preserves entries outside the current checkout set when replacing a refresh's artifacts. */
final class CatalogArtifactStore {
    private CatalogArtifactStore() {
    }

    record Snapshot(GeneratedCatalog catalog, String bitmap) {
    }

    static Snapshot merge(GeneratedCatalog generated, String bitmap, Path catalogDir, Path bitmapFile, Path reportFile)
            throws IOException {
        Map<String, String> entries = new TreeMap<>();
        if (Files.isDirectory(catalogDir)) {
            try (var files = Files.list(catalogDir)) {
                for (Path file : files.filter(path -> path.toString().endsWith(".json"))
                        .filter(path -> !path.getFileName().toString().equals("index.json")).toList()) {
                    String name = file.getFileName().toString();
                    entries.put(name.substring(0, name.length() - 5), Files.readString(file));
                }
            }
        }
        entries.putAll(generated.entries());
        // Case-insensitive filesystems must never overwrite a retained entry under a different id.
        Map<String, String> seen = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (String id : entries.keySet()) {
            String previous = seen.put(id, id);
            if (previous != null) {
                throw new IllegalStateException("connector ids collide case-insensitively: " + previous + " and " + id);
            }
        }
        Map<?, ?> head = (Map<?, ?>) CatalogJson.parse(generated.index());
        String index = CatalogGenerator.index((String) head.get("specSha"), (String) head.get("capabilitySha"),
                List.copyOf(entries.keySet()));
        Map<String, String> bits = new TreeMap<>();
        if (Files.exists(bitmapFile)) {
            readLines(Files.readString(bitmapFile), bits);
        }
        // A visited connector that could not be derived loses its previous bits; other checkouts keep theirs.
        generated.entries().keySet().forEach(bits::remove);
        Map<String, String> refreshed = new TreeMap<>();
        readLines(bitmap, refreshed);
        refreshed.forEach((id, line) -> {
            if (generated.entries().containsKey(id)) {
                bits.put(id, line);
            }
        });
        String report = mergeReport(generated.report(), Files.exists(reportFile) ? Files.readString(reportFile) : "",
                generated.entries().keySet(), entries.size());
        return new Snapshot(new GeneratedCatalog(index, entries, report),
                bits.isEmpty() ? "" : String.join("\n", bits.values()) + "\n");
    }

    private static String mergeReport(String current, String previous, Set<String> refreshed, int total) {
        Map<String, Set<String>> sections = sections(previous);
        sections.values().forEach(items -> items.removeIf(item -> refreshed.stream()
                .anyMatch(id -> item.equals("- " + id) || item.startsWith("- " + id + ":"))));
        Map<String, Set<String>> currentSections = sections(current);
        currentSections.forEach((heading, items) -> sections.computeIfAbsent(heading, ignored -> new TreeSet<>())
                .addAll(items));
        String header = current.substring(0, current.indexOf("## "))
                .replaceFirst("Ingested connectors: [0-9]+", "Ingested connectors: " + total);
        StringBuilder report = new StringBuilder(header);
        boolean first = true;
        for (var section : sections.entrySet()) {
            if (!first) {
                report.append('\n');
            }
            first = false;
            report.append(section.getKey()).append('\n');
            if (section.getValue().isEmpty()) {
                report.append("(none)\n");
            } else {
                section.getValue().forEach(item -> report.append(item).append('\n'));
            }
        }
        return report.toString();
    }

    private static Map<String, Set<String>> sections(String report) {
        Map<String, Set<String>> sections = new LinkedHashMap<>();
        Set<String> items = null;
        for (String line : report.split("\n")) {
            if (line.startsWith("## ")) {
                items = sections.computeIfAbsent(line, ignored -> new TreeSet<>());
            } else if (items != null && line.startsWith("- ")) {
                items.add(line);
            }
        }
        return sections;
    }

    private static void readLines(String bitmap, Map<String, String> lines) {
        for (String line : bitmap.split("\r?\n")) {
            if (!line.isBlank()) {
                lines.put(line.split("\t", 2)[0], line);
            }
        }
    }

    static void write(GeneratedCatalog generated, String bitmap, Path catalogDir, Path bitmapFile,
                      Path reportFile) throws IOException {
        Snapshot snapshot = merge(generated, bitmap, catalogDir, bitmapFile, reportFile);
        Files.createDirectories(catalogDir);
        // Only replace ids actually visited by this refresh. An absent checkout has no deletion authority.
        for (var entry : generated.entries().entrySet()) {
            Files.writeString(catalogDir.resolve(entry.getKey() + ".json"), entry.getValue());
        }
        Files.writeString(catalogDir.resolve("index.json"), snapshot.catalog().index());
        Files.writeString(bitmapFile, snapshot.bitmap());
        Files.writeString(reportFile, snapshot.catalog().report());
    }
}
