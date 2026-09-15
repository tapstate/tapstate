package io.tapstate.e2e;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** The optional CI selection is made only after discovering the complete source set. */
final class PublishedExampleSelection {
    static final String PROPERTY = "tapstate.e2e.published-examples";
    static final String MARKER = "tapstate.published-case=";

    private PublishedExampleSelection() {}

    static List<Path> select(List<Path> discovered, String selection) {
        if (selection == null) {
            return discovered;
        }
        Set<String> available = new HashSet<>();
        discovered.forEach(path -> available.add(path.toString().replace('\\', '/')));
        Set<String> requested = new HashSet<>();
        for (String value : selection.split(",", -1)) {
            if (value.isBlank() || !available.contains(value) || !requested.add(value)) {
                throw new IllegalArgumentException("Invalid published example selection: " + value);
            }
        }
        return discovered.stream().filter(path -> requested.contains(path.toString().replace('\\', '/'))).toList();
    }

    static void reportIdentity(Path specification, Tiers tier) {
        System.out.println(MARKER + identity(specification, tier));
    }

    static String identity(Path specification, Tiers tier) {
        return specification.toString().replace('\\', '/') + " on " + tier.name();
    }
}
