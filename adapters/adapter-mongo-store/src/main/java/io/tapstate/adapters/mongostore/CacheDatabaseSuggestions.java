package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import java.util.Optional;
import java.util.HashSet;
import java.util.Set;
import java.util.function.IntSupplier;
import java.util.function.Predicate;

final class CacheDatabaseSuggestions {
    private static final int MAX_ATTEMPTS = 5;

    private CacheDatabaseSuggestions() {}

    static Optional<String> find(String database, IntSupplier suffixes, Predicate<String> containsCollections) {
        Set<String> checked = new HashSet<>();
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            Optional<String> candidate = candidateName(database, suffixes.getAsInt(), checked);
            if (candidate.isEmpty()) continue;
            try {
                if (!containsCollections.test(candidate.orElseThrow())) return candidate;
            } catch (MongoException unavailable) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    private static Optional<String> candidateName(String database, int suffix, Set<String> checked) {
        if (suffix < 100_000 || suffix > 999_999) return Optional.empty();
        String candidate = withSuffix(database, suffix);
        if (candidate == null || !checked.add(candidate)) return Optional.empty();
        return Optional.of(candidate);
    }

    private static String withSuffix(String database, int suffix) {
        String ending = "_" + suffix;
        int end = database.length();
        while (end >= 0) {
            String candidate = database.substring(0, end) + ending;
            if (MongoDatabaseNames.isValid(candidate)) return candidate;
            if (end == 0) return null;
            end = database.offsetByCodePoints(end, -1);
        }
        return null;
    }
}
