package io.tapstate.core.model;

import java.util.List;
import java.util.Objects;

/**
 * Tiered storage of a view (§7): hot = Hz, warm = Mongo, cold = Paimon
 * (runtime GA; grammar reserved).
 */
@Doc("Tiered storage configuration for a view: hot in-memory, warm database, and cold data-lake layers.")
public record Storage(
        @Doc("Hot in-memory layer settings.")
        Hot hot,
        @Doc("Warm database layer settings.")
        Warm warm,
        @Doc("Cold data-lake layer settings.")
        Cold cold) {

    @Doc("Hot in-memory storage layer of a view.")
    public record Hot(
            @Doc(value = "Time-to-live for hot entries, as a duration string.", required = true)
            String ttl) {
        public Hot {
            Objects.requireNonNull(ttl, "ttl");
        }
    }

    @Doc("Warm database storage layer of a view.")
    public record Warm(
            @Doc(value = "Database collection that backs the warm layer.", required = true)
            String collection,
            @Doc("Indexes to create on the warm collection.")
            List<String> indexes) {
        public Warm {
            Objects.requireNonNull(collection, "collection");
            indexes = indexes == null ? null : List.copyOf(indexes);
        }
    }

    @Doc("Cold data-lake storage layer of a view.")
    public record Cold(
            @Doc("Fields to partition cold data by.")
            List<String> partitionBy) {
        public Cold {
            partitionBy = partitionBy == null ? null : List.copyOf(partitionBy);
        }
    }
}
