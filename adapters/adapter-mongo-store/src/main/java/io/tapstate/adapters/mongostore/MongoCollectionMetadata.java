package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/** Reads complete collection descriptions without query operators restricted by managed deployments. */
final class MongoCollectionMetadata {
    private MongoCollectionMetadata() {}

    static List<Document> matching(MongoDatabase database, Collection<String> names) {
        Set<String> required = Set.copyOf(names);
        if (required.isEmpty()) return List.of();
        var result = new ArrayList<Document>();
        // Retain only requested descriptions while consuming every cursor batch and preserving type/options.
        try (var cursor = database.listCollections().iterator()) {
            while (cursor.hasNext()) {
                Document row = cursor.next();
                if (row.get("name") instanceof String name && required.contains(name)) result.add(row);
            }
        }
        return result;
    }
}
