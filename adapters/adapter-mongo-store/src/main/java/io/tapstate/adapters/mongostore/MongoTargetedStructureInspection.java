package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.cursor.TimeoutMode;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;

/** An exact physical namespace lookup, never a database-wide metadata scan. */
final class MongoTargetedStructureInspection {
    static List<MongoStructureInspection.Difference> inspect(MongoDatabase database,
            MongoStructureInspection.Namespace namespace, MongoReadBudget budget) {
        var rows = budget.database(database).listCollections()
                .filter(new Document("name", namespace.physicalName()))
                .timeoutMode(TimeoutMode.CURSOR_LIFETIME).into(new ArrayList<>());
        budget.remainingMillis();
        if (rows.isEmpty()) return List.of(difference(database, namespace,
                MongoStructureInspection.Problem.COLLECTION_MISSING, "collection", "absent"));
        var row = rows.getFirst();
        if (!"collection".equals(row.get("type"))) return List.of(difference(database, namespace,
                MongoStructureInspection.Problem.COLLECTION_TYPE, "collection", "other namespace type"));
        var differences = new ArrayList<MongoStructureInspection.Difference>();
        var optionDifference = MongoStructureInspection.collectionOptionsDifference(database.getName(), namespace, row);
        if (optionDifference != null) differences.add(optionDifference);
        var bounded = budget.database(database);
        var collection = namespace.physicalName().equals(namespace.descriptor().indexTarget())
                ? namespace.descriptor().indexTargetOn(bounded) : namespace.descriptor().chunksOn(bounded);
        var indexes = collection.listIndexes().timeoutMode(TimeoutMode.CURSOR_LIFETIME).into(new ArrayList<>());
        budget.remainingMillis();
        differences.addAll(MongoStructureInspection.compareIndexes(database.getName(), namespace, indexes));
        return List.copyOf(differences);
    }

    private static MongoStructureInspection.Difference difference(MongoDatabase db,
            MongoStructureInspection.Namespace namespace, MongoStructureInspection.Problem problem,
            String expected, String actual) {
        return new MongoStructureInspection.Difference(namespace.descriptor().database(), db.getName(),
                namespace.physicalName(), null, problem, expected, actual, null);
    }
    private MongoTargetedStructureInspection() {}
}
