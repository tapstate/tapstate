package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.util.Map;

/** Keeps the binary compatibility fence separate from CONFIG's component composition. */
final class MongoFirstClearingStopSchema {
    static final int VERSION = 4;
    private static final String ENABLEMENT = "firstClearingStop";
    private static final String COMPONENT_VERSION = "componentVersion";

    private MongoFirstClearingStopSchema() {}

    static Document enablement(String clusterId, int componentVersion) {
        return new Document("installedVersion", VERSION)
                .append(ENABLEMENT, marker(clusterId)).append(COMPONENT_VERSION, componentVersion);
    }

    private static Document marker(String clusterId) {
        return new Document("formatVersion", 1).append("clusterId", clusterId);
    }

    static boolean enabled(ClientSession session, MongoDatabase database, String clusterId) {
        return enabled(SystemCollections.SCHEMA_MIGRATIONS.on(database)
                .find(session, new Document("_id", "config")).first(), clusterId);
    }

    static boolean enabled(Document schema, String clusterId) {
        if (componentVersion(schema) == schema.getInteger("installedVersion")) return false;
        if (!marker(clusterId).equals(schema.get(ENABLEMENT))) throw unreadable();
        return true;
    }

    static int componentVersion(Document schema) {
        if (schema == null || !(schema.get("installedVersion") instanceof Integer installed)) throw unreadable();
        if (installed == VERSION) {
            if (!(schema.get(COMPONENT_VERSION) instanceof Integer component) || component < 1 || component > 3
                    || !(schema.get(ENABLEMENT) instanceof Document marker)
                    || !(marker.get("clusterId") instanceof String cluster) || cluster.isBlank()
                    || !marker(cluster).equals(marker)) throw unreadable();
            return component;
        }
        if (installed < 1 || installed > 3 || schema.containsKey(ENABLEMENT) || schema.containsKey(COMPONENT_VERSION))
            throw unreadable();
        return installed;
    }

    static Document validationVersionUpdate(Document schema) {
        return new Document(Integer.valueOf(VERSION).equals(schema.get("installedVersion"))
                ? COMPONENT_VERSION : "installedVersion", MongoValidationSchema.preparedConfigurationVersion(schema));
    }

    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "config", "field", ENABLEMENT), null);
    }
}
