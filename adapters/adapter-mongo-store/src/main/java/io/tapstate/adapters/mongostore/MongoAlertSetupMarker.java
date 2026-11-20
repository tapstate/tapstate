package io.tapstate.adapters.mongostore;

import org.bson.Document;

/** Identifies the original setup operation in a validated alert schema marker. */
final class MongoAlertSetupMarker {
    private static final String OPERATION_ID = "operationId";

    private MongoAlertSetupMarker() {}

    static boolean hasOriginalOperation(Document marker, Document query, String operationId) {
        return operationId.equals(marker.getString(OPERATION_ID))
                && operationId.equals(marker.getString("authorityPreparationOperationId"))
                && operationId.equals(query.getString(OPERATION_ID));
    }
}
