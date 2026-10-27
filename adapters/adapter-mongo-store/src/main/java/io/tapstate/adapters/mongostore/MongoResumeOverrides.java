package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SourceResumeOverride;
import org.bson.Document;
import java.util.Date;
import java.util.Map;
import java.util.Set;

/** Strict encoding within the original source progress resource. */
final class MongoResumeOverrides {
    private MongoResumeOverrides() {}
    static Document stored(SourceResumeOverride value) {
        if (value == null) return null;
        var row = new Document("token", value.token()).append("fromEpoch", value.fromEpoch()).append("editedAt", Date.from(value.editedAt()));
        return row.append("contentHash", CanonicalHash.ofText(row.toJson()));
    }
    static SourceResumeOverride read(Object value) {
        if (value == null) return null;
        if (!(value instanceof Document row) || !row.keySet().equals(Set.of("token", "fromEpoch", "editedAt", "contentHash"))
                || !(row.get("token") instanceof String token) || !(row.get("fromEpoch") instanceof Long epoch)
                || !(row.get("editedAt") instanceof Date at) || epoch < 0 || epoch == Long.MAX_VALUE) throw unreadable();
        var result = new SourceResumeOverride(token, epoch, at.toInstant());
        if (!stored(result).equals(row)) throw unreadable();
        return result;
    }
    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "source-position", "field", "resume-override"), null);
    }
}
