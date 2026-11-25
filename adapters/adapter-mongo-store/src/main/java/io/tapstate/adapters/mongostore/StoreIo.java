package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoWriteException;
import com.mongodb.MongoWriteConcernException;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import org.bson.BsonMaximumSizeExceededException;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The single translation point from Mongo driver exceptions to io-domain coded diagnostics: a store
 * runs its driver call through here, so a driver failure surfaces as a coded {@code io.*} diagnostic
 * and no driver type escapes the module (rule R3). A security failure maps to
 * {@code io.store-unauthorized}; a document the store will not take for its size maps to
 * {@code io.document-too-large}, whether the driver refused it or the endpoint did; any other driver
 * explicitly confirmed quota refusal maps to {@code io.store-quota-exceeded}; another driver
 * failure maps to {@code io.store-unavailable}
 * carrying only a safe driver type and numeric code. Driver messages and causes may contain
 * connection credentials or document values and never cross this boundary. A non-driver throwable
 * — a coded reconstruction failure ({@code io.document-unreadable}) or a bare invariant crash —
 * passes straight through, never relabelled as a driver failure.
 */
final class StoreIo {

    /**
     * The id reported when a size failure happens on a call that did not name one. Every write that
     * can plausibly reach the limit names its document; this keeps the rest coded rather than raw.
     */
    private static final String UNNAMED = "unknown";

    /**
     * The endpoint's own refusals of a document too large to store: {@code 17419} from an update whose
     * result passes the limit, {@code 10334} from a document that passes it outright. The driver reports
     * these as ordinary command errors, so a size failure raised by the server rather than caught in the
     * driver reached callers as "a store operation could not complete" — which sends whoever reads it to
     * check a store that is healthy, the misreading {@code io.document-too-large} exists to stop.
     */
    private static final Set<Integer> DOCUMENT_TOO_LARGE_CODES = Set.of(17419, 10334);

    private static final java.util.regex.Pattern ATLAS_QUOTA_MESSAGE = java.util.regex.Pattern.compile(
            "you are over your space quota(?:, using [0-9]+(?:\\.[0-9]+)? (?:bytes|KB|MB|GB|TB) of [0-9]+(?:\\.[0-9]+)? (?:bytes|KB|MB|GB|TB))?",
            java.util.regex.Pattern.CASE_INSENSITIVE);

    private StoreIo() {
    }

    /** Runs a store operation, translating a driver failure into a coded io diagnostic. */
    static <T> T call(Supplier<T> operation) {
        return call(UNNAMED, operation);
    }

    /** As {@link #call(Supplier)}, naming the document the operation is for, for a size failure. */
    static <T> T call(String id, Supplier<T> operation) {
        try {
            return operation.get();
        } catch (BsonMaximumSizeExceededException e) {
            throw new TapstateException(IoError.DOCUMENT_TOO_LARGE, Map.of("id", id), null);
        } catch (MongoException e) {
            if (DOCUMENT_TOO_LARGE_CODES.contains(errorCode(e))) {
                throw new TapstateException(IoError.DOCUMENT_TOO_LARGE, Map.of("id", id), null);
            }
            throw coded(e);
        }
    }

    /** Runs a store operation with no result, translating a driver failure into a coded io diagnostic. */
    static void run(Runnable operation) {
        run(UNNAMED, operation);
    }

    /** As {@link #run(Runnable)}, naming the document written, so a size failure can point at it. */
    static void run(String id, Runnable operation) {
        call(id, () -> {
            operation.run();
            return null;
        });
    }

    /** Translates a driver failure into its coded io diagnostic (without throwing it). */
    static TapstateException coded(MongoException e) {
        if (DOCUMENT_TOO_LARGE_CODES.contains(errorCode(e))) {
            return new TapstateException(IoError.DOCUMENT_TOO_LARGE, Map.of("id", UNNAMED), null);
        }
        if (e instanceof MongoSecurityException) {
            return new TapstateException(IoError.STORE_UNAUTHORIZED, Map.of(), null);
        }
        if (quotaExceeded(e)) return new TapstateException(IoError.STORE_QUOTA_EXCEEDED, Map.of(), null);
        return new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", detail(e)), null);
    }

    /** Inspect only structured server errors; no server message, response document or cause leaves this adapter. */
    private static boolean quotaExceeded(MongoException failure) {
        if (failure instanceof MongoCommandException command)
            return quotaMessage(command.getErrorCode(), command.getErrorMessage());
        if (failure instanceof MongoWriteException write)
            return quotaMessage(write.getError().getCode(), write.getError().getMessage());
        if (failure instanceof MongoWriteConcernException concern)
            return quotaMessage(concern.getWriteConcernError().getCode(), concern.getWriteConcernError().getMessage());
        if (failure instanceof MongoBulkWriteException bulk) {
            for (var error : bulk.getWriteErrors())
                if (quotaMessage(error.getCode(), error.getMessage())) return true;
            var concern = bulk.getWriteConcernError();
            return concern != null && quotaMessage(concern.getCode(), concern.getMessage());
        }
        return false;
    }

    private static boolean quotaMessage(int code, String message) {
        if (code != 8000 || message == null || message.length() > 8192
                || message.chars().anyMatch(value -> value < 32 || value == 127 || value == 133 || value == 8232 || value == 8233))
            return false;
        var prefix = ATLAS_QUOTA_MESSAGE.matcher(message);
        if (!prefix.lookingAt()) return false;
        if (prefix.end() == message.length()) return true;
        // Atlas may append deployment-specific advice and links. Confirm its fixed blocked-write
        // sentence after the exact quota prefix, but never retain or forward the explanatory tail.
        return message.startsWith(". Writes are blocked on your cluster.", prefix.end());
    }

    /**
     * The code the endpoint answered with. A write failure carries it on the write error it reports rather
     * than on the exception. Bulk failures likewise carry per-document codes below their wrapper;
     * the outer code alone cannot classify the endpoint refusal.
     */
    private static int errorCode(MongoException e) {
        if (e instanceof MongoWriteException write) return write.getError().getCode();
        if (e instanceof MongoBulkWriteException bulk) return bulkErrorCode(bulk);
        if (e instanceof MongoWriteConcernException concern) return concern.getWriteConcernError().getCode();
        return e.getCode();
    }

    private static int bulkErrorCode(MongoBulkWriteException bulk) {
        // The bulk wrapper's code is not the server's per-document refusal. Any size refusal makes
        // replaying the unchanged batch impossible; retain that diagnostic even among other errors.
        for (var error : bulk.getWriteErrors()) {
            if (DOCUMENT_TOO_LARGE_CODES.contains(error.getCode())) return error.getCode();
        }
        if (!bulk.getWriteErrors().isEmpty()) return bulk.getWriteErrors().getFirst().getCode();
        return bulk.getWriteConcernError() == null ? bulk.getCode() : bulk.getWriteConcernError().getCode();
    }

    /** Stable diagnostic metadata from the driver, without untrusted message or cause content. */
    private static String detail(MongoException e) {
        String type = e.getClass().getSimpleName();
        int code = errorCode(e);
        return code <= 0 ? type : type + " code=" + code;
    }
}
