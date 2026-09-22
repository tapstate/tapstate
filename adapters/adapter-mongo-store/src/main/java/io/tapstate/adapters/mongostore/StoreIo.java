package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoWriteException;
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
 * failure maps to {@code io.store-unavailable}
 * carrying the driver's detail. A non-driver throwable — a coded reconstruction failure
 * ({@code io.document-unreadable}) or a bare invariant crash — passes straight through, never
 * relabelled as a driver failure.
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
            throw new TapstateException(IoError.DOCUMENT_TOO_LARGE, Map.of("id", id), e);
        } catch (MongoException e) {
            if (DOCUMENT_TOO_LARGE_CODES.contains(errorCode(e))) {
                throw new TapstateException(IoError.DOCUMENT_TOO_LARGE, Map.of("id", id), e);
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
        if (e instanceof MongoSecurityException) {
            return new TapstateException(IoError.STORE_UNAUTHORIZED, Map.of(), e);
        }
        return new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", detail(e)), e);
    }

    /**
     * The code the endpoint answered with. A write failure carries it on the write error it reports rather
     * than on the exception, so both are read: taking only the exception's own code reads a refused write
     * as code zero, which matches nothing.
     */
    private static int errorCode(MongoException e) {
        return e instanceof MongoWriteException write ? write.getError().getCode() : e.getCode();
    }

    /** The driver's failure detail — its message, or its type when it carries none (never a credential). */
    private static String detail(MongoException e) {
        String message = e.getMessage();
        return message == null ? e.getClass().getSimpleName() : message;
    }
}
