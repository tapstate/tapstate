package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import org.bson.BsonMaximumSizeExceededException;

import java.util.Map;
import java.util.function.Supplier;

/**
 * The single translation point from Mongo driver exceptions to io-domain coded diagnostics: a store
 * runs its driver call through here, so a driver failure surfaces as a coded {@code io.*} diagnostic
 * and no driver type escapes the module (rule R3). A security failure maps to
 * {@code io.store-unauthorized}; any other driver failure maps to {@code io.store-unavailable}
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

    /** The driver's failure detail — its message, or its type when it carries none (never a credential). */
    private static String detail(MongoException e) {
        String message = e.getMessage();
        return message == null ? e.getClass().getSimpleName() : message;
    }
}
