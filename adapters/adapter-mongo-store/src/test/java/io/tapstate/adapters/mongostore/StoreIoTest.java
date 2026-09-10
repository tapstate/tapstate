package io.tapstate.adapters.mongostore;

import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import org.bson.BsonMaximumSizeExceededException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * StoreIo is the single translation point from driver exceptions to io-domain coded diagnostics, so
 * no driver type escapes the module (rule R3). A driver security failure maps to store-unauthorized,
 * any other driver failure to store-unavailable carrying the driver's detail, and a non-driver
 * throwable passes straight through — a coded reconstruction failure or a bare invariant crash must
 * not be relabelled a driver failure.
 */
class StoreIoTest {

    @Test
    void returnsTheOperationResultOnSuccess() {
        assertThat(StoreIo.call(() -> "ok")).isEqualTo("ok");
    }

    @Test
    void mapsADriverFailureToStoreUnavailableCarryingTheDetail() {
        Throwable thrown = catchThrowable(() -> StoreIo.call(() -> {
            throw new MongoException("connection reset");
        }));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        TapstateException coded = (TapstateException) thrown;
        assertThat(coded.code()).isEqualTo(IoError.STORE_UNAVAILABLE);
        assertThat(coded.args()).containsEntry("detail", "connection reset");
    }

    @Test
    void mapsADriverSecurityFailureToStoreUnauthorized() {
        MongoSecurityException auth = new MongoSecurityException(
                MongoCredential.createCredential("u", "admin", "p".toCharArray()), "auth failed");

        Throwable thrown = catchThrowable(() -> StoreIo.call(() -> {
            throw auth;
        }));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        TapstateException coded = (TapstateException) thrown;
        assertThat(coded.code()).isEqualTo(IoError.STORE_UNAUTHORIZED);
        assertThat(coded.args()).isEmpty();
    }

    @Test
    void mapsADriverFailureWithNoMessageToItsType() {
        Throwable thrown = catchThrowable(() -> StoreIo.call(() -> {
            throw new MongoException((String) null);
        }));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).args()).containsEntry("detail", "MongoException");
    }

    @Test
    void passesANonDriverThrowableThrough() {
        assertThatThrownBy(() -> StoreIo.call(() -> {
            throw new IllegalStateException("bug");
        })).isInstanceOf(IllegalStateException.class).hasMessage("bug");
    }

    /**
     * A document grown past what the store accepts does not arrive as a {@code MongoException}:
     * {@link BsonMaximumSizeExceededException} extends {@code BSONException}, so it went straight
     * past the catch that exists to stop a driver type escaping, and reached callers raw. It is
     * deliberately not the unavailable store: nothing is wrong with the store and a retry cannot
     * succeed, so that code would send whoever read it to check something healthy.
     */
    @Test
    void mapsAnOversizeWriteToDocumentTooLargeNamingTheDocument() {
        Throwable thrown = catchThrowable(() -> StoreIo.run("orders-db", () -> {
            throw new BsonMaximumSizeExceededException("payload document size is larger than maximum");
        }));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        TapstateException coded = (TapstateException) thrown;
        assertThat(coded.code()).isEqualTo(IoError.DOCUMENT_TOO_LARGE);
        assertThat(coded.args()).containsEntry("id", "orders-db");
    }

    @Test
    void mapsAnOversizeWriteOnACallNamingNoDocumentToTheSameCode() {
        Throwable thrown = catchThrowable(() -> StoreIo.call(() -> {
            throw new BsonMaximumSizeExceededException("payload document size is larger than maximum");
        }));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        TapstateException coded = (TapstateException) thrown;
        assertThat(coded.code()).isEqualTo(IoError.DOCUMENT_TOO_LARGE);
        assertThat(coded.args()).containsEntry("id", "unknown");
    }

    /** Carrying the id is for the size failure alone; it must not swallow every other driver failure. */
    @Test
    void namingTheDocumentLeavesAnOrdinaryDriverFailureUnavailable() {
        Throwable thrown = catchThrowable(() -> StoreIo.run("orders-db", () -> {
            throw new MongoException("connection reset");
        }));

        assertThat(((TapstateException) thrown).code()).isEqualTo(IoError.STORE_UNAVAILABLE);
    }
}
