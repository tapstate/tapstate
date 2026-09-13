package io.tapstate.adapters.mongostore;

import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import org.bson.BsonDocument;
import org.bson.BsonMaximumSizeExceededException;
import org.junit.jupiter.api.Test;

import java.util.Set;

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

    /**
     * The other half of the size failure, and the half the driver does not catch: the endpoint itself
     * refuses the write, and reports it as an ordinary command failure. An update whose result would pass
     * the limit answers {@code 17419}, a document that passes it outright {@code 10334}, and neither is
     * distinguishable from a store that could not be reached unless it is named — which is the reading
     * {@code io.document-too-large} exists to prevent, since nothing is wrong with the store and no retry
     * can help.
     */
    @Test
    void mapsAnEndpointSizeRefusalToDocumentTooLargeNamingTheDocument() {
        Throwable fromUpdate = catchThrowable(() -> StoreIo.run("orders@mysql-1", () -> {
            throw new MongoWriteException(
                    new WriteError(17419, "Resulting document after update is larger than 16777216",
                            new BsonDocument()),
                    new ServerAddress(), Set.of());
        }));

        assertThat(fromUpdate).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) fromUpdate).code()).isEqualTo(IoError.DOCUMENT_TOO_LARGE);
        assertThat(((TapstateException) fromUpdate).args()).containsEntry("id", "orders@mysql-1");

        Throwable fromCommand = catchThrowable(() -> StoreIo.run("orders@mysql-1", () -> {
            throw new MongoException(10334, "BSONObj size is invalid");
        }));

        assertThat(((TapstateException) fromCommand).code()).isEqualTo(IoError.DOCUMENT_TOO_LARGE);
    }

    /**
     * And a command failure that is not about size stays the unavailable store, whether it carries its
     * code on the exception or on a write error — the codes are the whole of what tells them apart, so a
     * check that matched too widely would file every rejected write as a document too large.
     */
    @Test
    void leavesACommandFailureThatIsNotAboutSizeUnavailable() {
        Throwable fromCommand = catchThrowable(() -> StoreIo.run("orders@mysql-1", () -> {
            throw new MongoException(26, "ns not found");
        }));

        assertThat(((TapstateException) fromCommand).code()).isEqualTo(IoError.STORE_UNAVAILABLE);

        Throwable fromWrite = catchThrowable(() -> StoreIo.run("orders@mysql-1", () -> {
            throw new MongoWriteException(
                    new WriteError(11000, "E11000 duplicate key error", new BsonDocument()),
                    new ServerAddress(), Set.of());
        }));

        assertThat(((TapstateException) fromWrite).code()).isEqualTo(IoError.STORE_UNAVAILABLE);
    }
}
