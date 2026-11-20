package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The capability check: an authenticated credential may invoke an operation only when its grade is at
 * least the grade the operation requires. A credential that carries enough passes silently; one that
 * falls short is refused with the coded {@code TS0210004} diagnostic, which names the
 * operation and the grade it needs (safe to echo — the caller is already authenticated).
 */
class AuthorizationTest {

    // Real registered operations of each grade, so the check is exercised against actual scopes.
    private static final Operation READ_OP = ControlOperations.ARTIFACT_GET;    // read
    private static final Operation WRITE_OP = ControlOperations.ARTIFACT_APPLY; // write
    private static final Operation ADMIN_OP = ControlOperations.TOKEN_CREATE;   // admin

    @Test
    void aCredentialAtOrAboveTheRequiredGradePasses() {
        assertThatCode(() -> Authorization.require(credential(Scope.WRITE), WRITE_OP)).doesNotThrowAnyException();
        assertThatCode(() -> Authorization.require(credential(Scope.ADMIN), WRITE_OP)).doesNotThrowAnyException();
        assertThatCode(() -> Authorization.require(credential(Scope.READ), READ_OP)).doesNotThrowAnyException();
        assertThatCode(() -> Authorization.require(credential(Scope.ADMIN), ADMIN_OP)).doesNotThrowAnyException();
    }

    @Test
    void aReadCredentialIsForbiddenFromAWriteOperation() {
        TapstateException thrown = catchThrowableOfType(TapstateException.class,
                () -> Authorization.require(credential(Scope.READ), WRITE_OP));

        assertThat(thrown).isNotNull();
        assertThat(thrown.code().code()).isEqualTo("TS0210004");
        // The operation and the grade it needs are echoed to help the caller, not the credential's own grade.
        assertThat(thrown.args()).containsEntry("op", "artifact.apply").containsEntry("required", "write");
    }

    @Test
    void aWriteCredentialIsForbiddenFromAnAdminOperation() {
        TapstateException thrown = catchThrowableOfType(TapstateException.class,
                () -> Authorization.require(credential(Scope.WRITE), ADMIN_OP));

        assertThat(thrown).isNotNull();
        assertThat(thrown.code().code()).isEqualTo("TS0210004");
        assertThat(thrown.args()).containsEntry("op", "token.create").containsEntry("required", "admin");
    }

    @Test
    void requireRejectsNullArguments() {
        assertThatThrownBy(() -> Authorization.require(null, WRITE_OP)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Authorization.require(credential(Scope.ADMIN), null))
                .isInstanceOf(NullPointerException.class);
    }

    private static VerifiedToken credential(Scope scope) {
        return new VerifiedToken("principal", scope);
    }
}
