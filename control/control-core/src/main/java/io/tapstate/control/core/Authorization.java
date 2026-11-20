package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * The capability check every operation dispatch passes: an authenticated credential may invoke an
 * operation only when its grade is at least the grade the operation requires. A credential that falls
 * short is refused with the coded {@code TS0210004} diagnostic — the caller is known (already
 * authenticated), they simply lack the grade, so this is distinct from an authentication failure and
 * may name the operation and the grade it needs.
 *
 * <p>Pure logic with no dependencies: the grade order lives on {@link Scope#permits(Scope)}, and the
 * refusal is a coded control-domain error. The dispatcher wires this in after authentication and
 * before the audit gate, so an under-privileged caller never reaches an audited effect.
 */
public final class Authorization {

    private Authorization() {
    }

    /**
     * Permits the invocation when {@code credential} carries at least the operation's required grade;
     * throws {@code TS0210004} otherwise, naming the operation and the grade it requires.
     */
    public static void require(VerifiedToken credential, Operation op) {
        Objects.requireNonNull(credential, "credential");
        Objects.requireNonNull(op, "op");
        if (!credential.scope().permits(op.scope())) {
            throw new TapstateException(
                    ControlError.FORBIDDEN,
                    Map.of("op", op.id(), "required", op.scope().name().toLowerCase(Locale.ROOT)),
                    null);
        }
    }
}
