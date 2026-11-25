package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import org.springframework.security.authentication.AuthenticationServiceException;

/** A safe dependency diagnosis carried through Spring Security without retaining a driver exception. */
final class CodedAuthenticationFailure extends AuthenticationServiceException {
    private final ControlError code;
    CodedAuthenticationFailure(ControlError code) {
        super("authentication dependency unavailable"); this.code = code;
    }
    ControlError code() { return code; }
}
