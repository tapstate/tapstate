package io.tapstate.control.restapi;

import io.tapstate.core.common.TapstateException;

/** An explicitly retryable verb refusal; the shared domain code keeps its ordinary status elsewhere. */
final class UnavailableCodedException extends TapstateException {
    UnavailableCodedException(TapstateException cause) { super(cause.code(), cause.args(), cause); }
}
