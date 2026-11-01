package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import java.util.Map;

/** Local cancellation only; authority, native read and cleanup failures retain their own diagnosis. */
final class NativeValidationCancellation extends TapstateException {
    NativeValidationCancellation(String node) {
        super(ValidationError.EXECUTION_CANCELLED, Map.of("node", node), null);
    }
}
