package io.tapstate.control.client;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Structured result of one HTTP control-plane exchange. */
public sealed interface ControlResponse {

    record Success(int status, Object body) implements ControlResponse {
    }

    /**
     * @param detail what the refusal carried besides its code, message and parameters -- a start stopped by its
     *               start checks carries their report -- and empty when it carried nothing else
     */
    record Rejected(int status, String code, String message, Map<String, Object> params, Map<String, Object> detail)
            implements ControlResponse {
        public Rejected {
            params = params == null ? Map.of() : Map.copyOf(params);
            detail = detail == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(detail));
        }

        public Rejected(int status, String code, String message, Map<String, Object> params) {
            this(status, code, message, params, Map.of());
        }
    }

    record Unreachable() implements ControlResponse {
    }
}
