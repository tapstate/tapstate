package io.tapstate.control.restapi;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.servlet.HandlerExecutionChain;

/** The existing verb controllers of one admitted runtime; this never supplies an alternative operation dispatcher. */
@FunctionalInterface
public interface RuntimeHttpHandlers {
    HandlerExecutionChain handlerFor(HttpServletRequest request) throws Exception;
}
