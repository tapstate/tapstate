package io.tapstate.spi.capture;

import io.tapstate.core.logging.LogSink;
import io.tapstate.core.model.PipelineNode;

/**
 * Optional capture capability for an explicitly admitted logical run's diagnostic owner. The returned
 * view freezes that owner for every handle it opens; it must never borrow a caller's context or rebind
 * an already opened physical reader. Ports without connector logs keep the ordinary capture contract.
 */
public interface LogScopedCapturePort {

    /** Returns a capture view whose handles retain this node and execution's log identity. */
    CapturePort forLogOwner(PipelineNode node, LogSink.Scope scope);
}
