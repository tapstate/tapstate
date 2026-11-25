package io.tapstate.control.core;

/** The single local projection from runtime state to the credential-free Cloud report. */
@FunctionalInterface
public interface CloudRuntimeStatusProvider {

    CloudRuntimeStatus snapshot();
}
