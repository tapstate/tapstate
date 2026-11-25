package io.tapstate.control.core;

/** The verified credential mechanism that established a control-plane principal. */
public enum CredentialType {
    HUMAN_JWT,
    MACHINE_TOKEN,
    CLOUD_SESSION
}
