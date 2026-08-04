package io.tapstate.e2e;

/**
 * One store a specification asks for, named so its address can be referenced.
 *
 * <p>The name is the specification's own handle on it: a request named {@code src} publishes its address
 * under {@code SRC_*}, which the resource files then interpolate. That indirection is what lets a
 * checked-in resource name an endpoint whose address is only known once a container is up.
 *
 * @param kind what sort of store, which settles provisioning, address shape and driver at once
 */
record DatabaseRequest(DatabaseKind kind) {
}
