package io.tapstate.control.core;

/**
 * A protocol face that projects the operation registry into its own operation surface.
 *
 * <p>A face may only translate protocol and clip by {@link Maturity}; it composes registered operations,
 * it never invents new ones. REST currently carries the web Pipeline draft authoring surface.
 */
public enum Frontend {
    CLI,
    MCP,
    REST
}
