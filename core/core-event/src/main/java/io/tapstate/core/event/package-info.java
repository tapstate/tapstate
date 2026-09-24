/**
 * The standard event envelope, as pure data with no third-party dependency.
 *
 * <p>{@link io.tapstate.core.event.Envelope} is one change event as every transform sees it — the
 * common currency of the capture, transform and sink ports. Its {@link io.tapstate.core.event.Op} is
 * a closed set of five change kinds. Mapping a connector's native change event onto this shape
 * lives in an adapter; this package only defines the in-memory shape and its invariants, so the
 * port contracts single-source the event currency without depending on the authoring model.
 *
 * <p>{@link io.tapstate.core.event.PayloadBytes} is the one thing here that is a definition rather
 * than a shape: how many bytes of payload an event carries. Nothing in a running pipeline holds that
 * number - every figure that could be read off a row belongs to a driver, a transport format or the
 * heap - so it is decided here, per kind of the tapstate type namespace, and it lives with the event
 * because it is a property of the event and of nothing that carries one.
 */
package io.tapstate.core.event;
