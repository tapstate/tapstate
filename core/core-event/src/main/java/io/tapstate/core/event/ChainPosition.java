package io.tapstate.core.event;

import java.io.Serializable;

/**
 * Where one event sits on one chain: the order the engine assigns it, and the token the connector gave
 * for that spot. The two answer different questions and travel as a pair — {@code order} is what any
 * comparison, minimum or prefix is computed on, {@code token} is what is persisted and handed back to
 * the connector to resume a read.
 *
 * <p>They must stay together. A bound reported without its token cannot be persisted, and a token
 * without its order cannot be compared against anything — which is why a bound is always an event that
 * really happened rather than a value constructed between two of them.
 *
 * <p>Both components may be absent, and absence means different things: a snapshot row is ordered but
 * carries no token, because it is not a spot in a change stream; a synthetic or test event may carry
 * neither. An order that is absent where a stateful node needs one is an engine invariant violation
 * for that node to reject — comparing a missing order would silently reorder data — not something this
 * value judges.
 *
 * <p>{@link Serializable} because held events keep their position while they wait.
 */
public record ChainPosition(SourceOrder order, String token) implements Serializable {
}
