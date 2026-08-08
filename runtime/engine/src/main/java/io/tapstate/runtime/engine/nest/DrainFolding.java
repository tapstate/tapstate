package io.tapstate.runtime.engine.nest;

/**
 * How many keys one drain may hold locally before it writes back what it has and starts again.
 *
 * <p>Both stateful vertices fold a batch: the state for each key the drain touches is held locally, worked
 * through, and written once at the end, so a key hit many times in one batch costs one write instead of
 * many. Held state is heap, and nothing about a batch bounds how many distinct keys it carries - a wide
 * drain would hold a whole document per key, all of them at once, before writing any of them.
 *
 * <p>The ceiling sits where folding has already stopped paying. What folding saves is proportional to
 * hits per key per drain, and a batch spread over hundreds of distinct keys is one where that ratio is
 * already close to one - the writes were going to happen anyway. Cutting the batch there therefore costs
 * approximately nothing and bounds the heap; putting it much lower would start charging for the batching
 * a busy key relies on, which is where the saving actually comes from.
 *
 * <p>Provisional: the number that would be right is a function of document size, which is a property of
 * the user's data rather than of this code, and there is no measurement of it yet.
 */
final class DrainFolding {

    /** Keys held at once during one drain, after which what is held is written back. */
    static final int MAX_KEYS_HELD = 256;

    private DrainFolding() {
    }
}
