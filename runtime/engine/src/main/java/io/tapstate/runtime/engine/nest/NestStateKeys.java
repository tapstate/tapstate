package io.tapstate.runtime.engine.nest;

import io.tapstate.core.common.JsonWriter;
import java.util.List;

/**
 * How a vertex's key becomes the name its state is filed under in the cold layer.
 *
 * <p>The key inside the engine is the tuple of values the vertex is partitioned by; the layer below
 * stores under a name. The rendering has one hard requirement and one soft one. It must be
 * <b>injective</b> - two keys that are not equal must never render the same, because two keys sharing a
 * name is one key reading and overwriting another's state, silently and with the right shape. And it
 * should stay readable, because the name is what an operator sees when looking at what was stored.
 *
 * <p>Rendering the values alone satisfies neither on its own: {@code 1} the whole number and {@code 1.0}
 * the decimal are different keys that a reader is free to render alike, and a string is only kept apart
 * from a number by its quotes. So the rendering carries the types beside the values: the values as
 * compact JSON, then a letter per value naming which kind it was. Equal renderings then mean equal types
 * and equal values within each type, which is equality.
 *
 * <p>A key value of a kind that has no letter here is a programmer error - the engine chose the
 * partitioning fields, and a kind nothing can name is a defect in that choice rather than anything a user
 * did. It crashes bare rather than being given a name that might collide with another kind's.
 */
final class NestStateKeys {

    /** Separates the rendered values from the letters naming their kinds. */
    private static final char TYPES = '~';

    private NestStateKeys() {
    }

    /** The name {@code key} is stored under - injective over keys, and readable at a glance. */
    static String nameOf(Object key) {
        StringBuilder types = new StringBuilder();
        if (key instanceof List<?> values) {
            for (Object value : values) {
                types.append(letterOf(value));
            }
        } else {
            types.append(letterOf(key));
        }
        return JsonWriter.write(key) + TYPES + types;
    }

    /**
     * The letter naming what kind of value this is. Kinds that render alike must not share one: whole
     * numbers and decimals both render as digits, and a decimal keeps its own letter for that reason.
     */
    private static char letterOf(Object value) {
        return switch (value) {
            case null -> 'n';
            case String ignored -> 's';
            case Boolean ignored -> 'b';
            case Long ignored -> 'l';
            case Integer ignored -> 'i';
            case Short ignored -> 'h';
            case Byte ignored -> 'y';
            case Double ignored -> 'd';
            case Float ignored -> 'f';
            case java.math.BigDecimal ignored -> 'm';
            case java.math.BigInteger ignored -> 'g';
            default -> throw new IllegalArgumentException(
                    "a nest vertex is partitioned by a value of type " + value.getClass().getName()
                            + ", which has no name in the state layer");
        };
    }
}
