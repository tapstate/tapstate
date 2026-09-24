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
 *
 * <p><b>Not every key here is a tuple of partition values.</b> What is held between documents is filed by an
 * address - which embed, and which element inside it - because that address is the one thing both halves of
 * a move arrive at without either knowing anything about the other. Those get {@link #nameOf(List, List)},
 * and the two shapes cannot be read as one another: a rendered path is a balanced JSON array and so ends
 * where it ends, and the character following it is the one that says which shape this is.
 */
final class NestStateKeys {

    /** Separates the rendered values from the letters naming their kinds. */
    private static final char TYPES = '~';

    /** Separates the embed an address names from the identity inside it. */
    private static final char WITHIN = '#';

    /**
     * Introduces which piece of a hand-over an address names, and appears only where there is one. It cannot
     * be read into the name it follows: a name ends in the letters saying what kinds its values were, and no
     * letter is this character or a digit.
     */
    private static final char PIECE = '@';

    private NestStateKeys() {
    }

    /** The name {@code key} is stored under - injective over keys, and readable at a glance. */
    static String nameOf(Object key) {
        // A bucket of the identities pointing at a row says which row places it, so that every bucket of
        // one row is held where that row is. What it is named by is the other question, and the answer is
        // the one it always had: the values it carries, flat.
        if (key instanceof NestLookup.At bucket) {
            return nameOf(bucket.flattened());
        }
        if (key instanceof ParkedSubtree.At at) {
            String address = nameOf(at.pathId(), at.elementKey());
            return at.piece() == 0 ? address : address + PIECE + at.piece();
        }
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
     * The name an address is filed under: which embed, then the identity inside it. Both are needed - one
     * embed's element and another's can carry the same key value, and the same value under two embeds is two
     * different elements.
     *
     * <p>Injective, on the same footing as the tuple form and by one further argument. The embed is rendered
     * as a JSON array and is therefore self-delimiting: no rendering of one path is a prefix of another's,
     * since a strict prefix of a balanced array is not itself balanced. So the two halves cannot be read into
     * each other - a deeper path holding less is never the same name as a shallower one holding more - and
     * the identity beside it carries its own types for the reason every key here does.
     *
     * <p>It cannot be read as a tuple's name either. Both begin with a balanced array where the tuple is one,
     * and what follows that array says which this is: the separator here, the types marker there.
     */
    static String nameOf(List<String> pathId, List<Object> elementKey) {
        return JsonWriter.write(pathId) + WITHIN + nameOf(elementKey);
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
