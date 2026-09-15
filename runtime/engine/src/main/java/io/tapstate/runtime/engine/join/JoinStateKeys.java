package io.tapstate.runtime.engine.join;

import java.util.Objects;

/**
 * How a join map's key becomes the name its state is filed under in the cold layer.
 *
 * <p>The requirement is injectivity, and it is correctness rather than tidiness: two keys sharing one
 * name is one key reading and overwriting another's state, with the right shape and no error anywhere.
 *
 * <p>Two kinds of key are filed here and they must not be able to render alike. A mirror is keyed by a
 * rendered row key; a reverse-index page is keyed by a dimension key and a page number. A page's name
 * begins with the digits of its page number and a mirror's begins with a letter, so no name of one kind
 * can be read as a name of the other; and within a page name the first {@code /} separates the number
 * from the key, which is unambiguous because a number holds no {@code /}.
 */
final class JoinStateKeys {

    /** Introduces a mirror key. A letter, so that it cannot begin a page's name. */
    private static final char ROW = 'k';

    /** Separates a page number from the dimension key whose bucket it belongs to. */
    private static final char OF = '/';

    private JoinStateKeys() {
    }

    /** The name {@code key} is stored under - injective over keys, and readable at a glance. */
    static String nameOf(Object key) {
        Objects.requireNonNull(key, "key");
        if (key instanceof ReverseBucket.At at) {
            return at.page() + String.valueOf(OF) + at.dimensionKey();
        }
        if (key instanceof String row) {
            return ROW + row;
        }
        throw new IllegalArgumentException("a join map is keyed by a value of type "
                + key.getClass().getName() + ", which has no name in the state layer");
    }
}
