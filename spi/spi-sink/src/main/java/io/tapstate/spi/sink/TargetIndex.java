package io.tapstate.spi.sink;

import java.io.Serializable;
import java.util.List;

/**
 * One index on the table a sink writes to: the fields it covers, in order, whether it is unique, and
 * whether that uniqueness is known to hold for every row.
 *
 * <p>An index travels with the target model rather than being applied out of band, so whoever creates
 * the table creates its indexes in the same act. A store that cannot create indexes ignores them; the
 * model states what the table should have, not how a particular store gets there.
 *
 * <p>Uniqueness is part of the index, not a property of the fields: the same columns can carry a
 * plain index in one table and a unique one in another, and only the second is a claim that they
 * identify a row.
 *
 * <p><b>That claim is not by itself a proof, which is why it is carried as two components.</b> A
 * store reports the unique bit for indexes that constrain only part of a table - a sparse or
 * partially filtered index leaves the rows it does not qualify unconstrained, and a nullable column
 * under a plain unique index does not have its nulls compared against each other - and most
 * discovery hands that bit over with nothing beside it to tell the two apart. So {@code unique}
 * stays what the source said, because that is what a store needs to create the index again, and
 * {@code constrainsEveryRow} is set only where something established that no row escapes it. Whoever
 * chooses a key to match writes on reads the second: keying on a claim nobody proved lands distinct
 * rows on one key and replaces them with each other, and no write path can see that happen.
 *
 * <p>Serializable so a resolved model travels with the sink factory the engine ships onto the DAG.
 */
public record TargetIndex(List<String> fields, boolean unique, boolean constrainsEveryRow)
        implements Serializable {

    public TargetIndex {
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("an index must cover at least one field");
        }
        fields = List.copyOf(fields);
        // An index that constrains no row at all cannot constrain every row. Normalized rather than
        // refused so the two components can only ever say one thing between them.
        constrainsEveryRow = unique && constrainsEveryRow;
    }

    /**
     * An index stated from a key this product chose itself - a declared view key, a join's published
     * fact key - where the uniqueness is its own claim rather than a reading of another catalogue,
     * and a unique one therefore covers every row it writes. An index read from a source's discovery
     * has to use the full form and say separately what, if anything, proved its claim.
     */
    public TargetIndex(List<String> fields, boolean unique) {
        this(fields, unique, unique);
    }
}
