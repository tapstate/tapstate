package io.tapstate.core.sql;

import java.util.List;

/**
 * What a join declaration means, in terms no execution carrier appears in.
 *
 * <p>This type is the boundary the carrier is swapped behind: nothing in its signature may name a
 * type from the SQL library that produced it, because a consumer that can see those types ends up
 * depending on them and the swap stops being possible.
 *
 * <p>It carries the output row today. The rest of what a carrier needs -- which table is the fact
 * side, which are dimensions, on which keys, and the join kind -- lands here as it is derived.
 */
public record JoinPlan(List<OutputField> outputFields) {
}
