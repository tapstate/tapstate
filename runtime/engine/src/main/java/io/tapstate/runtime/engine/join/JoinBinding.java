package io.tapstate.runtime.engine.join;

import io.tapstate.core.model.Step;
import io.tapstate.core.sql.JoinPlan;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * What the assembly root supplies for a join node: what the SQL compiles to, what the driving source's
 * rows are identified by, and where the state lives.
 *
 * <p>All three are things the engine deliberately does not work out for itself.
 *
 * <ul>
 *   <li><b>The plan</b> is derived by a SQL front end this ring cannot see - the library that parses
 *       and validates is granted to one core module and to nothing else, and the engine's dependency
 *       on that module excludes it wholesale. So the plan arrives already compiled, which is also what
 *       lets a second execution carrier be handed the same one.
 *   <li><b>The key columns</b> come from what each source declares, which means reading a discovered
 *       schema - a store the engine ring is not allowed to reach for itself. They are not derivable
 *       from the SQL either: a join need not select the driving row's key, and the columns it matches
 *       a dimension on need not identify that dimension row. The first files driving rows; the second
 *       distinguishes two dimension rows that happen to share a join key.
 *   <li><b>The state</b> would mean choosing between a heap and a disk, and between one member and a
 *       cluster. The graph is the same either way.
 *   <li><b>Where a lost dimension row is said</b> is a fourth, for the reason the others are here: this
 *       ring has no logging framework and settles no wording. It knows the row was displaced and
 *       nothing about how an operator should be told.
 * </ul>
 *
 * <p>{@code plans}, {@code factKeyColumns}, and {@code dimensionRowKeyColumns} are asked while the
 * graph is built and stay behind; {@code stores} and {@code displaced} are carried to the member that
 * runs the vertex, which is why they are serializable.
 */
public record JoinBinding(
        Function<Step, JoinPlan> plans,
        Function<Step, List<String>> factKeyColumns,
        Function<Step, Map<String, List<String>>> dimensionRowKeyColumns,
        JoinStoresBinding stores,
        DimensionRowDisplacedAlert displaced) implements Serializable {

    public JoinBinding {
        Objects.requireNonNull(plans, "plans");
        Objects.requireNonNull(factKeyColumns, "factKeyColumns");
        Objects.requireNonNull(dimensionRowKeyColumns, "dimensionRowKeyColumns");
        Objects.requireNonNull(stores, "stores");
        Objects.requireNonNull(displaced, "displaced");
    }

    /** As above, for a caller with nowhere to report a displaced row to. */
    public JoinBinding(Function<Step, JoinPlan> plans, Function<Step, List<String>> factKeyColumns,
            JoinStoresBinding stores) {
        this(plans, factKeyColumns, step -> Map.of(), stores, DimensionRowDisplacedAlert.NONE);
    }

    /** A binding whose state lives on the cluster, which is what every deployment runs. */
    public JoinBinding(Function<Step, JoinPlan> plans, Function<Step, List<String>> factKeyColumns) {
        this(plans, factKeyColumns, JoinStoresBinding.onTheCluster());
    }
}
