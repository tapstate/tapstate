package io.tapstate.core.alerting;

import java.util.List;
import java.util.Objects;

/** A bounded, three-valued condition group; there is no script or expression interpreter. */
public final class AlertConditionGroup {
    private AlertConditionGroup() { }
    public enum Operator { ALL, ANY }
    public static AlertSignal.Condition evaluate(Operator operator, List<AlertSignal.Condition> conditions) {
        Objects.requireNonNull(operator, "operator");
        AlertValidation.require(conditions != null && !conditions.isEmpty() && conditions.size() <= 8,
                "conditions", "outside-1-8");
        AlertValidation.require(conditions.stream().allMatch(Objects::nonNull), "conditions", "missing-condition");
        if (operator == Operator.ALL && conditions.contains(AlertSignal.Condition.GOOD)) { return AlertSignal.Condition.GOOD; }
        if (operator == Operator.ANY && conditions.contains(AlertSignal.Condition.BAD)) { return AlertSignal.Condition.BAD; }
        if (conditions.contains(AlertSignal.Condition.UNKNOWN)) { return AlertSignal.Condition.UNKNOWN; }
        return operator == Operator.ALL ? AlertSignal.Condition.BAD : AlertSignal.Condition.GOOD;
    }
}
