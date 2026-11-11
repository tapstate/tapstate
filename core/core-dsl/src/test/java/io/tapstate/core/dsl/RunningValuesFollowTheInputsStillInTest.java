package io.tapstate.core.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.dsl.aggregate.AggregateFailure;
import io.tapstate.core.dsl.aggregate.Accumulator;
import io.tapstate.core.dsl.aggregate.Aggregates;
import io.tapstate.core.dsl.aggregate.Contribution;
import io.tapstate.core.dsl.aggregate.RollupError;
import io.tapstate.core.dsl.aggregate.RunningValue;
import io.tapstate.core.dsl.aggregate.ValueType;
import io.tapstate.core.model.RollupFunction;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * A value kept across adds and retracts is a function of the inputs still in: the same as folding exactly
 * those inputs from nothing, whatever came and went before.
 */
class RunningValuesFollowTheInputsStillInTest {

    private static final ValueType MONEY = ValueType.decimal(2);

    @Test
    void anIntegerSumEqualsTheLibraryFoldOverTheSurvivors() {
        survivorsDecide(RollupFunction.SUM, ValueType.INT64, random -> (Object) (long) random.nextInt(-1000, 1000));
        survivorsDecide(RollupFunction.AVG, ValueType.INT64, random -> (Object) (long) random.nextInt(-1000, 1000));
        survivorsDecide(RollupFunction.COUNT, ValueType.INT64, random -> (Object) (long) random.nextInt(3));
    }

    @Test
    void aDecimalSumEqualsTheLibraryFoldOverTheSurvivors() {
        Function<Random, Object> cents = random -> BigDecimal.valueOf(random.nextLong(-10_000_000, 10_000_000), 2);
        survivorsDecide(RollupFunction.SUM, MONEY, cents);
        survivorsDecide(RollupFunction.AVG, MONEY, cents);
    }

    /** Runs random adds and retracts, comparing with the library's fold of the survivors after every step. */
    private static void survivorsDecide(RollupFunction function, ValueType argument, Function<Random, Object> next) {
        Random random = new Random(20261111L + function.ordinal() * 31L + argument.type().ordinal());
        for (int run = 0; run < 50; run++) {
            RunningValue running = RunningValue.of(function, argument);
            List<Object> in = new ArrayList<>();
            for (int step = 0; step < 200; step++) {
                if (!in.isEmpty() && random.nextInt(100) < 45) {
                    Object gone = in.remove(random.nextInt(in.size()));
                    running.retract(gone);
                } else {
                    Object value = next.apply(random);
                    in.add(value);
                    running.add(value);
                }
                if (random.nextInt(10) == 0) {
                    running = RunningValue.restore(function, argument, running.state());
                }
                Accumulator fold = Aggregates.of(function).init(argument);
                in.forEach(value -> fold.add(Contribution.of(value, List.of())));
                assertThat(running.result()).as("%s after step %d of run %d over %s", function, step, run, in)
                        .isEqualTo(fold.result());
                assertThat(running.contributed()).isEqualTo(fold.contributed());
            }
        }
    }

    @Test
    void anIntegerSumThatLeftTheRangeIsAValueAgainOnceItFits() {
        RunningValue sum = RunningValue.of(RollupFunction.SUM, ValueType.INT64);
        sum.add(Long.MAX_VALUE);
        sum.add(5L);
        assertThatThrownBy(sum::result).isInstanceOfSatisfying(AggregateFailure.class,
                failure -> assertThat(failure.code()).isEqualTo(RollupError.OVERFLOW));
        // A total that wrapped or was refused at the add could not come back: the 5 would be lost.
        sum.retract(Long.MAX_VALUE);
        assertThat(sum.result()).isEqualTo(5L);
    }

    @Test
    void aDecimalSumKeepsEveryDigitADoubleWouldRound() {
        RunningValue sum = RunningValue.of(RollupFunction.SUM, MONEY);
        sum.add(new BigDecimal("9007199254740993.01"));
        sum.add(new BigDecimal("0.02"));
        assertThat(sum.result()).isEqualTo(new BigDecimal("9007199254740993.03"));
        assertThat(new BigDecimal(9007199254740993.01d + 0.02d)).isNotEqualByComparingTo((BigDecimal) sum.result());
    }

    @Test
    void aDecimalSumPastThirtyEightDigitsIsWithheldAndComesBack() {
        RunningValue sum = RunningValue.of(RollupFunction.SUM, MONEY);
        BigDecimal widest = new BigDecimal("999999999999999999999999999999999999.99");
        sum.add(widest);
        sum.add(new BigDecimal("0.01"));
        assertThatThrownBy(sum::result).isInstanceOf(AggregateFailure.class);
        sum.retract(new BigDecimal("0.01"));
        assertThat(sum.result()).isEqualTo(widest);
    }

    @Test
    void aFloatingSumReturnsExactlyToWhereItWas() {
        RunningValue sum = RunningValue.of(RollupFunction.SUM, ValueType.DOUBLE);
        sum.add(0.1);
        // A running double would keep the rounding of 1e17 + 0.1 and give 0.0 or 16.0 here.
        sum.add(1e17);
        sum.retract(1e17);
        assertThat(sum.result()).isEqualTo(0.1);
    }

    @Test
    void aFloatingSumIsTheExactSumRoundedOnce() {
        Random random = new Random(7);
        for (int run = 0; run < 200; run++) {
            RunningValue sum = RunningValue.of(RollupFunction.SUM, ValueType.DOUBLE);
            RunningValue average = RunningValue.of(RollupFunction.AVG, ValueType.DOUBLE);
            List<Double> in = new ArrayList<>();
            for (int step = 0; step < 60; step++) {
                if (!in.isEmpty() && random.nextInt(100) < 40) {
                    Double gone = in.remove(random.nextInt(in.size()));
                    sum.retract(gone);
                    average.retract(gone);
                } else {
                    double value = (random.nextDouble() - 0.5) * Math.pow(10, random.nextInt(-20, 20));
                    in.add(value);
                    sum.add(value);
                    average.add(value);
                }
            }
            BigDecimal exact = in.stream().map(BigDecimal::new).reduce(BigDecimal.ZERO, BigDecimal::add);
            if (in.isEmpty()) {
                assertThat(sum.result()).isNull();
                assertThat(average.result()).isNull();
            } else {
                assertThat(sum.result()).isEqualTo(exact.doubleValue());
                assertThat(average.result()).isEqualTo(exact.doubleValue() / in.size());
            }
        }
    }

    @Test
    void anInfinityOrNotANumberLeavesWhenItIsRetracted() {
        RunningValue sum = RunningValue.of(RollupFunction.SUM, ValueType.DOUBLE);
        sum.add(2.5);
        sum.add(Double.POSITIVE_INFINITY);
        assertThat(sum.result()).isEqualTo(Double.POSITIVE_INFINITY);
        sum.add(Double.NEGATIVE_INFINITY);
        assertThat((Double) sum.result()).isNaN();
        sum.retract(Double.POSITIVE_INFINITY);
        assertThat(sum.result()).isEqualTo(Double.NEGATIVE_INFINITY);
        sum.retract(Double.NEGATIVE_INFINITY);
        sum.add(Double.NaN);
        assertThat((Double) sum.result()).isNaN();
        sum.retract(Double.NaN);
        assertThat(sum.result()).isEqualTo(2.5);
    }

    @Test
    void aValueOfAnotherTypeIsRefusedBeforeItChangesAnything() {
        RunningValue sum = RunningValue.of(RollupFunction.SUM, MONEY);
        sum.add(new BigDecimal("1.50"));
        assertThatThrownBy(() -> sum.add(new BigDecimal("1.505"))).isInstanceOfSatisfying(AggregateFailure.class,
                failure -> assertThat(failure.code()).isEqualTo(RollupError.VALUE_TYPE_MISMATCH));
        assertThatThrownBy(() -> sum.add(3L)).isInstanceOf(AggregateFailure.class);
        assertThat(sum.result()).isEqualTo(new BigDecimal("1.50"));
        assertThat(sum.inputs()).isEqualTo(1);
    }

    @Test
    void anEmptyValueIsNullAndAnEmptyCountIsZero() {
        RunningValue sum = RunningValue.of(RollupFunction.SUM, ValueType.INT64);
        sum.add(4L);
        sum.retract(4L);
        assertThat(sum.result()).isNull();
        assertThat(sum.contributed()).isFalse();
        assertThat(RunningValue.of(RollupFunction.COUNT, ValueType.INT64).result()).isEqualTo(0L);
    }

    @Test
    void aFunctionThatKeepsItsInputsHasNoRunningValue() {
        assertThatThrownBy(() -> RunningValue.of(RollupFunction.MAX, ValueType.INT64))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
