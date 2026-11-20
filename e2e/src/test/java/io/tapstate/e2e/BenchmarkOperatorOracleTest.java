package io.tapstate.e2e;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

class BenchmarkOperatorOracleTest {
    @Test
    void handoffRequiresExactlyOneUpdateAndOneSnapshotReadPerInitialRow() {
        assertTrue(BenchmarkRunIT.handoffCountsMatch(32, 32, 32, 1, 1));
        assertFalse(BenchmarkRunIT.handoffCountsMatch(32, 32, 31, 1, 0));
        assertThrows(RuntimeException.class, () -> BenchmarkRunIT.handoffCountsMatch(32, 32, 32, 2, 2));
        assertThrows(RuntimeException.class, () -> BenchmarkRunIT.handoffCountsMatch(32, 32, 33, 1, 1));
    }

    @Test
    void aggregateReadbackChecksGroupCountsAndSums() {
        var a = BsonDocument.parse("{'_id':1,'value':9}");
        var b = BsonDocument.parse("{'_id':9,'value':-4}");
        var c = BsonDocument.parse("{'_id':2,'value':-6}");
        var expected = BenchmarkOperatorOracle.aggregate(List.of(a, b, c), 8);

        assertEquals(List.of("1", "2"), List.copyOf(expected.keySet()));
        assertTrue(BenchmarkOperatorOracle.equivalent(
                BsonDocument.parse("{'_id':1,'row_count':2,'total_value':5}"), expected.get("1")));
        assertFalse(BenchmarkOperatorOracle.equivalent(
                BsonDocument.parse("{'_id':1,'row_count':2,'total_value':9}"), expected.get("1")));
        assertEquals(List.of("1"), List.copyOf(BenchmarkOperatorOracle.aggregate(List.of(a, b), 8).keySet()));
    }

    @Test
    void numeric_readback_preserves_decimal128_but_allows_integer_width_changes() {
        var decimal = new org.bson.BsonDecimal128(new org.bson.types.Decimal128(new java.math.BigDecimal("12.50")));
        assertFalse(BenchmarkOperatorOracle.equivalent(decimal, new org.bson.BsonDouble(12.5)));
        assertTrue(BenchmarkOperatorOracle.equivalent(decimal, decimal));
        assertTrue(BenchmarkOperatorOracle.equivalent(new org.bson.BsonInt32(12), new org.bson.BsonInt64(12)));
    }

    @Test
    void filteringAndProjectionCheckKeysAndValuesRatherThanCount() {
        var odd = BsonDocument.parse("{'_id':1,'label':'odd','payload':'left','value':2}");
        var even = BsonDocument.parse("{'_id':2,'label':'even','payload':'right','value':7}");
        var input = Map.of("records", List.of(odd, even));

        assertEquals(Map.of("2", even), BenchmarkOperatorOracle.expected("filter-basic", input));
        var actual = BenchmarkOperatorOracle.expected("map-basic", input);
        var expected = BsonDocument.parse("{'_id':2,'payload':'right','display_label':'even','marker':'mapped','adjusted_value':8}");
        assertEquals(2, actual.size());
        assertTrue(BenchmarkOperatorOracle.equivalent(expected, actual.get("2")));
        var wrong = BsonDocument.parse("{'_id':2,'payload':'left','display_label':'even','marker':'mapped','adjusted_value':8}");
        assertFalse(BenchmarkOperatorOracle.equivalent(wrong, actual.get("2")));
        var wrongKey = BsonDocument.parse("{'_id':9,'payload':'right','display_label':'even','marker':'mapped','adjusted_value':8}");
        assertFalse(BenchmarkOperatorOracle.equivalent(wrongKey, actual.get("2")));
    }

    @Test
    void aMissingUnionInputCannotBeReplacedByExtraChangesFromTheOtherInput() {
        var expected = Map.of("left", 2L, "right", 2L);
        assertFalse(BenchmarkRunIT.capturedInputsMatch(expected, Map.of("left", 2L)));
        var excess = assertThrows(RuntimeException.class,
                () -> BenchmarkRunIT.capturedInputsMatch(expected, Map.of("left", 4L)));
        assertTrue(excess.getMessage().contains("Captured extra changes for left"));
        assertTrue(BenchmarkRunIT.capturedInputsMatch(expected, Map.of("left", 2L, "right", 2L)));
    }

    @Test
    void warmupUsesVerifiedPerInputCountsBeforeMeasurementStarts() {
        var operations = new io.tapstate.e2e.benchmark.MongoWorkloadGenerator.Operations(8, 0, 0);
        var stream = new io.tapstate.e2e.benchmark.MongoWorkloadGenerator.StreamReport(
                operations, operations, operations, null, null, null, null, 8, 0, "sequence", null);
        var report = new io.tapstate.e2e.benchmark.MongoWorkloadGenerator.PhaseReport(
                "warmup", null, null, 1, 1, null, 8, 8, 8, null, Map.of("records", stream), true);
        var expected = BenchmarkRunIT.expectedCapturedInputs(Map.of("records", 3L), report);
        assertEquals(Map.of("records", 11L), expected);
        assertTrue(BenchmarkRunIT.capturedInputsMatch(expected, Map.of("records", 11L)));
        assertThrows(RuntimeException.class,
                () -> BenchmarkRunIT.capturedInputsMatch(expected, Map.of("records", 12L)));
    }

    @Test
    void nestingRejectsTheSameMembersInTheWrongOrder() {
        var root = BsonDocument.parse("{'_id':10,'label':'root'}");
        var child = BsonDocument.parse("{'_id':1,'root_id':10,'value':'one'}");
        var next = BsonDocument.parse("{'_id':2,'root_id':10,'value':'two'}");
        var expected = BenchmarkOperatorOracle.expected("nest-basic",
                Map.of("roots", List.of(root), "children", List.of(child, next))).get("10");

        var reversed = root.clone().append("children", new BsonArray(List.of(next, child)));
        assertEquals(2, expected.getArray("children").size());
        assertFalse(BenchmarkOperatorOracle.equivalent(expected, reversed));
    }
}
