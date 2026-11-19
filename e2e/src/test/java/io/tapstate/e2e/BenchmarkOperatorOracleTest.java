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
