package io.tapstate.core.sql;

import io.tapstate.core.common.TapstateType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The output columns a representative set of statements derives to, locked byte for byte.
 *
 * <p><b>This is the first line of defence for the drift check, not a duplicate of it.</b> The
 * assembly-side check catches a derivation that has changed by refusing to start a pipeline whose
 * recorded columns no longer match - on a user's server, at the worst possible moment, and only for
 * the shapes that user happens to have written. This one catches the same change here, on a
 * dependency upgrade, before it is released. Without it the user-facing code is the discovery
 * mechanism, which means paying for our compatibility breaks in somebody else's downtime.
 *
 * <p>A red here is not automatically a fault. Read what moved: a widened type or a new nullability is
 * a real behaviour change to rule on, and the golden is updated deliberately in the same change set
 * as the reasoning. Regenerating it to get green is how a lock becomes a formality.
 *
 * <p>The statements live here and the answers live in the file, so a red diff names the statement
 * that moved rather than a line number.
 */
class DerivedTypesGoldenTest {

    private static final List<SourceTable> TABLES = List.of(
            new SourceTable("orders", List.of(
                    new SourceColumn("o_id", TapstateType.INT64, false),
                    new SourceColumn("o_cust_id", TapstateType.INT64, true),
                    new SourceColumn("o_qty", TapstateType.INT64, false),
                    new SourceColumn("o_price", TapstateType.DECIMAL, false),
                    new SourceColumn("o_placed", TapstateType.DATETIME, false))),
            new SourceTable("customers", List.of(
                    new SourceColumn("c_id", TapstateType.INT64, false),
                    new SourceColumn("c_name", TapstateType.STRING, false),
                    new SourceColumn("c_tier", TapstateType.STRING, true))));

    private static final String INNER =
            " FROM orders o JOIN customers c ON o.o_cust_id = c.c_id";
    private static final String OUTER =
            " FROM orders o LEFT JOIN customers c ON o.o_cust_id = c.c_id";

    /**
     * One case per kind of derivation the type rules actually decide something about. Plain columns
     * pass a source type through; the rest are worked out, and each of them is a rule that a library
     * upgrade can change under us.
     */
    private static final List<String[]> CASES = List.of(
            new String[] {"plain-columns", "SELECT o.o_id, c.c_name, c.c_tier" + INNER},
            new String[] {"numeric-arithmetic", "SELECT o.o_qty * o.o_price AS amount" + INNER},
            new String[] {"mixed-arithmetic", "SELECT o.o_qty + 1 AS bumped, o.o_price / 2 AS half" + INNER},
            new String[] {"compound-expression",
                    "SELECT (o.o_qty * o.o_price) + o.o_price AS total" + INNER},
            new String[] {"string-function",
                    "SELECT UPPER(c.c_name) AS shouty, SUBSTRING(c.c_name, 1, 3) AS prefix" + INNER},
            new String[] {"concatenation", "SELECT c.c_name || '-' || c.c_tier AS label" + INNER},
            new String[] {"case-expression",
                    "SELECT CASE WHEN o.o_qty > 10 THEN 'bulk' ELSE 'retail' END AS kind" + INNER},
            new String[] {"case-over-nullable",
                    "SELECT CASE WHEN c.c_tier IS NULL THEN 'none' ELSE c.c_tier END AS tier" + INNER},
            new String[] {"literals", "SELECT 1 AS int_lit, 'x' AS str_lit, 1.5 AS dec_lit" + INNER},
            new String[] {"comparison", "SELECT o.o_qty > 10 AS is_bulk" + INNER},
            // The one nobody writes down and everybody relies on: an outer join makes the right side's
            // columns nullable whatever the source said, and a release that stopped doing that would
            // declare a column NOT NULL that holds nulls the first time a fact row misses.
            new String[] {"outer-join-nullability", "SELECT o.o_id, c.c_name, c.c_tier" + OUTER},
            new String[] {"outer-join-expression", "SELECT UPPER(c.c_name) AS shouty" + OUTER});

    @Test
    @DisplayName("the derived output columns match the checked-in golden")
    void derivedTypesMatchGolden() throws IOException {
        List<String> derived = new ArrayList<>();
        for (String[] testCase : CASES) {
            JoinPlan plan = SqlFrontEnd.derive(testCase[1], TABLES);
            for (OutputField field : plan.outputFields()) {
                derived.add(testCase[0] + "\t" + field.name() + "\t" + field.type()
                        + "\t" + (field.nullable() ? "NULL" : "NOT NULL"));
            }
        }
        Path goldenFile = Path.of("src", "test", "resources", "derived-types.golden");
        List<String> golden = Files.readAllLines(goldenFile).stream()
                .filter(line -> !line.isBlank() && !line.startsWith("#"))
                .toList();
        assertThat(derived)
                .as("derived output columns drifted from derived-types.golden. This is a change to what "
                        + "every join in the field produces, so read what moved before touching the file: "
                        + "a pipeline nobody edited will start producing the new shape, and the "
                        + "assembly-side check will refuse to start it until somebody rules on the "
                        + "difference. Update the golden deliberately, in the same change set as the "
                        + "reasoning; regenerating it to get green turns this lock into a formality")
                .isEqualTo(golden);
    }
}
