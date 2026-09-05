package io.tapstate.e2e;

import io.tapstate.runtime.engine.join.BuiltinJoinExecutor;
import io.tapstate.runtime.engine.join.JoinExecutor;
import io.tapstate.runtime.engine.join.MapJoinStores;
import io.tapstate.testsupport.RequiresDocker;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reference answer machine, run against a real MySQL: a random sequence of changes goes into both
 * the database and a carrier, and the two are asked the same question after every one of them.
 *
 * <p><b>Why random rather than a list of cases.</b> A written case covers the shape whoever wrote it
 * thought of, and the shapes this half of a join gets wrong are the ones nobody pictures - a fact row
 * whose key moves onto a dimension row that is deleted two changes later, a row that missed and then
 * matched and then missed again. A seeded sequence walks those on its own, and the seed is written down
 * so a failure is re-runnable.
 *
 * <p><b>Compared after every change, not at the end.</b> A divergence that heals itself a few changes
 * later is invisible to an end-of-run comparison, and it is not benign: it is the target table holding
 * a wrong row for however long the healing takes, which in a real stream is unbounded.
 *
 * <p><b>The join column is nullable on both sides, which is not decoration.</b> The classic way to get
 * a join wrong is to treat a null key as an ordinary value - SQL says a null matches nothing, a hash
 * table says every null key matches every other one - and the surplus rows that follow look exactly
 * like real ones. That mistake is only observable when both sides can hold a null: with nulls on the
 * driving side alone there is nothing on the other side for them to wrongly match, and the run passes
 * either way. Measured here, on this suite: joining on the customers' primary key, a carrier that had
 * been changed to treat null as an ordinary value passed all three cases. Joining on a nullable column
 * instead, it fails.
 *
 * <p><b>Parameterised over carriers on purpose.</b> This is the acceptance surface every carrier is
 * held to, so a second one is added as a row in {@link #carriers()} rather than as a suite of its own -
 * which is what keeps them held to the same thing rather than to two things that drifted.
 */
@RequiresDocker
class JoinConformanceIT {

    /** How many changes one sequence walks. Enough for the interesting states to be reached repeatedly. */
    private static final int ROUNDS = 200;

    /** Written down rather than drawn, so a failure is re-runnable exactly. */
    private static final long SEED = 20260903L;

    private static final int ORDERS = 25;
    private static final int CUSTOMERS = 8;

    private static final String LEFT_JOIN = """
            SELECT o.id AS order_id, c.name AS customer_name, o.qty * o.price AS amt
            FROM orders o LEFT JOIN customers c ON o.customer_ref = c.ref_no""";

    private static final String INNER_JOIN = """
            SELECT o.id AS order_id, c.name AS customer_name, o.qty * o.price AS amt
            FROM orders o JOIN customers c ON o.customer_ref = c.ref_no""";

    /** Every carrier this run holds to the same answer. A new one is a row here and nothing else. */
    static Stream<Arguments> carriers() {
        BiFunction<List<String>, String, JoinExecutor> builtin =
                (keyColumns, stream) -> new BuiltinJoinExecutor(keyColumns, stream, new MapJoinStores());
        return Stream.of(Arguments.of("builtin", builtin));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("carriers")
    @DisplayName("a random change sequence leaves a left join agreeing with the source's own answer")
    void aRandomSequenceAgreesOnALeftJoin(String carrier,
            BiFunction<List<String>, String, JoinExecutor> factory) throws Exception {
        walkASequence("joinconf_left_" + carrier, LEFT_JOIN, factory);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("carriers")
    @DisplayName("a random change sequence leaves an inner join agreeing with the source's own answer")
    void aRandomSequenceAgreesOnAnInnerJoin(String carrier,
            BiFunction<List<String>, String, JoinExecutor> factory) throws Exception {
        walkASequence("joinconf_inner_" + carrier, INNER_JOIN, factory);
    }

    /**
     * The control group, and it is not decoration: an instrument that reported no differences whatever
     * happened would pass both cases above, and "the carrier is right" and "nothing is being compared"
     * produce the same empty list. Here the database is written behind the carrier's back, so a
     * difference exists by construction and the run says which row it is.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("carriers")
    @DisplayName("a row written behind the carrier's back is reported, so an empty answer means something")
    void aDivergenceIsReported(String carrier,
            BiFunction<List<String>, String, JoinExecutor> factory) throws Exception {
        Map<String, Object> settings = SharedMySql.settings("joinconf_control_" + carrier);
        try (Connection db = SharedMySql.connect(settings)) {
            createSchema(db);
            try (JoinConformance conformance =
                         JoinConformance.of(db, LEFT_JOIN, List.of("order_id"), factory)) {
                conformance.upsert("customers", customer(1L, 1L, "Ada", "unpublished"));
                conformance.upsert("orders", order(10L, 1L, 2L, new BigDecimal("3.50")));
                assertThat(conformance.differences()).isEmpty();

                try (Statement behindItsBack = db.createStatement()) {
                    behindItsBack.executeUpdate(
                            "INSERT INTO orders (id, customer_ref, qty, price) VALUES (11, 1, 4, 1.25)");
                }

                assertThat(conformance.differences())
                        .singleElement(InstanceOfAssertFactories.STRING)
                        .contains("missing")
                        .contains("11");
            }
        }
    }

    private void walkASequence(String database, String sql,
            BiFunction<List<String>, String, JoinExecutor> factory) throws Exception {
        Map<String, Object> settings = SharedMySql.settings(database);
        try (Connection db = SharedMySql.connect(settings)) {
            createSchema(db);
            try (JoinConformance conformance =
                         JoinConformance.of(db, sql, List.of("order_id"), factory)) {
                Random random = new Random(SEED);
                for (int round = 1; round <= ROUNDS; round++) {
                    String change = mutate(conformance, random);
                    assertThat(conformance.differences())
                            .as("round %d, after %s", round, change)
                            .isEmpty();
                }
            }
        }
    }

    /** One change drawn from the sequence, described so a failure names what produced it. */
    private static String mutate(JoinConformance conformance, Random random) throws SQLException {
        int draw = random.nextInt(10);
        if (draw < 4) {
            Map<String, Object> row = order(random.nextInt(ORDERS),
                    random.nextInt(CUSTOMERS + 2) - 1, random);
            conformance.upsert("orders", row);
            return "upsert orders " + row;
        }
        if (draw < 6) {
            long id = random.nextInt(ORDERS);
            conformance.delete("orders", Map.of("id", id));
            return "delete orders " + id;
        }
        if (draw < 9) {
            long id = random.nextInt(CUSTOMERS);
            Map<String, Object> row = customer(id, refOf(id, random), "name-" + random.nextInt(4),
                    "note-" + random.nextInt(4));
            conformance.upsert("customers", row);
            return "upsert customers " + row;
        }
        long id = random.nextInt(CUSTOMERS);
        conformance.delete("customers", Map.of("id", id));
        return "delete customers " + id;
    }

    /**
     * A customer's join column, drawn from three values that are its own alone: its id, its id in a
     * disjoint range, or null.
     *
     * <p>Two things ride on this. <b>Null on this side is what makes a null key observable at all</b> -
     * with nulls on the driving side only there is nothing for them to wrongly match, see the class
     * note. <b>And a dimension row whose own join key moves is the case the admission filter is most
     * dangerous around</b>: the published columns may be untouched while both buckets are wrong, so a
     * filter reading those columns alone discards it and leaves rows joined to a customer they no
     * longer match. Drawing between two ranges makes that move happen throughout the sequence.
     *
     * <p>The values are derived from the id rather than drawn freely so that no two customers ever
     * share a non-null one: the dimension mirror holds one row per join key, so two dimension rows
     * under one key is a fan-out this release cannot state, and a sequence that produced one would be
     * reporting a known limit rather than a defect.
     */
    private static Long refOf(long id, Random random) {
        int draw = random.nextInt(4);
        if (draw == 3) {
            return null;
        }
        return draw == 2 ? id + 100 : id;
    }

    /**
     * One order. {@code customerRef} of -1 is written as null on purpose: a null join key matches
     * nothing in SQL, and the natural implementation treats it as an ordinary value and matches every
     * such row against every other - producing rows that look exactly like real ones.
     */
    private static Map<String, Object> order(int id, int customerRef, Random random) {
        return order((long) id, customerRef < 0 ? null : (long) customerRef,
                (long) (1 + random.nextInt(9)),
                new BigDecimal(1 + random.nextInt(400)).movePointLeft(2));
    }

    private static Map<String, Object> order(Long id, Long customerRef, Long qty, BigDecimal price) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("customer_ref", customerRef);
        row.put("qty", qty);
        row.put("price", price);
        return row;
    }

    private static Map<String, Object> customer(long id, Long ref, String name, String note) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("ref_no", ref);
        row.put("name", name);
        row.put("note", note);
        return row;
    }

    private static void createSchema(Connection db) throws SQLException {
        try (Statement statement = db.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS orders");
            statement.execute("DROP TABLE IF EXISTS customers");
            statement.execute("""
                    CREATE TABLE customers (
                      id BIGINT NOT NULL PRIMARY KEY,
                      ref_no BIGINT,
                      name VARCHAR(64),
                      note VARCHAR(64)
                    )""");
            statement.execute("""
                    CREATE TABLE orders (
                      id BIGINT NOT NULL PRIMARY KEY,
                      customer_ref BIGINT,
                      qty BIGINT,
                      price DECIMAL(10,2)
                    )""");
        }
    }
}
