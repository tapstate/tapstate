package io.tapstate.adapters.pdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.DataBrowserChange;
import io.tapstate.spi.store.DataBrowserFilter.All;
import io.tapstate.spi.store.DataBrowserFilter.Any;
import io.tapstate.spi.store.DataBrowserFilter.Match;
import io.tapstate.spi.store.DataBrowserFilter.Operator;
import io.tapstate.spi.store.DataBrowserPreview;
import io.tapstate.spi.store.DataBrowserQuery;
import io.tapstate.spi.store.DataBrowserSort;
import io.tapstate.spi.store.DataBrowserSort.Direction;
import io.tapstate.spi.store.DataBrowserSubscription;
import io.tapstate.spi.store.DataBrowserTableInfo;
import io.tapstate.spi.store.DataBrowserTailRequest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The data-browser PDK bridge: {@link PdkDataBrowser} driving the three read-face functions a connector
 * may register — {@code getTableNames}, {@code getTableInfo} and {@code executeCommand}. Synthetic
 * connectors compiled at test time prove the drive and the coded-error paths without a real connector
 * jar or the PDK runtime; each is shaped after the behaviour a real connector actually exhibits, so a
 * drive that holds here holds there.
 */
class PdkDataBrowserTest {

    private final List<PdkDataBrowser> readers = new ArrayList<>();

    @AfterEach
    void closeReaders() {
        readers.forEach(PdkDataBrowser::close);
        System.clearProperty("synthetic.marker");
    }

    @Test
    void followLeavesOutASchemaChangeRatherThanPassingItAlongAsNothing(@TempDir Path dir) throws Exception {
        // A stream carries schema changes as well as rows, and a view of rows has nowhere to put one. It
        // is left out here, which is invisible from the outside unless something asks: passing it along
        // as the nothing it projects to reads the same way at this seam, and only breaks further on, in
        // whoever unpacks the change. So the case counts what the follower is handed, not what it shows.
        PdkDataBrowser reader = reader(Synthetic.ddlEmittingSource(dir), "synthetic.DdlEmittingSource");
        List<DataBrowserChange> handed = java.util.Collections.synchronizedList(new ArrayList<>());
        // Counts every delivery, not every row: a delivery that carries nothing has to open the latch
        // too, or the assertion below would be waiting for a row that a broken seam never sends.
        CountDownLatch twoDeliveries = new CountDownLatch(2);

        try (DataBrowserSubscription following = reader.tail(config(),
                new DataBrowserTailRequest("t1"), change -> {
                    handed.add(change);
                    twoDeliveries.countDown();
                })) {
            assertThat(twoDeliveries.await(5, TimeUnit.SECONDS))
                    .as("two deliveries reached the follower")
                    .isTrue();
        }

        assertThat(handed)
                .as("the schema change between them was left out, not handed over as nothing")
                .doesNotContainNull();
        assertThat(handed).extracting(DataBrowserChange::kind)
                .containsExactly(DataBrowserChange.Kind.INSERT, DataBrowserChange.Kind.INSERT);
    }

    @Test
    void followRendersAValueNoJsonWriterKnowsAsItsOwnTextRatherThanHandingTheObjectOn(@TempDir Path dir)
            throws Exception {
        // A connector hands on whatever its driver produced, and a document store's key arrives as a
        // driver object. Everything downstream of this seam writes rows as JSON, where such a value has
        // no spelling: passing it on ends the stream on the connector's own thread, which reads from
        // outside exactly like a collection nobody is changing. So the rendering happens here, at the
        // seam that knows a value came from a connector, rather than being caught where it breaks.
        PdkDataBrowser reader = reader(Synthetic.opaqueValueSource(dir), "synthetic.OpaqueValue");
        List<DataBrowserChange> handed = java.util.Collections.synchronizedList(new ArrayList<>());
        CountDownLatch delivered = new CountDownLatch(1);

        try (DataBrowserSubscription following = reader.tail(config(),
                new DataBrowserTailRequest("t1"), change -> {
                    handed.add(change);
                    delivered.countDown();
                })) {
            assertThat(delivered.await(5, TimeUnit.SECONDS))
                    .as("the insert reached the follower")
                    .isTrue();
        }

        Map<String, Object> after = handed.get(0).after();
        assertThat(after.get("key"))
                .as("the driver's own value is handed on as its text, not as the object")
                .isEqualTo("00000000-0000-0000-0000-00000000002a");
        assertThat(((Map<?, ?>) after.get("meta")).get("ref"))
                .as("a value nested in a document is rendered too, not only a top-level one")
                .isEqualTo("00000000-0000-0000-0000-00000000002a");
        assertThat(((List<?>) after.get("refs")).get(0))
                .as("a value inside a list is rendered too")
                .isEqualTo("00000000-0000-0000-0000-00000000002a");
        // The half that discriminates: rendering every value as text would satisfy the three above
        // while turning a number into a string, which no reader of this face could tell from a column
        // that really holds text.
        // The box is the PDK's business (an int arrives widened); what this pins is that it is still a
        // number rather than the text of one.
        assertThat(after.get("id")).as("a number stays a number").isInstanceOf(Number.class);
        assertThat(((Number) after.get("id")).longValue()).isEqualTo(7L);
        assertThat(after.get("flag")).as("a boolean stays a boolean").isEqualTo(Boolean.TRUE);
        assertThat(after.get("name")).as("text stays text").isEqualTo("row-7");
    }

    /** A reader over a provisioner that hands back one fixed connector ref, whatever id is asked for. */
    private PdkDataBrowser reader(Path jar, String className) {
        return reader(jar, className, ConnectorInstancePool.DEFAULTS);
    }

    private PdkDataBrowser reader(Path jar, String className, ConnectorInstancePool.Limits limits) {
        ConnectorRef ref = new ConnectorRef(List.of(jar), className, "2.0.8", null);
        PdkDataBrowser reader = new PdkDataBrowser(connectorId -> ref, limits, Clock.systemUTC());
        readers.add(reader);
        return reader;
    }

    private static ConnectionConfig config() {
        return new ConnectionConfig("conn-1", "demo", Map.of());
    }

    private static ConnectionConfig config(String database) {
        return new ConnectionConfig("conn-1", "demo", Map.of("database", database));
    }

    /**
     * Points the lifecycle-recording connector at a file in {@code dir} and hands it back. Reading it
     * is how a test counts drives of a connector that gets a fresh class loader every time it is
     * opened — which is precisely what makes an in-connector counter useless here.
     */
    private static Path marker(Path dir) {
        Path marker = dir.resolve("lifecycle.log");
        System.setProperty("synthetic.marker", marker.toString());
        return marker;
    }

    private static List<String> drives(Path marker) throws IOException {
        return Files.exists(marker) ? Files.readAllLines(marker) : List.of();
    }

    // ---- the pooled instance ---------------------------------------------------------------------

    @Test
    void reusesOneConnectorAcrossReadsOfTheSameConnection(@TempDir Path dir) throws IOException {
        // Opening is a class loader, a linked jar and a constructed connector, and initializing is what
        // builds the driver's own connection pool behind it. Paying that per query is what rules out any
        // caller that reads on a timer.
        PdkDataBrowser reader = reader(Synthetic.lifecycleRecordingSource(dir), "synthetic.LifecycleRecording");
        Path marker = marker(dir);

        reader.collections(config());
        reader.collections(config());

        assertThat(drives(marker)).containsExactly("init");
    }

    @Test
    void opensASecondConnectorOnceTheConnectionSettingsChange(@TempDir Path dir) throws IOException {
        // The instance holds the settings it was opened with, so an applied change has to reach the next
        // read. Kept across it, the read answers from the old database and reports nothing wrong.
        PdkDataBrowser reader = reader(Synthetic.lifecycleRecordingSource(dir), "synthetic.LifecycleRecording");
        Path marker = marker(dir);

        reader.collections(config("one"));
        reader.collections(config("two"));

        assertThat(drives(marker)).containsExactly("init", "init");
    }

    @Test
    void stopsThePooledConnectorWhenTheReaderCloses(@TempDir Path dir) throws IOException {
        // A pooled instance is live: it holds its class loader open and its driver's connections with it,
        // so shutting the face down has to hand them back rather than drop the reference.
        PdkDataBrowser reader = reader(Synthetic.lifecycleRecordingSource(dir), "synthetic.LifecycleRecording");
        Path marker = marker(dir);
        reader.collections(config());

        reader.close();

        assertThat(drives(marker)).containsExactly("init", "stop");
    }

    @Test
    void stopsAConnectorThatHasSatIdleWithoutAnyFurtherReads(@TempDir Path dir) throws Exception {
        // Eviction has to happen on its own. Checked only when the next read arrives, an idle instance on
        // a face nobody is using holds its connections for as long as nobody uses it - which is exactly
        // when they should have been given back.
        ConnectorInstancePool.Limits limits = ConnectorInstancePool.DEFAULTS.withIdle(Duration.ofMillis(50));
        PdkDataBrowser reader = reader(Synthetic.lifecycleRecordingSource(dir), "synthetic.LifecycleRecording", limits);
        Path marker = marker(dir);
        reader.collections(config());

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!drives(marker).contains("stop") && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }

        assertThat(drives(marker)).containsExactly("init", "stop");
    }

    // ---- getTableNames ---------------------------------------------------------------------------

    @Test
    void collectionsCollectsEveryBatchTheConnectorEmits(@TempDir Path dir) {
        // The function hands its names to a consumer it may call more than once - mongodb calls it per
        // batchSize names. A drive that keeps only the batch it saw last silently loses collections, and
        // a lost collection reads downstream as "that collection does not exist".
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        List<String> names = reader.collections(config());

        assertThat(names).containsExactly("orders", "shipments");
    }

    @Test
    void collectionsFailsWithACodeWhenTheConnectorDoesNotRegisterIt(@TempDir Path dir) {
        PdkDataBrowser reader = reader(Synthetic.emittingSource(dir), "synthetic.EmittingSource");

        assertThatThrownBy(() -> reader.collections(config()))
                .isInstanceOf(TapstateException.class)
                .extracting(e -> ((TapstateException) e).code().code())
                .isEqualTo("connector.capability-missing");
    }

    // ---- getTableInfo ----------------------------------------------------------------------------

    @Test
    void statsCarriesTheRowCountAndSizesTheConnectorReports(@TempDir Path dir) {
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        DataBrowserTableInfo info = reader.stats(config(), "orders");

        assertThat(info.numOfRows()).isEqualTo(512L);
        assertThat(info.storageSize()).isEqualTo(4096L);
        assertThat(info.avgObjSize()).isEqualTo(8L);
    }

    @Test
    void statsFailsWithACodeWhenTheConnectorDoesNotRegisterIt(@TempDir Path dir) {
        PdkDataBrowser reader = reader(Synthetic.emittingSource(dir), "synthetic.EmittingSource");

        assertThatThrownBy(() -> reader.stats(config(), "orders"))
                .isInstanceOf(TapstateException.class)
                .extracting(e -> ((TapstateException) e).code().code())
                .isEqualTo("connector.capability-missing");
    }

    // ---- executeCommand --------------------------------------------------------------------------

    @Test
    void aConversionTheConnectorRegisteredIsRunAndItsResultIsWhatTheFollowerSees(@TempDir Path dir)
            throws Exception {
        // The whole chain in one run: the registry a connector writes into is kept, the conversion it
        // registered is applied to the row, and what reaches a follower is the converted value rather
        // than the carrier holding it. The conversion produces a string nothing else on this path
        // produces, so "it never ran" and "it ran" cannot be read as each other - without it, the
        // driver object renders as its own text, which is a perfectly plausible-looking answer.
        PdkDataBrowser reader = reader(Synthetic.codecValueSource(dir), "synthetic.CodecValue");
        List<DataBrowserChange> handed = java.util.Collections.synchronizedList(new ArrayList<>());
        CountDownLatch delivered = new CountDownLatch(1);

        try (DataBrowserSubscription following = reader.tail(config(),
                new DataBrowserTailRequest("t1"), change -> {
                    handed.add(change);
                    delivered.countDown();
                })) {
            assertThat(delivered.await(5, TimeUnit.SECONDS)).isTrue();
        }

        Map<String, Object> after = handed.get(0).after();
        assertThat(after.get("key"))
                .as("the connector's own conversion ran, and its result is what travels")
                .isEqualTo("converted:00000000-0000-0000-0000-00000000002a");
        assertThat(((Map<?, ?>) after.get("meta")).get("ref"))
                .as("a value one level down takes the same lane")
                .isEqualTo("converted:00000000-0000-0000-0000-00000000002a");
        // The connector registers nothing for these, and the frozen surface registers nothing for them
        // either: they must come through bare, not wrapped for the sake of a uniform row.
        assertThat(after.get("id")).isInstanceOf(Number.class);
        assertThat(after.get("name")).isEqualTo("row-7");
    }


    @Test
    void theQueryFaceRendersAValueTheSameWayTheFollowFaceDoes(@TempDir Path dir) {
        // The same document read two ways must read as the same document. It did not: only the follow
        // face rendered, and the query face handed the driver's object on to be serialized by whatever
        // sat above - so a key came back as its own text on one face and as a two-field object on the
        // other, and a reader comparing them saw two rows. Neither face lost anything; they disagreed.
        PdkDataBrowser reader = reader(Synthetic.opaqueQuerySource(dir), "synthetic.OpaqueQuery");

        Map<String, Object> row = reader.find(config(), new DataBrowserQuery("orders", null, 10)).rows().get(0);

        assertThat(row.get("key")).isEqualTo("00000000-0000-0000-0000-00000000002a");
        assertThat(((Map<?, ?>) row.get("meta")).get("ref"))
                .as("a value nested in a document is rendered on this face too")
                .isEqualTo("00000000-0000-0000-0000-00000000002a");
        assertThat(((List<?>) row.get("refs")).get(0))
                .as("a value inside a list is rendered on this face too")
                .isEqualTo("00000000-0000-0000-0000-00000000002a");
        // The half that discriminates, same as on the follow face: rendering everything as text would
        // satisfy the three above while turning a number into a string no reader could tell from text.
        assertThat(row.get("id")).as("a number stays a number").isInstanceOf(Number.class);
        assertThat(row.get("flag")).as("a boolean stays a boolean").isEqualTo(Boolean.TRUE);
        assertThat(row.get("name")).as("text stays text").isEqualTo("row-7");
    }


    @Test
    void findPinsTheCommandToExecuteQuery(@TempDir Path dir) {
        // The command name is the connector's dispatch key: "execute" and "update" reach write paths on
        // the same function. It is assembled here and is not a caller input, so the read face has no
        // spelling that reaches anything but a query.
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery("orders", null, 10));

        assertThat(echoed(preview, "command")).isEqualTo("executeQuery");
    }

    @Test
    void findCollectsEveryResultBatchTheConnectorEmits(@TempDir Path dir) {
        // executeQuery hands its rows to a consumer per batch (mongodb's default batch is 1000), so a
        // drive that assumes one callback returns a truncated page - and a truncated page is read
        // downstream as "that is all there is", with nothing reporting otherwise.
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery("orders", null, 10));

        assertThat(preview.rows()).hasSize(5);
    }

    @Test
    void findCarriesTheRequestedSortIntoTheParams(@TempDir Path dir) {
        // The seam carries an order as a neutral field-and-direction pair; turning that into the encoding
        // one connector's query expects belongs here, in the bridge that already knows which connector it
        // is driving, and nowhere above it.
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        DataBrowserPreview preview = reader.find(config(),
                new DataBrowserQuery("orders", null, new DataBrowserSort("status", Direction.DESC), 10));

        assertThat(echoed(preview, "sort")).isEqualTo(Map.of("status", -1));
    }

    @Test
    void findOmitsTheSortParamWhenTheRequestAsksForNoOrder(@TempDir Path dir) {
        // No order means the database's own, and that is a real answer rather than a missing one. Sending
        // an empty or null sort instead would be a request for an order nobody asked for, which is the one
        // thing this face promised not to impose.
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery("orders", null, 10));

        assertThat(echoed(preview, "sort")).isEqualTo("<none-was-sent>");
    }

    @Test
    void findTranslatesAComparisonIntoTheOperatorKeyedFormTheConnectorExpects(@TempDir Path dir) {
        // The seam carries a neutral term - field, operator, value - and this bridge is the only place
        // that knows what one connector spells those as. A term handed straight through would be a
        // request in a dialect the surface above was never allowed to know.
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery(
                "orders", new Match("total", Operator.GTE, 100), 10));

        assertThat(echoed(preview, "filter")).isEqualTo(Map.of("total", Map.of("$gte", 100)));
    }

    @Test
    void findTranslatesAMembershipTermIntoTheSetTheValueNames(@TempDir Path dir) {
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery(
                "orders", new Match("status", Operator.IN, List.of("Paid", "Shipped")), 10));

        assertThat(echoed(preview, "filter"))
                .isEqualTo(Map.of("status", Map.of("$in", List.of("Paid", "Shipped"))));
    }

    @Test
    void findTranslatesAPresenceTermIntoTheConnectorsOwnPresenceTest(@TempDir Path dir) {
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery(
                "orders", new Match("note", Operator.EXISTS, false), 10));

        assertThat(echoed(preview, "filter")).isEqualTo(Map.of("note", Map.of("$exists", false)));
    }

    @Test
    void findTranslatesASubstringTermIntoAPatternThatMatchesItLiterally(@TempDir Path dir) {
        // The vocabulary has no pattern in it, so every character of the value is a character to find. A
        // value spliced into a pattern raw would let a caller express a pattern through the one word that
        // takes free text - the whole of what the vocabulary was narrowed to prevent.
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery(
                "orders", new Match("carrier", Operator.CONTAINS, "Fed.x"), 10));

        assertThat(echoed(preview, "filter")).isEqualTo(Map.of("carrier", Map.of("$regex", "\\QFed.x\\E")));
    }

    @Test
    void findAsksForAColumnWhoseNameHoldsADotByNameRatherThanAsAPath(@TempDir Path dir) {
        // The backend's query language spells a path with dots and gives no way to escape one, so a column
        // actually named `price.usd` cannot be asked for in that language at all: the obvious spelling asks
        // for a `usd` inside a `price`, matches nothing, and reports nothing wrong. Naming the field
        // instead of pathing to it is the one form that reaches it.
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery(
                "orders", new Match("price\\.usd", Operator.EQ, 100), 10));

        assertThat(echoed(preview, "filter")).isEqualTo(Map.of(
                "$expr", Map.of("$eq", List.of(Map.of("$getField", "price.usd"), 100))));
    }

    @Test
    void findStillAsksForAPathAsAPathWhenNoNameHoldsADot(@TempDir Path dir) {
        // The half that keeps the fix from being a regression. Naming every field instead of pathing to it
        // would answer the same for a nested field and cost a form the backend cannot serve from an index,
        // so the more expensive shape is reached for only when the cheaper one cannot express the request.
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery(
                "orders", new Match("shipping.city", Operator.EQ, "Berlin"), 10));

        assertThat(echoed(preview, "filter"))
                .isEqualTo(Map.of("shipping.city", Map.of("$eq", "Berlin")));
    }

    @Test
    void asksWhetherAColumnWhoseNameHoldsADotIsThereByTypeBecauseTheExpressionFormHasNoPresenceTest(
            @TempDir Path dir) {
        // The expression language carries no twin of the presence test, so the question goes as a question
        // about type: a field that is not there reports the one type no value has. Both directions are
        // asked here because which way round it is put is the entire answer -- one document decides both,
        // and a form that asked the same thing for either would report every absent column as present.
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");
        Map<String, Object> named = Map.of("$getField", "price.usd");

        DataBrowserPreview there = reader.find(config(), new DataBrowserQuery(
                "orders", new Match("price\\.usd", Operator.EXISTS, true), 10));
        DataBrowserPreview absent = reader.find(config(), new DataBrowserQuery(
                "orders", new Match("price\\.usd", Operator.EXISTS, false), 10));

        assertThat(echoed(there, "filter")).isEqualTo(Map.of(
                "$expr", Map.of("$ne", List.of(Map.of("$type", named), "missing"))));
        assertThat(echoed(absent, "filter")).isEqualTo(Map.of(
                "$expr", Map.of("$eq", List.of(Map.of("$type", named), "missing"))));
    }

    @Test
    void asksATypeBeforeAPatternOnAColumnWhoseNameHoldsADotSoOneWrongTypedRowCannotFailTheRead(
            @TempDir Path dir) {
        // The two languages disagree about a pattern meeting a value that is not text: the query language
        // declines that row, the expression language fails the whole read. Since only the field's spelling
        // decides which language a term travels in, an unguarded pattern would give the same request two
        // different answers -- and the harsher one is not a stricter match but an error where there were
        // rows. Asking the type first keeps the answer the one every other field already gives.
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");
        Map<String, Object> named = Map.of("$getField", "price.usd");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery(
                "orders", new Match("price\\.usd", Operator.CONTAINS, "Fed.x"), 10));

        assertThat(echoed(preview, "filter")).isEqualTo(Map.of("$expr", Map.of("$cond", List.of(
                Map.of("$eq", List.of(Map.of("$type", named), "string")),
                Map.of("$regexMatch", Map.of("input", named, "regex", "\\QFed.x\\E")),
                false))));
    }

    @Test
    void findTranslatesACombinationIntoTheConnectorsOwnConjunction(@TempDir Path dir) {
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery("orders",
                new All(List.of(new Match("status", Operator.EQ, "Paid"),
                        new Match("total", Operator.GT, 100))),
                10));

        assertThat(echoed(preview, "filter")).isEqualTo(Map.of("$and", List.of(
                Map.of("status", Map.of("$eq", "Paid")),
                Map.of("total", Map.of("$gt", 100)))));
    }

    @Test
    void findTranslatesAnAlternativeIntoTheConnectorsOwnDisjunction(@TempDir Path dir) {
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery("orders",
                new Any(List.of(new Match("status", Operator.EQ, "Paid"),
                        new Match("status", Operator.EQ, "Shipped"))),
                10));

        assertThat(echoed(preview, "filter")).isEqualTo(Map.of("$or", List.of(
                Map.of("status", Map.of("$eq", "Paid")),
                Map.of("status", Map.of("$eq", "Shipped")))));
    }

    @Test
    void findKeepsAnAlternativeNestedInsideAConjunctionRatherThanFlatteningIt(@TempDir Path dir) {
        // `a AND (b OR c)` flattened into `a AND b AND c` is a stricter filter that still returns rows,
        // so nothing downstream reports it - the read simply answers a question nobody asked.
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery("orders",
                new All(List.of(
                        new Match("id", Operator.EQ, 1),
                        new Any(List.of(new Match("status", Operator.EQ, "0"),
                                new Match("status", Operator.EQ, "1"))))),
                10));

        assertThat(echoed(preview, "filter")).isEqualTo(Map.of("$and", List.of(
                Map.of("id", Map.of("$eq", 1)),
                Map.of("$or", List.of(
                        Map.of("status", Map.of("$eq", "0")),
                        Map.of("status", Map.of("$eq", "1")))))));
    }

    @Test
    void findAsksForEveryRowWhenNoTermWasGiven(@TempDir Path dir) {
        // A connector hands an absent filter straight to its driver, which refuses a null one, so the
        // empty document has to be assembled here rather than left out.
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery("orders", null, 10));

        assertThat(echoed(preview, "filter")).isEqualTo(Map.of());
    }

    @Test
    void findNamesTheConnectionsOwnDatabaseInTheParams(@TempDir Path dir) {
        // Which database a read may touch follows from the connection, never from the request. Leaving
        // the param out happens to work against one connector, which fills its own in when the request
        // omits it - the other mongo variants do not, and a read that lands in the wrong database, or in
        // none, reports nothing wrong. Three databases share one mongod here, two of them ours.
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        DataBrowserPreview preview = reader.find(config("shop"), new DataBrowserQuery("orders", null, 10));

        assertThat(echoed(preview, "database-as-it-arrived")).isEqualTo("shop");
    }

    @Test
    void findOmitsTheDatabaseParamWhenTheConnectionCarriesNone(@TempDir Path dir) {
        // Nothing validates that a stored connection's settings name a database, so this is reachable.
        // Sending the key with a null value is the one answer that is worse than either alternative: it
        // names no database and, being present, stops the connector filling its own in. Omit it instead,
        // which leaves that connection exactly where it was before this face existed.
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery("orders", null, 10));

        assertThat(echoed(preview, "database-as-it-arrived")).isEqualTo("<none-was-sent>");
    }

    @Test
    void findCarriesTheCollectionIntoTheParams(@TempDir Path dir) {
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery("orders", null, 10));

        assertThat(echoed(preview, "collection")).isEqualTo("orders");
    }

    @Test
    void findHandsTheConnectorAParamsMapItCanWriteInto(@TempDir Path dir) {
        // A connector fills a missing param into the caller's own map rather than a copy - mongodb puts
        // the connection's database in when the request omits it. An immutable map throws there, and
        // only on the paths that omit that param, so it stays green until it does not.
        PdkDataBrowser reader = reader(Synthetic.readFaceSource(dir), "synthetic.ReadFace");

        assertThatCode(() -> reader.find(config(), new DataBrowserQuery("orders", null, 10)))
                .doesNotThrowAnyException();
    }

    // ---- what the read leaves behind -------------------------------------------------------------

    @Test
    void findReportsThereIsMoreWhenTheCollectionHoldsPastTheLimit(@TempDir Path dir) {
        // Ten rows off a collection of twenty-five and ten rows off a collection of ten are the same
        // answer to look at, and the smaller reading is the one believed. The read is one-shot, so
        // there is no continuation token whose presence would hint otherwise either.
        PdkDataBrowser reader = reader(Synthetic.boundedQuerySource(dir, 25), "synthetic.BoundedQuery");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery("orders", null, 10));

        assertThat(preview.moreAvailable()).isTrue();
        // The row asked for to learn that is the bridge's business and not part of the answer: a caller
        // that asked for ten and is handed eleven has had its bound broken to satisfy a footnote.
        assertThat(preview.rows()).hasSize(10);
    }

    @Test
    void findReportsThereIsNoMoreWhenTheCollectionEndsExactlyAtTheLimit(@TempDir Path dir) {
        // The case that decides how "is there more" may be computed at all. A full page is the obvious
        // signal and it is wrong here - the collection ends exactly there - so the only honest answer
        // comes from asking for one row past the bound and seeing whether it arrives.
        PdkDataBrowser reader = reader(Synthetic.boundedQuerySource(dir, 10), "synthetic.BoundedQuery");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery("orders", null, 10));

        assertThat(preview.moreAvailable()).isFalse();
        assertThat(preview.rows()).hasSize(10);
    }

    @Test
    void findReportsTheCollectionTotalWhenNothingWasFilteredOut(@TempDir Path dir) {
        // The count comes off the store's own metadata rather than a scan, which is what makes it
        // affordable to offer at all - and why it is an estimate that drifts rather than a total.
        PdkDataBrowser reader = reader(Synthetic.boundedQuerySource(dir, 25), "synthetic.BoundedQuery");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery("orders", null, 10));

        assertThat(preview.approximateTotal()).isEqualTo(25L);
    }

    @Test
    void findWithholdsTheTotalWhenTheReadWasFiltered(@TempDir Path dir) {
        // Metadata counts the collection, not the filter, so the only true answer here would be a
        // counted one - a full scan on the first read of a large collection, and the read timeout with
        // it. Withholding it is the deliberate trade: a footer that says less over a read that stalls.
        PdkDataBrowser reader = reader(Synthetic.boundedQuerySource(dir, 25), "synthetic.BoundedQuery");

        DataBrowserPreview preview = reader.find(
                config(), new DataBrowserQuery("orders", new Match("status", Operator.EQ, "paid"), 10));

        assertThat(preview.approximateTotal()).isNull();
        assertThat(preview.rows()).hasSize(10);
    }

    @Test
    void findWithholdsTheTotalWhenTheConnectorDoesNotReportOne(@TempDir Path dir) {
        // A count nobody can supply is a missing footnote, never a failed read: refusing here would
        // deny a working read over a connector that simply registers one function fewer.
        PdkDataBrowser reader = reader(
                Synthetic.unreportedSizeQuerySource(dir, 25), "synthetic.UnreportedSizeQuery");

        DataBrowserPreview preview = reader.find(config(), new DataBrowserQuery("orders", null, 10));

        assertThat(preview.approximateTotal()).isNull();
        assertThat(preview.rows()).hasSize(10);
        assertThat(preview.moreAvailable()).isTrue();
    }

    @Test
    void findFailsWithACodeWhenTheConnectorGaveUpPartWayThrough(@TempDir Path dir) {
        // A connector's read loop asks whether it is still alive between batches and, when it is not,
        // returns without throwing and without reporting - dropping whatever it had. Every other signal
        // this face has says the read went fine, and the rows that did arrive are a short answer
        // indistinguishable from a small collection. Asking afterwards is the only way to tell.
        PdkDataBrowser reader = reader(Synthetic.abandoningQuerySource(dir), "synthetic.AbandoningQuery");

        assertThatThrownBy(() -> reader.find(config(), new DataBrowserQuery("orders", null, 10)))
                .isInstanceOf(TapstateException.class)
                .extracting(e -> ((TapstateException) e).code().code())
                .isEqualTo("connector.read-abandoned");
    }

    @Test
    void findStillServesTheNextReadAfterOneWasAbandoned(@TempDir Path dir) {
        // The interrupt that ended the abandoned read is left on a pooled thread, and reads are handed
        // to whichever of those is free - so a flag left set would end the next read on that thread too,
        // reporting a healthy connector as having given up. Whether it is cleared between calls belongs
        // to the executor rather than to this class, which is why it is asserted and not assumed.
        ConnectorRef abandoning = new ConnectorRef(
                List.of(Synthetic.abandoningQuerySource(dir)), "synthetic.AbandoningQuery", "2.0.8", null);
        ConnectorRef healthy = new ConnectorRef(
                List.of(Synthetic.boundedQuerySource(dir, 4)), "synthetic.BoundedQuery", "2.0.8", null);
        PdkDataBrowser reader = new PdkDataBrowser(
                connectorId -> "abandoning".equals(connectorId) ? abandoning : healthy,
                ConnectorInstancePool.DEFAULTS, Clock.systemUTC());
        readers.add(reader);
        DataBrowserQuery query = new DataBrowserQuery("orders", null, 10);

        assertThatThrownBy(() -> reader.find(new ConnectionConfig("c1", "abandoning", Map.of()), query))
                .isInstanceOf(TapstateException.class);

        DataBrowserPreview preview = reader.find(new ConnectionConfig("c2", "healthy", Map.of()), query);
        assertThat(preview.rows()).hasSize(4);
    }

    @Test
    void findFailsWithACodeWhenTheConnectorReportsAnError(@TempDir Path dir) {
        // The failure arrives through the result rather than as a throw, so a drive that reads only
        // getResult() returns an empty page for a query that in fact failed.
        PdkDataBrowser reader = reader(Synthetic.erroringQuerySource(dir), "synthetic.ErroringQuery");

        assertThatThrownBy(() -> reader.find(config(), new DataBrowserQuery("orders", null, 10)))
                .isInstanceOf(TapstateException.class)
                .extracting(e -> ((TapstateException) e).code().code())
                .isEqualTo("connector.read-failed");
    }

    @Test
    void findFailsWithACodeWhenTheConnectorThrows(@TempDir Path dir) {
        PdkDataBrowser reader = reader(Synthetic.throwingQuerySource(dir), "synthetic.ThrowingQuery");

        assertThatThrownBy(() -> reader.find(config(), new DataBrowserQuery("orders", null, 10)))
                .isInstanceOf(TapstateException.class)
                .extracting(e -> ((TapstateException) e).code().code())
                .isEqualTo("connector.read-failed");
    }

    @Test
    void findFailsWithACodeWhenTheConnectorDoesNotRegisterIt(@TempDir Path dir) {
        // The read face is reachable by a user, so a connector that cannot serve it is a coded refusal
        // naming the connector and the capability - not the bare crash a caller invariant would take.
        PdkDataBrowser reader = reader(Synthetic.emittingSource(dir), "synthetic.EmittingSource");

        assertThatThrownBy(() -> reader.find(config(), new DataBrowserQuery("orders", null, 10)))
                .isInstanceOf(TapstateException.class)
                .extracting(e -> ((TapstateException) e).code().code())
                .isEqualTo("connector.capability-missing");
    }

    /** The value the read-face connector echoed back for {@code what}, or null if it echoed no such row. */
    private static Object echoed(DataBrowserPreview preview, String what) {
        return preview.rows().stream()
                .filter(row -> what.equals(row.get("echoed")))
                .map(row -> row.get("value"))
                .findFirst()
                .orElse(null);
    }
}
