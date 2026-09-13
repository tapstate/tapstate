package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceIndex;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The discovered-schema document codec is the mapping core of the schema store: a discovery envelope —
 * the connection id as the key, the connector id and discovery time it reports, and the source model's
 * tables (with their fields, primary key and indexes) as nested sub-documents — is stored as one
 * structured document and reconstructed from it on read. These witness the mapping deterministically,
 * without a Mongo server: the document shape, a full round-trip (including a field with no resolved
 * type and both a unique and a non-unique index), an empty model, and that a structurally corrupt
 * stored document surfaces as a coded {@code io.document-unreadable} diagnostic rather than a bare
 * crash. A real Mongo round-trip is exercised by {@code MongoSchemaStoreIT} (skipped where Docker is
 * absent).
 */
class MongoSchemaStoreTest {

    private static SourceModel ordersModel() {
        SourceTable orders = new SourceTable(
                "orders",
                List.of(new SourceField("id", "bigint"), new SourceField("note", null)),
                List.of("id"),
                List.of(
                        new SourceIndex("pk_orders", List.of("id"), true),
                        new SourceIndex("by_note", List.of("note"), false)));
        SourceTable customers = new SourceTable(
                "customers",
                List.of(new SourceField("email", "varchar")),
                List.of("email"),
                List.of());
        return new SourceModel(List.of(orders, customers));
    }

    private static DiscoveredSourceModel discovered(String connectionId, SourceModel model) {
        return new DiscoveredSourceModel(connectionId, "mysql", 1783998000000L, model);
    }

    /** The envelope this build writes for a discovery, under a generation the test fixes. */
    private static Document envelopeOf(DiscoveredSourceModel discovered) {
        return MongoSchemaStore.envelope(discovered, "gen-1");
    }

    /** The table documents this build writes for a discovery, in discovery order. */
    private static List<Document> tableDocsOf(DiscoveredSourceModel discovered) {
        List<Document> tables = new ArrayList<>();
        for (SourceTable table : discovered.model().tables()) {
            tables.add(MongoSchemaStore.tableDocument(table));
        }
        return tables;
    }

    /** A discovery encoded and read straight back, the way a save and a get carry it between them. */
    private static DiscoveredSourceModel roundTrip(DiscoveredSourceModel discovered) {
        return MongoSchemaStore.toDiscovered(envelopeOf(discovered), tableDocsOf(discovered));
    }

    @Test
    void numericAttributesRoundTripBeyondDecimal128AndLegacyRecordsStayAbsent() {
        var number = new io.tapstate.core.common.NumericType(256, true, null, false,
                new java.math.BigDecimal("-1.23456789012345678901234567890123456789E+1000"),
                new java.math.BigDecimal("1.23456789012345678901234567890123456789E+1000"), 40, -5);
        SourceField amount = new SourceField("amount", "source_number", TapstateType.DECIMAL, null, number);
        var envelope = discovered("numeric", new SourceModel(List.of(
                new SourceTable("orders", List.of(amount), List.of(), List.of()))));
        assertThat(roundTrip(envelope)).isEqualTo(envelope);
        var field = tableDocsOf(envelope).getFirst().getList("fields", Document.class).getFirst();
        assertThat(field.get("numericType", Document.class).getString("maxValue"))
                .isEqualTo(number.maxValue().toString());
        field.remove("numericType");
        var legacy = new Document("name", "orders").append("fields", List.of(field))
                .append("primaryKey", List.of()).append("indexes", List.of());
        assertThat(MongoSchemaStore.toDiscovered(envelopeOf(envelope), List.of(legacy))
                .model().tables().getFirst().fields().getFirst().numericType()).isNull();
        field.append("numericType", new Document("precision", "corrupt"));
        assertThat(catchThrowable(() -> MongoSchemaStore.toDiscovered(envelopeOf(envelope), List.of(legacy))))
                .isInstanceOf(TapstateException.class);
    }

    @Test
    void documentCarriesIdConnectorIdDiscoveredAtAndTables() {
        DiscoveredSourceModel envelope = discovered("orders-db", ordersModel());
        Document document = envelopeOf(envelope);

        assertThat(document.getString("_id")).isEqualTo("orders-db");
        assertThat(document.getString("connectorId")).isEqualTo("mysql");
        assertThat(document.getLong("discoveredAt")).isEqualTo(1783998000000L);
        assertThat(document.getString(MongoSchemaStore.GENERATION)).isEqualTo("gen-1");
        // The tables are documents of their own now, so the envelope must not carry them as well.
        assertThat(document.get("tables")).isNull();
        List<Document> tables = tableDocsOf(envelope);
        assertThat(tables).extracting(t -> t.getString("name")).containsExactly("orders", "customers");

        Document orders = tables.get(0);
        assertThat(orders.getList("primaryKey", String.class)).containsExactly("id");
        assertThat(orders.getList("fields", Document.class))
                .extracting(f -> f.getString("name"))
                .containsExactly("id", "note");
        assertThat(orders.getList("indexes", Document.class))
                .extracting(i -> i.getString("name"), i -> i.getBoolean("unique"))
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("pk_orders", true),
                        org.assertj.core.groups.Tuple.tuple("by_note", false));
    }

    @Test
    void roundTripReconstructsTheSameEnvelope() {
        DiscoveredSourceModel envelope = discovered("orders-db", ordersModel());

        assertThat(roundTrip(envelope)).isEqualTo(envelope);
    }

    @Test
    void emptyModelRoundTrips() {
        DiscoveredSourceModel envelope = discovered("bare", new SourceModel(List.of()));

        assertThat(roundTrip(envelope)).isEqualTo(envelope);
    }

    @Test
    void aFieldWithNoDeclaredTypeRoundTripsAsUnresolved() {
        DiscoveredSourceModel envelope = discovered("x", new SourceModel(List.of(
                new SourceTable("t", List.of(new SourceField("c", null)), List.of(), List.of()))));

        DiscoveredSourceModel read = roundTrip(envelope);

        SourceField field = read.model().tables().get(0).fields().get(0);
        assertThat(field.dataType()).isNull();
        assertThat(field.type()).isEqualTo(TapstateType.UNKNOWN);
        assertThat(read).isEqualTo(envelope);
    }

    @Test
    void aFieldsResolvedTypeSurvivesTheRoundTrip() {
        DiscoveredSourceModel envelope = discovered("x", new SourceModel(List.of(new SourceTable(
                "t",
                List.of(new SourceField("amount", "decimal(18,4)", TapstateType.DECIMAL)),
                List.of(),
                List.of()))));

        DiscoveredSourceModel read = roundTrip(envelope);

        assertThat(read.model().tables().get(0).fields().get(0).type())
                .as("the resolution happens once, at discovery, so the store has to carry it")
                .isEqualTo(TapstateType.DECIMAL);
    }

    @Test
    void aStoredFieldFromBeforeTypesWereResolvedReadsBackAsUnknown() {
        // A document written before discovery resolved types carries the declared type and no resolved one.
        // The model is a derived observation a re-discovery replaces, so an older document is read, not
        // refused - and what it is read as must be unknown rather than any type that would be acted on.
        DiscoveredSourceModel stored = discovered("x", new SourceModel(List.of(
                new SourceTable("t", List.of(new SourceField("amount", "decimal(18,4)")), List.of(), List.of()))));
        List<Document> tables = tableDocsOf(stored);
        tables.get(0).getList("fields", Document.class).get(0).remove("tapstateType");

        DiscoveredSourceModel read = MongoSchemaStore.toDiscovered(envelopeOf(stored), tables);

        assertThat(read.model().tables().get(0).fields().get(0).type()).isEqualTo(TapstateType.UNKNOWN);
        assertThat(read.model().tables().get(0).fields().get(0).dataType()).isEqualTo("decimal(18,4)");
    }

    @Test
    void theThreeWaysAStoredFieldComesBackWithoutATypeAreToldApart() {
        // Three different problems arriving as one value: a record from before types were kept needs a
        // re-discovery, a spelling this build has no type for is a compatibility break, and a stored
        // unknown that never said which unknown it was is a record from before the reason existed.
        // Read as one text they all say "the type did not resolve", which is the one nobody acts on.
        DiscoveredSourceModel stored = discovered("x", new SourceModel(List.of(
                new SourceTable("t", List.of(new SourceField("amount", "decimal(18,4)")), List.of(), List.of()))));

        List<Document> beforeTypes = tableDocsOf(stored);
        beforeTypes.get(0).getList("fields", Document.class).get(0).remove("tapstateType");
        List<Document> notAType = tableDocsOf(stored);
        notAType.get(0).getList("fields", Document.class).get(0).append("tapstateType", "GEOGRAPHY");
        List<Document> unattributed = tableDocsOf(stored);
        unattributed.get(0).getList("fields", Document.class).get(0).remove("unknownBecause");

        List<String> reasons = Stream.of(beforeTypes, notAType, unattributed)
                .map(tables -> MongoSchemaStore.toDiscovered(envelopeOf(stored), tables))
                .map(read -> read.model().tables().get(0).fields().get(0).unknownBecause())
                .toList();

        assertThat(reasons).doesNotContainNull();
        assertThat(Set.copyOf(reasons)).as("three routes, three attributions").hasSize(3);
        assertThat(reasons.get(1)).as("the spelling is what names the break").contains("GEOGRAPHY");
    }

    @Test
    void theReasonWrittenAtDiscoveryOutlivesTheRecordRatherThanBeingReplaced() {
        // The connector was open when the reason was worked out and is not now, so nothing here could
        // arrive at it again. Overwriting it with a storage-side remark turns every such column into
        // "the record was read", which is the fact of least use to whoever has to fix something.
        DiscoveredSourceModel stored = discovered("x", new SourceModel(List.of(
                new SourceTable("t", List.of(new SourceField("shape", "geometry", TapstateType.UNKNOWN,
                        "the connector's TapRaw has no member in the tapstate type namespace")),
                        List.of(), List.of()))));

        DiscoveredSourceModel read =
                MongoSchemaStore.toDiscovered(envelopeOf(stored), tableDocsOf(stored));

        assertThat(read.model().tables().get(0).fields().get(0).unknownBecause())
                .isEqualTo("the connector's TapRaw has no member in the tapstate type namespace");
    }

    @Test
    void aTablesRowCountSurvivesTheRoundTrip() {
        DiscoveredSourceModel envelope = discovered("x", new SourceModel(List.of(
                new SourceTable("orders", List.of(), List.of(), List.of(), 4_200_000L))));

        DiscoveredSourceModel read = roundTrip(envelope);

        assertThat(read.model().tables().get(0).approximateRowCount()).isEqualTo(4_200_000L);
        assertThat(read).isEqualTo(envelope);
    }

    @Test
    void aStoredTableFromBeforeCountingReadsBackUncountedRatherThanEmpty() {
        // A document written before discovery counted rows carries no count at all. Read as zero it
        // would describe every table discovered until now as empty, which is the one answer a reader
        // sizing something off it would act on.
        DiscoveredSourceModel stored = discovered("x", new SourceModel(List.of(
                new SourceTable("orders", List.of(), List.of(), List.of(), 4_200_000L))));
        List<Document> tables = tableDocsOf(stored);
        tables.get(0).remove("approximateRowCount");

        DiscoveredSourceModel read = MongoSchemaStore.toDiscovered(envelopeOf(stored), tables);

        assertThat(read.model().tables().get(0).approximateRowCount()).isNull();
    }

    @Test
    void aStoredDocumentCarriesTheStampThatSaysItsTypesAreResolved() {
        Document document = envelopeOf(
                new DiscoveredSourceModel("conn_1", "mysql", 1000L, ordersModel()));

        assertThat(MongoSchemaStore.carriesResolvedTypes(document)).isTrue();
    }

    @Test
    void aDocumentWithoutTheStampIsNotADiscoveryThisBuildCanRead() {
        // What a discovery written before the types were resolved looks like: every column reads back
        // with no resolved type, which is refused wherever a resolved type is needed - and refused
        // while pointing at the columns, telling the author to change an expression that is not wrong.
        Document old = new Document("_id", "conn_1")
                .append("connectorId", "mysql")
                .append("discoveredAt", 1000L)
                .append("tables", List.of(new Document("name", "orders")
                        .append("fields", List.of(new Document("name", "id").append("type", "bigint")))
                        .append("primaryKey", List.of("id"))
                        .append("indexes", List.of())));

        assertThat(MongoSchemaStore.carriesResolvedTypes(old)).isFalse();
    }

    @Test
    void aStampedModelHoldingNothingIsStillADiscovery() {
        // The case that rules out reading the content instead of a stamp: a connection that legitimately
        // holds no table has no field carrying a resolved type either, so "no resolved type anywhere"
        // cannot tell it from a document that predates the resolution. Read that way, an empty source
        // would stay undiscoverable however often it is discovered - and "discovered nothing" has to
        // stay a different answer from "not discovered".
        Document document = envelopeOf(
                new DiscoveredSourceModel("conn_1", "mysql", 1000L, new SourceModel(List.of())));

        assertThat(MongoSchemaStore.carriesResolvedTypes(document)).isTrue();
    }

    @Test
    void toDiscoveredWithAnAbsentTablesFieldReadsBackEmpty() {
        DiscoveredSourceModel read = MongoSchemaStore.toDiscovered(
                new Document("_id", "bare").append("connectorId", "mysql").append("discoveredAt", 1L),
                List.of());

        assertThat(read.model().tables()).isEmpty();
    }

    @Test
    void toDiscoveredOnADocumentMissingItsConnectorIdIsDocumentUnreadable() {
        Document corrupt = new Document("_id", "orders-db").append("discoveredAt", 1L);

        Throwable thrown = catchThrowable(() -> MongoSchemaStore.toDiscovered(corrupt, List.of()));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        TapstateException coded = (TapstateException) thrown;
        assertThat(coded.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
        assertThat(coded.args()).containsEntry("id", "orders-db");
    }

    @Test
    void toDiscoveredOnADocumentWithoutANumericDiscoveredAtIsDocumentUnreadable() {
        Document corrupt = new Document("_id", "orders-db").append("connectorId", "mysql").append("discoveredAt", "oops");

        Throwable thrown = catchThrowable(() -> MongoSchemaStore.toDiscovered(corrupt, List.of()));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
    }

    @Test
    void toDiscoveredOnATableMissingItsNameIsDocumentUnreadable() {
        Document corrupt = new Document("_id", "orders-db")
                .append("connectorId", "mysql")
                .append("discoveredAt", 1L);

        Throwable thrown = catchThrowable(() ->
                MongoSchemaStore.toDiscovered(corrupt, List.of(new Document("fields", List.of()))));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        TapstateException coded = (TapstateException) thrown;
        assertThat(coded.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
        assertThat(coded.args()).containsEntry("id", "orders-db");
    }

    @Test
    void toDiscoveredOnATablesFieldsThatAreNotAListIsDocumentUnreadable() {
        // The tables themselves arrive already separated, so the array-shaped guard is now reachable
        // through what a table document holds rather than through the envelope.
        Document corrupt = new Document("_id", "orders-db")
                .append("connectorId", "mysql")
                .append("discoveredAt", 1L);

        Throwable thrown = catchThrowable(() -> MongoSchemaStore.toDiscovered(
                corrupt, List.of(new Document("name", "orders").append("fields", "oops"))));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
    }

    @Test
    void toDiscoveredOnAnIndexMissingItsNameIsDocumentUnreadable() {
        Document corrupt = new Document("_id", "orders-db")
                .append("connectorId", "mysql")
                .append("discoveredAt", 1L);

        Throwable thrown = catchThrowable(() -> MongoSchemaStore.toDiscovered(corrupt, List.of(
                new Document("name", "orders").append("indexes", List.of(new Document("unique", true))))));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) thrown).code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
    }
}
