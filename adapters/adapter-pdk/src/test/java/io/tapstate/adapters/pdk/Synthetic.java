package io.tapstate.adapters.pdk;

import java.nio.file.Path;
import java.util.Map;

/**
 * Compiles full synthetic {@code TapConnector}s at test time (via {@link SyntheticJar}), so the PDK
 * bridge's drive and coded-error paths can be proven without any real connector jar or the PDK
 * runtime. Each connector implements the frozen contract directly and registers exactly the read /
 * write behaviour a test needs; the lifecycle and discovery methods are inert unless the test drives
 * them.
 */
final class Synthetic {

    private Synthetic() {
    }

    /** The shared source scaffold: a connector whose ctor and registered functions the caller fills in. */
    private static String source(String simpleName, String ctorBody, String registerBody) {
        return ""
                + "package synthetic;"
                + "import io.tapdata.pdk.apis.TapConnector;"
                + "import io.tapdata.pdk.apis.functions.ConnectorFunctions;"
                + "import io.tapdata.entity.codec.TapCodecsRegistry;"
                + "import io.tapdata.pdk.apis.context.TapConnectionContext;"
                + "import io.tapdata.pdk.apis.entity.ConnectionOptions;"
                + "import io.tapdata.pdk.apis.entity.TestItem;"
                + "import io.tapdata.entity.schema.TapTable;"
                + "import io.tapdata.entity.schema.TapField;"
                + "import io.tapdata.entity.event.TapEvent;"
                + "import io.tapdata.entity.event.dml.TapInsertRecordEvent;"
                + "import io.tapdata.entity.event.dml.TapUpdateRecordEvent;"
                + "import io.tapdata.entity.event.dml.TapDeleteRecordEvent;"
                + "import io.tapdata.entity.event.ddl.TapDDLUnknownEvent;"
                + "import java.util.ArrayList;"
                + "import java.util.LinkedHashMap;"
                + "import java.util.List;"
                + "import java.util.Map;"
                + "import java.util.function.Consumer;"
                + "public class " + simpleName + " implements TapConnector {"
                + "  public " + simpleName + "() {" + ctorBody + "}"
                + "  public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {"
                + registerBody
                + "  }"
                + "  public void init(TapConnectionContext c) {}"
                + "  public void stop(TapConnectionContext c) {}"
                + "  public void discoverSchema(TapConnectionContext c, List<String> t, int n, Consumer<List<TapTable>> s) {"
                + "    TapTable table = new TapTable(\"t1\");"
                + "    table.add(new TapField(\"id\", \"int\"));"
                + "    List<TapTable> tables = new ArrayList<>();"
                + "    tables.add(table);"
                + "    s.accept(tables);"
                + "  }"
                + "  public ConnectionOptions connectionTest(TapConnectionContext c, Consumer<TestItem> s) {"
                + "    s.accept(new TestItem(\"ping\", TestItem.RESULT_SUCCESSFULLY));"
                + "    return ConnectionOptions.create();"
                + "  }"
                + "  public int tableCount(TapConnectionContext c) { return 1; }"
                + "}";
    }

    /** A row map {@code {id: value}} as a Java expression string. */
    private static String row(String var, int id) {
        return "Map<String,Object> " + var + " = new LinkedHashMap<>(); " + var + ".put(\"id\", " + id + ");";
    }

    /**
     * Streams an insert, then a schema change, then another insert. A source really does interleave the
     * two, and the schema change is the one a view of rows has nowhere to put — so a follow driven by
     * this connector is what tells "left out" apart from "passed along as nothing".
     */
    static Path ddlEmittingSource(Path dir) {
        String register = ""
                + "functions.supportStreamRead((context, tables, offset, size, consumer) -> {"
                + "  consumer.streamReadStarted();"
                + row("first", 1)
                + "  List<TapEvent> one = new ArrayList<>();"
                + "  one.add(TapInsertRecordEvent.create().table(\"t1\").referenceTime(1L).after(first));"
                + "  consumer.accept(one, null);"
                + "  List<TapEvent> ddl = new ArrayList<>();"
                + "  TapDDLUnknownEvent schemaChange = new TapDDLUnknownEvent();"
                + "  schemaChange.setTableId(\"t1\"); schemaChange.setReferenceTime(2L);"
                + "  ddl.add(schemaChange);"
                + "  consumer.accept(ddl, null);"
                + row("second", 2)
                + "  List<TapEvent> two = new ArrayList<>();"
                + "  two.add(TapInsertRecordEvent.create().table(\"t1\").referenceTime(3L).after(second));"
                + "  consumer.accept(two, null);"
                + "  consumer.streamReadEnded();"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.DdlEmittingSource",
                source("DdlEmittingSource", "", register));
    }

    /** Batch-reads two snapshot rows and streams an insert / update / delete; a full source connector. */
    static Path emittingSource(Path dir) {
        String register = ""
                + "functions.supportBatchRead((context, table, offset, size, consumer) -> {"
                + "  List<TapEvent> evs = new ArrayList<>();"
                + row("r1", 1)
                + row("r2", 2)
                + "  evs.add(TapInsertRecordEvent.create().table(\"t1\").referenceTime(100L).after(r1));"
                + "  evs.add(TapInsertRecordEvent.create().table(\"t1\").referenceTime(100L).after(r2));"
                + "  consumer.accept(evs, null);"
                + "});"
                + "functions.supportStreamRead((context, tables, offset, size, consumer) -> {"
                + "  consumer.streamReadStarted();"
                + row("a", 1)
                + "  List<TapEvent> ins = new ArrayList<>();"
                + "  ins.add(TapInsertRecordEvent.create().table(\"t1\").referenceTime(1L).after(a));"
                + "  consumer.accept(ins, null);"
                + row("before", 1)
                + "  Map<String,Object> after = new LinkedHashMap<>(); after.put(\"id\", 1); after.put(\"v\", 2);"
                + "  List<TapEvent> upd = new ArrayList<>();"
                + "  upd.add(TapUpdateRecordEvent.create().table(\"t1\").referenceTime(2L).before(before).after(after));"
                + "  consumer.accept(upd, null);"
                + "  List<TapEvent> del = new ArrayList<>();"
                + "  del.add(TapDeleteRecordEvent.create().table(\"t1\").referenceTime(3L).before(before));"
                + "  consumer.accept(del, null);"
                + "  consumer.streamReadEnded();"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.EmittingSource", source("EmittingSource", "", register));
    }

    /**
     * Reads its own state map, writes to it, and reports on every row it emits what it read there.
     *
     * <p>A connector's notes to itself are invisible from outside it - nothing above the connector can
     * look into the map it was handed. So the connector is made to say what it found: each row carries a
     * {@code seen} column holding the value the map answered before this drive overwrote it, or the
     * string {@code null} when the map was empty. A drive that finds what an earlier drive wrote reads
     * {@code written}; one handed a fresh map reads {@code null}, and the two are not confusable.
     *
     * <p>Both read functions do the same thing, so the same connector answers two different questions:
     * a snapshot followed by a stream asks whether the two phases of one run share a map, and a snapshot
     * followed by another snapshot asks whether one run's map is still there for the next.
     */
    static Path stateRecordingSource(Path dir) {
        String recallAndRecord = ""
                + "Object seen = context.getStateMap().get(\"mark\");"
                + "context.getStateMap().put(\"mark\", \"written\");"
                + "Map<String,Object> r = new LinkedHashMap<>();"
                + "r.put(\"id\", 1); r.put(\"seen\", String.valueOf(seen));"
                + "List<TapEvent> evs = new ArrayList<>();";
        String register = ""
                + "functions.supportBatchRead((context, table, offset, size, consumer) -> {"
                + recallAndRecord
                + "  evs.add(TapInsertRecordEvent.create().table(\"t1\").referenceTime(100L).after(r));"
                + "  consumer.accept(evs, null);"
                + "});"
                + "functions.supportStreamRead((context, tables, offset, size, consumer) -> {"
                + "  consumer.streamReadStarted();"
                + recallAndRecord
                + "  evs.add(TapInsertRecordEvent.create().table(\"t1\").referenceTime(1L).after(r));"
                + "  consumer.accept(evs, null);"
                + "  consumer.streamReadEnded();"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.StateRecordingSource",
                source("StateRecordingSource", "", register));
    }

    /**
     * A source whose batchRead emits one row per column of the table it is handed, so a read through a
     * bare, fieldless table yields nothing. The scaffold's discovery reports one-column {@code t1}, so a
     * drive that passes the discovered table reads one row and a drive that passes a bare name reads zero.
     */
    static Path tableAwareSource(Path dir) {
        String register = ""
                + "functions.supportBatchRead((context, table, offset, size, consumer) -> {"
                + "  List<TapEvent> evs = new ArrayList<>();"
                + "  int n = (table.getNameFieldMap() == null) ? 0 : table.getNameFieldMap().size();"
                + "  for (int i = 0; i < n; i++) {"
                + "    Map<String,Object> r = new LinkedHashMap<>(); r.put(\"id\", i);"
                + "    evs.add(TapInsertRecordEvent.create().table(\"t1\").referenceTime(1L).after(r));"
                + "  }"
                + "  consumer.accept(evs, null);"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.TableAware", source("TableAware", "", register));
    }

    /**
     * A source whose batchRead reads each field's PDK type and refuses to read when one is unset, the
     * way a connector that decodes by type does. Discovery supplies the database's own type name and
     * leaves the PDK type null; only the connector's own mapping can fill it in. So a descriptor that
     * never went through that step reaches the connector with every type missing, and a connector that
     * needs one throws from inside itself.
     */
    static Path typeAwareSource(Path dir) {
        String register = ""
                + "functions.supportBatchRead((context, table, offset, size, consumer) -> {"
                + "  for (TapField f : table.getNameFieldMap().values()) {"
                + "    if (f.getTapType() == null) {"
                + "      throw new IllegalStateException(\"field \" + f.getName() + \" has no PDK type\");"
                + "    }"
                + "  }"
                + "  List<TapEvent> evs = new ArrayList<>();"
                + "  Map<String,Object> r = new LinkedHashMap<>(); r.put(\"id\", 1);"
                + "  evs.add(TapInsertRecordEvent.create().table(\"t1\").referenceTime(1L).after(r));"
                + "  consumer.accept(evs, null);"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.TypeAware", source("TypeAware", "", register));
    }

    /**
     * A source whose streamRead reads what it was asked to watch by walking the context's table map
     * rather than asking for one name at a time, emitting one row per entry carrying that entry's name
     * and column count. A connector expands a partitioned table into its children this way, at the start
     * of the stream and before any change is decoded. A table map that answers only lookup cannot be
     * walked at all, so a drive through one fails outright rather than yielding fewer rows.
     */
    static Path tableMapIteratingStreamSource(Path dir) {
        String register = ""
                + "functions.supportStreamRead((context, tables, offset, size, consumer) -> {"
                + "  consumer.streamReadStarted();"
                + "  io.tapdata.entity.utils.cache.Iterator<io.tapdata.entity.utils.cache.Entry<TapTable>>"
                + "      entries = context.getTableMap().iterator();"
                + "  List<TapEvent> evs = new ArrayList<>();"
                + "  while (entries.hasNext()) {"
                + "    io.tapdata.entity.utils.cache.Entry<TapTable> entry = entries.next();"
                + "    Map<String,Object> r = new LinkedHashMap<>();"
                + "    r.put(\"name\", entry.getKey());"
                + "    r.put(\"columns\", entry.getValue().getNameFieldMap().size());"
                + "    evs.add(TapInsertRecordEvent.create().table(\"t1\").referenceTime(1L).after(r));"
                + "  }"
                + "  consumer.accept(evs, null);"
                + "  consumer.streamReadEnded();"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.TableMapIteratingStream",
                source("TableMapIteratingStream", "", register));
    }

    /** A connector whose constructor throws — instantiation fails, a load failure. */
    static Path ctorThrowsSource(Path dir) {
        return SyntheticJar.compileToJar(dir, "synthetic.CtorThrows",
                source("CtorThrows", "throw new RuntimeException(\"ctor boom\");", ""));
    }

    /** A connector whose batchRead throws — a connector-side read failure. */
    static Path throwingReadSource(Path dir) {
        String register = "functions.supportBatchRead((context, table, offset, size, consumer) -> {"
                + "  throw new RuntimeException(\"read boom\");"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.ThrowingRead", source("ThrowingRead", "", register));
    }

    /** A source whose streamRead starts then throws — a cdc tail that dies mid-stream. */
    static Path throwingStreamSource(Path dir) {
        String register = "functions.supportStreamRead((context, tables, offset, size, consumer) -> {"
                + "  consumer.streamReadStarted();"
                + "  throw new RuntimeException(\"stream boom\");"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.ThrowingStream", source("ThrowingStream", "", register));
    }

    /**
     * Streams one insert whose row holds values no JSON writer knows, at three depths. A connector
     * hands on whatever its driver produced, and a document store's own key arrives as a driver object
     * rather than as a string or a number; {@code UUID} stands in for one here, because what matters is
     * a value outside the map/list/string/number/boolean set and not which class it is.
     *
     * <p>The plain values beside them are the half that discriminates: a projection that renders every
     * value as text would satisfy "the driver object became a string" while quietly turning a number
     * into one.
     */
    static Path opaqueValueSource(Path dir) {
        String opaque = "java.util.UUID.fromString(\"00000000-0000-0000-0000-00000000002a\")";
        String register = ""
                + "functions.supportStreamRead((context, tables, offset, size, consumer) -> {"
                + "  consumer.streamReadStarted();"
                + "  Map<String,Object> r = new LinkedHashMap<>();"
                + "  r.put(\"id\", 7);"
                + "  r.put(\"flag\", Boolean.TRUE);"
                + "  r.put(\"name\", \"row-7\");"
                + "  r.put(\"key\", " + opaque + ");"
                + "  Map<String,Object> nested = new LinkedHashMap<>();"
                + "  nested.put(\"ref\", " + opaque + ");"
                + "  r.put(\"meta\", nested);"
                + "  List<Object> refs = new ArrayList<>();"
                + "  refs.add(" + opaque + ");"
                + "  r.put(\"refs\", refs);"
                + "  List<TapEvent> evs = new ArrayList<>();"
                + "  evs.add(TapInsertRecordEvent.create().table(\"t1\").referenceTime(1L).after(r));"
                + "  consumer.accept(evs, null);"
                + "  consumer.streamReadEnded();"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.OpaqueValue",
                source("OpaqueValue", "", register));
    }

    /** A connector whose batchRead emits a delete-shaped event — unprojectable as a snapshot row. */
    static Path badRowSource(Path dir) {
        String register = "functions.supportBatchRead((context, table, offset, size, consumer) -> {"
                + "  List<TapEvent> evs = new ArrayList<>();"
                + row("b", 1)
                + "  evs.add(TapDeleteRecordEvent.create().table(\"t1\").referenceTime(1L).before(b));"
                + "  consumer.accept(evs, null);"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.BadRow", source("BadRow", "", register));
    }

    /** A sink connector whose writeRecord counts events and reports them all inserted. */
    static Path countingSink(Path dir) {
        String register = "functions.supportWriteRecord((context, events, table, consumer) -> {"
                + "  consumer.accept(new io.tapdata.pdk.apis.entity.WriteListResult<>((long) events.size(), 0L, 0L));"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.CountingSink", source("CountingSink", "", register));
    }

    /** A sink connector whose writeRecord rejects any event that is not an insert — proves append reforge. */
    static Path insertsOnlySink(Path dir) {
        String register = "functions.supportWriteRecord((context, events, table, consumer) -> {"
                + "  for (Object e : events) {"
                + "    if (!(e instanceof io.tapdata.entity.event.dml.TapInsertRecordEvent)) {"
                + "      throw new RuntimeException(\"non-insert event: \" + e.getClass().getName());"
                + "    }"
                + "  }"
                + "  consumer.accept(new io.tapdata.pdk.apis.entity.WriteListResult<>((long) events.size(), 0L, 0L));"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.InsertsOnly", source("InsertsOnly", "", register));
    }

    /** A sink whose writeRecord reports the target table's primary-key count — proves the key reached it. */
    static Path keyCountingSink(Path dir) {
        String register = "functions.supportWriteRecord((context, events, table, consumer) -> {"
                + "  int pk = table.primaryKeys() == null ? 0 : table.primaryKeys().size();"
                + "  consumer.accept(new io.tapdata.pdk.apis.entity.WriteListResult<>((long) pk, 0L, 0L));"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.KeyCounting", source("KeyCounting", "", register));
    }

    /** A sink whose writeRecord reports the target table's column count — proves the schema reached it. */
    static Path fieldCountingSink(Path dir) {
        String register = "functions.supportWriteRecord((context, events, table, consumer) -> {"
                + "  int cols = table.getNameFieldMap() == null ? 0 : table.getNameFieldMap().size();"
                + "  consumer.accept(new io.tapdata.pdk.apis.entity.WriteListResult<>((long) cols, 0L, 0L));"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.FieldCounting", source("FieldCounting", "", register));
    }

    /** A sink connector whose writeRecord throws — a connector-side write failure. */
    static Path throwingWriteSink(Path dir) {
        String register = "functions.supportWriteRecord((context, events, table, consumer) -> {"
                + "  throw new RuntimeException(\"write boom\");"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.ThrowingWrite", source("ThrowingWrite", "", register));
    }

    /**
     * A sink connector that refuses the write and says which identifier it refused, the way a real
     * store does when a name breaks its rules (MongoDB answers an illegal collection name with
     * "Invalid collection name: &lt;name&gt;"). What matters for the test is that the name is only in
     * the connector's own message: nothing else in the chain knows it.
     */
    static Path identifierRejectingSink(Path dir) {
        String register = "functions.supportWriteRecord((context, events, table, consumer) -> {"
                + "  throw new RuntimeException(\"Invalid collection name: ord$ers\");"
                + "});";
        return SyntheticJar.compileToJar(
                dir, "synthetic.IdentifierRejecting", source("IdentifierRejecting", "", register));
    }

    /** A sink connector whose writeRecord reports each batch in two flushes — proves count accumulation. */
    static Path multiFlushSink(Path dir) {
        String register = "functions.supportWriteRecord((context, events, table, consumer) -> {"
                + "  consumer.accept(new io.tapdata.pdk.apis.entity.WriteListResult<>(1L, 0L, 0L));"
                + "  consumer.accept(new io.tapdata.pdk.apis.entity.WriteListResult<>((long) (events.size() - 1), 0L, 0L));"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.MultiFlush", source("MultiFlush", "", register));
    }

    /** A source that refuses a second init on the same instance — proves the drive inits exactly once. */
    static Path singleInitSource(Path dir) {
        String src = ""
                + "package synthetic;"
                + "import io.tapdata.pdk.apis.TapConnector;"
                + "import io.tapdata.pdk.apis.functions.ConnectorFunctions;"
                + "import io.tapdata.entity.codec.TapCodecsRegistry;"
                + "import io.tapdata.pdk.apis.context.TapConnectionContext;"
                + "import io.tapdata.pdk.apis.entity.ConnectionOptions;"
                + "import io.tapdata.pdk.apis.entity.TestItem;"
                + "import io.tapdata.entity.schema.TapTable;"
                + "import io.tapdata.entity.schema.TapField;"
                + "import io.tapdata.entity.event.TapEvent;"
                + "import io.tapdata.entity.event.dml.TapInsertRecordEvent;"
                + "import java.util.ArrayList;"
                + "import java.util.LinkedHashMap;"
                + "import java.util.List;"
                + "import java.util.Map;"
                + "import java.util.function.Consumer;"
                + "public class SingleInit implements TapConnector {"
                + "  private int inits = 0;"
                + "  public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {"
                + "    functions.supportBatchRead((context, table, offset, size, consumer) -> {"
                + "      List<TapEvent> evs = new ArrayList<>();"
                + "      Map<String,Object> r = new LinkedHashMap<>(); r.put(\"id\", 1);"
                + "      evs.add(TapInsertRecordEvent.create().table(\"t1\").referenceTime(1L).after(r));"
                + "      consumer.accept(evs, null);"
                + "    });"
                + "  }"
                + "  public void init(TapConnectionContext c) { if (++inits > 1) throw new RuntimeException(\"init called twice\"); }"
                + "  public void stop(TapConnectionContext c) {}"
                + "  public void discoverSchema(TapConnectionContext c, List<String> t, int n, Consumer<List<TapTable>> s) {"
                + "    TapTable table = new TapTable(\"t1\"); table.add(new TapField(\"id\", \"int\"));"
                + "    List<TapTable> tables = new ArrayList<>(); tables.add(table); s.accept(tables);"
                + "  }"
                + "  public ConnectionOptions connectionTest(TapConnectionContext c, Consumer<TestItem> s) { return ConnectionOptions.create(); }"
                + "  public int tableCount(TapConnectionContext c) { return 1; }"
                + "}";
        return SyntheticJar.compileToJar(dir, "synthetic.SingleInit", src);
    }

    /** A connector whose static initializer throws — construction fails with a link/init Error, not a RuntimeException. */
    static Path staticThrowsSource(Path dir) {
        String src = ""
                + "package synthetic;"
                + "import io.tapdata.pdk.apis.TapConnector;"
                + "import io.tapdata.pdk.apis.functions.ConnectorFunctions;"
                + "import io.tapdata.entity.codec.TapCodecsRegistry;"
                + "import io.tapdata.pdk.apis.context.TapConnectionContext;"
                + "import io.tapdata.pdk.apis.entity.ConnectionOptions;"
                + "import io.tapdata.pdk.apis.entity.TestItem;"
                + "import io.tapdata.entity.schema.TapTable;"
                + "import java.util.List;"
                + "import java.util.function.Consumer;"
                + "public class StaticThrows implements TapConnector {"
                + "  static { if (Boolean.parseBoolean(\"true\")) throw new RuntimeException(\"static boom\"); }"
                + "  public void registerCapabilities(ConnectorFunctions f, TapCodecsRegistry c) {}"
                + "  public void init(TapConnectionContext c) {}"
                + "  public void stop(TapConnectionContext c) {}"
                + "  public void discoverSchema(TapConnectionContext c, List<String> t, int n, Consumer<List<TapTable>> s) {}"
                + "  public ConnectionOptions connectionTest(TapConnectionContext c, Consumer<TestItem> s) { return ConnectionOptions.create(); }"
                + "  public int tableCount(TapConnectionContext c) { return 0; }"
                + "}";
        return SyntheticJar.compileToJar(dir, "synthetic.StaticThrows", src);
    }

    /**
     * A source that registers batch read but throws from every lifecycle method (init / stop /
     * discover / connectionTest / tableCount). A capability derive reads only the registered ids, so a
     * successful derive proves it inited nothing and opened no connection.
     */
    static Path initHostileSource(Path dir) {
        String src = ""
                + "package synthetic;"
                + "import io.tapdata.pdk.apis.TapConnector;"
                + "import io.tapdata.pdk.apis.functions.ConnectorFunctions;"
                + "import io.tapdata.entity.codec.TapCodecsRegistry;"
                + "import io.tapdata.pdk.apis.context.TapConnectionContext;"
                + "import io.tapdata.pdk.apis.entity.ConnectionOptions;"
                + "import io.tapdata.pdk.apis.entity.TestItem;"
                + "import io.tapdata.entity.schema.TapTable;"
                + "import java.util.List;"
                + "import java.util.function.Consumer;"
                + "public class InitHostile implements TapConnector {"
                + "  public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {"
                + "    functions.supportBatchRead((a, b, c, d, e) -> {});"
                + "  }"
                + "  public void init(TapConnectionContext c) { throw new AssertionError(\"init called during capability derive\"); }"
                + "  public void stop(TapConnectionContext c) { throw new AssertionError(\"stop called during capability derive\"); }"
                + "  public void discoverSchema(TapConnectionContext c, List<String> t, int n, Consumer<List<TapTable>> s) { throw new AssertionError(\"discoverSchema called during capability derive\"); }"
                + "  public ConnectionOptions connectionTest(TapConnectionContext c, Consumer<TestItem> s) { throw new AssertionError(\"connectionTest called during capability derive\"); }"
                + "  public int tableCount(TapConnectionContext c) { throw new AssertionError(\"tableCount called during capability derive\"); }"
                + "}";
        return SyntheticJar.compileToJar(dir, "synthetic.InitHostile", src);
    }

    // ---- connection-test connectors (drive TapConnectorNode.connectionTest) -----------------------

    /**
     * A minimal connector whose connectionTest body the caller fills in — no read/write functions, inert
     * lifecycle and discovery. The body streams TestItems to the consumer and returns ConnectionOptions,
     * or throws to exercise the coded drive-failure path. connectionTest creates and releases its own
     * connection, so the drive never inits or stops the node around it.
     */
    private static String connectionTestSource(String simpleName, String testBody) {
        return ""
                + "package synthetic;"
                + "import io.tapdata.pdk.apis.TapConnector;"
                + "import io.tapdata.pdk.apis.functions.ConnectorFunctions;"
                + "import io.tapdata.entity.codec.TapCodecsRegistry;"
                + "import io.tapdata.pdk.apis.context.TapConnectionContext;"
                + "import io.tapdata.pdk.apis.entity.ConnectionOptions;"
                + "import io.tapdata.pdk.apis.entity.TestItem;"
                + "import io.tapdata.pdk.apis.exception.TapTestItemException;"
                + "import io.tapdata.entity.schema.TapTable;"
                + "import java.util.List;"
                + "import java.util.function.Consumer;"
                + "public class " + simpleName + " implements TapConnector {"
                + "  public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {}"
                + "  public void init(TapConnectionContext c) {}"
                + "  public void stop(TapConnectionContext c) {}"
                + "  public void discoverSchema(TapConnectionContext c, List<String> t, int n, Consumer<List<TapTable>> s) {}"
                + "  public int tableCount(TapConnectionContext c) { return 0; }"
                + "  public ConnectionOptions connectionTest(TapConnectionContext c, Consumer<TestItem> s) {" + testBody + "}"
                + "}";
    }

    /** A connector whose connectionTest reports a single passing check. */
    static Path passingTest(Path dir) {
        String body = ""
                + "s.accept(new TestItem(\"Connection\", TestItem.RESULT_SUCCESSFULLY));"
                + "return ConnectionOptions.create();";
        return SyntheticJar.compileToJar(dir, "synthetic.PassingTest", connectionTestSource("PassingTest", body));
    }

    /** A connector whose connectionTest passes but attaches a warning item carrying free-text information. */
    static Path warningTest(Path dir) {
        String body = ""
                + "s.accept(new TestItem(\"Connection\", TestItem.RESULT_SUCCESSFULLY));"
                + "s.accept(new TestItem(\"Time detection\", TestItem.RESULT_SUCCESSFULLY_WITH_WARN, \"clock skew 3s\"));"
                + "return ConnectionOptions.create();";
        return SyntheticJar.compileToJar(dir, "synthetic.WarningTest", connectionTestSource("WarningTest", body));
    }

    /** A connector whose connectionTest reports a failed check with a full structured diagnostic. */
    static Path failingTest(Path dir) {
        String body = ""
                + "s.accept(new TestItem(\"Connection\", TestItem.RESULT_SUCCESSFULLY));"
                + "TapTestItemException ex = new TapTestItemException();"
                + "ex.setMessage(\"Login denied\");"
                + "ex.setReason(\"bad credentials\");"
                + "ex.setSolution(\"check username/password\");"
                + "ex.setErrorCode(\"28000\");"
                + "s.accept(new TestItem(\"Login\", ex, TestItem.RESULT_FAILED));"
                + "return ConnectionOptions.create();";
        return SyntheticJar.compileToJar(dir, "synthetic.FailingTest", connectionTestSource("FailingTest", body));
    }

    /**
     * A connector reporting a check the way the postgres connector reports a failed replication-slot
     * probe: a coded exception carrying a bare numeric code and the statements it tried, and no
     * message, reason or solution at all. Everything a reader could use is in those statements.
     */
    static Path codeOnlyTest(Path dir) {
        String body = ""
                + "TapTestItemException ex = new TapTestItemException();"
                + "ex.setErrorCode(\"410003\");"
                + "ex.setDynamicDescriptionParameters(new String[]{"
                + "  \"SELECT pg_create_logical_replication_slot('test_tapdata_x','pgoutput')\"});"
                + "s.accept(new TestItem(\"read log\", ex, TestItem.RESULT_SUCCESSFULLY_WITH_WARN));"
                + "return ConnectionOptions.create();";
        return SyntheticJar.compileToJar(dir, "synthetic.CodeOnlyTest", connectionTestSource("CodeOnlyTest", body));
    }

    /** A connector whose connectionTest throws — the test could not be completed, a coded drive failure. */
    static Path throwingTest(Path dir) {
        String body = "throw new RuntimeException(\"test boom\");";
        return SyntheticJar.compileToJar(dir, "synthetic.ThrowingTest", connectionTestSource("ThrowingTest", body));
    }

    // ---- discover-schema connectors (drive TapConnector.discoverSchema) ---------------------------

    /**
     * A minimal connector whose discoverSchema body the caller fills in — no read/write functions, inert
     * lifecycle and connection test. The body streams TapTables to the consumer, or throws to exercise
     * the coded discover-failure path.
     */
    private static String discoverSource(String simpleName, String discoverBody) {
        return discoverSource(simpleName, discoverBody, "");
    }

    /** As above, with a registerCapabilities body — used to give a discoverable connector a count function. */
    private static String discoverSource(String simpleName, String discoverBody, String registerBody) {
        return ""
                + "package synthetic;"
                + "import io.tapdata.pdk.apis.TapConnector;"
                + "import io.tapdata.pdk.apis.functions.ConnectorFunctions;"
                + "import io.tapdata.entity.codec.TapCodecsRegistry;"
                + "import io.tapdata.pdk.apis.context.TapConnectionContext;"
                + "import io.tapdata.pdk.apis.entity.ConnectionOptions;"
                + "import io.tapdata.pdk.apis.entity.TestItem;"
                + "import io.tapdata.entity.schema.TapTable;"
                + "import io.tapdata.entity.schema.TapField;"
                + "import io.tapdata.entity.schema.TapIndex;"
                + "import io.tapdata.entity.schema.TapIndexField;"
                + "import java.util.ArrayList;"
                + "import java.util.List;"
                + "import java.util.function.Consumer;"
                + "public class " + simpleName + " implements TapConnector {"
                + "  public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {" + registerBody + "}"
                + "  public void init(TapConnectionContext c) {}"
                + "  public void stop(TapConnectionContext c) {}"
                + "  public ConnectionOptions connectionTest(TapConnectionContext c, Consumer<TestItem> s) { return ConnectionOptions.create(); }"
                + "  public int tableCount(TapConnectionContext c) { return 1; }"
                + "  public void discoverSchema(TapConnectionContext c, List<String> t, int n, Consumer<List<TapTable>> s) {" + discoverBody + "}"
                + "}";
    }

    /**
     * A connector reporting an index one of whose parts names no column, the way a real database
     * describes a functional index: the part is an expression over the row rather than a column, so
     * there is no column name to give and the driver reports none. A composite index mixing the two is
     * the ordinary shape - {@code UNIQUE KEY (user_id, scene, (ifnull(circle_type,'')))}.
     */
    static Path functionalIndexSource(Path dir) {
        String body = ""
                + "TapTable table = new TapTable(\"orders\");"
                + "table.add(new TapField(\"id\", \"int\").isPrimaryKey(true).primaryKeyPos(1));"
                + "table.add(new TapField(\"amount\", \"decimal\"));"
                + "table.add(new TapIndex().name(\"uq_mixed\").unique(true)"
                + "    .indexField(new TapIndexField().name(\"amount\").fieldAsc(true))"
                + "    .indexField(new TapIndexField().fieldAsc(true)));"
                + "List<TapTable> tables = new ArrayList<>();"
                + "tables.add(table);"
                + "s.accept(tables);";
        return SyntheticJar.compileToJar(dir, "synthetic.FunctionalIndex",
                discoverSource("FunctionalIndex", body));
    }

    /** The one-table discoverSchema body the discovery fixtures share. */
    private static final String ORDERS_TABLE = ""
            + "TapTable table = new TapTable(\"orders\");"
            + "table.add(new TapField(\"id\", \"int\").isPrimaryKey(true).primaryKeyPos(1));"
            + "table.add(new TapField(\"amount\", \"decimal\"));"
            + "table.add(new TapIndex().name(\"idx_amount\").unique(false)"
            + "    .indexField(new TapIndexField().name(\"amount\").fieldAsc(true)));"
            + "List<TapTable> tables = new ArrayList<>();"
            + "tables.add(table);"
            + "s.accept(tables);";

    /**
     * A connector whose discoverSchema reports one table with a primary-key field, a plain field and a
     * non-unique secondary index — enough to prove field, primary-key and index normalization. It
     * registers no count function, so its tables are the uncounted shape.
     */
    static Path discoverableSource(Path dir) {
        return SyntheticJar.compileToJar(dir, "synthetic.Discoverable",
                discoverSource("Discoverable", ORDERS_TABLE));
    }

    /** As above, plus a count function reporting a fixed row count for whatever table it is handed. */
    static Path countingDiscoverableSource(Path dir) {
        return SyntheticJar.compileToJar(dir, "synthetic.CountingDiscoverable",
                discoverSource("CountingDiscoverable", ORDERS_TABLE,
                        "functions.supportBatchCount((context, table) -> 4200000L);"));
    }

    /** The two-table discoverSchema body the multi-table counting fixtures share. */
    private static final String ORDERS_AND_ITEMS = ""
            + "TapTable orders = new TapTable(\"orders\");"
            + "orders.add(new TapField(\"id\", \"int\").isPrimaryKey(true).primaryKeyPos(1));"
            + "TapTable items = new TapTable(\"items\");"
            + "items.add(new TapField(\"id\", \"int\").isPrimaryKey(true).primaryKeyPos(1));"
            + "List<TapTable> tables = new ArrayList<>();"
            + "tables.add(orders);"
            + "tables.add(items);"
            + "s.accept(tables);";

    /**
     * Two tables whose count function throws for the first and answers for the second — the shape that
     * tells "this table could not be counted" apart from "counting stopped at the first refusal".
     */
    static Path partiallyCountableSource(Path dir) {
        return SyntheticJar.compileToJar(dir, "synthetic.PartiallyCountable",
                discoverSource("PartiallyCountable", ORDERS_AND_ITEMS,
                        "functions.supportBatchCount((context, table) -> {"
                                + "if (\"orders\".equals(table.getId())) { throw new RuntimeException(\"count boom\"); }"
                                + "return 7L; });"));
    }

    /** How long one count of the slow fixture takes. Any budget under test is set well either side of it. */
    static final long SLOW_COUNT_MILLIS = 150L;

    /**
     * Two tables the connector answers for with counts of its own, each count taking real time to come
     * back. Both halves matter: because neither table is one the connector refuses, a table that ends up
     * uncounted can only have been left so by the caller giving up, and because a count is slow, a budget
     * far shorter than one count is spent by the time the second table comes round.
     */
    static Path slowCountableTwoTableSource(Path dir) {
        return SyntheticJar.compileToJar(dir, "synthetic.SlowCountableTwoTable",
                discoverSource("SlowCountableTwoTable", ORDERS_AND_ITEMS,
                        "functions.supportBatchCount((context, table) -> {"
                                + "try { Thread.sleep(" + SLOW_COUNT_MILLIS + "L); }"
                                + "catch (InterruptedException e) { Thread.currentThread().interrupt(); }"
                                + "return \"orders\".equals(table.getId()) ? 11L : 7L; });"));
    }

    /** As above, but the count throws — the table is still discovered, just not counted. */
    static Path throwingCountSource(Path dir) {
        return SyntheticJar.compileToJar(dir, "synthetic.ThrowingCount",
                discoverSource("ThrowingCount", ORDERS_TABLE,
                        "functions.supportBatchCount((context, table) -> { throw new RuntimeException(\"count boom\"); });"));
    }

    /** A connector whose discoverSchema throws — discovery could not complete, a coded drive failure. */
    static Path throwingDiscoverSource(Path dir) {
        String body = "throw new RuntimeException(\"discover boom\");";
        return SyntheticJar.compileToJar(dir, "synthetic.ThrowingDiscover", discoverSource("ThrowingDiscover", body));
    }

    // ---- self-scan connectors (drive ConnectorIntrospector) ---------------------------------------

    /** The imports every self-scan fixture's source needs. */
    private static final String SELF_SCAN_IMPORTS = ""
            + "package synthetic;"
            + "import io.tapdata.pdk.apis.TapConnector;"
            + "import io.tapdata.pdk.apis.annotations.TapConnectorClass;"
            + "import io.tapdata.pdk.apis.functions.ConnectorFunctions;"
            + "import io.tapdata.entity.codec.TapCodecsRegistry;"
            + "import io.tapdata.pdk.apis.context.TapConnectionContext;"
            + "import io.tapdata.pdk.apis.entity.ConnectionOptions;"
            + "import io.tapdata.pdk.apis.entity.TestItem;"
            + "import io.tapdata.entity.schema.TapTable;"
            + "import java.util.List;"
            + "import java.util.function.Consumer;";

    /** The frozen TapConnector methods, all inert: introspection reads a class's annotation, spec and
     *  manifest and never drives it, so a successful introspect proves none of these ran. */
    private static final String INERT_CONNECTOR_BODY = ""
            + "  public void registerCapabilities(ConnectorFunctions f, TapCodecsRegistry c) {}"
            + "  public void init(TapConnectionContext c) {}"
            + "  public void stop(TapConnectionContext c) {}"
            + "  public void discoverSchema(TapConnectionContext c, List<String> t, int n, Consumer<List<TapTable>> s) {}"
            + "  public ConnectionOptions connectionTest(TapConnectionContext c, Consumer<TestItem> s) { return ConnectionOptions.create(); }"
            + "  public int tableCount(TapConnectionContext c) { return 0; }";

    /**
     * A single-connector jar: one {@code @TapConnectorClass} entry class, the {@code orders-spec.json}
     * resource its annotation names, and a {@code PDK-API-Version} manifest attribute — the exact shape
     * a real connector dist jar has, and what self-scan introspects.
     */
    static Path annotatedConnector(Path dir) {
        String src = SELF_SCAN_IMPORTS
                + "@TapConnectorClass(\"orders-spec.json\")"
                + "public class OrdersConnector implements TapConnector {" + INERT_CONNECTOR_BODY + "}";
        return SyntheticJar.compileToJar(dir, "synthetic.OrdersConnector", src,
                Map.of("orders-spec.json", "{\"id\":\"orders\"}"),
                Map.of("PDK-API-Version", "1.3.5"));
    }

    /** A jar with a plain class and no {@code @TapConnectorClass} anywhere — not a connector artifact. */
    static Path jarWithoutConnectorClass(Path dir) {
        return SyntheticJar.compileToJar(dir, "synthetic.NotAConnector",
                "package synthetic; public class NotAConnector {}");
    }

    /** An annotated connector whose {@code @TapConnectorClass} names a spec the jar does not contain. */
    static Path annotatedConnectorMissingSpec(Path dir) {
        String src = SELF_SCAN_IMPORTS
                + "@TapConnectorClass(\"absent-spec.json\")"
                + "public class GhostConnector implements TapConnector {" + INERT_CONNECTOR_BODY + "}";
        return SyntheticJar.compileToJar(dir, "synthetic.GhostConnector", src,
                Map.of(), // the annotation names a spec, but none is packaged
                Map.of("PDK-API-Version", "1.3.5"));
    }

    /** One jar carrying two unrelated annotated connectors — an ambiguous artifact for registration. */
    static Path twoDistinctConnectors(Path dir) {
        String src = SELF_SCAN_IMPORTS
                + "@TapConnectorClass(\"a-spec.json\")"
                + "public class ConnectorA implements TapConnector {" + INERT_CONNECTOR_BODY + "}"
                + "@TapConnectorClass(\"b-spec.json\")"
                + "class ConnectorB implements TapConnector {" + INERT_CONNECTOR_BODY + "}";
        return SyntheticJar.compileToJar(dir, "synthetic.ConnectorA", src,
                Map.of("a-spec.json", "{\"id\":\"a\"}", "b-spec.json", "{\"id\":\"b\"}"),
                Map.of("PDK-API-Version", "1.3.5"));
    }

    /** An annotated connector that registers a read capability — proves an introspected artifact drives
     *  capability derivation, closing the self-scan to capability-derive seam. */
    static Path annotatedEmittingConnector(Path dir) {
        String body = ""
                + "  public void registerCapabilities(ConnectorFunctions f, TapCodecsRegistry c) {"
                + "    f.supportBatchRead((context, table, offset, size, consumer) -> {});"
                + "  }"
                + "  public void init(TapConnectionContext c) {}"
                + "  public void stop(TapConnectionContext c) {}"
                + "  public void discoverSchema(TapConnectionContext c, List<String> t, int n, Consumer<List<TapTable>> s) {}"
                + "  public ConnectionOptions connectionTest(TapConnectionContext c, Consumer<TestItem> s) { return ConnectionOptions.create(); }"
                + "  public int tableCount(TapConnectionContext c) { return 0; }";
        String src = SELF_SCAN_IMPORTS
                + "@TapConnectorClass(\"reader-spec.json\")"
                + "public class ReaderConnector implements TapConnector {" + body + "}";
        // A registered PDK API version, so the introspected version passes the deriver's level gate.
        return SyntheticJar.compileToJar(dir, "synthetic.ReaderConnector", src,
                Map.of("reader-spec.json", "{\"id\":\"reader\"}"),
                Map.of("PDK-API-Version", "2.0.8"));
    }

    // ---- registrable connectors (drive ConnectorArtifactRegistrar / SeedConnectorSweep) -----------

    /**
     * A registrable connector artifact: one annotated entry class and a spec shaped the way a real
     * connector spec is — the connector id declared under {@code properties.id} — which is the
     * identity registration files the artifact under.
     */
    static Path seedableOrdersConnector(Path dir) {
        String src = SELF_SCAN_IMPORTS
                + "@TapConnectorClass(\"orders-spec.json\")"
                + "public class SeedOrders implements TapConnector {" + INERT_CONNECTOR_BODY + "}";
        return SyntheticJar.compileToJar(dir, "synthetic.SeedOrders", src,
                Map.of("orders-spec.json", "{\"properties\":{\"id\":\"orders\"}}"),
                Map.of("PDK-API-Version", "1.3.5"));
    }

    /**
     * A second registrable connector declaring the SAME id as {@link #seedableOrdersConnector} but with
     * different bytes (a distinct entry class) — a different artifact competing for one connector id,
     * the register-time conflict the registrar refuses.
     */
    static Path conflictingOrdersConnector(Path dir) {
        String src = SELF_SCAN_IMPORTS
                + "@TapConnectorClass(\"orders-spec.json\")"
                + "public class OtherOrders implements TapConnector {" + INERT_CONNECTOR_BODY + "}";
        return SyntheticJar.compileToJar(dir, "synthetic.OtherOrders", src,
                Map.of("orders-spec.json", "{\"properties\":{\"id\":\"orders\"}}"),
                Map.of("PDK-API-Version", "1.3.5"));
    }

    /**
     * A registrable connector declaring whichever id the caller names — for driving the acceptance
     * guard across a set of ids rather than one representative.
     *
     * <p>The entry class name is the id with everything that cannot appear in a Java identifier removed,
     * so that ids carrying dashes still compile. That mangling is invisible to the guard, which reads
     * the id from the spec and never from the class.
     */
    static Path seedableConnector(Path dir, String connectorId) {
        String type = "Seed" + connectorId.replaceAll("[^A-Za-z0-9]", "");
        String src = SELF_SCAN_IMPORTS
                + "@TapConnectorClass(\"" + connectorId + "-spec.json\")"
                + "public class " + type + " implements TapConnector {" + INERT_CONNECTOR_BODY + "}";
        return SyntheticJar.compileToJar(dir, "synthetic." + type, src,
                Map.of(connectorId + "-spec.json", "{\"properties\":{\"id\":\"" + connectorId + "\"}}"),
                Map.of("PDK-API-Version", "1.3.5"));
    }

    /**
     * A registrable connector declaring an officially supported id, so it passes the runtime-register
     * guard. Used wherever a test drives the register path itself rather than the guard — post-guard
     * those paths only ever run for an official connector, so an unofficial fixture would exercise a
     * combination production no longer reaches.
     */
    static Path seedableMysqlConnector(Path dir) {
        String src = SELF_SCAN_IMPORTS
                + "@TapConnectorClass(\"mysql-spec.json\")"
                + "public class SeedMysql implements TapConnector {" + INERT_CONNECTOR_BODY + "}";
        return SyntheticJar.compileToJar(dir, "synthetic.SeedMysql", src,
                Map.of("mysql-spec.json", "{\"properties\":{\"id\":\"mysql\"}}"),
                Map.of("PDK-API-Version", "1.3.5"));
    }

    /**
     * A second artifact declaring the SAME official id as {@link #seedableMysqlConnector} with different
     * bytes — the register-time conflict, reached through the guard rather than short-circuited by it.
     */
    static Path conflictingMysqlConnector(Path dir) {
        String src = SELF_SCAN_IMPORTS
                + "@TapConnectorClass(\"mysql-spec.json\")"
                + "public class OtherMysql implements TapConnector {" + INERT_CONNECTOR_BODY + "}";
        return SyntheticJar.compileToJar(dir, "synthetic.OtherMysql", src,
                Map.of("mysql-spec.json", "{\"properties\":{\"id\":\"mysql\"}}"),
                Map.of("PDK-API-Version", "1.3.5"));
    }

    /**
     * A second registrable connector declaring an officially supported id, whose manifest declares no
     * PDK API version — the pair to {@link #seedableMysqlConnector} wherever two accepted connectors
     * are needed at once.
     */
    static Path seedableMongodbConnector(Path dir) {
        String src = SELF_SCAN_IMPORTS
                + "@TapConnectorClass(\"mongodb-spec.json\")"
                + "public class SeedMongodb implements TapConnector {" + INERT_CONNECTOR_BODY + "}";
        return SyntheticJar.compileToJar(dir, "synthetic.SeedMongodb", src,
                Map.of("mongodb-spec.json", "{\"properties\":{\"id\":\"mongodb\"}}"),
                Map.of());
    }

    /** A second registrable connector under a distinct id, whose manifest declares no PDK API version. */
    static Path seedablePaymentsConnector(Path dir) {
        String src = SELF_SCAN_IMPORTS
                + "@TapConnectorClass(\"payments-spec.json\")"
                + "public class SeedPayments implements TapConnector {" + INERT_CONNECTOR_BODY + "}";
        return SyntheticJar.compileToJar(dir, "synthetic.SeedPayments", src,
                Map.of("payments-spec.json", "{\"properties\":{\"id\":\"payments\"}}"),
                Map.of());
    }

    /**
     * A connector whose class scans and links but whose annotation attribute bytes are corrupt: the
     * {@code RuntimeVisibleAnnotations} count is bumped past the bytes the attribute actually holds,
     * so a reflective annotation read fails with an {@code AnnotationFormatError}.
     */
    static Path corruptAnnotationConnector(Path dir) {
        String src = SELF_SCAN_IMPORTS
                + "@TapConnectorClass(\"corrupt-spec.json\")"
                + "public class CorruptAnnotation implements TapConnector {" + INERT_CONNECTOR_BODY + "}";
        byte[] doctored = bumpAnnotationCount(SyntheticJar.classBytes(dir, "synthetic.CorruptAnnotation", src));
        return SyntheticJar.jarWithEntries(dir,
                Map.of("synthetic/CorruptAnnotation.class", doctored,
                        "corrupt-spec.json", "{\"properties\":{\"id\":\"corrupt\"}}"
                                .getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                Map.of("PDK-API-Version", "1.3.5"));
    }

    /**
     * Bumps the count of a one-annotation {@code RuntimeVisibleAnnotations} attribute (attribute
     * length 11, count 1 — the trailing {@code 00 00 00 0B 00 01} pattern) to two, leaving the
     * attribute one whole annotation short. The constant pool and class structure stay valid; only
     * reflective annotation parsing runs off the end. The last match is taken because class-level
     * annotation attributes trail the class file.
     */
    private static byte[] bumpAnnotationCount(byte[] classBytes) {
        byte[] pattern = {0x00, 0x00, 0x00, 0x0B, 0x00, 0x01};
        for (int i = classBytes.length - pattern.length; i >= 0; i--) {
            boolean match = true;
            for (int j = 0; j < pattern.length; j++) {
                if (classBytes[i + j] != pattern[j]) {
                    match = false;
                    break;
                }
            }
            if (match) {
                byte[] doctored = classBytes.clone();
                doctored[i + 5] = 0x02;
                return doctored;
            }
        }
        throw new IllegalStateException("no one-annotation RuntimeVisibleAnnotations attribute found to corrupt");
    }

    /** An annotated connector whose spec resource is not JSON at all — no identity can come off it. */
    static Path unparsableSpecConnector(Path dir) {
        String src = SELF_SCAN_IMPORTS
                + "@TapConnectorClass(\"broken-spec.json\")"
                + "public class BrokenSpec implements TapConnector {" + INERT_CONNECTOR_BODY + "}";
        return SyntheticJar.compileToJar(dir, "synthetic.BrokenSpec", src,
                Map.of("broken-spec.json", "{not json"),
                Map.of("PDK-API-Version", "1.3.5"));
    }

    /** An annotated connector that subclasses an also-annotated base — the variant is the real entry. */
    static Path baseAndVariantConnector(Path dir) {
        String src = SELF_SCAN_IMPORTS
                + "@TapConnectorClass(\"base-spec.json\")"
                + "class BaseConnector implements TapConnector {" + INERT_CONNECTOR_BODY + "}"
                + "@TapConnectorClass(\"variant-spec.json\")"
                + "public class VariantConnector extends BaseConnector {}";
        return SyntheticJar.compileToJar(dir, "synthetic.VariantConnector", src,
                Map.of("base-spec.json", "{\"id\":\"base\"}", "variant-spec.json", "{\"id\":\"variant\"}"),
                Map.of("PDK-API-Version", "1.3.5"));
    }

    /** The shared scaffold for a data-browser connector: the three read-face functions the caller fills in. */
    private static String readFace(String simpleName, String registerBody) {
        return readFace(simpleName, registerBody,
                "  public void init(TapConnectionContext c) {}"
                        + "  public void stop(TapConnectionContext c) {}");
    }

    /**
     * A data-browser connector that appends every {@code init} and {@code stop} it is driven through to
     * the file named by the {@code synthetic.marker} system property. Counting these from the outside
     * needs a channel that outlives one connector's isolated class loader — a static counter inside the
     * connector is reset by every fresh load, which is exactly the thing under test — and a file read
     * back by the test is that channel.
     */
    static Path lifecycleRecordingSource(Path dir) {
        String register = "functions.supportGetTableNamesFunction((c, batchSize, consumer) -> {"
                + "  List<String> names = new ArrayList<>(); names.add(\"orders\"); consumer.accept(names);"
                + "});";
        String lifecycle = ""
                + "  public void init(TapConnectionContext c) { mark(\"init\"); }"
                + "  public void stop(TapConnectionContext c) { mark(\"stop\"); }"
                + "  private static void mark(String what) {"
                + "    try {"
                + "      java.nio.file.Files.writeString("
                + "        java.nio.file.Path.of(System.getProperty(\"synthetic.marker\")), what + \"\\n\","
                + "        java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);"
                + "    } catch (java.io.IOException e) { throw new RuntimeException(e); }"
                + "  }";
        return SyntheticJar.compileToJar(dir, "synthetic.LifecycleRecording",
                readFace("LifecycleRecording", register, lifecycle));
    }

    private static String readFace(String simpleName, String registerBody, String lifecycleBody) {
        return ""
                + "package synthetic;"
                + "import io.tapdata.pdk.apis.TapConnector;"
                + "import io.tapdata.pdk.apis.functions.ConnectorFunctions;"
                + "import io.tapdata.entity.codec.TapCodecsRegistry;"
                + "import io.tapdata.pdk.apis.context.TapConnectionContext;"
                + "import io.tapdata.pdk.apis.entity.ConnectionOptions;"
                + "import io.tapdata.pdk.apis.entity.ExecuteResult;"
                + "import io.tapdata.pdk.apis.entity.TestItem;"
                + "import io.tapdata.pdk.apis.functions.connection.TableInfo;"
                + "import io.tapdata.entity.schema.TapTable;"
                + "import java.util.ArrayList;"
                + "import java.util.LinkedHashMap;"
                + "import java.util.List;"
                + "import java.util.Map;"
                + "import java.util.function.Consumer;"
                + "public class " + simpleName + " implements TapConnector {"
                + "  public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codecs) {"
                + registerBody
                + "  }"
                + lifecycleBody
                + "  public void discoverSchema(TapConnectionContext c, List<String> t, int n, Consumer<List<TapTable>> s) {}"
                + "  public ConnectionOptions connectionTest(TapConnectionContext c, Consumer<TestItem> s) { return ConnectionOptions.create(); }"
                + "  public int tableCount(TapConnectionContext c) { return 1; }"
                + "}";
    }

    /**
     * A data-browser connector registering all three read-face functions, each shaped after the real
     * mongodb connector's own behaviour: table names arrive in more than one consumer batch, query
     * results arrive in more than one {@code ExecuteResult}, and the query fills a missing
     * {@code database} into the caller's own params map — so a caller that passes an immutable map
     * fails here exactly as it would there. The query echoes what it was handed back as rows, so a
     * test can assert on the request the drive assembled.
     *
     * <p>The {@code database} echo is captured <em>before</em> the connector fills its own in, and the
     * fill stays unconditional. Both halves are load-bearing: reading the param after the fill would
     * report the connector's value whatever the caller sent, and making the fill conditional would take
     * the write away from the one test that proves the map is writable.
     */
    static Path readFaceSource(Path dir) {
        String register = ""
                + "functions.supportGetTableNamesFunction((c, batchSize, consumer) -> {"
                + "  List<String> first = new ArrayList<>(); first.add(\"orders\"); consumer.accept(first);"
                + "  List<String> second = new ArrayList<>(); second.add(\"shipments\"); consumer.accept(second);"
                + "});"
                + "functions.supportGetTableInfoFunction((c, table) ->"
                + "  TableInfo.create().numOfRows(512L).storageSize(4096L).avgObjSize(8L));"
                + "functions.supportExecuteCommandFunction((c, command, consumer) -> {"
                + "  Map<String,Object> params = command.getParams();"
                // Sentinels rather than null: the echo travels back as a row value, and a null one is
                // indistinguishable from "no such echo" at the far end. Absent and present-but-null are
                // kept apart because they are different mistakes -- the second one also defeats the fill
                // below, so a caller that sends null is worse off than one that sends nothing.
                + "  Object arrivedDatabase = params.get(\"database\") != null ? params.get(\"database\")"
                + "    : (params.containsKey(\"database\") ? \"<sent-but-null>\" : \"<none-was-sent>\");"
                + "  params.put(\"database\", \"filled-in-by-the-connector\");"
                + "  Map<String,Object> first = new LinkedHashMap<>();"
                + "  first.put(\"echoed\", \"command\"); first.put(\"value\", command.getCommand());"
                + "  List<Map<String,Object>> batch1 = new ArrayList<>(); batch1.add(first);"
                + "  consumer.accept(new ExecuteResult<List<Map<String,Object>>>().result(batch1));"
                + "  Map<String,Object> second = new LinkedHashMap<>();"
                + "  second.put(\"echoed\", \"collection\"); second.put(\"value\", params.get(\"collection\"));"
                + "  List<Map<String,Object>> batch2 = new ArrayList<>(); batch2.add(second);"
                + "  consumer.accept(new ExecuteResult<List<Map<String,Object>>>().result(batch2));"
                + "  Map<String,Object> third = new LinkedHashMap<>();"
                + "  third.put(\"echoed\", \"database-as-it-arrived\"); third.put(\"value\", arrivedDatabase);"
                + "  List<Map<String,Object>> batch3 = new ArrayList<>(); batch3.add(third);"
                + "  consumer.accept(new ExecuteResult<List<Map<String,Object>>>().result(batch3));"
                // Same sentinel treatment as the database above, and for the same reason: a request that
                // asks for no order must leave the key out entirely, because a connector reads a present
                // sort and an absent one differently.
                + "  Object arrivedSort = params.containsKey(\"sort\") ? params.get(\"sort\")"
                + "    : \"<none-was-sent>\";"
                + "  Map<String,Object> fourth = new LinkedHashMap<>();"
                + "  fourth.put(\"echoed\", \"sort\"); fourth.put(\"value\", arrivedSort);"
                + "  List<Map<String,Object>> batch4 = new ArrayList<>(); batch4.add(fourth);"
                + "  consumer.accept(new ExecuteResult<List<Map<String,Object>>>().result(batch4));"
                // The filter arrives already in this connector's own dialect: the seam above carries a
                // neutral vocabulary, and what a test needs to see is what the translation produced.
                + "  Map<String,Object> fifth = new LinkedHashMap<>();"
                + "  fifth.put(\"echoed\", \"filter\"); fifth.put(\"value\", params.get(\"filter\"));"
                + "  List<Map<String,Object>> batch5 = new ArrayList<>(); batch5.add(fifth);"
                + "  consumer.accept(new ExecuteResult<List<Map<String,Object>>>().result(batch5));"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.ReadFace", readFace("ReadFace", register));
    }

    /**
     * A data-browser connector holding {@code held} rows, whose query honours the limit it is given and
     * whose {@code getTableInfo} reports that same count. Honouring the limit is what makes "is there
     * more" observable at all: a connector that ignored it would answer every read with the whole
     * collection and no bound could be told apart from any other.
     */
    static Path boundedQuerySource(Path dir, int held) {
        String register = "functions.supportGetTableInfoFunction((c, table) ->"
                + "  TableInfo.create().numOfRows(" + held + "L).storageSize(4096L).avgObjSize(8L));"
                + boundedQuery(held);
        return SyntheticJar.compileToJar(dir, "synthetic.BoundedQuery", readFace("BoundedQuery", register));
    }

    /**
     * The same bounded query with no {@code getTableInfo} registered — the shape of a connector that
     * can serve a read but cannot say how much it holds. Reachable: which functions a connector offers
     * is a property of the connector, not of the request.
     */
    static Path unreportedSizeQuerySource(Path dir, int held) {
        return SyntheticJar.compileToJar(dir, "synthetic.UnreportedSizeQuery",
                readFace("UnreportedSizeQuery", boundedQuery(held)));
    }

    private static String boundedQuery(int held) {
        return "functions.supportExecuteCommandFunction((c, command, consumer) -> {"
                + "  int limit = ((Number) command.getParams().get(\"limit\")).intValue();"
                + "  int emitted = Math.min(" + held + ", limit);"
                + "  List<Map<String,Object>> batch = new ArrayList<>();"
                + "  for (int i = 0; i < emitted; i++) {"
                + "    Map<String,Object> row = new LinkedHashMap<>(); row.put(\"n\", i); batch.add(row);"
                + "  }"
                + "  consumer.accept(new ExecuteResult<List<Map<String,Object>>>().result(batch));"
                + "});";
    }

    /**
     * A data-browser connector whose query gives up part way through and returns as if it had not.
     * It emits one batch, then interrupts the thread it is running on — which is precisely the state
     * that makes a real connector's read loop stop: the loop asks whether it is still alive between
     * batches, and a connector reads that as "started and not interrupted". Nothing throws, nothing is
     * reported through the result, and the rows already accumulated are dropped. What arrives at the
     * bridge is a short answer that looks exactly like a small collection.
     */
    static Path abandoningQuerySource(Path dir) {
        String register = "functions.supportExecuteCommandFunction((c, command, consumer) -> {"
                + "  Map<String,Object> row = new LinkedHashMap<>(); row.put(\"n\", 0);"
                + "  List<Map<String,Object>> batch = new ArrayList<>(); batch.add(row);"
                + "  consumer.accept(new ExecuteResult<List<Map<String,Object>>>().result(batch));"
                + "  Thread.currentThread().interrupt();"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.AbandoningQuery",
                readFace("AbandoningQuery", register));
    }

    /** A data-browser connector whose query reports its failure through {@code ExecuteResult.error}. */
    static Path erroringQuerySource(Path dir) {
        String register = "functions.supportExecuteCommandFunction((c, command, consumer) -> {"
                + "  consumer.accept(new ExecuteResult<List<Map<String,Object>>>()"
                + "    .error(new RuntimeException(\"query boom\")));"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.ErroringQuery", readFace("ErroringQuery", register));
    }

    /** A data-browser connector whose query throws out of the function itself. */
    static Path throwingQuerySource(Path dir) {
        String register = "functions.supportExecuteCommandFunction((c, command, consumer) -> {"
                + "  throw new RuntimeException(\"execute boom\");"
                + "});";
        return SyntheticJar.compileToJar(dir, "synthetic.ThrowingQuery", readFace("ThrowingQuery", register));
    }
}
