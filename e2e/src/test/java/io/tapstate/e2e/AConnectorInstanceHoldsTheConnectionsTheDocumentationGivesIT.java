package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.PdkSinkPort;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.DdlPolicy;
import io.tapstate.spi.sink.OnFullLoad;
import io.tapstate.spi.sink.SinkConfig;
import io.tapstate.spi.sink.SinkWriter;
import io.tapstate.spi.sink.TargetField;
import io.tapstate.spi.sink.TargetIndex;
import io.tapstate.spi.sink.TargetTable;
import io.tapstate.spi.sink.WriteMode;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * How many connections one connector instance of each public connector holds, counted by the database itself: the
 * figure the public documentation gives beside the connector instances a run reports, held here to what it says.
 *
 * <p>A run reports how many connector instances its sinks open and says nothing about connections: how many a
 * connector opens is decided inside the connector, and a figure the product worked out for it would be a guess about
 * somebody else's pool. What a reader needs is the multiplier, so this case measures it where it cannot be guessed at
 * - by asking the database who is connected - and holds both the figure and the multiplication to account.
 *
 * <p>The figure is a ceiling, and so is the multiplication. Some connectors open a connection for part of a write and
 * close it again, so an instance holds its most only now and then; some keep part of their pool for all of their
 * instances in a process, so several hold fewer than as many times one. So one instance has to hold something and no
 * more than the documentation gives, four instances no more than four times that, and every connection has to be
 * gone once the instances close. A count sampled while connections come and go can miss one; a ceiling cannot be
 * passed by missing one, which is why the figure is written down rather than read off a single instance each run.
 *
 * <p>Each instance is opened as a sink's writer opens one for an artifact not certified to be shared - on its own -
 * and they write at the same time, one thread each, as a sink's writers do. What they hold is read after every one
 * of them has opened, so the connections a table's preparation takes and gives back are not counted as a writer's.
 * How many are running a statement at the same moment is read too and reported beside it; that figure is what the
 * samples saw, not a bound, and nothing is asserted about it.
 *
 * <p>One method per connector, so a run can leave out a database it has no image of; the measured figures are printed
 * on a line starting {@code CONNECTIONS}.
 */
class AConnectorInstanceHoldsTheConnectionsTheDocumentationGivesIT {

    private static final PipelineNode NODE = new PipelineNode("connections", "sink");
    private static final int INSTANCES = 4;
    private static final int BATCHES = 20;
    private static final int ROWS = 50;
    private static final Duration SAMPLE_EVERY = Duration.ofMillis(20);
    private static final Map<String, ConnectorRef> CONNECTORS = new ConcurrentHashMap<>();

    /**
     * The most connections one instance holds, as docs/observability/README.md gives them. A connector missing here
     * has not been measured yet: it is held only to holding something and letting it go, and its figures are printed
     * to be written down.
     */
    private static final Map<String, Long> DOCUMENTED =
            Map.of("mysql", 2L, "postgres", 1L, "mongodb", 3L, "sqlserver", 2L);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void mysql() throws Exception {
        RealConnectorGate.require("mysql");
        Map<String, Object> settings = SharedMySql.settings("connections_mysql");
        try (JdbcSessions sessions = new JdbcSessions(SharedMySql.connect(settings), """
                SELECT COUNT(*), COALESCE(SUM(CASE WHEN COMMAND <> 'Sleep' THEN 1 ELSE 0 END), 0)
                FROM information_schema.PROCESSLIST WHERE USER = ? AND ID <> CONNECTION_ID()""",
                String.valueOf(settings.get("username")))) {
            measure("mysql", settings, sessions);
        }
    }

    @Test
    void postgres() throws Exception {
        RealConnectorGate.require("postgres");
        Map<String, Object> settings = SharedPostgres.settings("connections_postgres");
        Map<String, Object> config = new LinkedHashMap<>(settings);
        config.put("user", config.remove("username"));
        config.put("schema", "public");
        try (JdbcSessions sessions = new JdbcSessions(SharedPostgres.connect(settings), """
                SELECT COUNT(*), COUNT(*) FILTER (WHERE state = 'active')
                FROM pg_stat_activity WHERE usename = ? AND pid <> pg_backend_pid()""",
                String.valueOf(settings.get("username")))) {
            measure("postgres", config, sessions);
        }
    }

    @Test
    void mongodb() throws Exception {
        RealConnectorGate.require("mongodb");
        String uri = SharedMongo.replicaSetUrl("connections_mongodb");
        try (MongoSessions sessions = new MongoSessions(uri)) {
            measure("mongodb", Map.of("uri", uri), sessions);
        }
    }

    @Test
    void sqlserver() throws Exception {
        RealConnectorGate.require("sqlserver");
        Map<String, Object> settings = SharedSqlServer.settings("connections_sqlserver");
        String url = "jdbc:sqlserver://" + settings.get("host") + ":" + settings.get("port") + ";databaseName="
                + settings.get("database") + ";encrypt=false;trustServerCertificate=true";
        try (JdbcSessions sessions = new JdbcSessions(DriverManager.getConnection(url,
                String.valueOf(settings.get("user")), String.valueOf(settings.get("password"))), """
                SELECT COUNT(*), COALESCE(SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END), 0)
                FROM sys.dm_exec_sessions
                WHERE login_name = ? AND database_id = DB_ID() AND is_user_process = 1 AND session_id <> @@SPID""",
                String.valueOf(settings.get("user")))) {
            measure("sqlserver", settings, sessions);
        }
    }

    @Test
    void oracle() throws Exception {
        RealConnectorGate.require("oracle");
        Map<String, Object> settings = SharedOracle.settings("connections_oracle");
        String url = "jdbc:oracle:thin:@//" + settings.get("host") + ":" + settings.get("port") + "/"
                + settings.get("pdb");
        try (JdbcSessions sessions = new JdbcSessions(DriverManager.getConnection(url,
                String.valueOf(settings.get("user")), String.valueOf(settings.get("password"))), """
                SELECT COUNT(*), COALESCE(SUM(CASE WHEN STATUS = 'ACTIVE' THEN 1 ELSE 0 END), 0)
                FROM V$SESSION WHERE USERNAME = ? AND SID <> SYS_CONTEXT('USERENV', 'SID')""",
                String.valueOf(settings.get("user")))) {
            measure("oracle", settings, sessions);
        }
    }

    /**
     * Reads the connections one instance of {@code connector} holds, then four, against what was connected before
     * either opened, and holds the second to four times the first and both to leaving nothing behind.
     */
    private static void measure(String connector, Map<String, Object> config, Sessions sessions) throws Exception {
        TargetTable target = new TargetTable("orders", List.of(
                new TargetField("id", "source_integer", true, TapstateType.INT64),
                new TargetField("seq", "source_integer", false, TapstateType.INT64)),
                List.of(new TargetIndex(List.of("id"), true)));
        Reading before = sessions.read();
        Held one = whileWriting(connector, config, target, 1, sessions).less(before);
        awaitNothingLeft(connector, sessions, before);
        Held four = whileWriting(connector, config, target, INSTANCES, sessions).less(before);
        awaitNothingLeft(connector, sessions, before);

        System.out.printf("CONNECTIONS %s held-per-instance=%d most-per-instance-while-writing=%d"
                        + " most-for-%d-while-writing=%d active-seen-one=%d active-seen-%d=%d%n",
                connector, one.between().open(), one.writing().open(), INSTANCES, four.writing().open(),
                one.writing().active(), INSTANCES, four.writing().active());
        long oneMost = Math.max(one.between().open(), one.writing().open());
        long fourMost = Math.max(four.between().open(), four.writing().open());
        assertThat(oneMost).as("the most connections one %s instance was seen holding", connector).isPositive();
        if (DOCUMENTED.containsKey(connector)) {
            long documented = DOCUMENTED.get(connector);
            assertThat(oneMost)
                    .as("the most connections one %s instance was seen holding, no more than the documentation "
                            + "gives", connector)
                    .isLessThanOrEqualTo(documented);
            assertThat(fourMost)
                    .as("the most connections %d %s instances were seen holding, no more than that many times what "
                            + "the documentation gives for one", INSTANCES, connector)
                    .isLessThanOrEqualTo(INSTANCES * documented);
        }
    }

    /**
     * What {@code count} instances held: the most at once while they wrote together, sampled throughout the writes,
     * and what they went on holding once every write had come back and the count had stopped moving.
     */
    private static Held whileWriting(String connector, Map<String, Object> config, TargetTable target, int count,
            Sessions sessions) throws Exception {
        List<SinkWriter> writers = new ArrayList<>();
        ExecutorService writing = Executors.newFixedThreadPool(count);
        ExecutorService sampling = Executors.newSingleThreadExecutor();
        try {
            // One after another: each prepares the table as a writer does, and preparing it twice at once is not
            // what a sink does - its preparation runs once, ahead of every writer.
            for (int i = 0; i < count; i++) {
                writers.add(new PdkSinkPort(
                        AConnectorInstanceHoldsTheConnectionsTheDocumentationGivesIT::connectorRef, new State())
                        .open(new SinkConfig(connector, config, WriteMode.UPSERT, DdlPolicy.FAIL, target, NODE,
                                OnFullLoad.APPEND, true)));
            }
            AtomicBoolean done = new AtomicBoolean();
            Future<Reading> sampled = sampling.submit(() -> {
                Reading most = Reading.NONE;
                do {
                    most = most.max(sessions.read());
                    TimeUnit.NANOSECONDS.sleep(SAMPLE_EVERY.toNanos());
                } while (!done.get());
                return most.max(sessions.read());
            });
            List<Future<?>> written = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                SinkWriter writer = writers.get(i);
                long first = (long) i * BATCHES * ROWS;
                written.add(writing.submit(() -> {
                    for (int batch = 0; batch < BATCHES; batch++) {
                        List<Envelope> rows = new ArrayList<>(ROWS);
                        for (int row = 0; row < ROWS; row++) {
                            long id = first + (long) batch * ROWS + row;
                            rows.add(Envelope.insert(id, "orders", Map.of("id", id, "seq", id), null));
                        }
                        writer.write(rows).toCompletableFuture().get(2, TimeUnit.MINUTES);
                    }
                    return null;
                }));
            }
            for (Future<?> each : written) {
                each.get(10, TimeUnit.MINUTES);
            }
            done.set(true);
            Reading most = sampled.get(1, TimeUnit.MINUTES);
            Reading[] last = {null};
            Await.until("the connections the idle " + connector + " instances hold to stop moving",
                    Duration.ofMinutes(1),
                    () -> {
                        Reading now = sessions.read();
                        boolean settled = now.equals(last[0]);
                        last[0] = now;
                        return settled;
                    },
                    () -> "last read " + last[0]);
            return new Held(most, last[0]);
        } finally {
            writing.shutdownNow();
            sampling.shutdownNow();
            for (SinkWriter writer : writers) {
                writer.close();
            }
        }
    }

    private static void awaitNothingLeft(String connector, Sessions sessions, Reading before) {
        Await.until("every connection the closed " + connector + " instances held to be gone", Duration.ofMinutes(1),
                () -> sessions.read().open() <= before.open(),
                () -> "connected now " + sessions.read() + ", before any opened " + before);
    }

    private static ConnectorRef connectorRef(String id) {
        return CONNECTORS.computeIfAbsent(id, missing -> {
            try (var paths = Files.list(Path.of(System.getProperty("tapstate.e2e.connectors-dir")))) {
                Path jar = paths.filter(path -> path.getFileName().toString().startsWith(missing)
                        && path.toString().endsWith(".jar")).findFirst().orElseThrow();
                var inspected = new ConnectorIntrospector().introspect(List.of(jar));
                return new ConnectorRef(List.of(jar), inspected.className(), inspected.pdkApiVersion(), null,
                        inspected.spec());
            } catch (Exception e) {
                throw new IllegalStateException("cannot load the " + missing + " connector", e);
            }
        });
    }

    /** The most held while writing, and what was held between writes. */
    record Held(Reading writing, Reading between) {

        Held less(Reading baseline) {
            return new Held(writing.less(baseline), between.less(baseline));
        }
    }

    /** How many connections there are, and how many of them are running a statement. */
    record Reading(long open, long active) {

        static final Reading NONE = new Reading(0, 0);

        Reading max(Reading other) {
            return new Reading(Math.max(open, other.open), Math.max(active, other.active));
        }

        Reading less(Reading baseline) {
            return new Reading(open - baseline.open, active - baseline.active);
        }
    }

    /** Who is connected, as the database says, leaving out the connection asking. */
    private interface Sessions extends AutoCloseable {
        Reading read();
    }

    private static final class JdbcSessions implements Sessions {

        private final Connection connection;
        private final PreparedStatement query;

        JdbcSessions(Connection connection, String sql, String login) throws SQLException {
            this.connection = connection;
            this.query = connection.prepareStatement(sql);
            query.setString(1, login);
        }

        @Override
        public Reading read() {
            try (ResultSet result = query.executeQuery()) {
                result.next();
                return new Reading(result.getLong(1), result.getLong(2));
            } catch (SQLException e) {
                throw new IllegalStateException("cannot read who is connected", e);
            }
        }

        @Override
        public void close() throws SQLException {
            connection.close();
        }
    }

    /** A document store counts every client connected to it, this one included; the baseline takes that out. */
    private static final class MongoSessions implements Sessions {

        private final MongoClient client;

        MongoSessions(String uri) {
            this.client = MongoClients.create(uri);
        }

        @Override
        public Reading read() {
            Document connections = client.getDatabase("admin").runCommand(new Document("serverStatus", 1))
                    .get("connections", Document.class);
            return new Reading(connections.getInteger("current"), connections.getInteger("active"));
        }

        @Override
        public void close() {
            client.close();
        }
    }

    /** Keeps what the sink is handed to keep, so every instance prepares as a first run does. */
    private static final class State implements KeyedStateStore {

        private final Map<String, byte[]> entries = new HashMap<>();

        @Override
        public synchronized Optional<byte[]> load(String namespace, String key) {
            return Optional.ofNullable(entries.get(namespace + "/" + key));
        }

        @Override
        public synchronized void save(String namespace, String key, byte[] value) {
            entries.put(namespace + "/" + key, value);
        }

        @Override
        public synchronized Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] value) {
            return Optional.ofNullable(entries.putIfAbsent(namespace + "/" + key, value));
        }

        @Override
        public synchronized void delete(String namespace, String key) {
            entries.remove(namespace + "/" + key);
        }

        @Override
        public synchronized void dropNamespace(String namespace) {
            entries.keySet().removeIf(key -> key.startsWith(namespace + "/"));
        }

        @Override
        public synchronized long count(String namespace) {
            return entries.keySet().stream().filter(key -> key.startsWith(namespace + "/")).count();
        }
    }
}
