package io.tapstate.adapters.pdk;

import static io.tapstate.core.validation.ValidationRootHintBatch.ChangeKind.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.core.validation.ValidationFailureLatch;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationKeyBinding;
import io.tapstate.core.validation.ValidationMode;
import io.tapstate.core.validation.ValidationOwner;
import io.tapstate.core.validation.ValidationRootHintBatch;
import io.tapstate.core.validation.ValidationRootHintBatch.Change;
import io.tapstate.core.validation.ValidationRunIdentity;
import io.tapstate.core.validation.ValidationValues;
import io.tapstate.runtime.engine.validation.ValidationCandidateQueue;
import io.tapstate.runtime.engine.validation.ValidationRootHintInbox;
import io.tapstate.runtime.engine.validation.ValidationRootHintOutbox;
import io.tapstate.app.ValidationCaptureHintTap;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Native controlled BIGINT images through local hint delivery, not a production subscription or Strict proof. */
@RequiresDocker
class ValidationJdbcCdcHintsIT {
    private static final String DATABASE = "validation_cdc_hints";
    private static final String TABLE = "source_records";
    private static final String PASSWORD = "validation-fixture-only";
    private static final long FIRST = 9_007_199_254_740_993L;

    @Test @Timeout(150)
    void mysqlFullImagesKeepOldAndNewKeysAcrossUpdatesAndDelete() throws Exception { verify(true); }

    @Test @Timeout(150)
    void postgresFullIdentityKeepsOldAndNewKeysAcrossUpdatesAndDelete() throws Exception { verify(false); }

    private static void verify(boolean mysql) throws Exception {
        String kind = mysql ? "mysql" : "postgres";
        ConnectorRef artifact = artifact(kind);
        int port = mysql ? 3306 : 5432;
        try (var source = new GenericContainer<>(DockerImageName.parse(mysql ? "mysql:8.0" : "postgres:16"))
                .withExposedPorts(port).withStartupTimeout(Duration.ofSeconds(75))) {
            if (mysql) {
                source.withEnv("MYSQL_ROOT_PASSWORD", PASSWORD).withEnv("MYSQL_DATABASE", DATABASE)
                        .withCommand("--innodb-buffer-pool-size=64M", "--max-connections=20", "--server-id=1",
                                "--log-bin=mysql-bin", "--binlog-format=ROW", "--binlog-row-image=FULL",
                                "--gtid-mode=ON", "--enforce-gtid-consistency=ON")
                        .waitingFor(Wait.forLogMessage(".*mysqld: ready for connections\\..*port: 3306.*\\n", 1));
            } else {
                source.withEnv("POSTGRES_PASSWORD", PASSWORD).withEnv("POSTGRES_USER", "validation")
                        .withEnv("POSTGRES_DB", DATABASE).withCommand("postgres", "-c", "fsync=off", "-c", "wal_level=logical",
                                "-c", "max_wal_senders=4", "-c", "max_replication_slots=4")
                        .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2));
            }
            source.start();
            sql(source, mysql, "CREATE TABLE " + TABLE + " (id BIGINT PRIMARY KEY, value VARCHAR(40)); "
                    + "INSERT INTO " + TABLE + " VALUES (1, 'before-capture');");
            if (!mysql) sql(source, false, "ALTER TABLE " + TABLE + " REPLICA IDENTITY FULL;");
            var settings = new LinkedHashMap<String, Object>();
            settings.put("host", source.getHost()); settings.put("port", source.getMappedPort(port));
            settings.put("database", DATABASE); settings.put(mysql ? "username" : "user", mysql ? "root" : "validation");
            settings.put("password", PASSWORD); settings.put("timezone", "+00:00"); settings.put("oldVersionTimezone", false);
            if (mysql) settings.put("addtionalString", "useSSL=false&allowPublicKeyRetrieval=true");
            else settings.put("schema", "public");
            var config = new CaptureConfig(kind, settings, List.of(TABLE), new PipelineNode("hint-fixture", "source"));
            var capture = new PdkCapturePort(id -> artifact);
            var run = new ValidationRunIdentity(new ValidationOwner("fixture", "hint-fixture", "owner", "boot", 1, 1, 1),
                    "task", "run", ValidationMode.INCREMENTAL, "spec", 1, "rules");
            var sender = new ValidationRootHintBatch.Sender("fixture-sender", "fixture-execution", "source", TABLE,
                    "capture", "boot", 1, 1, "route", "controlled-bigint", "mapping", "output",
                    List.of(new ValidationRootHintBatch.Domain(ValidationKey.Kind.EXACT_NUMBER)));
            var count = new AtomicInteger();
            var delivered = new ArrayList<Change>();
            var deliveryShapes = new ArrayList<List<ValidationRootHintBatch.ChangeKind>>();
            List<Change> expected = mysql ? List.of(new Change(INSERT, null, key(FIRST)),
                    new Change(UPDATE, key(FIRST), key(FIRST)), new Change(UPDATE, key(FIRST), key(FIRST + 1)),
                    new Change(DELETE, key(FIRST + 1), null))
                    : List.of(new Change(INSERT, null, key(FIRST)), new Change(UPDATE, key(FIRST), key(FIRST)),
                            new Change(DELETE, key(FIRST), null), new Change(INSERT, null, key(FIRST + 1)),
                            new Change(DELETE, key(FIRST + 1), null));
            var failures = new ValidationFailureLatch();
            var extractor = extractor();
            try (var queue = new ValidationCandidateQueue(run,
                        new ValidationCandidateQueue.Limits(8, 65536, 64, 2, 0, Duration.ofSeconds(60).toNanos()), System::nanoTime);
                 var inbox = new ValidationRootHintInbox(queue, List.of(sender), new ValidationRootHintInbox.Limits(1, 65536, 32, 65536));
                 var outbox = new ValidationRootHintOutbox(run, sender, new ValidationRootHintOutbox.Limits(16, 262144, 32, 65536,
                        2, Duration.ofMillis(10).toNanos(), Duration.ofSeconds(60).toNanos()), System::nanoTime)) {
                CaptureListener listener = new CaptureListener() {
                    @Override public void onBatch(List<Envelope> events, Optional<SourcePosition> position) {
                        try {
                            if (events.size() > 32) throw new AssertionError("unexpected fixture batch size");
                            for (Envelope event : events) {
                                if (event.op() == Op.DDL) continue;
                                assertThat(event.src()).isEqualTo(TABLE);
                                assertThat(event.op()).isIn(Op.INSERT, Op.UPDATE, Op.DELETE);
                                if (count.incrementAndGet() > 8) throw new AssertionError("unexpected fixture event count");
                            }
                        } catch (RuntimeException | Error failure) { failures.record(failure); }
                    }
                    @Override public void onError(Throwable failure) { failures.record(failure); }
                };
                CaptureListener tapped = ValidationCaptureHintTap.wrap(listener,
                        List.of(new ValidationCaptureHintTap.Binding(outbox, extractor::extract)),
                        new ValidationCaptureHintTap.Limits(32, 1, 65536));
                MysqlReader mysqlReader = null;
                try (var tail = capture.cdc(config, CaptureStart.present(), tapped)) {
                    awaitReader(source, mysql, failures, true);
                    if (mysql) {
                        mysqlReader = mysqlReader(source);
                        assertThat(mysqlSocketStates(source, mysqlReader.port())).containsExactly("01");
                    }
                    sql(source, mysql, "INSERT INTO " + TABLE + " VALUES (" + FIRST + ", 'inserted'); "
                            + "UPDATE " + TABLE + " SET value='same-key' WHERE id=" + FIRST + "; "
                            + "UPDATE " + TABLE + " SET id=" + (FIRST + 1) + " WHERE id=" + FIRST + "; "
                            + "DELETE FROM " + TABLE + " WHERE id=" + (FIRST + 1) + ";");
                    long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
                    do {
                        assertHealthy(failures, outbox, inbox);
                        pump(outbox, inbox, delivered, deliveryShapes);
                        if (delivered.size() == expected.size() && outbox.snapshot().packets() == 0) break;
                        Thread.sleep(20);
                    } while (System.nanoTime() < deadline);
                    assertHealthy(failures, outbox, inbox);
                    assertThat(delivered).containsExactlyElementsOf(expected);
                    System.out.println(kind + " admitted hint packet shapes: " + deliveryShapes);
                    assertThat(outbox.snapshot().packets()).isZero();
                    var roots = new ArrayList<ValidationKey>();
                    for (var next = queue.poll(); next.isPresent(); next = queue.poll()) {
                        var lease = next.orElseThrow(); roots.add(lease.observation().root().rootKey());
                        queue.settle(lease, ValidationCandidateQueue.Completion.SETTLED);
                    }
                    assertThat(roots).containsExactlyInAnyOrder(key(FIRST), key(FIRST + 1));
                    assertThat(queue.snapshot().bytes()).isZero();
                }
                // A post-close fixture change must not deliver another callback.
                sql(source, mysql, "INSERT INTO " + TABLE + " VALUES (2, 'after-close-probe');");
                if (mysql) awaitMysqlPeerClose(source, mysqlReader, failures);
                else awaitReader(source, false, failures, false);
                assertThat(count.get()).isEqualTo(expected.size());
                assertHealthy(failures, outbox, inbox);
            }
        }
    }

    private static void pump(ValidationRootHintOutbox outbox, ValidationRootHintInbox inbox, List<Change> delivered,
                             List<List<ValidationRootHintBatch.ChangeKind>> shapes) {
        var next = outbox.poll();
        if (next.isEmpty()) return;
        var lease = next.orElseThrow();
        var receipt = inbox.tryAccept(lease.batch());
        assertThat(outbox.settle(lease, receipt)).isEqualTo(ValidationRootHintOutbox.Settlement.ACCEPTED);
        assertThat(delivered.size() + lease.batch().changes().size()).isLessThanOrEqualTo(8);
        delivered.addAll(lease.batch().changes());
        shapes.add(lease.batch().changes().stream().map(Change::kind).toList());
    }
    private static void assertHealthy(ValidationFailureLatch failures, ValidationRootHintOutbox outbox, ValidationRootHintInbox inbox) {
        assertThat(failures.get()).isNull(); assertThat(outbox.failure()).isEmpty(); assertThat(inbox.snapshot().failure()).isNull();
    }
    private static void awaitReader(GenericContainer<?> source, boolean mysql, ValidationFailureLatch failures, boolean active) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(active ? 40 : 10).toNanos();
        String count;
        do {
            assertThat(failures.get()).isNull();
            count = sql(source, mysql, mysql
                    ? "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%';"
                    : "SELECT COUNT(*) FROM pg_replication_slots WHERE database='" + DATABASE + "' AND active;").trim();
            if (active ? count.equals("1") : count.equals("0")) return;
            Thread.sleep(50);
        } while (System.nanoTime() < deadline);
        assertThat(count).as("actual database capture reader active=%s", active).isEqualTo(active ? "1" : "0");
    }
    private record MysqlReader(String id, int port) {}

    private static MysqlReader mysqlReader(GenericContainer<?> source) throws Exception {
        String[] rows = sql(source, true, "SELECT ID, HOST FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%';").trim().split("\\R");
        assertThat(rows).hasSize(1);
        String[] fields = rows[0].split("\\s+");
        assertThat(fields).hasSize(2);
        return new MysqlReader(fields[0], Integer.parseInt(fields[1].substring(fields[1].lastIndexOf(':') + 1)));
    }

    private static List<String> mysqlSocketStates(GenericContainer<?> source, int remotePort) throws Exception {
        var result = source.execInContainer("sh", "-c", "cat /proc/net/tcp /proc/net/tcp6");
        assertThat(result.getExitCode()).isZero();
        String suffix = ":%04X".formatted(remotePort);
        var states = new ArrayList<String>();
        for (String row : result.getStdout().split("\\R")) {
            String[] fields = row.trim().split("\\s+");
            if (fields.length >= 4 && fields[1].endsWith(":0CEA") && fields[2].endsWith(suffix)) states.add(fields[3]);
        }
        return states;
    }

    private static void awaitMysqlPeerClose(GenericContainer<?> source, MysqlReader reader, ValidationFailureLatch failures) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        List<String> states;
        do {
            assertThat(failures.get()).isNull();
            states = mysqlSocketStates(source, reader.port());
            if (states.isEmpty() || states.stream().allMatch(state -> Set.of("06", "07", "08", "09").contains(state))) {
                // MySQL can retain its idle dump thread in PROCESSLIST after observing the client's FIN.
                // CLOSE_WAIT proves peer shutdown, not removal of the server's retained thread or slot.
                System.out.println("mysql reader " + reader.id() + " peer-close TCP states: " + states);
                return;
            }
            Thread.sleep(50);
        } while (System.nanoTime() < deadline);
        assertThat(states).as("the exact previously established binlog socket must observe peer shutdown")
                .allMatch(state -> Set.of("06", "07", "08", "09").contains(state));
    }

    private static PdkValidationHintKeys extractor() {
        // The controlled SQL fixture supplies this BIGINT model; this is not a production discovery receipt.
        var table = new TapTable(TABLE).add(new TapField("id", "BIGINT").tapType(new TapNumber().fixed(true).precision(19).scale(0)));
        var limits = new ValidationValues.Limits(16384, 100, 12);
        var binding = new ValidationKeyBinding("key", List.of(new ValidationKeyBinding.Component(List.of("id"),
                ValidationKey.Kind.EXACT_NUMBER, Set.of("exact-number"))), Set.of(), limits);
        return new PdkValidationHintKeys("source", table, new PdkValidationCodecs("fixture", "controlled-bigint", new TapCodecsRegistry()),
                binding, Map.of("id", PdkValidationRows.Rule.of(PdkValidationRows.Shape.EXACT_NUMBER)), limits);
    }
    private static ValidationKey key(long n) { return new ValidationKey(List.of(ValidationKey.Part.number(n))); }
    private static ConnectorRef artifact(String kind) {
        String configured = System.getProperty("tapstate.pdk.it." + kind + "-jar");
        assumeTrue(configured != null && !configured.isBlank(), "a real " + kind + " connector jar is required");
        var paths = List.of(Path.of(configured));
        var artifact = new ConnectorIntrospector().introspect(paths);
        return new ConnectorRef(paths, artifact.className(), artifact.pdkApiVersion(), null, artifact.spec());
    }
    private static String sql(GenericContainer<?> source, boolean mysql, String statement) throws Exception {
        var result = mysql ? source.execInContainer("mysql", "--protocol=TCP", "--host=127.0.0.1", "--user=root",
                "--password=" + PASSWORD, "--batch", "--skip-column-names", DATABASE, "-e", statement)
                : source.execInContainer("psql", "-U", "validation", "-d", DATABASE, "-tAc", statement);
        assertThat(result.getExitCode()).as(result.getStderr()).isZero();
        return result.getStdout();
    }
}
