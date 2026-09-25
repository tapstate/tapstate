package io.tapstate.e2e;

import io.tapstate.adapters.pdk.ConnectorClassLoader;
import io.tapstate.core.common.JsonReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Test-only comparison of two positions issued by one source in one benchmark fork. The caller must
 * obtain both tokens from the same {@link BenchmarkAckOracle.SourceChain} and pass that chain's
 * registered connector configuration. Neither database's opaque token can be compared across forks.
 */
final class BenchmarkConnectorPositionCoverage implements BenchmarkAckOracle.PositionCoverage, AutoCloseable {

    private static final int MAX_TOKEN_LENGTH = 262_144;
    private static final BigInteger MAX_UNSIGNED_LONG = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    enum Mode {
        MYSQL_DEFAULT("io.tapdata.connector.mysql.entity.MysqlStreamOffset"),
        POSTGRES_PGOUTPUT("io.tapdata.connector.postgres.cdc.offset.PostgresOffset");

        private final String offsetClass;

        Mode(String offsetClass) {
            this.offsetClass = offsetClass;
        }
    }

    private final Mode mode;
    private final ConnectorClassLoader connector;
    private final Class<?> offsetClass;
    private final MySqlSourceLineage mysqlSource;
    private boolean closed;

    /** The live server identity and address read from the fixed MySQL source in this fork. */
    record MySqlSourceLineage(String host, int port, String database, String serverUuid, long serverId) {
        MySqlSourceLineage {
            if (host == null || host.isBlank() || port <= 0 || port > 65_535
                    || database == null || database.isBlank() || serverUuid == null || serverId <= 0) {
                throw new IllegalArgumentException("incomplete MySQL source lineage");
            }
            UUID.fromString(serverUuid);
        }
    }

    private BenchmarkConnectorPositionCoverage(Mode mode, Path connectorJar, MySqlSourceLineage mysqlSource)
            throws ClassNotFoundException {
        this.mode = mode;
        this.mysqlSource = mysqlSource;
        this.connector = ConnectorClassLoader.open(List.of(connectorJar));
        try {
            this.offsetClass = connector.load(mode.offsetClass);
        } catch (ClassNotFoundException failure) {
            connector.close();
            throw failure;
        }
    }

    static BenchmarkConnectorPositionCoverage open(Mode mode, Path connectorJar, Map<String, ?> sourceConfig,
                                                   MySqlSourceLineage mysqlSource) throws ClassNotFoundException {
        if (mode == null || connectorJar == null || sourceConfig == null) {
            throw new IllegalArgumentException("a connector mode, jar and source configuration are required");
        }
        Object mysqlReader = sourceConfig.containsKey("highPerformance")
                ? sourceConfig.get("highPerformance") : Boolean.FALSE;
        Object postgresPlugin = sourceConfig.containsKey("logPluginName")
                ? sourceConfig.get("logPluginName") : "pgoutput";
        if (mode == Mode.MYSQL_DEFAULT && !Boolean.FALSE.equals(mysqlReader)) {
            throw new IllegalArgumentException("the MySQL benchmark requires the default reader");
        }
        if (mode == Mode.MYSQL_DEFAULT && (mysqlSource == null
                || !mysqlSource.host().equals(sourceConfig.get("host"))
                || !(sourceConfig.get("port") instanceof Number port)
                || !((port instanceof Integer || port instanceof Long)
                    && port.longValue() == mysqlSource.port())
                || !mysqlSource.database().equals(sourceConfig.get("database")))) {
            throw new IllegalArgumentException("MySQL source configuration differs from live lineage");
        }
        if (mode == Mode.POSTGRES_PGOUTPUT && !"pgoutput".equals(postgresPlugin)) {
            throw new IllegalArgumentException("the PostgreSQL benchmark requires pgoutput");
        }
        if (mode == Mode.POSTGRES_PGOUTPUT && mysqlSource != null) {
            throw new IllegalArgumentException("MySQL source lineage cannot qualify a PostgreSQL stream");
        }
        return new BenchmarkConnectorPositionCoverage(mode, connectorJar, mysqlSource);
    }

    @Override
    public boolean covers(String targetAck, String terminalPosition) {
        if (closed || targetAck == null || terminalPosition == null) {
            return false;
        }
        try {
            Object ack = decode(targetAck);
            Object terminal = decode(terminalPosition);
            return switch (mode) {
                case MYSQL_DEFAULT -> mysqlCovers(ack, terminal);
                case POSTGRES_PGOUTPUT -> postgresCovers(ack, terminal);
            };
        } catch (IOException | ReflectiveOperationException
                 | IllegalArgumentException | ClassCastException failure) {
            return false;
        }
    }

    /** A bounded numeric diagnostic for an opaque connector token, without printing the token itself. */
    String describe(String token) {
        if (closed || token == null) {
            return "ABSENT";
        }
        try {
            Object decoded = decode(token);
            return switch (mode) {
                case MYSQL_DEFAULT -> {
                    MysqlCoordinate coordinate = mysqlCoordinate(decoded);
                    yield "file=" + coordinate.filePrefix + "." + coordinate.fileNumber
                            + ",pos=" + coordinate.position + ",event=" + coordinate.event
                            + ",row=" + coordinate.row + ",serverId=" + coordinate.serverId;
                }
                case POSTGRES_PGOUTPUT -> "lsn=" + Long.toUnsignedString(postgresLsn(decoded));
            };
        } catch (IOException | ReflectiveOperationException
                 | IllegalArgumentException | ClassCastException invalid) {
            return "UNDECODABLE(" + invalid.getClass().getSimpleName() + ")";
        }
    }

    private Object decode(String token) throws IOException, ClassNotFoundException {
        if (token.isBlank() || token.length() > MAX_TOKEN_LENGTH) {
            throw new IllegalArgumentException("missing or oversized position token");
        }
        byte[] bytes = Base64.getDecoder().decode(token);
        ByteArrayInputStream source = new ByteArrayInputStream(bytes);
        try (ObjectInputStream input = new ObjectInputStream(source) {
            @Override
            protected Class<?> resolveClass(ObjectStreamClass descriptor) throws ClassNotFoundException {
                return Class.forName(descriptor.getName(), false, offsetClass.getClassLoader());
            }

            @Override
            protected Class<?> resolveProxyClass(String[] interfaces) throws ClassNotFoundException {
                throw new ClassNotFoundException("proxy offset types are unsupported");
            }
        }) {
            input.setObjectInputFilter(info -> {
                if (info.depth() > 16 || info.references() > 256 || info.streamBytes() > MAX_TOKEN_LENGTH
                        || info.arrayLength() > 16_384) {
                    return ObjectInputFilter.Status.REJECTED;
                }
                Class<?> type = info.serialClass();
                if (type == null) {
                    return ObjectInputFilter.Status.UNDECIDED;
                }
                while (type.isArray()) {
                    type = type.getComponentType();
                }
                return type.isPrimitive() || type == offsetClass
                        || type.getName().startsWith("java.lang.")
                        || type.getName().startsWith("java.util.")
                        ? ObjectInputFilter.Status.ALLOWED : ObjectInputFilter.Status.REJECTED;
            });
            Object value = input.readObject();
            if (value == null || value.getClass() != offsetClass || source.available() != 0) {
                throw new IllegalArgumentException("unexpected position type or trailing bytes");
            }
            return value;
        }
    }

    private boolean mysqlCovers(Object ack, Object terminal) throws ReflectiveOperationException {
        MysqlCoordinate a = mysqlCoordinate(ack);
        MysqlCoordinate t = mysqlCoordinate(terminal);
        BigInteger expectedServer = BigInteger.valueOf(mysqlSource.serverId());
        return expectedServer.equals(a.serverId) && expectedServer.equals(t.serverId)
                && a.filePrefix.equals(t.filePrefix)
                && compare(a.fileNumber, a.position, a.event, a.row,
                        t.fileNumber, t.position, t.event, t.row) >= 0;
    }

    private MysqlCoordinate mysqlCoordinate(Object offset) throws ReflectiveOperationException {
        Object nameValue = offsetClass.getMethod("getName").invoke(offset);
        Object partitionsValue = offsetClass.getMethod("getOffset").invoke(offset);
        if (!(nameValue instanceof String name) || name.isBlank()
                || !(partitionsValue instanceof Map<?, ?> partitions) || partitions.size() != 1) {
            throw new IllegalArgumentException("invalid MySQL stream offset");
        }
        Map.Entry<?, ?> entry = partitions.entrySet().iterator().next();
        if (!(entry.getKey() instanceof String partitionJson)
                || !(entry.getValue() instanceof String coordinateJson)) {
            throw new IllegalArgumentException("invalid MySQL offset map");
        }
        Map<?, ?> partition = object(partitionJson);
        Map<?, ?> coordinate = object(coordinateJson);
        if (!name.equals(partition.get("server"))) {
            throw new IllegalArgumentException("MySQL reader identity disagrees with its partition");
        }
        Object fileValue = coordinate.get("file");
        if (!(fileValue instanceof String file) || file.isBlank()) {
            throw new IllegalArgumentException("missing MySQL binlog file");
        }
        int separator = file.lastIndexOf('.');
        if (separator < 1 || separator == file.length() - 1) {
            throw new IllegalArgumentException("invalid MySQL binlog file");
        }
        String sequence = file.substring(separator + 1);
        if (!sequence.matches("[0-9]+")) {
            throw new IllegalArgumentException("invalid MySQL binlog sequence");
        }
        return new MysqlCoordinate(name, partition, file.substring(0, separator), new BigInteger(sequence),
                unsignedInteger(coordinate.get("pos")), optionalUnsigned(coordinate, "event"),
                optionalUnsigned(coordinate, "row"), unsignedInteger(coordinate.get("server_id")));
    }

    private boolean postgresCovers(Object ack, Object terminal) throws ReflectiveOperationException {
        return Long.compareUnsigned(postgresLsn(ack), postgresLsn(terminal)) >= 0;
    }

    private long postgresLsn(Object offset) throws ReflectiveOperationException {
        if (offsetClass.getMethod("getSortString").invoke(offset) != null
                || offsetClass.getMethod("getOffsetValue").invoke(offset) != null) {
            throw new IllegalArgumentException("PostgreSQL position is not a pgoutput stream offset");
        }
        Object sourceOffset = offsetClass.getMethod("getSourceOffset").invoke(offset);
        if (!(sourceOffset instanceof String json) || json.isBlank()) {
            throw new IllegalArgumentException("missing PostgreSQL source offset");
        }
        Object lsn = object(json).get("lsn");
        if (lsn instanceof Long value) {
            return value;
        }
        if (lsn instanceof BigInteger value && value.signum() >= 0
                && value.compareTo(MAX_UNSIGNED_LONG) <= 0) {
            return value.longValue();
        }
        if (lsn instanceof String value && value.matches("[0-9]+")) {
            return unsignedLong(new BigInteger(value));
        }
        if (lsn instanceof String value && value.matches("[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}")) {
            String[] halves = value.split("/");
            return (Long.parseLong(halves[0], 16) << 32) | Long.parseLong(halves[1], 16);
        }
        throw new IllegalArgumentException("invalid PostgreSQL LSN");
    }

    private static long unsignedLong(BigInteger value) {
        if (value.signum() < 0 || value.compareTo(MAX_UNSIGNED_LONG) > 0) {
            throw new IllegalArgumentException("LSN outside unsigned 64-bit range");
        }
        return value.longValue();
    }

    private static Map<?, ?> object(String json) {
        Object parsed = JsonReader.parse(json);
        if (!(parsed instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("position coordinate is not a JSON object");
        }
        return map;
    }

    private static BigInteger unsignedInteger(Object value) {
        if (value instanceof Long number && number >= 0) {
            return BigInteger.valueOf(number);
        }
        if (value instanceof BigInteger number && number.signum() >= 0) {
            return number;
        }
        throw new IllegalArgumentException("missing or invalid unsigned position component");
    }

    private static BigInteger optionalUnsigned(Map<?, ?> map, String key) {
        return map.containsKey(key) ? unsignedInteger(map.get(key)) : null;
    }

    private static int compare(BigInteger aFile, BigInteger aPos, BigInteger aEvent, BigInteger aRow,
                               BigInteger tFile, BigInteger tPos, BigInteger tEvent, BigInteger tRow) {
        int file = aFile.compareTo(tFile);
        if (file != 0) {
            return file;
        }
        int position = aPos.compareTo(tPos);
        if (position != 0) {
            return position;
        }
        if ((aEvent == null) != (tEvent == null) || (aRow == null) != (tRow == null)) {
            throw new IllegalArgumentException("incomparable MySQL intra-event positions");
        }
        if (aEvent != null) {
            int event = aEvent.compareTo(tEvent);
            if (event != 0) {
                return event;
            }
        }
        return aRow == null ? 0 : aRow.compareTo(tRow);
    }

    private record MysqlCoordinate(String name, Map<?, ?> partition, String filePrefix, BigInteger fileNumber,
                                   BigInteger position, BigInteger event, BigInteger row, BigInteger serverId) {}

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            connector.close();
        }
    }
}
