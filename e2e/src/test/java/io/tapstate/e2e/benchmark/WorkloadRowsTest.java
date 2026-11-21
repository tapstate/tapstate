package io.tapstate.e2e.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

class WorkloadRowsTest {
    @Test
    void logicalSizeIncludesEscapedFieldsAndValuesRatherThanBsonMetadata() {
        var record = new BsonDocument("a\"", new BsonString("quoted\n\u00e9"))
                .append("decimal", new BsonDecimal128(new Decimal128(new BigDecimal("-12.30"))))
                .append("date", new BsonDateTime(Instant.parse("2025-02-01T00:01:02.030Z").toEpochMilli()))
                .append("array", new BsonArray(List.of(BsonBoolean.TRUE, BsonNull.VALUE)))
                .append("binary", new BsonBinary(new byte[] {1, 2}));
        String expected = "{\"a\\\"\":\"quoted\\n\u00e9\",\"decimal\":-12.30,"
                + "\"date\":\"2025-02-01T00:01:02.030Z\",\"array\":[true,null],\"binary\":\"AQI=\"}";
        assertEquals(expected.getBytes(StandardCharsets.UTF_8).length, WorkloadRows.logicalBytes(record));
    }
}
