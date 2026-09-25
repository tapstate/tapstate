package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Live server identity must remain the same around one fork's terminal ACK. */
class BenchmarkSourceLineageIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void recordsBothLiveServerIdentitiesAndRejectsChangedLineage() {
        Map<String, Object> mysqlSettings = SharedMySql.settings("benchmark_lineage_mysql");
        Map<String, Object> postgresSettings = SharedPostgres.settings("benchmark_lineage_postgres");
        BenchmarkSourceLineage.MySql mysql = BenchmarkSourceLineage.readMySql(mysqlSettings);
        BenchmarkSourceLineage.Postgres postgres = BenchmarkSourceLineage.readPostgres(postgresSettings);

        assertThat(mysql.decoderLineage().serverUuid()).isEqualTo(mysql.serverUuid());
        assertThat(mysql.decoderLineage().serverId()).isEqualTo(mysql.serverId());
        assertThat(mysql.address().database()).isEqualTo(mysqlSettings.get("database"));
        assertThat(postgres.systemIdentifier()).matches("[0-9]+");
        assertThat(postgres.timeline()).isPositive();
        BenchmarkSourceLineage.verifyAfterTerminalAck(mysql, mysqlSettings);
        BenchmarkSourceLineage.verifyAfterTerminalAck(postgres, postgresSettings);

        UUID originalUuid = UUID.fromString(mysql.serverUuid());
        String changedUuid = new UUID(originalUuid.getMostSignificantBits() ^ 1,
                originalUuid.getLeastSignificantBits()).toString();
        BenchmarkSourceLineage.MySql changedMysql = new BenchmarkSourceLineage.MySql(
                mysql.address(), changedUuid, mysql.serverId());
        assertThatThrownBy(() -> BenchmarkSourceLineage.verifyAfterTerminalAck(changedMysql, mysqlSettings))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("lineage changed");
        BenchmarkSourceLineage.Postgres changedPostgres = new BenchmarkSourceLineage.Postgres(
                postgres.address(), postgres.systemIdentifier(), postgres.timeline() + 1);
        assertThatThrownBy(() -> BenchmarkSourceLineage.verifyAfterTerminalAck(changedPostgres, postgresSettings))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("lineage changed");

        BenchmarkSourceLineage.MySql changedAddress = new BenchmarkSourceLineage.MySql(
                new BenchmarkSourceLineage.Address("other-host", mysql.address().port(),
                        mysql.address().database()), mysql.serverUuid(), mysql.serverId());
        assertThatThrownBy(() -> BenchmarkSourceLineage.verifyAfterTerminalAck(changedAddress, mysqlSettings))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("lineage changed");
    }
}
