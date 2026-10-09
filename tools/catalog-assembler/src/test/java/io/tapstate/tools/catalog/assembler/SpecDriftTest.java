package io.tapstate.tools.catalog.assembler;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.Provenance;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What one scan of the upstream specifications found, against what the catalog last recorded.
 */
class SpecDriftTest {

    private static final String MYSQL_SPEC = "{\"properties\":{\"id\":\"mysql\"}}";
    private static final String MYSQL_PATH = "connectors/mysql-connector/src/main/resources/spec_mysql.json";
    private static final String KAFKA_SPEC = "{\"properties\":{\"id\":\"kafka\",\"name\":\"Kafka\"}}";
    private static final String KAFKA_PATH = "connectors/kafka-connector/src/main/resources/spec_kafka.json";
    private static final String ORACLE_SPEC = "{\"properties\":{\"id\":\"oracle\"}}";
    private static final String ORACLE_PATH = "connectors/oracle-connector/src/main/resources/spec_oracle.json";
    private static final String SQLSERVER_SPEC = "{\"properties\":{\"id\":\"sqlserver\"}}";
    private static final String SQLSERVER_PATH = "connectors/mssql-connector/src/main/resources/mssql-spec.json";
    private static final String DB2_SPEC = "{\"properties\":{\"id\":\"db2\"}}";
    private static final String DB2_PATH = "connectors/db2-connector/src/main/resources/spec_db2.json";

    @Test
    void unchangedEnterpriseRowsLetUnsupportedOssDriftWaitForCompany() {
        List<ConnectorCatalogEntry> snapshot = List.of(
                row("mysql", MYSQL_PATH, MYSQL_SPEC), row("oracle", ORACLE_PATH, ORACLE_SPEC),
                row("sqlserver", SQLSERVER_PATH, SQLSERVER_SPEC), row("db2", DB2_PATH, DB2_SPEC));

        SpecDrift.Report report = SpecDrift.compareRepositories(snapshot, Map.of(
                SpecRepository.OSS, Map.of(MYSQL_PATH, MYSQL_SPEC,
                        "connectors-unpackage/hudi-connector/src/main/resources/spec_hudi.json",
                        "{\"properties\":{\"id\":\"hudi\"}}"),
                SpecRepository.ENTERPRISE, Map.of(ORACLE_PATH, ORACLE_SPEC,
                        SQLSERVER_PATH, SQLSERVER_SPEC, DB2_PATH, DB2_SPEC)));

        assertThat(report.changedIds()).isEmpty();
        assertThat(report.vanishedIds()).isEmpty();
        assertThat(report.newConnectorIds()).containsExactly("hudi");
        assertThat(DriftTriage.decide(report.allIds(), 1, false)).isEqualTo(DriftTriage.Decision.HOLD);
    }

    @Test
    void reportsChangedAndDeletedEnterpriseSpecificationsWhenThatRepositoryWasScanned() {
        List<ConnectorCatalogEntry> snapshot = List.of(
                row("oracle", ORACLE_PATH, ORACLE_SPEC), row("sqlserver", SQLSERVER_PATH, SQLSERVER_SPEC),
                row("db2", DB2_PATH, DB2_SPEC));

        SpecDrift.Report report = SpecDrift.compareRepositories(snapshot, Map.of(
                SpecRepository.OSS, Map.of(),
                SpecRepository.ENTERPRISE, Map.of(ORACLE_PATH,
                        "{\"properties\":{\"id\":\"oracle\",\"name\":\"Changed\"}}", DB2_PATH, DB2_SPEC)));

        assertThat(report.changedIds()).containsExactly("oracle");
        assertThat(report.vanishedIds()).containsExactly("sqlserver");
        assertThat(DriftTriage.decide(report.allIds(), 1, false)).isEqualTo(DriftTriage.Decision.OPEN);
    }

    @Test
    void anEmptyEnterpriseScanReportsDeletionsButAnUnvisitedEnterpriseRepositoryDoesNot() {
        List<ConnectorCatalogEntry> snapshot = List.of(
                row("oracle", ORACLE_PATH, ORACLE_SPEC), row("sqlserver", SQLSERVER_PATH, SQLSERVER_SPEC),
                row("db2", DB2_PATH, DB2_SPEC));

        assertThat(SpecDrift.compare(snapshot, Map.of()).vanishedIds()).isEmpty();
        assertThat(SpecDrift.compareRepositories(snapshot,
                Map.of(SpecRepository.ENTERPRISE, Map.of())).vanishedIds())
                .containsExactly("oracle", "sqlserver", "db2");
    }

    @Test
    void aFileInTheWrongRepositoryCannotHideAnEnterpriseDeletion() {
        List<ConnectorCatalogEntry> snapshot = List.of(row("oracle", ORACLE_PATH, ORACLE_SPEC));

        SpecDrift.Report report = SpecDrift.compareRepositories(snapshot, Map.of(
                SpecRepository.OSS, Map.of(ORACLE_PATH, ORACLE_SPEC),
                SpecRepository.ENTERPRISE, Map.of()));

        assertThat(report.vanishedIds()).containsExactly("oracle");
    }

    @Test
    void anUnrelatedEnterpriseFileAtTheSamePathCannotHideAnOssDeletion() {
        SpecDrift.Report report = SpecDrift.compareRepositories(List.of(row("mysql", MYSQL_PATH, MYSQL_SPEC)),
                Map.of(SpecRepository.OSS, Map.of(),
                        SpecRepository.ENTERPRISE, Map.of(MYSQL_PATH, ORACLE_SPEC)));

        assertThat(report.vanishedIds()).containsExactly("mysql");
        assertThat(report.newConnectorIds()).containsExactly("oracle");
    }

    @Test
    void discoversAndTracksAnAdditionalEnterpriseConnector() {
        String path = "connectors/hana-connector/src/main/resources/spec_hana.json";
        String spec = "{\"properties\":{\"id\":\"hana\"}}";

        assertThat(SpecDrift.compareRepositories(List.of(),
                Map.of(SpecRepository.ENTERPRISE, Map.of(path, spec))).newConnectorIds())
                .containsExactly("hana");
        List<ConnectorCatalogEntry> snapshot = List.of(row("hana", path, spec));
        assertThat(SpecDrift.compareRepositories(snapshot, Map.of(
                SpecRepository.OSS, Map.of(), SpecRepository.ENTERPRISE, Map.of(path, spec))).allIds()).isEmpty();
        assertThat(SpecDrift.compareRepositories(snapshot, Map.of(
                SpecRepository.OSS, Map.of(), SpecRepository.ENTERPRISE, Map.of())).vanishedIds())
                .containsExactly("hana");
    }

    @Test
    void reportsARowWhoseUpstreamFileIsNoLongerThere() {
        List<ConnectorCatalogEntry> snapshot = List.of(row("mysql", MYSQL_PATH, MYSQL_SPEC));

        SpecDrift.Report report = SpecDrift.compare(snapshot, Map.of());

        assertThat(report.vanishedIds()).containsExactly("mysql");
    }

    private static ConnectorCatalogEntry row(String id, String specPath, String specContent) {
        return new ConnectorCatalogEntry(id, id, id, null, null, List.of(), null, null, false, List.of(),
                new Provenance(null, null, specPath, SpecHash.of(specContent), null, null, null));
    }

    @Test
    void reportsOnlyTheRowWhoseUpstreamContentMoved() {
        List<ConnectorCatalogEntry> snapshot = List.of(
                row("mysql", MYSQL_PATH, MYSQL_SPEC),
                row("kafka", KAFKA_PATH, KAFKA_SPEC));

        SpecDrift.Report report = SpecDrift.compare(snapshot, Map.of(
                MYSQL_PATH, MYSQL_SPEC,
                KAFKA_PATH, KAFKA_SPEC.replace("Kafka", "Apache Kafka")));

        assertThat(report.changedIds()).containsExactly("kafka");
    }

    @Test
    void reportsAnUpstreamConnectorNoRowClaims() {
        List<ConnectorCatalogEntry> snapshot = List.of(row("mysql", MYSQL_PATH, MYSQL_SPEC));

        SpecDrift.Report report = SpecDrift.compare(snapshot, Map.of(
                MYSQL_PATH, MYSQL_SPEC,
                "connectors-unpackage/hudi-connector/src/main/resources/spec_hudi.json",
                "{\"properties\":{\"id\":\"hudi\"}}"));

        assertThat(report.newConnectorIds()).containsExactly("hudi");
    }

    @Test
    void countsNoConnectorInAnUpstreamFileThatCarriesNoId() {
        List<ConnectorCatalogEntry> snapshot = List.of(row("mysql", MYSQL_PATH, MYSQL_SPEC));

        SpecDrift.Report report = SpecDrift.compare(snapshot, Map.of(
                MYSQL_PATH, MYSQL_SPEC,
                "connectors-javascript/github-connector/src/main/resources/postman_api_collection.json",
                "{\"item\":[]}"));

        assertThat(report.newConnectorIds()).isEmpty();
    }

    @Test
    void countsNoConnectorForAnIdThatIsNotShapedLikeOne() {
        // This id is a string an unrelated project chose, and it leaves here for a report read as
        // key=value and, from there, for a pull request body. Carrying a newline it is not a
        // connector nobody catalogued - it is extra lines in whatever reads the report next, which
        // on the drift lane is $GITHUB_OUTPUT, where a second decision= would gate the run open.
        List<ConnectorCatalogEntry> snapshot = List.of(row("mysql", MYSQL_PATH, MYSQL_SPEC));

        SpecDrift.Report report = SpecDrift.compare(snapshot, Map.of(
                MYSQL_PATH, MYSQL_SPEC,
                "connectors/odd-connector/src/main/resources/spec.json",
                "{\"properties\":{\"id\":\"odd\\ndecision=OPEN\"}}"));

        assertThat(report.newConnectorIds()).isEmpty();
    }
}
