package io.tapstate.adapters.pdk;

import io.tapstate.spi.store.SourceModelComparison;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class PdkSourceModelComparisonTest {
    @Test void anIdOrTypeSpellingCannotGrantTheDocumentPolicy() {
        var known = artifact("io.tapdata.mongodb.MongodbConnector", "a".repeat(64));
        assertThat(PdkSourceModelComparison.forVerifiedArtifact("mongodb", known)).isEqualTo(SourceModelComparison.SAMPLED_DOCUMENT);
        assertThat(PdkSourceModelComparison.forVerifiedArtifact("postgres", known)).isEqualTo(SourceModelComparison.DECLARED_STRUCTURE);
        assertThat(PdkSourceModelComparison.forVerifiedArtifact("mongodb", artifact("example.Mongo", "a".repeat(64))))
                .isEqualTo(SourceModelComparison.DECLARED_STRUCTURE);
        assertThat(PdkSourceModelComparison.forVerifiedArtifact("mongodb", artifact(known.className(), null)))
                .isEqualTo(SourceModelComparison.DECLARED_STRUCTURE);
        assertThat(PdkSourceModelComparison.forVerifiedArtifact("mongodb", artifact(known.className(), "unbound")))
                .isEqualTo(SourceModelComparison.DECLARED_STRUCTURE);
    }

    private static ConnectorRef artifact(String implementation, String hash) {
        return new ConnectorRef(List.of(), implementation, "1.0", null, null, hash, false);
    }
}
