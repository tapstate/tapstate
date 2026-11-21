package io.tapstate.control.core;

import io.tapstate.core.common.ProductErrorModule;
import io.tapstate.core.common.Severity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ArtifactErrorTest {

    @Test
    void everyCodeIsInTheArtifactDomainAndErrorSeverity() {
        assertThat(ProductErrorModule.ARTIFACT.number()).isEqualTo("024");
        for (ArtifactError e : ArtifactError.values()) {
            assertThat(e.code()).matches("TS024[0-9]{4}");
            assertThat(e.severity()).isEqualTo(Severity.ERROR);
        }
    }

    @Test
    void carriesTheResourceTypeAgnosticEditAndDeleteVocabulary() {
        assertThat(ArtifactError.values()).extracting(ArtifactError::code).containsExactlyInAnyOrder(
                // the id names no stored artifact
                "TS0240001",
                // a delete arrived without the mandatory content-hash precondition
                "TS0240002",
                // the supplied content hash is not the stored artifact's current one
                "TS0240003",
                // another stored resource still references the id
                "TS0240004",
                // the id is a pipeline that is running or is about to run
                "TS0240005",
                // the artifact was removed and some of its dependent bookkeeping was not
                "TS0240006");
    }

    @Test
    void declaresThePlaceholderContractPerCode() {
        // id = the artifact the caller asked to edit or delete
        assertThat(ArtifactError.NOT_FOUND.placeholders()).containsExactlyInAnyOrder("id");
        assertThat(ArtifactError.PRECONDITION_REQUIRED.placeholders()).containsExactlyInAnyOrder("id");
        assertThat(ArtifactError.VERSION_CONFLICT.placeholders()).containsExactlyInAnyOrder("id");
        // referrers = the ids still pointing at it, so the caller can act without a second query
        assertThat(ArtifactError.IN_USE.placeholders()).containsExactlyInAnyOrder("id", "referrers");
        // actual / desired = both halves of the lifecycle verdict, since one alone cannot explain it
        assertThat(ArtifactError.PIPELINE_NOT_STOPPED.placeholders())
                .containsExactlyInAnyOrder("id", "actual", "desired");
        // residue = what the removal left behind, which is the only part of this failure anyone can act
        // on: the removal itself is done and cannot be retried
        // reason = which of the two ways it ended incomplete, since the next step differs: a failed step
        // is cleared by hand, a pipeline that came back up must be stopped before anything is cleared
        assertThat(ArtifactError.RECLAIM_INCOMPLETE.placeholders())
                .containsExactlyInAnyOrder("id", "reason", "residue");
    }
}
