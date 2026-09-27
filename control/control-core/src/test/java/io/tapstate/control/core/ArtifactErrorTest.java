package io.tapstate.control.core;

import io.tapstate.core.common.Domain;
import io.tapstate.core.common.Severity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ArtifactErrorTest {

    @Test
    void everyCodeIsInTheArtifactDomainAndErrorSeverity() {
        assertThat(Domain.isRegistered("artifact")).isTrue();
        for (ArtifactError e : ArtifactError.values()) {
            assertThat(e.code()).startsWith("artifact.");
            // The one exception is not a refusal: it reports an adoption that has already happened.
            assertThat(e.severity()).isEqualTo(e == ArtifactError.PROJECT_CLAIMED ? Severity.WARNING : Severity.ERROR);
        }
    }

    @Test
    void carriesTheResourceTypeAgnosticEditAndDeleteVocabulary() {
        assertThat(ArtifactError.values()).extracting(ArtifactError::code).containsExactlyInAnyOrder(
                // the id names no stored artifact
                "artifact.not-found",
                // a delete arrived without the mandatory content-hash precondition
                "artifact.precondition-required",
                // the supplied content hash is not the stored artifact's current one
                "artifact.version-conflict",
                // another stored resource still references the id
                "artifact.in-use",
                // the id is a pipeline that is running or is about to run
                "artifact.pipeline-not-stopped",
                // the artifact was removed and some of its dependent bookkeeping was not
                "artifact.reclaim-incomplete",
                // another project on this server already owns the id
                "artifact.project-id-taken",
                // a resource stored with no project was adopted by the project that applied it
                "artifact.project-claimed");
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
        // owner = the project holding the id, project = the one refused: both, since the remedy names both
        assertThat(ArtifactError.PROJECT_ID_TAKEN.placeholders())
                .containsExactlyInAnyOrder("id", "kind", "owner", "project");
        assertThat(ArtifactError.PROJECT_CLAIMED.placeholders())
                .containsExactlyInAnyOrder("id", "kind", "project");
    }
}
