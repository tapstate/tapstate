package io.tapstate.spi.store;

import io.tapstate.core.model.PipelineNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.tapstate.core.model.PipelineNode.Purpose.CAPTURE;
import static io.tapstate.core.model.PipelineNode.Purpose.SYNC;
import static io.tapstate.spi.store.ConnectorRunBindings.Phase.PREPARED;
import static io.tapstate.spi.store.ConnectorRunBindings.Phase.PUBLISHED;
import static io.tapstate.spi.store.ConnectorRunBindings.Phase.REQUESTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class ConnectorRunBindingsTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 2, "cache", "reader", 3);
    private static final String FIRST = "a".repeat(64);
    private static final String SECOND = "b".repeat(64);

    @Test
    void aNodeCanKeepSeparateCaptureAndSinkBindingsButCannotHaveTwoOfEitherPurpose() {
        var source = privateBinding("reader", CAPTURE, FIRST, 3);
        var target = privateBinding("reader", SYNC, SECOND, 3);
        var bindings = new ConnectorRunBindings(SCOPE, 1, List.of(source, target), null);
        assertThat(bindings.find("node", CAPTURE)).contains(source);
        assertThat(bindings.find("node", SYNC)).contains(target);
        assertThat(bindings.find("unknown", CAPTURE)).isEmpty();
        assertThatIllegalArgumentException().isThrownBy(() ->
                new ConnectorRunBindings(SCOPE, 1, List.of(source, privateBinding("reader", CAPTURE, SECOND, 3)), null));
    }

    @Test
    void publicationRequiresTheCompleteCandidateAndExactlyItsIntentRevision() {
        var old = List.of(privateBinding("reader", SYNC, FIRST, 3));
        var current = List.of(privateBinding("reader", SYNC, SECOND, 3));
        var published = new ConnectorRunBindings.Handoff(2, PUBLISHED, current, old);
        assertThat(new ConnectorRunBindings(SCOPE, 2, current, published).effective()).isEqualTo(current);
        assertThatIllegalArgumentException().isThrownBy(() -> new ConnectorRunBindings(SCOPE, 1, current, published));
        assertThatIllegalArgumentException().isThrownBy(() -> new ConnectorRunBindings(SCOPE, 2, old, published));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new ConnectorRunBindings.Handoff(2, REQUESTED, current, List.of()));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new ConnectorRunBindings.Handoff(2, PREPARED, current, old));
        assertThatIllegalArgumentException().isThrownBy(() -> new ConnectorRunBindings(SCOPE, 2, old,
                new ConnectorRunBindings.Handoff(2, PREPARED, current, List.of())));
    }

    @Test
    void aBindingCannotKeepNotesFromAnotherPipelineGenerationOrSharedChain() {
        var wrongPipeline = privateBinding("other", SYNC, FIRST, 3);
        var oldGeneration = privateBinding("reader", SYNC, FIRST, 2);
        var otherChain = new RequiredNoteScope(new RequiredNoteOwner(2, "pdk.chain.other",
                RequiredNoteOwner.Kind.CHAIN, "other", 0), 1);
        var wrongChain = new ConnectorRunBindings.Binding("node", CAPTURE, "mongodb", "2.11.0", FIRST,
                otherChain, "capture", "selected");
        for (var wrong : List.of(wrongPipeline, oldGeneration, wrongChain)) {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new ConnectorRunBindings(SCOPE, 1, List.of(wrong), null));
        }
        assertThatIllegalArgumentException().isThrownBy(() -> new ConnectorRunBindings.Binding("node", SYNC,
                "mongodb", "2.11.0", FIRST, otherChain, "capture", "other"));
    }

    private static ConnectorRunBindings.Binding privateBinding(String pipeline, PipelineNode.Purpose purpose,
                                                               String hash, long generation) {
        var owner = RequiredNoteOwner.forNode(2, new PipelineNode(pipeline, "node"), purpose, generation);
        return new ConnectorRunBindings.Binding("node", purpose, "mongodb", "2.11.0", hash,
                new RequiredNoteScope(owner, 1), null, null);
    }
}
