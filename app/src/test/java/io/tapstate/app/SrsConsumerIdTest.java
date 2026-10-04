package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.spi.store.SrsConsumerId;
import org.junit.jupiter.api.Test;

class SrsConsumerIdTest {

    @Test
    void lengthDelimitedSourceCoordinatesAreReversibleAndCannotCollide() {
        String first = SrsConsumerId.of("pipe:4", "source").value();
        String second = SrsConsumerId.of("pipe", "4:source").value();

        assertThat(first).isNotEqualTo(second);
        assertThat(SrsConsumerId.pipelineOf(first)).isEqualTo("pipe:4");
        assertThat(SrsConsumerId.sourceOf(first)).contains("source");
        assertThat(SrsConsumerId.pipelineOf(second)).isEqualTo("pipe");
        assertThat(SrsConsumerId.sourceOf(second)).contains("4:source");
        assertThat(SrsConsumerId.belongsTo(first, "pipe:4")).isTrue();
        assertThat(SrsConsumerId.belongsTo(first, "pipe")).isFalse();
    }

    @Test
    void legacyPipelineOnlyIdsStayIdentifiableWithoutInventingASource() {
        assertThat(SrsConsumerId.pipelineOf("legacy-pipe")).isEqualTo("legacy-pipe");
        assertThat(SrsConsumerId.sourceOf("legacy-pipe")).isEmpty();
        assertThat(SrsConsumerId.belongsTo("legacy-pipe", "legacy-pipe")).isTrue();
    }

    @Test
    void aMalformedVersionedIdIsRefusedInsteadOfBecomingLegacyProgress() {
        assertThatThrownBy(() -> SrsConsumerId.sourceOf("~srs-consumer:v1:4:pipe20:source"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
