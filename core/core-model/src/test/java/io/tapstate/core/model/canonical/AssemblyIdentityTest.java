package io.tapstate.core.model.canonical;

import io.tapstate.core.model.Metadata;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.SourceRef;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whitelist, read as an equality: two definitions share an assembly identity exactly when
 * everything they differ in is safe to re-assemble against.
 */
class AssemblyIdentityTest {

    @Test
    @DisplayName("a pipeline that differs only in its own srs switch keeps the same assembly identity")
    void theSwitchIsWhitelisted() {
        PipelineResource before = pipeline(null, SourceRef.spec("orders", true));
        PipelineResource after = pipeline(null, SourceRef.spec("orders", false));

        assertThat(AssemblyIdentity.of(after)).isEqualTo(AssemblyIdentity.of(before));
    }

    @Test
    @DisplayName("carrying a switch at all, versus none, is still the same assembly identity")
    void recordingTheSwitchForTheFirstTimeIsWhitelistedToo() {
        PipelineResource before = pipeline(null, SourceRef.bare("orders"));
        PipelineResource after = pipeline(null, SourceRef.spec("orders", true));

        assertThat(AssemblyIdentity.of(after)).isEqualTo(AssemblyIdentity.of(before));
    }

    @Test
    @DisplayName("a change outside the whitelist moves the assembly identity")
    void anythingElseIsNotWhitelisted() {
        PipelineResource before = pipeline(meta("reads orders"), SourceRef.spec("orders", true));
        PipelineResource after = pipeline(meta("reads orders, differently"), SourceRef.spec("orders", true));

        assertThat(AssemblyIdentity.of(after)).isNotEqualTo(AssemblyIdentity.of(before));
    }

    /**
     * The case a whitelist has to get right to be worth having: a whitelisted change sitting next to a
     * change that is not whitelisted must not launder it. An implementation that answered "did anything
     * whitelisted change" rather than "is everything that changed whitelisted" passes the two above and
     * fails here.
     */
    @Test
    @DisplayName("a whitelisted change does not launder one sitting beside it")
    void aWhitelistedChangeDoesNotLaunderOneBesideIt() {
        PipelineResource before = pipeline(meta("reads orders"), SourceRef.spec("orders", true));
        PipelineResource after = pipeline(meta("reads orders, differently"), SourceRef.spec("orders", false));

        assertThat(AssemblyIdentity.of(after)).isNotEqualTo(AssemblyIdentity.of(before));
    }

    @Test
    @DisplayName("adding a source is not whitelisted")
    void addingASourceIsNotWhitelisted() {
        PipelineResource before = pipeline(null, SourceRef.bare("orders"));
        PipelineResource after = pipeline(null, SourceRef.bare("orders"), SourceRef.bare("customers"));

        assertThat(AssemblyIdentity.of(after)).isNotEqualTo(AssemblyIdentity.of(before));
    }

    /**
     * The assembly identity is a second reading of the same canonical text, not a rename of the content
     * hash. Without this, an implementation that simply returned the content hash would pass every
     * "is not whitelisted" case above and only the two positive ones would catch it.
     */
    @Test
    @DisplayName("it is not the content hash: the switch moves one and leaves the other alone")
    void itIsNotTheContentHash() {
        PipelineResource before = pipeline(null, SourceRef.spec("orders", true));
        PipelineResource after = pipeline(null, SourceRef.spec("orders", false));
        CanonicalWriter writer = new CanonicalWriter();

        assertThat(CanonicalHash.of(writer.write(after)))
                .isNotEqualTo(CanonicalHash.of(writer.write(before)));
        assertThat(AssemblyIdentity.of(after)).isEqualTo(AssemblyIdentity.of(before));
    }

    private static PipelineResource pipeline(Metadata metadata, SourceRef... sources) {
        return new PipelineResource("p1", metadata, List.of(sources), null, null, null, null, null);
    }

    private static Metadata meta(String description) {
        return new Metadata(Map.of(), description);
    }
}
