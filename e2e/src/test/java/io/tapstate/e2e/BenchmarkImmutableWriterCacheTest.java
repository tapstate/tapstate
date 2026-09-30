package io.tapstate.e2e;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkImmutableWriterCacheTest {
    private static final String BINARY = "org.bson.BsonBinaryWriter";
    private static final String DOCUMENT = "org.bson.BsonDocumentWriter";
    private record Writer(long id) { }
    private static final class Access implements BenchmarkImmutableWriterCache.Access<Writer> {
        final Map<Long, BenchmarkImmutableWriterCache.Node<Writer>> nodes = new HashMap<>();
        int reads;
        @Override public long identity(Writer writer) { return writer.id(); }
        @Override public BenchmarkImmutableWriterCache.Node<Writer> inspect(Writer writer) {
            reads++;
            if (!nodes.containsKey(writer.id())) { throw new AssertionError("pinned loader or bytecodes mismatch"); }
            return nodes.get(writer.id());
        }
        Writer terminal(long id, String type) {
            nodes.put(id, new BenchmarkImmutableWriterCache.Node<>(type, null, false));
            return new Writer(id);
        }
        Writer wrapper(long id, Writer delegate, boolean immutable) {
            nodes.put(id, new BenchmarkImmutableWriterCache.Node<>(null, delegate, immutable));
            return new Writer(id);
        }
    }

    @Test
    void recursiveEncodersReuseTheVerifiedPrimaryDelegatePath() throws Exception {
        var access = new Access();
        Writer binary = access.terminal(1, BINARY);
        Writer wrapper = access.wrapper(2, binary, true);
        var cache = new BenchmarkImmutableWriterCache<Writer>();
        assertThat(cache.classify(wrapper, access)).isEqualTo(BINARY);
        for (int i = 0; i < 100; i++) {
            assertThat(cache.classify(new Writer(2), access)).isEqualTo(BINARY);
            assertThat(cache.classify(binary, access)).isEqualTo(BINARY);
        }
        assertThat(access.reads).as("each immutable writer is inspected once in this command").isEqualTo(2);
    }

    @Test
    void aNewCommandCannotReuseAnEarlierCommandsIdentityClassification() throws Exception {
        var first = new Access();
        var second = new Access();
        Writer sameId = first.terminal(1, BINARY);
        second.terminal(1, DOCUMENT);
        assertThat(new BenchmarkImmutableWriterCache<Writer>().classify(sameId, first)).isEqualTo(BINARY);
        assertThat(new BenchmarkImmutableWriterCache<Writer>().classify(sameId, second)).isEqualTo(DOCUMENT);
        assertThat(second.reads).isEqualTo(1);
    }

    @Test
    void aMutablePrimaryDelegateIsRejectedBeforeCaching() throws Exception {
        var access = new Access();
        Writer wrapper = access.wrapper(2, access.terminal(1, BINARY), false);
        var cache = new BenchmarkImmutableWriterCache<Writer>();
        assertThatThrownBy(() -> cache.classify(wrapper, access)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("not immutable");
        access.wrapper(2, new Writer(1), true);
        assertThat(cache.classify(wrapper, access)).isEqualTo(BINARY);
        assertThat(access.reads).isEqualTo(3);
    }

    @Test
    void cyclesCannotEnterTheCache() {
        var access = new Access();
        Writer root = access.wrapper(1, new Writer(2), true);
        access.wrapper(2, root, true);
        assertThatThrownBy(() -> new BenchmarkImmutableWriterCache<Writer>().classify(root, access))
                .isInstanceOf(AssertionError.class).hasMessageContaining("delegate cycle");
    }

    @Test
    void uncachedPathsPreserveTheEightWriterDepthLimit() {
        var access = new Access();
        Writer root = access.terminal(1, BINARY);
        for (int id = 2; id <= 9; id++) { root = access.wrapper(id, root, true); }
        Writer tooDeep = root;
        assertThatThrownBy(() -> new BenchmarkImmutableWriterCache<Writer>().classify(tooDeep, access))
                .isInstanceOf(AssertionError.class).hasMessageContaining("depth exceeded");
    }

    @Test
    void cachedSuffixesCannotHideExcessiveDelegateDepth() throws Exception {
        var access = new Access();
        Writer root = access.terminal(1, BINARY);
        for (int id = 2; id <= 8; id++) { root = access.wrapper(id, root, true); }
        var cache = new BenchmarkImmutableWriterCache<Writer>();
        assertThat(cache.classify(root, access)).isEqualTo(BINARY);
        Writer tooDeep = access.wrapper(9, root, true);
        assertThatThrownBy(() -> cache.classify(tooDeep, access)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("depth exceeded");
        assertThat(access.reads).isEqualTo(9);
    }

    @Test
    void distinctWritersHaveAFixedCommandLocalCapacity() throws Exception {
        var access = new Access();
        var cache = new BenchmarkImmutableWriterCache<Writer>();
        for (int id = 1; id <= 128; id++) { cache.classify(access.terminal(id, BINARY), access); }
        assertThatThrownBy(() -> cache.classify(access.terminal(129, BINARY), access))
                .isInstanceOf(AssertionError.class).hasMessageContaining("cache exceeded its bound");
        assertThat(cache.classify(new Writer(1), access)).isEqualTo(BINARY);
        assertThat(access.reads).isEqualTo(129);
    }

    @Test
    void failedVerificationDoesNotPublishAPartialCachedPath() throws Exception {
        var access = new Access();
        Writer wrapper = access.wrapper(2, new Writer(1), true);
        var cache = new BenchmarkImmutableWriterCache<Writer>();
        assertThatThrownBy(() -> cache.classify(wrapper, access)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("loader or bytecodes mismatch");
        access.terminal(1, BINARY);
        assertThat(cache.classify(wrapper, access)).isEqualTo(BINARY);
        assertThat(access.reads).isEqualTo(4);
    }
}
