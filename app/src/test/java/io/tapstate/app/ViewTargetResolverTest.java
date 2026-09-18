package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.Storage;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.spi.sink.TargetIndex;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The rules that turn a declared view into the address its materialization writes to. Pure
 * resolution over the model, so these assert the naming contract without a store in reach.
 */
class ViewTargetResolverTest {

    @Test
    void a_view_with_no_key_is_refused_rather_than_materialized_without_one() {
        // Upsert matches on the key. Without one every change would append a second copy of the record
        // the write path believed it was updating - the silent corruption this refusal exists to stop.
        ViewBlock.Inline view = new ViewBlock.Inline(
                "order_state", FromRef.literal("orders_src"), null, null);

        assertThatThrownBy(() -> ViewTargetResolver.resolve(view))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code().code())
                        .isEqualTo("actuation.view-key-missing"));
    }

    @Test
    void a_hot_tier_is_refused_by_name_rather_than_silently_ignored() {
        ViewBlock.Inline view = new ViewBlock.Inline(
                "order_state", FromRef.literal("orders_src"), "order_id",
                new Storage(new Storage.Hot("10m"), new Storage.Warm("order_state", null), null));

        assertThatThrownBy(() -> ViewTargetResolver.resolve(view))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code().code())
                        .isEqualTo("actuation.view-storage-tier-unsupported"));
    }

    @Test
    void a_cold_tier_is_refused_by_the_same_code_as_hot() {
        ViewBlock.Inline view = new ViewBlock.Inline(
                "order_state", FromRef.literal("orders_src"), "order_id",
                new Storage(null, new Storage.Warm("order_state", null), new Storage.Cold(List.of("day"))));

        assertThatThrownBy(() -> ViewTargetResolver.resolve(view))
                .isInstanceOf(TapstateException.class)
                .satisfies(e -> assertThat(((TapstateException) e).code().code())
                        .isEqualTo("actuation.view-storage-tier-unsupported"));
    }

    @Test
    void a_view_without_storage_materializes_under_its_own_id() {
        // Declaring a view is the whole instruction: the author names no collection and gets one.
        ViewBlock.Inline view = new ViewBlock.Inline(
                "order_state", FromRef.literal("orders_src"), "order_id", null);

        assertThat(ViewTargetResolver.resolve(view).collection()).isEqualTo("order_state");
    }

    @Test
    void an_explicit_warm_collection_overrides_the_default_physical_name() {
        ViewBlock.Inline view = new ViewBlock.Inline(
                "order_state", FromRef.literal("orders_src"), "order_id",
                new Storage(null, new Storage.Warm("orders_flat", null), null));

        assertThat(ViewTargetResolver.resolve(view).collection()).isEqualTo("orders_flat");
    }

    @Test
    void every_view_lands_in_the_one_managed_state_store() {
        // The store's source id is deliberately not per-view: it is the single place the deployment's
        // name for the managed store is written down, so renaming it is one edit rather than a sweep.
        ViewBlock.Inline first = new ViewBlock.Inline(
                "order_state", FromRef.literal("orders_src"), "order_id", null);
        ViewBlock.Inline second = new ViewBlock.Inline(
                "shipment_state", FromRef.literal("ships_src"), "shipment_id",
                new Storage(null, new Storage.Warm("ships_flat", null), null));

        assertThat(ViewTargetResolver.resolve(first).sourceId())
                .isEqualTo(ViewTargetResolver.resolve(second).sourceId())
                .isEqualTo(ViewTargetResolver.STATE_STORE_SOURCE_ID);
    }

    @Test
    void the_managed_store_is_named_views() {
        // Pin the literal, not just the constant's self-consistency. The test above proves every view
        // lands in the *same* store, which stays true under any rename and so cannot notice one. This
        // name is not internal: the demo workspace declares a source under it, the walkthrough addresses
        // the database by it, and a reader of `show collections` sees it as the prefix on every listing.
        // Those live outside this module and no compiler relates them, so the coupling needs an assertion
        // that fails when the name moves and sends whoever moved it to look at the other three.
        assertThat(ViewTargetResolver.STATE_STORE_SOURCE_ID)
                .as("the managed state store's user-visible name, also spelled in the demo workspace, "
                        + "the walkthrough, and every collection listing")
                .isEqualTo("views");
    }

    @Test
    void the_primary_key_is_indexed_by_default_and_uniquely() {
        // Nothing else makes the key a viable sort key: a non-unique index leaves a range start
        // ambiguous, and no index at all fails only once the data outgrows an in-memory sort.
        ViewBlock.Inline view = new ViewBlock.Inline(
                "order_state", FromRef.literal("orders_src"), "order_id", null);

        assertThat(ViewTargetResolver.resolve(view).indexes())
                .containsExactly(new TargetIndex(List.of("order_id"), true));
    }

    @Test
    void declared_warm_indexes_are_created_alongside_the_key_index_and_are_not_unique() {
        ViewBlock.Inline view = new ViewBlock.Inline(
                "order_state", FromRef.literal("orders_src"), "order_id",
                new Storage(null, new Storage.Warm("order_state", List.of("customer_id")), null));

        assertThat(ViewTargetResolver.resolve(view).indexes()).containsExactly(
                new TargetIndex(List.of("order_id"), true),
                new TargetIndex(List.of("customer_id"), false));
    }

    @Test
    void a_declared_index_naming_the_key_column_does_not_duplicate_the_key_index() {
        // An author listing the key under storage.warm.indexes is redundancy, not a second index: two
        // definitions over the one field - one unique, one not - is a shape a store may refuse outright.
        ViewTargetResolver.ViewTarget target = ViewTargetResolver.resolve(new ViewBlock.Inline(
                "order_state", FromRef.literal("orders"), "id",
                new Storage(null, new Storage.Warm("order_state", List.of("id", "name")), null)));

        assertThat(target.indexes())
                .as("one definition per field, the key's unique one winning")
                .containsExactly(
                        new TargetIndex(List.of("id"), true),
                        new TargetIndex(List.of("name"), false));
    }
}
