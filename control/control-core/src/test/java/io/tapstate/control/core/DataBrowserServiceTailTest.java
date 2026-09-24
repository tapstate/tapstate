package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.spi.store.DataBrowserChange;
import io.tapstate.spi.store.DataBrowserChangeListener;
import io.tapstate.spi.store.DataBrowserSubscription;
import io.tapstate.spi.store.DataBrowserTableInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The followed half of the read face. Two of its rules are the ones that fail quietly if they are
 * wrong: the confinement, which a follow reaches by a different road than a read and therefore has to
 * be checked again on that road; and what a filter does to a removal, which has no row to test.
 */
class DataBrowserServiceTailTest {

    private static final String VIEWS = "views";

    private static ArtifactStore store(String sourceId) {
        return new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> artifacts) {
            }

            @Override
            public Optional<Resource> get(String id) {
                return id.equals(sourceId)
                        ? Optional.of(new SourceResource(id, null, "mongodb", Map.of("uri", "mongodb://db.local"),
                                null, null, null, null))
                        : Optional.empty();
            }

            @Override
            public List<Resource> list() {
                return List.of();
            }
        };
    }

    /** A service whose collections listing is {@code held}, and whose follow records and replays. */
    private static DataBrowserService service(List<String> held, AtomicReference<DataBrowserChangeListener> opened,
            AtomicInteger closes) {
        return new DataBrowserService(
                store(VIEWS),
                new EmptySchemaStore(),
                config -> held,
                (config, collection) -> new DataBrowserTableInfo(0L, 0L, 0L),
                (config, query) -> {
                    throw new AssertionError("a follow must not run a bounded read");
                },
                (config, request, listener) -> {
                    opened.set(listener);
                    return closes::incrementAndGet;
                });
    }

    @Test
    @DisplayName("a follow of a collection the source's database does not hold is refused before it opens")
    void refusesACollectionTheDatabaseDoesNotHold() {
        AtomicReference<DataBrowserChangeListener> opened = new AtomicReference<>();
        DataBrowserService service = service(List.of("order_state"), opened, new AtomicInteger());

        assertThatThrownBy(() -> service.tail(VIEWS, "tokens", null, change -> { }))
                .as("a follow reaches the store through a different function than a read, and that "
                        + "function takes its table from whoever calls it - so the check that the "
                        + "collection is one this source's own database holds has to happen on this "
                        + "road too. Enforced on one of two roads it is not enforced")
                .isInstanceOf(TapstateException.class)
                .satisfies(refused -> assertThat(((TapstateException) refused).code().code())
                        .isEqualTo("data-browser.unknown-collection"));
        assertThat(opened.get())
                .as("and refused before the stream opens, not after it is already running")
                .isNull();
    }

    @Test
    @DisplayName("a filter narrows what the reader is shown, one change at a time")
    void filterNarrowsWhatIsDelivered() {
        AtomicReference<DataBrowserChangeListener> opened = new AtomicReference<>();
        DataBrowserService service = service(List.of("order_state"), opened, new AtomicInteger());
        List<DataBrowserChangeEvent> seen = new ArrayList<>();

        service.tail(VIEWS, "order_state",
                new DataBrowserCriteria.Match("status", DataBrowserCriteria.Operator.EQ, "Paid"),
                seen::add);

        opened.get().onChange(new DataBrowserChange(
                DataBrowserChange.Kind.INSERT, null, Map.of("status", "Paid"), 1L));
        opened.get().onChange(new DataBrowserChange(
                DataBrowserChange.Kind.INSERT, null, Map.of("status", "Shipped"), 2L));

        assertThat(seen).extracting(DataBrowserChangeEvent::at).containsExactly(1L);
    }

    @Test
    @DisplayName("a removal is tested on the row it carried, and reaches the reader when it carried none")
    void aRemovalIsTestedOnWhatItCarried() {
        AtomicReference<DataBrowserChangeListener> opened = new AtomicReference<>();
        DataBrowserService service = service(List.of("order_state"), opened, new AtomicInteger());
        List<DataBrowserChangeEvent> seen = new ArrayList<>();

        service.tail(VIEWS, "order_state",
                new DataBrowserCriteria.Match("status", DataBrowserCriteria.Operator.EQ, "Paid"),
                seen::add);
        opened.get().onChange(new DataBrowserChange(
                DataBrowserChange.Kind.DELETE, Map.of("status", "Paid"), null, 3L));
        opened.get().onChange(new DataBrowserChange(DataBrowserChange.Kind.DELETE, null, null, 4L));

        assertThat(seen).extracting(DataBrowserChangeEvent::at)
                .as("a removal that carried the row is tested on it like any other change; one that "
                        + "carried nothing is admitted, because dropping it would lose an event the "
                        + "store really made and there was nothing to judge it by")
                .containsExactly(3L, 4L);
        assertThat(seen.get(0).kind()).isEqualTo(DataBrowserChangeEvent.Kind.DELETE);
    }

    @Test
    @DisplayName("a stream that fails ends the follow through the sink instead of going quiet")
    void aStreamThatFailsIsReportedRatherThanLeftAsSilence() {
        AtomicReference<DataBrowserChangeListener> opened = new AtomicReference<>();
        DataBrowserService service = service(List.of("order_state"), opened, new AtomicInteger());
        List<DataBrowserChangeEvent> seen = new ArrayList<>();
        List<TapstateErrorCode> ended = new ArrayList<>();

        service.tail(VIEWS, "order_state", null, new DataBrowserChangeSink() {
            @Override
            public void onChange(DataBrowserChangeEvent change) {
                seen.add(change);
            }

            @Override
            public void onEnded(TapstateErrorCode reason) {
                ended.add(reason);
            }
        });
        opened.get().onChange(new DataBrowserChange(
                DataBrowserChange.Kind.INSERT, null, Map.of("status", "Paid"), 1L));
        opened.get().onError(new IllegalStateException("the driver went away"));

        assertThat(ended)
                .as("the stream runs on its own thread, so a failure has nowhere to be returned to; "
                        + "unreported, the reader keeps an open connection to a stream that ended, "
                        + "which is what a collection nobody is changing looks like")
                .containsExactly(DataBrowserError.FOLLOW_STOPPED);
        assertThat(seen)
                .as("the change that did arrive before the failure is still the reader's")
                .hasSize(1);
    }

    @Test
    @DisplayName("closing the follow closes the stream underneath it")
    void closingTheFollowClosesTheStream() {
        AtomicInteger closes = new AtomicInteger();
        DataBrowserService service = service(
                List.of("order_state"), new AtomicReference<>(), closes);

        DataBrowserFollow follow = service.tail(VIEWS, "order_state", null, change -> { });
        follow.close();

        assertThat(closes.get())
                .as("what a follow holds is a connector instance and a place in the host's ceiling; a "
                        + "close that stopped at this layer would leak both with nothing to report it")
                .isEqualTo(1);
    }
}
