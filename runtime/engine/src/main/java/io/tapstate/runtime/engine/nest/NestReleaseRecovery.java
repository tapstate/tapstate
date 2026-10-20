package io.tapstate.runtime.engine.nest;

import com.hazelcast.jet.core.AbstractProcessor;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.Staged;
import io.tapstate.spi.store.NestReleaseStore;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/** A finite, paged startup input; each wake follows the resolver's ordinary partitioned key route. */
final class NestReleaseRecovery extends AbstractProcessor implements Staged {
    record Wake(Object key) implements Serializable {
        Wake { Objects.requireNonNull(key, "key"); }
    }
    private final Function<String, List<NestReleaseStore.PendingKey>> pages;
    private List<NestReleaseStore.PendingKey> page = List.of();
    private String after;
    private int next;
    private boolean done;

    NestReleaseRecovery(Function<String, List<NestReleaseStore.PendingKey>> pages) {
        this.pages = Objects.requireNonNull(pages, "pages");
    }
    @Override public Stage stage() { return Stage.NEST; }
    @Override public boolean isCooperative() { return false; }

    @Override public boolean complete() {
        if (done) return true;
        while (true) {
            while (next < page.size()) {
                if (!tryEmit(new Wake(NestStateMapStore.releaseKey(page.get(next))))) return false;
                next++;
            }
            page = List.copyOf(pages.apply(after)); next = 0;
            if (page.isEmpty()) { done = true; return true; }
            if (page.size() > NestReleaseStore.MAX_PAGE_KEYS
                    || page.getLast().stateKey().equals(after))
                throw new IllegalStateException("invalid pending-release page");
            after = page.getLast().stateKey();
        }
    }
}
