package io.tapstate.cli;

import io.tapstate.core.model.DdlPolicy;
import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.QueryElement;
import io.tapstate.core.model.QueryType;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.WriteMode;

import java.util.ArrayList;
import java.util.List;

/**
 * Collects a standalone serve's publish surfaces: 0..n of sync / query / push, at least one. Each sync
 * gets a running auto id ({@code sync_1}, {@code sync_2}, …) so a query can name it as a backend; a
 * query offered no sync is a parallel egress (no backend). This richer surface flow is exclusive to the
 * standalone serve wizard — the pipeline's inline serve stays a single shallow surface.
 */
final class ServeSurfaceWizard {

    /** The menu choice that ends the surface-adding loop; offered only once a surface exists. */
    private static final String DONE = "(done)";
    /** The backend choice declining a sink (a parallel egress from the view store). */
    private static final String NONE = "(none)";

    private final Prompter prompter;
    private final List<String> existingSourceIds;

    ServeSurfaceWizard(Prompter prompter, List<String> existingSourceIds) {
        this.prompter = prompter;
        this.existingSourceIds = existingSourceIds;
    }

    /** A serve's three optional surface lists; each is null when empty (canonical omits empty blocks). */
    record Surfaces(List<SyncElement> sync, List<QueryElement> query, List<PushElement> push) {
    }

    Surfaces collect() {
        List<SyncElement> sync = new ArrayList<>();
        List<QueryElement> query = new ArrayList<>();
        List<PushElement> push = new ArrayList<>();
        while (true) {
            int count = sync.size() + query.size() + push.size();
            String choice = prompter.choose("Add a surface?", surfaceMenu(count));
            if (DONE.equals(choice)) {
                break; // offered only once a surface exists, so a serve always carries at least one
            }
            switch (choice) {
                case "push" -> push.add(askPush(push.size() + 1));
                case "query" -> query.add(askQuery(sync));
                default -> sync.add(askSync(sync.size() + 1)); // "sync" or an exhausted (default) reply
            }
        }
        return new Surfaces(nullIfEmpty(sync), nullIfEmpty(query), nullIfEmpty(push));
    }

    /**
     * With no surface yet, {@code sync} is the safe default (last) and {@code (done)} is withheld — a
     * serve must carry at least one surface. Once one exists, {@code (done)} is the default (last).
     */
    private static List<String> surfaceMenu(int count) {
        if (count == 0) {
            return List.of("push", "query", "sync");
        }
        return List.of("sync", "push", "query", DONE);
    }

    private SyncElement askSync(int n) {
        String source =
                WizardPrompts.askSourceRef(prompter, existingSourceIds, "Sync to (target source id)", true);
        WriteMode writeMode = askWriteMode();
        DdlPolicy ddl = askDdl();
        return new SyncElement("sync_" + n, source, writeMode, null, ddl);
    }

    /** Upsert is the canonical default, listed last so an empty reply selects it (and is then omitted). */
    private WriteMode askWriteMode() {
        String chosen = prompter.choose("Write mode?", List.of("append", "upsert"));
        for (WriteMode m : WriteMode.values()) {
            if (m.yaml().equals(chosen)) {
                return m;
            }
        }
        return WriteMode.UPSERT; // unreachable: choose returns one of the offered options
    }

    /** Fail is the canonical default, listed last so an empty reply selects it (and is then omitted). */
    private DdlPolicy askDdl() {
        String chosen = prompter.choose("DDL policy?", List.of("apply", "ignore", "fail"));
        for (DdlPolicy d : DdlPolicy.values()) {
            if (d.yaml().equals(chosen)) {
                return d;
            }
        }
        return DdlPolicy.FAIL; // unreachable: choose returns one of the offered options
    }

    private QueryElement askQuery(List<SyncElement> syncs) {
        String type = prompter.choose("Query type?", List.of("graphql", "mcp", "rest"));
        return new QueryElement(queryType(type), askBackend(syncs));
    }

    /** A query backed by a sink names an earlier sync id; with no sync it is a parallel egress (no backend). */
    private String askBackend(List<SyncElement> syncs) {
        if (syncs.isEmpty()) {
            return null;
        }
        List<String> options = new ArrayList<>();
        for (SyncElement s : syncs) {
            options.add(s.id());
        }
        options.add(NONE);
        String chosen = prompter.choose("Query backend?", options);
        return NONE.equals(chosen) ? null : chosen;
    }

    private PushElement askPush(int n) {
        String source =
                WizardPrompts.askSourceRef(prompter, existingSourceIds, "Push to (target source id)", true);
        String topic = WizardPrompts.blankToNull(prompter.ask("Topic (blank for none)", null));
        return new PushElement("push_" + n, source, topic, null);
    }

    private static QueryType queryType(String yaml) {
        for (QueryType type : QueryType.values()) {
            if (type.yaml().equals(yaml)) {
                return type;
            }
        }
        throw new IllegalStateException("unhandled query type: " + yaml);
    }

    private static <T> List<T> nullIfEmpty(List<T> list) {
        return list.isEmpty() ? null : list;
    }
}
