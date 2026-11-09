package io.tapstate.spi.store;

import java.util.List;
import java.util.Objects;

/**
 * A keyed state store whose namespaces can be committed together: every namespace one vertex instance wrote
 * since its last commit lands in one store transaction, or none of it does.
 *
 * <p>A group belongs to one vertex instance, and the instance is named when the group opens. The store fences
 * each commit on that instance alone rather than on anything the instance shares with its siblings, so
 * instances of one pipeline committing at the same time do not serialize on each other.
 */
public interface GroupCommitStateStore extends KeyedStateStore {

    /**
     * Opens the commit group of one vertex instance over {@code namespaces}, each named on the store that
     * serves it. The group proves its authority before it returns, so nothing the instance reads afterwards
     * can predate that proof. Every namespace must be served by the same pipeline execution and database;
     * a namespace that is not is refused here, before anything is written.
     *
     * @param vertexInstance the vertex and its instance index, stable across restarts of the same graph
     */
    StateGroup openGroup(String vertexInstance, List<GroupNamespace> namespaces);

    /**
     * The store and namespace this one writes {@code namespace} through. A store that translates names in
     * front of another answers with the one behind it, so a group can be opened on the store that does
     * the writing. A store that writes itself answers with itself, which is the default.
     */
    default GroupNamespace underlying(String namespace) {
        return new GroupNamespace(this, namespace);
    }

    /** One namespace as a group addresses it: the store that serves it and the name it is asked for by. */
    record GroupNamespace(GroupCommitStateStore store, String namespace) {
        public GroupNamespace {
            Objects.requireNonNull(store, "store");
            Objects.requireNonNull(namespace, "namespace");
        }
    }
}
