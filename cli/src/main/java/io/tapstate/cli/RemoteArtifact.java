package io.tapstate.cli;

import java.util.Objects;

/**
 * The truth-layer view of one stored artifact a read returns: its id, kind, canonical form and content
 * hash as held by the server (server-as-truth). The response-side value the {@code get} and {@code ls}
 * verbs decode from the server's JSON. The CLI carries no shared control type (rule R6: it reaches the
 * server over HTTP only), so this mirrors the server's stored-artifact shape independently.
 *
 * <p>The hash is carried rather than derived. It is taken over the resource's structure, so the
 * canonical text beside it is not enough to recompute it, and a client that tried would produce a value
 * the server has never stored -- refused as a stale precondition on every single request.
 */
record RemoteArtifact(String id, String kind, String canonicalForm, String contentHash, boolean readable) {

    RemoteArtifact(String id, String kind, String canonicalForm) {
        this(id, kind, canonicalForm, null, true);
    }

    RemoteArtifact(String id, String kind, String canonicalForm, boolean readable) {
        this(id, kind, canonicalForm, null, readable);
    }

    RemoteArtifact {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(kind, "kind");
    }
}
