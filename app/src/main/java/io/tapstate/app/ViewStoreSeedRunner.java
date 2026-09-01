package io.tapstate.app;

import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactStore;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;

/**
 * Registers the managed state store views materialize into, once at startup, so a deployment has one
 * without anyone declaring it.
 *
 * <p>It used to be a file the demo script wrote and the user applied, which made the store look like part
 * of the workspace an author owns. It is not: it is the deployment's, the same store the server already
 * keeps its own state in, and requiring a person to hand it back to the server was a step that existed
 * only because nothing else created it.
 *
 * <p>The connection is derived from the server's own store URI rather than configured separately. There is
 * nothing else it could point at in this release -- the bundled store is one instance shared by the
 * control plane and the views, which is a documented boundary of the preview rather than an oversight --
 * and a second setting would be one more thing to get wrong for a deployment that has no second instance
 * to name.
 *
 * <p>Seeding never overwrites. The insert is the store's atomic create, so an author who has declared
 * their own resource under this id keeps it, and the seed reports that it found one rather than replacing
 * what somebody meant to put there.
 */
final class ViewStoreSeedRunner implements SmartInitializingSingleton {

    private static final Logger LOG = LoggerFactory.getLogger(ViewStoreSeedRunner.class);

    /** The connector the bundled store speaks; the only shape the view sink writes today. */
    private static final String CONNECTOR = "mongodb";

    /** Where the driver authenticates when a connection string names no database of its own. */
    private static final String SPEC_DEFAULT_AUTH_SOURCE = "admin";

    /** TLS asked for in the URI, in either of the two spellings the driver accepts. */
    private static final Pattern TLS_ENABLED = Pattern.compile("[?&](tls|ssl)=true", Pattern.CASE_INSENSITIVE);

    private final ArtifactStore artifacts;
    private final String serverStoreUri;
    private final String tlsCaFile;

    ViewStoreSeedRunner(ArtifactStore artifacts, String serverStoreUri, String tlsCaFile) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.serverStoreUri = Objects.requireNonNull(serverStoreUri, "serverStoreUri");
        this.tlsCaFile = tlsCaFile;
    }

    @Override
    public void afterSingletonsInstantiated() {
        seed();
    }

    void seed() {
        String id = ViewTargetResolver.STATE_STORE_SOURCE_ID;
        if (trustCannotTravelInTheUri()) {
            // Seeding anyway would register a connection that can never be made: the connector would
            // reach the right host and fail the handshake, reporting a store that is running perfectly.
            // Better to have no store and say why than one that is wrong in a way nothing here can fix.
            LOG.warn("Not registering the managed state store '{}': this server trusts its own store through a "
                    + "certificate authority file, and that trust cannot be carried in a connection URI. "
                    + "Declare a source under that id with the connector's own TLS settings.", id);
            return;
        }
        SourceResource store = new SourceResource(
                id, null, CONNECTOR,
                Map.of("isUri", true, "uri", viewsUri(serverStoreUri)),
                // No mode and no tables: this is a connection supplier, not something to read from. The
                // distinction is load-bearing -- a resource under this id that declares capture settings
                // is refused as an authored source rather than written into.
                null, null, null, null);
        ArtifactMutation outcome = artifacts.create(store);
        if (outcome == ArtifactMutation.CREATED) {
            LOG.info("Registered the managed state store '{}' for views to materialize into", id);
        } else {
            LOG.info("The managed state store '{}' is already declared; leaving it as it is", id);
        }
    }

    /**
     * The server's store URI with its database swapped for the view store's.
     *
     * <p>Views are kept in their own database rather than beside the control plane's collections: the two
     * are the deployment's and the user's data respectively, and a listing of one should not enumerate the
     * other. Host, credentials and every option are carried across, because whatever it took to reach the
     * one database is what it takes to reach the other on the same server.
     *
     * <p>Which is why the database is not the only thing the path decides. With no explicit
     * {@code authSource}, the connection string spec authenticates against the database named in the URI
     * -- so rewriting the path alone moves the authentication database too, and a deployment whose
     * credentials live in the control-plane database would derive a URI that authenticates against one
     * where that user does not exist. The store would be registered and permanently unusable, and the
     * failure would arrive as a login error against a database nobody configured. The original database
     * is therefore pinned as {@code authSource} whenever the URI named one and did not already say.
     *
     * <p>And where it named none but does carry credentials, the fallback is pinned instead. The spec
     * falls back to {@code admin} only because no database was named -- which stops being true of the
     * derived URI the moment this method names one, so an inherited default would land on the view store
     * exactly as a rewritten path does.
     */
    static String viewsUri(String serverStoreUri) {
        String database = ViewTargetResolver.STATE_STORE_SOURCE_ID;
        int scheme = serverStoreUri.indexOf("://");
        int authorityStart = scheme < 0 ? 0 : scheme + 3;
        int query = serverStoreUri.indexOf('?', authorityStart);
        String beforeQuery = query < 0 ? serverStoreUri : serverStoreUri.substring(0, query);
        String fromQuery = query < 0 ? "" : serverStoreUri.substring(query);
        int path = beforeQuery.indexOf('/', authorityStart);
        String authority = path < 0 ? beforeQuery : beforeQuery.substring(0, path);
        String originalDatabase = path < 0 ? "" : beforeQuery.substring(path + 1);
        return authority + "/" + database
                + withAuthSource(fromQuery, originalDatabase, carriesCredentials(authority, authorityStart));
    }

    /** Whether the URI carries credentials at all -- userinfo ahead of the host list. */
    private static boolean carriesCredentials(String authority, int authorityStart) {
        return authority.indexOf('@', authorityStart) >= 0;
    }

    /**
     * The query with {@code authSource} pinned to whatever the original authenticated against, unless the
     * caller already said or there was never anywhere to move. A named database is pinned whether or not
     * this URI authenticates today, because it is the value the rewrite silently changes and a credential
     * added later would inherit the change. With no database named, the spec's own fallback is pinned --
     * but only for a URI that actually carries credentials, since the option means nothing otherwise and
     * would have to be explained to whoever reads the seeded resource.
     */
    private static String withAuthSource(String query, String originalDatabase, boolean carriesCredentials) {
        if (namesAuthSource(query)) {
            return query;
        }
        String source = originalDatabase.isEmpty()
                ? (carriesCredentials ? SPEC_DEFAULT_AUTH_SOURCE : "")
                : originalDatabase;
        if (source.isEmpty()) {
            return query;
        }
        return query + (query.isEmpty() ? "?" : "&") + "authSource=" + source;
    }

    /**
     * Whether reaching the store takes trust material that a URI has no room for.
     *
     * <p>A certificate authority file is consulted only when the URI asks for TLS, so one configured
     * beside a plaintext connection is inert and changes nothing here.
     */
    private boolean trustCannotTravelInTheUri() {
        return tlsCaFile != null && TLS_ENABLED.matcher(serverStoreUri).find();
    }

    /**
     * Whether the query already carries an {@code authSource} option.
     *
     * <p>Compared as an option name, case-insensitively, because that is how the connection string is
     * read: a substring test takes {@code authsource=admin} for something else and appends a second
     * option beside it, and it takes the same characters appearing inside another option's value for a
     * setting nobody made.
     */
    private static boolean namesAuthSource(String query) {
        String options = query.startsWith("?") ? query.substring(1) : query;
        for (String option : options.split("&")) {
            int equals = option.indexOf('=');
            String name = equals < 0 ? option : option.substring(0, equals);
            if (name.equalsIgnoreCase("authSource")) {
                return true;
            }
        }
        return false;
    }
}
