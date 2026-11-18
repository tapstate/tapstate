package io.tapstate.control.restapi;

import io.tapstate.control.core.DataBrowserCriteria;
import io.tapstate.control.core.DataBrowserPreviewReport;
import io.tapstate.control.core.DataBrowserService;
import io.tapstate.control.core.DataBrowserSortOrder;
import io.tapstate.control.core.DataBrowserStatsReport;
import io.tapstate.core.common.TapstateException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;

/**
 * The three data-browser verbs projected onto HTTP: list a declared source's collections, read one
 * collection's rows, report one collection's size. Each drives the control-core service, which resolves
 * the source to the connection its reads run on and drives the whitelisted runtime probe. A thin
 * projection with no business logic of its own; the service speaks control-ring types, so this face never
 * reaches into the storage ports.
 *
 * <p>They read through to the connector and persist nothing, not even the result, so all three are
 * read-scoped and unaudited — that is what separates them from the two connection probes, which look
 * similar but store what they found. The grade is the registry's, not the HTTP method's: the read is
 * posted because it carries a request body, not because it changes anything.
 *
 * <p>Every call stands alone. The read is one-shot, so no continuation state travels in either direction:
 * nothing in the request names a position to resume from, and nothing in the response offers one. What
 * the response does carry is whether more rows remain — the only thing separating a preview of ten from a
 * collection of ten.
 *
 * <p>Which database a read reaches is settled by the source's own connection. Neither path nor body has a
 * field for one, so nothing a caller sends can move a read off the connection it was resolved from.
 */
@RestController
class DataBrowserController {

    /**
     * Compared as a string because the ring that owns this code is not on this module's classpath, and a
     * read face has no business depending on the connector ring to name one refusal.
     */
    private static final String CAPABILITY_MISSING =
            io.tapstate.core.common.ConnectorErrorDefinitions.CAPABILITY_MISSING.code();

    private final DataBrowserService browser;

    DataBrowserController(DataBrowserService browser) {
        this.browser = Objects.requireNonNull(browser, "browser");
    }

    @Verb("data-browser.collections")
    @GetMapping("/sources/{sourceId}/collections")
    CollectionList collections(@PathVariable("sourceId") String sourceId) {
        // The path variable is named explicitly: this build compiles without -parameters, so an inferred
        // name would not resolve at runtime.
        return served(() -> CollectionList.of(browser.collections(sourceId)));
    }

    @Verb("data-browser.stats")
    @GetMapping("/sources/{sourceId}/collections/{collection}/stats")
    DataBrowserStatsReport stats(
            @PathVariable("sourceId") String sourceId, @PathVariable("collection") String collection) {
        return served(() -> browser.stats(sourceId, collection));
    }

    @Verb("data-browser.find")
    @PostMapping("/sources/{sourceId}/collections/{collection}:find")
    DataBrowserPreviewReport find(
            @PathVariable("sourceId") String sourceId,
            @PathVariable("collection") String collection,
            @RequestBody(required = false) DataBrowserFindRequest request) {
        // A body-less read is the whole request shape left unasked-for, which is a valid request: every
        // field is optional and an absent one takes the control plane's own answer.
        DataBrowserFindRequest body =
                request == null ? new DataBrowserFindRequest(null, null, null) : request;
        return served(() -> browser.find(
                sourceId, collection, criteria(body.filter()), order(body.sort()), body.limit()));
    }

    /**
     * Runs a read, attributing a connector that cannot answer it to the request that named the source.
     *
     * <p>A capability the connector does not implement is a fact about that source: no retry changes it,
     * and the caller acts on it by reading a different one. Every other connector failure — a read that
     * broke part-way, a runtime that would not link — is the server's, and keeps the status the code
     * table gives it. Deciding it here rather than in that table is what lets the same code stay
     * server-side on the paths where it really is the server's: resolving a connection, discovering a
     * schema. Answered as a 500 instead, a caller could not tell their request had been refused from the
     * product having fallen over, which is the one thing a refusal has to be distinguishable from.
     */
    private static <T> T served(Supplier<T> read) {
        try {
            return read.get();
        } catch (TapstateException e) {
            if (CAPABILITY_MISSING.equals(e.code().code()) || "connector.capability-missing".equals(e.code().code())) {
                throw new BadRequestCodedException(e);
            }
            throw e;
        }
    }

    /**
     * The control-ring criteria for a requested filter, or null when none was asked for — which reads
     * every row.
     *
     * <p>Every refusal here is a coded 400 rather than a decode failure, because every mistake this
     * catches is one a caller can only make by sending something: an operator outside the vocabulary, a
     * combination nested inside a combination, or a document in the store's own query language sent as
     * though this face still forwarded one. The last is the one worth naming — it used to work.
     */
    static DataBrowserCriteria criteria(DataBrowserFindRequest.Filter filter) {
        if (filter == null) {
            return null;
        }
        boolean isTerm = filter.field() != null || filter.op() != null || filter.value() != null;
        boolean isCombination = filter.all() != null || filter.any() != null;
        if (isTerm == isCombination) {
            // Both, or neither. Neither is the shape a backend query document arrives in, since none of
            // its keys are ours; both is two requests in one body with no reading that is not a guess.
            throw MalformedRequest.rejecting("a `filter` is either one term (`field`, `op`, `value`) "
                    + "or one combination (`all` or `any`) of them", null);
        }
        if (isTerm) {
            return term(filter);
        }
        if (filter.all() != null && filter.any() != null) {
            throw MalformedRequest.rejecting("a `filter` combines with `all` or with `any`, not both", null);
        }
        if (filter.any() != null) {
            // An alternative holds terms. A choice between groups of conditions has no term in the
            // vocabulary, so it is refused here rather than flattened into something narrower.
            List<DataBrowserCriteria.Match> terms = new ArrayList<>(filter.any().size());
            filter.any().forEach(written -> terms.add(term(written)));
            return new DataBrowserCriteria.Any(terms);
        }
        // A conjunction holds terms and alternatives alike: several conditions with one of them a choice
        // is the shape a reader writes most, and it is the one nesting the vocabulary allows.
        List<DataBrowserCriteria.Conjunct> members = new ArrayList<>(filter.all().size());
        filter.all().forEach(written -> members.add(conjunct(written)));
        return new DataBrowserCriteria.All(members);
    }

    /** One member of a conjunction: a term, or one alternative between terms. */
    private static DataBrowserCriteria.Conjunct conjunct(DataBrowserFindRequest.Filter filter) {
        if (filter.any() == null) {
            return term(filter);
        }
        if (filter.all() != null) {
            throw MalformedRequest.rejecting("a `filter` combines with `all` or with `any`, not both", null);
        }
        List<DataBrowserCriteria.Match> terms = new ArrayList<>(filter.any().size());
        filter.any().forEach(written -> terms.add(term(written)));
        return new DataBrowserCriteria.Any(terms);
    }

    /** One term of the vocabulary, refusing anything that is not one. */
    private static DataBrowserCriteria.Match term(DataBrowserFindRequest.Filter filter) {
        if (filter.all() != null || filter.any() != null) {
            // The nesting bound. It holds by shape everywhere above this line — a conjunction may hold an
            // alternative, an alternative holds terms, and there is no fourth level to build. Here, where
            // a caller can still write a deeper one down, it holds by refusal.
            throw MalformedRequest.rejecting(
                    "a `filter` alternative holds terms, not further combinations", null);
        }
        MalformedRequest.requireText(filter.field(), "a filter term needs the `field` to test");
        MalformedRequest.requireText(filter.op(), "a filter term needs an `op`");
        MalformedRequest.require(filter.value(), "a filter term needs a `value` to test against");
        return new DataBrowserCriteria.Match(filter.field(), operator(filter.op()), filter.value());
    }

    /** The vocabulary's operator for a spelling, or a refusal naming the word and the whole vocabulary. */
    private static DataBrowserCriteria.Operator operator(String spelling) {
        for (DataBrowserCriteria.Operator candidate : DataBrowserCriteria.Operator.values()) {
            if (candidate.name().equalsIgnoreCase(spelling)) {
                return candidate;
            }
        }
        throw MalformedRequest.rejecting("`" + spelling + "` is not a filter operator; the ones there are: "
                + Arrays.stream(DataBrowserCriteria.Operator.values())
                        .map(operator -> operator.name().toLowerCase(Locale.ROOT))
                        .collect(joining(", ")), null);
    }

    /**
     * The control-ring order for a requested one, or null when none was asked for — which leaves the order
     * to the database rather than imposing a default.
     *
     * <p>A direction that is neither {@code asc} nor {@code desc} is refused here as a coded 400: it is
     * client-attributable input, and the alternative — quietly reading it as one of the two — would return
     * rows in an order the caller did not ask for and cannot tell apart from the one they did.
     */
    private static DataBrowserSortOrder order(DataBrowserFindRequest.Sort sort) {
        if (sort == null) {
            return null;
        }
        MalformedRequest.requireText(sort.field(), "a `sort` needs the `field` to order by");
        MalformedRequest.requireText(sort.dir(), "a `sort` needs a `dir` of `asc` or `desc`");
        return new DataBrowserSortOrder(sort.field(), switch (sort.dir().toLowerCase(Locale.ROOT)) {
            case "asc" -> DataBrowserSortOrder.Direction.ASC;
            case "desc" -> DataBrowserSortOrder.Direction.DESC;
            default -> throw MalformedRequest.rejecting(
                    "a `sort` direction must be `asc` or `desc`, not `" + sort.dir() + "`", null);
        });
    }
}
