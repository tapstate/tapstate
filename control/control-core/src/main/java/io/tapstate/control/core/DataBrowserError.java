package io.tapstate.control.core;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.DataBrowserErrorDefinitions;
import io.tapstate.core.common.TapstateErrorCode;

import java.util.Set;

/** Stable coded failures for the read face over a declared source's own database. */
public enum DataBrowserError implements TapstateErrorCode {

    /** The collection asked for is not one the source's own database holds. */
    UNKNOWN_COLLECTION(DataBrowserErrorDefinitions.UNKNOWN_COLLECTION),

    /**
     * The read asked for a number of rows this face will not serve — more than the cap, or none at
     * all. {@code limit} is what was asked for; {@code max} is the cap. The cap is what keeps a
     * preview a preview: this face reads whole rows into memory in one shot and hands them to a
     * terminal, so an unbounded request is a way to pull a collection through it. Below one there is
     * no read to serve either — the answer would be no rows and "there is more", which reads as an
     * empty collection with a contradiction attached.
     */
    INVALID_LIMIT(DataBrowserErrorDefinitions.INVALID_LIMIT),

    /**
     * The order asked for names a field whose own name holds a dot, which no order can reach.
     * {@code field} is the spelling the reader wrote. A filter may name such a field, because a query
     * can carry an expression and an expression can name a field rather than path to it; a sort key is
     * a path and has no second form. Served anyway it would order by a path resolving nowhere — every
     * row equal, rows back in no particular order, nothing reported.
     */
    UNORDERABLE_FIELD(DataBrowserErrorDefinitions.UNORDERABLE_FIELD),

    /**
     * The source's connector cannot be asked for rows in the shape this face asks in. {@code connector}
     * is the one declared; {@code browsable} names the ones that can.
     *
     * <p>Asked before anything is sent, and that is the whole point of it. Having the command a request
     * travels on says nothing about whether the request can be understood: most connectors register it
     * and read it as SQL, so one carrying a document-shaped query reaches a driver that finds no
     * statement in it and fails somewhere inside itself. What came back then was a server failure
     * quoting a driver — accurate about the symptom, silent about the cause, and blaming the product
     * for a source the product simply does not browse.
     *
     * <p>Listing and sizing are left alone, deliberately. Those two ask nothing shaped: they are the
     * connector's own table names and table info, and they answer correctly for every connector here.
     * Refusing them too would hide a source a reader can perfectly well look at the outline of.
     */
    CONNECTOR_NOT_BROWSABLE(DataBrowserErrorDefinitions.CONNECTOR_NOT_BROWSABLE),

    /**
     * The source being followed was deleted, so the follow was stopped and its connection closed. The
     * close is the whole point: a stopped stream on an open connection is a collection nothing is
     * happening to, and a reader cannot tell those apart.
     */
    SOURCE_DELETED(DataBrowserErrorDefinitions.SOURCE_DELETED),

    /**
     * The follow's stream failed, so it was stopped and its connection closed. Carries no detail of the
     * failure on purpose - it reaches a reader who asked to watch a collection, not the operator of the
     * connector that broke; the failure itself is logged where the connector's own words survive.
     */
    FOLLOW_STOPPED(DataBrowserErrorDefinitions.FOLLOW_STOPPED),

    /**
     * The follow was reclaimed after a long stretch with nothing to show. A follow holds a connector
     * instance for as long as it is open and the host's ceiling counts it, so one left running by
     * somebody who walked away is a place nobody else can have. A reader who is still there asks
     * again; nothing is lost but the connection.
     */
    FOLLOW_IDLE(DataBrowserErrorDefinitions.FOLLOW_IDLE);

    private final ProductErrorDefinition definition;

    DataBrowserError(ProductErrorDefinition definition) { this.definition = definition; }

    @Override public String code() { return definition.code(); }
    @Override public Severity severity() { return definition.severity(); }
    @Override public Set<String> placeholders() { return definition.parameters(); }
}
