package io.tapstate.e2e;

import io.tapstate.core.dsl.DslException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.dsl.Interpolator;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * Binds a specification to a running product over its HTTP surface.
 *
 * <p>There is one of these, not one per tier: everything that differs between tiers is how the
 * server came to be listening, which is {@link ServerHandle}'s business and settled before this
 * class sees a URL. A second binding per tier would be two chances to drift, and drift is precisely
 * what "the same specification runs on both tiers" is a promise against.
 *
 * <p>Endpoints are learned by applying them. That mirrors the product's own rule - a source must be
 * applied before a pipeline can reference it by id - so the harness cannot address an endpoint the
 * product does not have either.
 */
final class HttpTierBinding implements TierBinding {

    private final ControlPlane control;
    private final Path workspace;
    private final DslParser parser = new DslParser();
    private final Map<String, SourceResource> sourcesById = new LinkedHashMap<>();

    /**
     * The driver per connector id. Which store an endpoint lives in is decided by the connector the
     * resource names, so that is what chooses the reader — the harness reaches a Mongo collection with a
     * Mongo driver and a directory of files with a file reader, and neither is the product. A run
     * supplies drivers for the connectors its specification names and nothing else, so a specification
     * that reaches for an endpoint this run cannot read independently says so rather than reading it
     * through the product.
     */
    private final Map<String, Endpoints> endpointsByConnector;

    /**
     * What a specification's {@code ${...}} references resolve to. The harness is the client here, and
     * the client is the side that interpolates, so the values are the harness's own to supply — which is
     * what lets a checked-in resource name an endpoint whose address is only known once a container is
     * up. It also works identically on both tiers: an in-process server shares this JVM, whose real
     * environment no test could set anyway.
     */
    private final UnaryOperator<String> env;

    /**
     * How this run holds one store's traffic, and how it dials a store that has a hold in front of it.
     *
     * <p>Both belong to whoever brought the stores up, which is why they arrive as seams rather than
     * being decided here: a run that provisioned nothing has no gate to hold and no address to undo,
     * and it says so instead of quietly doing neither.
     */
    private final StreamHolds streamHolds;

    private final UnaryOperator<EndpointAddress> asDialledByTheHarness;

    HttpTierBinding(
            ControlPlane control,
            Path workspace,
            Map<String, Endpoints> endpointsByConnector,
            UnaryOperator<String> env) {
        this(control, workspace, endpointsByConnector, env, StreamHolds.none(), UnaryOperator.identity());
    }

    HttpTierBinding(
            ControlPlane control,
            Path workspace,
            Map<String, Endpoints> endpointsByConnector,
            UnaryOperator<String> env,
            StreamHolds streamHolds,
            UnaryOperator<EndpointAddress> asDialledByTheHarness) {
        this.control = control;
        this.workspace = workspace;
        this.endpointsByConnector = Map.copyOf(endpointsByConnector);
        this.env = env;
        this.streamHolds = streamHolds;
        this.asDialledByTheHarness = asDialledByTheHarness;
    }

    @Override
    public void registerConnector(String connectorId) {
        control.registerConnector(connectorId, ConnectorJars.bytesFor(connectorId));
    }

    /**
     * Sends every listed resource in one apply, because the product resolves references within the
     * submitted set: a pipeline and the source it names must arrive together or the reference points
     * at nothing.
     */
    @Override
    public void applyResources(List<String> resourceFiles) {
        Map<String, String> yamlByFile = new LinkedHashMap<>();
        for (String resourceFile : resourceFiles) {
            String yaml = read(resourceFile);
            rememberEndpoint(resourceFile, yaml);
            yamlByFile.put(resourceFile, yaml);
        }
        control.apply(yamlByFile);
    }

    @Override
    public void readResources(List<String> resourceFiles) {
        for (String resourceFile : resourceFiles) {
            rememberEndpoint(resourceFile, read(resourceFile));
        }
    }

    /** The connector and settings come from the declared source, which is the only place they are stated. */
    @Override
    public void discoverSchema(String resourceId) {
        SourceResource source = requireSource(resourceId);
        control.discoverSchema(resourceId, source.connector(), source.config());
    }

    @Override
    public void seed(TableAlias table, List<Map<String, Object>> rows) {
        Endpoint endpoint = endpoint(table);
        endpoint.driver().seed(endpoint.address(), table.table(), rows);
    }

    @Override
    public Optional<Map<String, Object>> fetch(TableAlias table, Map<String, Object> where) {
        Endpoint endpoint = endpoint(table);
        return endpoint.driver().fetch(endpoint.address(), table.table(), where);
    }

    @Override
    public void drive(String pipelineId, LifecycleVerb verb) {
        if (verb == LifecycleVerb.STOP) {
            // The declarative word "stop" is the product's stop, and the product's stop clears. A step
            // that means to stop and keep what the pipeline has is a different word, not this one with
            // a quietly different answer behind it.
            control.stop(pipelineId, true);
            return;
        }
        control.lifecycle(pipelineId, verb);
    }

    @Override
    public void cdc(TableAlias table, CdcOp op, long rows) {
        Endpoint endpoint = endpoint(table);
        endpoint.driver().cdc(endpoint.address(), table.table(), op, rows);
    }

    @Override
    public void update(TableAlias table, Map<String, Object> where, Map<String, Object> set) {
        Endpoint endpoint = endpoint(table);
        endpoint.driver().update(endpoint.address(), table.table(), where, set);
    }

    @Override
    public void delete(TableAlias table, Map<String, Object> where) {
        Endpoint endpoint = endpoint(table);
        endpoint.driver().delete(endpoint.address(), table.table(), where);
    }

    @Override
    public void insert(TableAlias table, List<Map<String, Object>> rows) {
        Endpoint endpoint = endpoint(table);
        endpoint.driver().insert(endpoint.address(), table.table(), rows);
    }

    @Override
    public void redeliver(TableAlias table) {
        Endpoint endpoint = endpoint(table);
        endpoint.driver().redeliver(endpoint.address(), table.table());
    }

    /**
     * The address a table's resource carries, as the harness resolved it. This is the bridge a guard
     * outside this class needs: which provisioned store a resource actually landed on can only be told
     * from the address, and the address lives here.
     */
    EndpointAddress addressOf(TableAlias table) {
        return endpoint(table).address();
    }

    /**
     * Holds or releases one source's traffic. Nothing is sent to the product: the source is named, its
     * declared address is looked up, and whoever brought that store up puts the hold on.
     */
    @Override
    public void driveStream(String sourceId, StreamVerb verb) {
        SourceResource source = requireSource(sourceId);
        streamHolds.drive(new EndpointAddress(sourceId, source.config()), verb);
    }

    @Override
    public long count(TableAlias table) {
        Endpoint endpoint = endpoint(table);
        return endpoint.driver().count(endpoint.address(), table.table());
    }

    @Override
    public Optional<PipelineState> state(String pipelineId) {
        return control.state(pipelineId);
    }

    @Override
    public Optional<Long> errorCount(String pipelineId) {
        return control.errorCount(pipelineId);
    }

    @Override
    public Optional<Long> deadLettered(String pipelineId) {
        return control.deadLettered(pipelineId);
    }

    @Override
    public Optional<String> failureCode(String pipelineId) {
        return control.failureCode(pipelineId);
    }

    /**
     * A source carries its own connection settings, so applying one teaches the harness where that
     * endpoint lives. Resources that are not endpoints teach it nothing, which is correct.
     */
    private void rememberEndpoint(String resourceFile, String yaml) {
        Resource resource = parse(resourceFile, yaml);
        if (resource instanceof SourceResource source) {
            sourcesById.put(source.id(), source);
        }
    }

    private SourceResource requireSource(String resourceId) {
        SourceResource source = sourcesById.get(resourceId);
        if (source == null) {
            throw new EnvelopeException(
                    "no source declared for " + resourceId + "; list its resource under setup.apply");
        }
        return source;
    }

    /**
     * Where a table lives and what reads it, both taken from the resource that declares it: the
     * connector chooses the driver, and the settings the resource carries are the address handed to it.
     * Resolving both from the one applied resource is what keeps the address the harness dials identical
     * to the one the product was given.
     *
     * <p>Which of those settings is the address is deliberately not decided here. This method is the one
     * piece of the harness every store shares, and a store answering on a host and a port rather than a
     * uri is not an exception to be special-cased but the ordinary case for anything reached over JDBC.
     * The whole mapping goes to the driver, which is the only party that knows its own store.
     */
    private Endpoint endpoint(TableAlias table) {
        SourceResource source = requireSource(table.resourceId());
        Endpoints driver = endpointsByConnector.get(source.connector());
        if (driver == null) {
            throw new EnvelopeException(
                    table + " is reached with the " + source.connector()
                            + " connector, which this run has no independent driver for; it knows "
                            + endpointsByConnector.keySet());
        }
        // Behind whatever gate publishes it: the harness has to reach a store whose stream is held,
        // because writing rows into a held source is the whole point of holding one. Identity for a
        // store with no gate, which is every store on a run that provisions none.
        return new Endpoint(
                driver,
                asDialledByTheHarness.apply(new EndpointAddress(table.resourceId(), source.config())));
    }

    /** One addressable endpoint: the driver that reads it and the address it answers on. */
    private record Endpoint(Endpoints driver, EndpointAddress address) {}

    /**
     * Reads a resource and resolves its references, exactly as the CLI does before an apply — so what the
     * product is handed here is what an author's own apply would hand it.
     *
     * <p>The resolved text is then used for both halves of the harness's job: it goes to the product, and
     * it is what {@link #rememberEndpoint} reads the endpoint address out of. One substitution feeds both,
     * so the address the harness dials cannot drift from the one the product was given.
     */
    private String read(String resourceFile) {
        String yaml;
        try {
            yaml = Files.readString(workspace.resolve(resourceFile));
        } catch (IOException e) {
            throw new EnvelopeException("cannot read the resource referenced as " + resourceFile, e);
        }
        try {
            return Interpolator.interpolate(yaml, env);
        } catch (DslException e) {
            throw new EnvelopeException(resourceFile + " has a reference that does not resolve: "
                    + e.getMessage(), e);
        }
    }

    /** Only a DSL error is the author's; anything else the parser throws is the parser's own defect. */
    private Resource parse(String resourceFile, String yaml) {
        try {
            return parser.parse(yaml);
        } catch (DslException e) {
            throw new EnvelopeException(resourceFile + " does not parse: " + e.getMessage(), e);
        }
    }
}
