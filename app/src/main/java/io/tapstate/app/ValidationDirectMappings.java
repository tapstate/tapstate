package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.TableRename;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationTaskSpec;
import io.tapstate.spi.store.SourceTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Resolves fixed direct mappings against independently obtained table metadata. This compiler does
 * not certify native ordering, codec/model versions, driver budgets or run authority: the actual
 * reader factory must establish those before opening each query. COUNT may supply name-only table
 * metadata from table enumeration; no business value model or production state is required here.
 */
final class ValidationDirectMappings {
    private ValidationDirectMappings() {}

    record Mapping(ValidationTaskSpec.Mapping original, SourceResource source, SourceResource target,
                   SourceTable sourceModel, SourceTable targetModel) {
        @Override public String toString() { return "DirectValidationMapping[id=" + original.mappingId() + "]"; }
    }

    record Declared(ValidationTaskSpec.Mapping mapping, SourceResource source, SourceResource target,
            io.tapstate.core.model.SyncElement sync, io.tapstate.core.model.FromClause from) {
        @Override public String toString(){return "DirectValidationDeclaration[id="+mapping.mappingId()+"]";}
    }

    static List<Mapping> compile(ValidationSavedDeclarations saved,
            Function<ValidationTaskSpec.Table, SourceTable> independentMetadata) {
        Objects.requireNonNull(saved); Objects.requireNonNull(independentMetadata);
        var declarations=declare(saved);
        var compiled = new ArrayList<Mapping>();
        for (var declaration : declarations) {
            var mapping = declaration.mapping();
            var sourceModel = model(mapping.source(), independentMetadata);
            var targetModel = model(mapping.target(), independentMetadata);
            requireSourceTable(declaration,sourceModel);
            compiled.add(new Mapping(mapping, declaration.source(), declaration.target(), sourceModel, targetModel));
        }
        return List.copyOf(compiled);
    }

    /** Static direct declarations only. Native table existence and selection are still read obligations. */
    static List<Declared> declare(ValidationSavedDeclarations saved) {
        Objects.requireNonNull(saved);
        return declare(saved.artifacts(),saved.original().task().comparison().mappings(),mapping->saved.mapping(mapping.mappingId()));
    }

    /** Server-derived candidates and saved definitions share the same direct-path restrictions. */
    static List<Declared> declare(io.tapstate.spi.store.ArtifactStore artifacts,List<ValidationTaskSpec.Mapping> mappings,
            Function<ValidationTaskSpec.Mapping,io.tapstate.core.dsl.ValidationDeclarations.Inspection> inspections){
        Objects.requireNonNull(artifacts);Objects.requireNonNull(mappings);Objects.requireNonNull(inspections);
        var declarations = new ArrayList<Declared>();
        var targets=new java.util.HashSet<List<Object>>();
        var resources=new java.util.HashMap<String,SourceResource>();
        for (var mapping : mappings) {
            var selected = Objects.requireNonNull(inspections.apply(mapping));
            if (mapping.outputKind() != ValidationTaskSpec.OutputKind.SYNC || selected.sync() == null
                    || selected.view() != null) throw unsupported(mapping.outputId(), "from", "direct.source-to-destination-required");
            if (!selected.steps().isEmpty()) {
                var step = selected.steps().values().stream().sorted(java.util.Comparator.comparing(value -> value.id())).findFirst().orElseThrow();
                throw unsupported(step.id(), "type", "direct.transform-" + step.body().type());
            }
            // A selected pending reference with downstream operators must not bypass this gate.
            for (var pending : selected.pending()) if (!pending.downstreamSteps().isEmpty())
                throw unsupported(pending.downstreamSteps().getFirst().id(), "type", "direct.transform-required");
            if (!selected.pending().isEmpty())
                throw unsupported(mapping.outputId(), "from", "direct.table-scope-unknown");
            // These are private, read-only declarations. Native reads decode their own settings.
            var source = resources.computeIfAbsent(mapping.source().sourceId(),id->StoredArtifacts.requireSource(artifacts,id));
            var target = resources.computeIfAbsent(mapping.target().sourceId(),id->StoredArtifacts.requireSource(artifacts,id));
            if (!mapping.source().nodeId().equals(source.id()) || !mapping.target().nodeId().equals(mapping.outputId()))
                throw invalid("nodeBinding");
            if (!TableRename.apply(mapping.source().table(), selected.sync().rename()).equals(mapping.target().table()))
                throw invalid("targetTableScope");
            // The output scope includes routes omitted from the user's comparison selection.
            for (var route : selected.routes()) {
                if (route.table() == null) {
                    // The native reader audits this source's complete namespace. Another source
                    // needs its own namespace admission before this output can be called one-to-one.
                    if (!route.source().equals(source.id()))
                        throw unsupported(mapping.outputId(), "from", "direct.table-scope-unknown");
                    continue;
                }
                if (route.source().equals(source.id()) && route.table().equals(mapping.source().table())) continue;
                if (TableRename.apply(route.table(), selected.sync().rename()).equals(mapping.target().table()))
                    throw unsupported(mapping.outputId(), "table", "direct.target-fan-in");
            }
            var targetTable=mapping.target();
            var database=targetTable.database()!=null?targetTable.database():target.config().get("database");
            var schema=targetTable.schema()!=null?targetTable.schema():target.config().get("schema");
            if(!targets.add(java.util.Arrays.asList(target.id(),database,schema,targetTable.table())))
                throw unsupported(mapping.outputId(),"table","direct.target-fan-in");
            declarations.add(new Declared(mapping, source, target, selected.sync(), selected.from()));
        }
        return List.copyOf(declarations);
    }

    static void requireSourceTable(Declared declaration,SourceTable actual) {
        if(!actual.name().equals(declaration.mapping().source().table())
                ||!PipelineTableSelection.selects(declaration.from(),declaration.source(),actual))throw invalid("sourceTableScope");
    }

    /** Other observed namespaces may reveal a rename collision even when the task selects one table. */
    static void requireObservedSourceTable(Declared declaration,String nativeName){
        var actual=new SourceTable(nativeName,List.of(),List.of(),List.of());
        if(nativeName.equals(declaration.mapping().source().table())){requireSourceTable(declaration,actual);return;}
        if(PipelineTableSelection.selects(declaration.from(),declaration.source(),actual)
                &&TableRename.apply(nativeName,declaration.sync().rename()).equals(declaration.mapping().target().table()))
            throw unsupported(declaration.mapping().outputId(),"table","direct.target-fan-in");
    }

    private static SourceTable model(ValidationTaskSpec.Table table, Function<ValidationTaskSpec.Table, SourceTable> metadata) {
        var model = Objects.requireNonNull(metadata.apply(table), "independent table metadata");
        if (!model.name().equals(table.table())) throw invalid("modelTableScope");
        return model;
    }
    private static TapstateException unsupported(String node, String field, String rule) {
        return new TapstateException(ValidationError.RULE_UNSUPPORTED, Map.of("node", node, "field", field, "rule", rule), null);
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "direct." + field), null);
    }
}
