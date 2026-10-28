package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationDirectMappingsTest {
    @Test void multipleDirectTablesUseTheSavedRenameRulesAndIndependentPhysicalMetadata() {
        var saved=saved("serve: { from: [input.orders, input.items], sync: [{ id: out, source: target, rename: { prefix: checked_ } }] }",
                List.of("orders","items"),List.of("checked_orders","checked_items"),false);
        var compiled=ValidationDirectMappings.compile(saved,ValidationDirectMappingsTest::metadata);
        assertThat(compiled).extracting(m->m.original().source().table()).containsExactly("orders","items");
        assertThat(compiled).extracting(m->m.targetModel().name()).containsExactly("checked_orders","checked_items");
        assertThat(compiled.toString()).doesNotContain("secret","password");
        assertThat(compiled.getFirst().source().config()).containsEntry("password","source-secret");
    }
    @Test void transformationsAreRefusedBeforeAnyIndependentReadIncludingCount() {
        var saved=saved("""
                transforms:
                  - { id: filtered, from: input.orders, type: filter, expr: 'true' }
                serve: { from: filtered, sync: [{ id: out, source: target }] }
                """,List.of("orders"),List.of("orders"),false);
        var reads=new AtomicInteger();
        assertThatThrownBy(()->ValidationDirectMappings.compile(saved,table->{reads.incrementAndGet();return metadata(table);}))
                .isInstanceOfSatisfying(TapstateException.class,failed->{
                    assertThat(failed.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED);
                    assertThat(failed.args()).containsEntry("node","filtered");
                });
        assertThat(reads).hasValue(0);
    }
    @Test void everySelectedMappingIsPreflightedBeforeTheFirstMetadataRead() {
        var saved=saved("""
                view: { id: view_out, from: input.orders, primary_key: id }
                serve: { from: input.orders, sync: [{ id: out, source: target }] }
                """,List.of("orders"),List.of("orders"),false,true);
        var reads=new AtomicInteger();
        assertThatThrownBy(()->ValidationDirectMappings.compile(saved,table->{reads.incrementAndGet();return metadata(table);}))
                .isInstanceOfSatisfying(TapstateException.class,failed->assertThat(failed.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED));
        assertThat(reads).hasValue(0);
    }
    @Test void forgedPhysicalDestinationCannotBypassTheSavedRenameBeforeReadingIt() {
        var saved=saved("serve: { from: input.orders, sync: [{ id: out, source: target, rename: { prefix: checked_ } }] }",
                List.of("orders"),List.of("unrelated"),false);
        var reads=new AtomicInteger();
        assertThatThrownBy(()->ValidationDirectMappings.compile(saved,table->{reads.incrementAndGet();return metadata(table);}))
                .isInstanceOf(TapstateException.class).hasMessageContaining("targetTableScope");
        assertThat(reads).hasValue(0);
    }
    @Test void anotherBranchCannotWidenTheSelectedOutputToAnUnrelatedDeclaredTable() {
        var saved=saved("""
                transforms:
                  - { id: other, from: input.items, type: filter, expr: 'true' }
                serve: { from: input.orders, sync: [{ id: out, source: target }] }
                """,List.of("items"),List.of("items"),false);
        assertThatThrownBy(()->ValidationDirectMappings.compile(saved,ValidationDirectMappingsTest::metadata))
                .isInstanceOf(TapstateException.class).hasMessageContaining("sourceTableScope");
    }
    @Test void dynamicTableSelectorsAreCheckedAgainstTheActualObservedTableName() {
        var saved=saved("serve: { from: input, sync: [{ id: out, source: target }] }",
                List.of("orders"),List.of("orders"),true);
        assertThat(ValidationDirectMappings.compile(saved,ValidationDirectMappingsTest::metadata)).hasSize(1);
        var excluded=saved("serve: { from: input, sync: [{ id: out, source: target }] }",
                List.of("items"),List.of("items"),true);
        assertThatThrownBy(()->ValidationDirectMappings.compile(excluded,ValidationDirectMappingsTest::metadata))
                .isInstanceOf(TapstateException.class).hasMessageContaining("sourceTableScope");
    }
    @Test void aReturnedModelFromAnotherTableCannotSupplyTheSavedMapping() {
        var saved=saved("serve: { from: input.orders, sync: [{ id: out, source: target }] }",
                List.of("orders"),List.of("orders"),false);
        assertThatThrownBy(()->ValidationDirectMappings.compile(saved,table->new SourceTable("other",List.of(),List.of(),List.of())))
                .isInstanceOf(TapstateException.class).hasMessageContaining("modelTableScope");
    }
    private static SourceTable metadata(ValidationTaskSpec.Table table) {
        return new SourceTable(table.table(),List.of(new SourceField("id","integer")),List.of("id"),List.of());
    }
    private static ValidationSavedDeclarations saved(String body,List<String> sourceTables,List<String> targetTables,boolean regex) {
        return saved(body,sourceTables,targetTables,regex,false);
    }
    private static ValidationSavedDeclarations saved(String body,List<String> sourceTables,List<String> targetTables,boolean regex,boolean view) {
        var input=new SourceResource("input",null,"fixture",Map.of("database","source","password","source-secret"),SourceMode.CDC,
                regex?List.of(TableRef.regex("ord.*")):List.of(TableRef.literal("orders"),TableRef.literal("items")),null,null);
        var target=new SourceResource("target",null,"fixture",Map.of("database","target","password","target-secret"),null,null,null,null);
        var pipeline=new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: flow\nsource: [input]\n"+body+"\n");
        var resources=List.of(input,target,pipeline);var hashes=new TreeMap<String,String>();var canonical=new TreeMap<String,String>();
        resources.forEach(resource->{hashes.put(resource.id(),CanonicalHash.of(resource));canonical.put(resource.id(),new CanonicalWriter().write(resource));});
        var mappings=new ArrayList<ValidationTaskSpec.Mapping>();
        for(int index=0;index<sourceTables.size();index++)mappings.add(new ValidationTaskSpec.Mapping("mapping"+index,ValidationTaskSpec.OutputKind.SYNC,"out",
                new ValidationTaskSpec.Table("input","input","source",null,sourceTables.get(index),null,null),
                new ValidationTaskSpec.Table("out","target","target",null,targetTables.get(index),null,null),null));
        if(view)mappings.add(new ValidationTaskSpec.Mapping("view-mapping",ValidationTaskSpec.OutputKind.VIEW,"view_out",
                new ValidationTaskSpec.Table("input","input","source",null,"orders",null,null),
                new ValidationTaskSpec.Table("view_out","target","target",null,"view_out",null,null),null));
        var comparison=new ValidationTaskSpec("flow",hashes,1,"rules",mappings);
        var task=new ValidationReportAdmission.Task("cluster","flow","task",UUID.randomUUID());var command=UUID.randomUUID();
        var snapshot=new ValidationTaskStore.Snapshot(task,1,command,comparison,ValidationTaskStore.Settings.defaults(ValidationMethod.COUNT,0),
                1,command,false,new ValidationTaskStore.Mode(1,null),new ValidationTaskStore.Mode(1,null));
        return ValidationSavedDeclarations.inspect(new ValidationTaskStore.Inputs(snapshot,5,canonical),"target");
    }
}
