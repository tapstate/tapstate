package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.spi.store.ValidationTaskStore;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationSelectedDeclarationsTest {
    @Test void physicalBindingsAreDerivedFromTheSelectedSnapshotBeforeAnyTaskOrModelExists(){
        var inputs=inputs("serve: { from: [input.orders, input.items], sync: [{ id: out, source: target, rename: { prefix: checked_ } }] }",false);
        var selected=ValidationSelectedDeclarations.inspect(inputs,List.of(choice("orders"),choice("items")));
        assertThat(selected.original()).isSameAs(inputs);
        assertThat(selected.mappings()).extracting(value->value.mapping().source().table()).containsExactly("orders","items");
        assertThat(selected.mappings()).extracting(value->value.mapping().target().table()).containsExactly("checked_orders","checked_items");
        assertThat(selected.mappings()).allSatisfy(value->{
            assertThat(value.mapping().source().database()).isEqualTo("source_db");assertThat(value.mapping().source().schema()).isEqualTo("source_schema");
            assertThat(value.mapping().target().sourceId()).isEqualTo("target");assertThat(value.mapping().target().database()).isEqualTo("target_db");
            assertThat(value.mapping().keys()).isNull();assertThat(value.mapping().source().modelVersion()).isNull();assertThat(value.mapping().target().codecBinding()).isNull();
        });
        assertThat(selected.toString()).doesNotContain("password","source-secret","target-secret");
        assertThat(selected.mappings().toString()).doesNotContain("password","source-secret","target-secret");
        assertThatThrownBy(()->selected.artifacts().saveAll(List.of())).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(()->selected.mappings().clear()).isInstanceOf(UnsupportedOperationException.class);
    }
    @Test void candidatesRejectTransformsAndUnselectedFanInBeforeAnyIndependentMetadataCall(){
        var filtered=inputs("transforms: [{ id: filtered, from: input.orders, type: filter, expr: 'true' }]\nserve: { from: filtered, sync: [{ id: out, source: target }] }",false);
        assertThatThrownBy(()->ValidationSelectedDeclarations.inspect(filtered,List.of(choice("orders"))))
                .isInstanceOfSatisfying(TapstateException.class,failure->{assertThat(failure.code()).isEqualTo(ValidationError.RULE_UNSUPPORTED);assertThat(failure.args()).containsEntry("node","filtered");});
        var fanIn=inputs("serve: { from: [input.orders, input.items], sync: [{ id: out, source: target, rename: { map: { orders: joined, items: joined } } }] }",false);
        assertThatThrownBy(()->ValidationSelectedDeclarations.inspect(fanIn,List.of(choice("orders"))))
                .isInstanceOf(TapstateException.class).hasMessageContaining("target-fan-in");
    }
    @Test void selectedOutputCannotAuthorizeAnotherTableDeclaredOnTheSameSource(){
        var inputs=inputs("serve: { from: input.orders, sync: [{ id: out, source: target }] }",false);
        assertThatThrownBy(()->ValidationSelectedDeclarations.inspect(inputs,List.of(choice("items"))))
                .isInstanceOf(TapstateException.class).hasMessageContaining("sourceTableScope");
    }
    @Test void choicesCannotInjectAnotherOutputDestinationOrRepeatedMappingIdentity(){
        var inputs=inputs("serve: { from: input.orders, sync: [{ id: out, source: target }] }",false);
        assertThatThrownBy(()->ValidationSelectedDeclarations.inspect(inputs,List.of(new ValidationSelectedDeclarations.Choice("mapping","foreign","input","orders"))))
                .isInstanceOf(TapstateException.class).hasMessageContaining("outputSelection");
        assertThatThrownBy(()->ValidationSelectedDeclarations.inspect(inputs,List.of(new ValidationSelectedDeclarations.Choice("mapping","out","target","orders"))))
                .isInstanceOf(TapstateException.class).hasMessageContaining("sourceScope");
        assertThatThrownBy(()->ValidationSelectedDeclarations.inspect(inputs,List.of(choice("orders"),choice("orders"))))
                .isInstanceOf(TapstateException.class).hasMessageContaining("duplicateMapping");
    }
    @Test void selectedBodiesAndExactDependencyClosureAreValidatedIndependentlyOfTheirInternalDescriptor(){
        var inputs=inputs("serve: { from: input.orders, sync: [{ id: out, source: target }] }",false);
        var changed=new HashMap<>(inputs.canonicalResources());changed.put("input",changed.get("input").replace("source-secret","changed-secret"));
        var forged=new ValidationTaskStore.DeclarationInputs(inputs.selection(),inputs.configurationRevision(),inputs.dependencyHashes(),changed,inputs.parentGeneration());
        assertThatThrownBy(()->ValidationSelectedDeclarations.inspect(forged,List.of(choice("orders"))))
                .isInstanceOf(TapstateException.class).hasMessageContaining("validationInputs");
        assertThatThrownBy(()->ValidationSelectedDeclarations.inspect(inputs("serve: { from: input.orders, sync: [{ id: out, source: target }] }",true),List.of(choice("orders"))))
                .isInstanceOf(TapstateException.class).hasMessageContaining("dependencyClosure");
    }
    private static ValidationSelectedDeclarations.Choice choice(String table){return new ValidationSelectedDeclarations.Choice("mapping_"+table,"out","input",table);}
    private static ValidationTaskStore.DeclarationInputs inputs(String body,boolean extra){
        var input=new SourceResource("input",null,"fixture",Map.of("database","source_db","schema","source_schema","password","source-secret"),
                SourceMode.CDC,List.of(TableRef.literal("orders"),TableRef.literal("items")),null,null);
        var target=new SourceResource("target",null,"fixture",Map.of("database","target_db","password","target-secret"),null,null,null,null);
        var resources=new ArrayList<Resource>(List.of(input,target,new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: flow\nsource: [input]\n"+body+"\n")));
        if(extra)resources.add(new SourceResource("unrelated",null,"fixture",Map.of("password","unrelated-secret"),null,null,null,null));
        var hashes=new TreeMap<String,String>();var canonical=new TreeMap<String,String>();
        for(var resource:resources){hashes.put(resource.id(),CanonicalHash.of(resource));canonical.put(resource.id(),new CanonicalWriter().write(resource));}
        return new ValidationTaskStore.DeclarationInputs(new ValidationTaskStore.Selection("flow",List.of("out")),5,hashes,canonical,UUID.randomUUID());
    }
}
