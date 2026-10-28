package io.tapstate.adapters.pdk;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class PdkValidationModelVersionTest {
    private static TapTable table(){
        var fields=new LinkedHashMap<String,TapField>();
        fields.put("id",new TapField("id","BIGINT").pos(1).primaryKeyPos(1).tapType(new TapNumber().bit(64).fixed(true).precision(19).scale(0)));
        fields.put("value",new TapField("value","VARCHAR(64)").pos(2).tapType(new TapString().bytes(64L)));
        var model=new TapTable("records");model.setNameFieldMap(fields);return model;
    }
    @Test void equivalentMetadataMapsAndRuntimeLabelsDoNotChangeTheSchemaIdentity(){
        var first=table();var second=table();
        var reversed=new LinkedHashMap<String,TapField>();
        reversed.put("value",second.getNameFieldMap().get("value"));reversed.put("id",second.getNameFieldMap().get("id"));
        second.setNameFieldMap(reversed);
        first.setTableAttr(Map.of("alpha",1,"beta",List.of("a","b")));
        var attributes=new LinkedHashMap<String,Object>();attributes.put("beta",List.of("a","b"));attributes.put("alpha",1);
        second.setTableAttr(attributes);second.pdkId("another-runtime").lastUpdate(123456L);
        assertThat(PdkValidationModelVersion.of(first)).isEqualTo(PdkValidationModelVersion.of(second));
        assertThat(first.getNameFieldMap().get("id").getTapType()).isInstanceOf(TapNumber.class);
    }
    @Test void attributesLostByThePdkCloneStillInvalidateTheOriginalReadShape(){
        var model=table();var field=model.getNameFieldMap().get("id");var number=(TapNumber)field.getTapType();
        var original=PdkValidationModelVersion.of(model);
        field.setVirtual(true);assertThat(PdkValidationModelVersion.of(model)).isNotEqualTo(original);
        field.setVirtual(false);assertThat(PdkValidationModelVersion.of(model)).isEqualTo(original);
        number.fixed(false);assertThat(PdkValidationModelVersion.of(model)).isNotEqualTo(original);
        number.fixed(true);number.setCannotWrite(true);assertThat(PdkValidationModelVersion.of(model)).isNotEqualTo(original);
        assertThat(field.getTapType()).isSameAs(number);
    }
    @Test void aSharedNumericTypeIdDoesNotCollapseDistinctDeclaredRepresentations(){
        var number=table();var single=table();var twice=table();
        single.getNameFieldMap().get("id").tapType(new TapNumber().bit(32).fixed(false));
        twice.getNameFieldMap().get("id").tapType(new TapNumber().bit(64).fixed(false));
        assertThat(List.of(PdkValidationModelVersion.of(number),PdkValidationModelVersion.of(single),
                PdkValidationModelVersion.of(twice))).doesNotHaveDuplicates();
    }
    @Test void exactMetadataScaleAndTimePrecisionAreRetained(){
        var first=table();var second=table();
        first.setTableAttr(Map.of("default",new BigDecimal("1.00"),"at",Instant.ofEpochSecond(7,1)));
        second.setTableAttr(Map.of("default",new BigDecimal("1.0"),"at",Instant.ofEpochSecond(7,1)));
        assertThat(PdkValidationModelVersion.of(first)).isNotEqualTo(PdkValidationModelVersion.of(second));
        second.setTableAttr(Map.of("default",new BigDecimal("1.00"),"at",Instant.ofEpochSecond(7,2)));
        assertThat(PdkValidationModelVersion.of(first)).isNotEqualTo(PdkValidationModelVersion.of(second));
    }
    @Test void aUserContainerCannotImpersonateTheEncodingOfAScalarOrAnotherContainer(){
        var scalar=table();var disguised=table();
        scalar.setTableAttr(Map.of("value",1));
        disguised.setTableAttr(Map.of("value",List.of("Integer","1")));
        assertThat(PdkValidationModelVersion.of(scalar)).isNotEqualTo(PdkValidationModelVersion.of(disguised));
        scalar.setTableAttr(Map.of("value",java.util.Set.of("a","b")));
        disguised.setTableAttr(Map.of("value",List.of("a","b")));
        assertThat(PdkValidationModelVersion.of(scalar)).isNotEqualTo(PdkValidationModelVersion.of(disguised));
    }
    @Test void malformedUtf16CannotCollapseIntoTheUtf8ReplacementCharacter(){
        var malformed=table();
        malformed.setTableAttr(Map.of("value",Character.toString((char)0xd800)));
        assertThatThrownBy(()->PdkValidationModelVersion.of(malformed)).isInstanceOfSatisfying(TapstateException.class,
                failure->assertThat(failure.code()).isEqualTo(ValidationError.INVALID_CONTRACT));
        malformed.setTableAttr(Map.of(Character.toString((char)0xdc00),"value"));
        assertThatThrownBy(()->PdkValidationModelVersion.of(malformed)).isInstanceOf(TapstateException.class);
    }
    @Test void unknownKindsOpaqueDefaultsAndCyclesCannotAcquireAnIncompleteIdentity(){
        var model=table();model.getNameFieldMap().get("id").tapType(new ExoticNumber());
        assertThatThrownBy(()->PdkValidationModelVersion.of(model)).isInstanceOfSatisfying(TapstateException.class,
                failure->assertThat(failure.code()).isEqualTo(ValidationError.VALUE_TYPE_UNSUPPORTED));
        var opaque=table();opaque.setTableAttr(Map.of("opaque",new Object()));
        assertThatThrownBy(()->PdkValidationModelVersion.of(opaque)).isInstanceOf(TapstateException.class);
        var cyclic=table();var loop=new LinkedHashMap<String,Object>();loop.put("self",loop);cyclic.setTableAttr(loop);
        assertThatThrownBy(()->PdkValidationModelVersion.of(cyclic)).isInstanceOfSatisfying(TapstateException.class,
                failure->assertThat(failure.code()).isEqualTo(ValidationError.INVALID_CONTRACT));
    }
    @Test void metadataGrowthIsRefusedInsteadOfBeingClipped(){
        var model=table();model.setComment("x".repeat(1024*1024));
        assertThatThrownBy(()->PdkValidationModelVersion.of(model)).isInstanceOfSatisfying(TapstateException.class,
                failure->assertThat(failure.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
    }
    @Test void aFieldMapCannotBindAnotherPhysicalFieldUnderTheConfirmedName(){
        var model=table();model.getNameFieldMap().put("id",new TapField("another","BIGINT").tapType(new TapNumber().bit(64)));
        assertThatThrownBy(()->PdkValidationModelVersion.of(model)).isInstanceOfSatisfying(TapstateException.class,
                failure->assertThat(failure.code()).isEqualTo(ValidationError.INVALID_CONTRACT));
    }
    @Test void codecScopeRequiresAndBindsTheActualArtifactAndConfiguration(){
        var version=PdkValidationModelVersion.of(table());
        var first=new ConnectorRef(List.of(),"fixture","2.0.8",null,null,"a".repeat(64),false);
        var second=new ConnectorRef(List.of(),"fixture","2.0.8",null,null,"b".repeat(64),false);
        var changedSpec=new ConnectorRef(List.of(),"fixture","2.0.8",null,"{\"dataTypes\":{}}","a".repeat(64),false);
        assertThat(PdkValidationModelVersion.codecBinding(first,"c".repeat(64),version))
                .isNotEqualTo(PdkValidationModelVersion.codecBinding(second,"c".repeat(64),version))
                .isNotEqualTo(PdkValidationModelVersion.codecBinding(first,"d".repeat(64),version))
                .isNotEqualTo(PdkValidationModelVersion.codecBinding(changedSpec,"c".repeat(64),version));
        assertThatThrownBy(()->PdkValidationModelVersion.codecBinding(new ConnectorRef(List.of(),"fixture","2.0.8",null),"c".repeat(64),version))
                .isInstanceOf(TapstateException.class);
    }
    @Test void sampledDocumentPositionsAreNotColumnsButKeyOrderAndTypesStillBind(){
        var artifact=new ConnectorRef(List.of(),"io.tapdata.mongodb.MongodbConnector","2.0.8",null,null,"a".repeat(64),false);
        var first=table();var second=table();second.getNameFieldMap().get("value").pos(4);
        assertThat(PdkValidationModelVersion.of(first)).isNotEqualTo(PdkValidationModelVersion.of(second));
        assertThat(PdkValidationModelVersion.of(first,artifact)).isEqualTo(PdkValidationModelVersion.of(second,artifact));
        second.getNameFieldMap().get("id").primaryKeyPos(2);
        assertThat(PdkValidationModelVersion.of(first,artifact)).isNotEqualTo(PdkValidationModelVersion.of(second,artifact));
        second=table();second.getNameFieldMap().get("value").tapType(new TapString().bytes(65L));
        assertThat(PdkValidationModelVersion.of(first,artifact)).isNotEqualTo(PdkValidationModelVersion.of(second,artifact));
    }
    @Test void documentModelProfileDoesNotDropUnknownTableAttributesOrGrantCapabilityByLogicalId(){
        var actual=new ConnectorRef(List.of(),"io.tapdata.mongodb.MongodbConnector","2.0.8",null,null,"a".repeat(64),false);
        var ordinary=new ConnectorRef(List.of(),"synthetic.Ordinary","2.0.8",null,null,"a".repeat(64),false);
        var first=table();var second=table();first.setTableAttr(Map.of("schema-choice",1));second.setTableAttr(Map.of("schema-choice",2));
        assertThat(PdkValidationModelVersion.of(first,actual)).isNotEqualTo(PdkValidationModelVersion.of(second,actual));
        second=table();second.getNameFieldMap().get("value").pos(4);
        assertThat(PdkValidationModelVersion.of(table(),ordinary)).isNotEqualTo(PdkValidationModelVersion.of(second,ordinary));
    }
    @Test void documentLayoutNormalizationCannotHideAFieldInsideUnknownMetadata(){
        var artifact=new ConnectorRef(List.of(),"io.tapdata.mongodb.MongodbConnector","2.0.8",null,null,"a".repeat(64),false);
        var first=table();var second=table();
        first.setTableAttr(Map.of("extension",new TapField("nested","VARCHAR").pos(1).tapType(new TapString())));
        second.setTableAttr(Map.of("extension",new TapField("nested","VARCHAR").pos(2).tapType(new TapString())));
        assertThat(PdkValidationModelVersion.of(first,artifact)).isNotEqualTo(PdkValidationModelVersion.of(second,artifact));
    }
    @Test void knownPdkDefaultFunctionsRetainTheirOwnDomainAndIdentity(){
        var first=table();var second=table();
        first.getNameFieldMap().get("value").defaultFunction(TapField.TapDefaultFunction._CURRENT_TIMESTAMP);
        second.getNameFieldMap().get("value").defaultFunction(TapField.TapDefaultFunction._CURRENT_TIMESTAMP);
        assertThat(PdkValidationModelVersion.of(first)).isEqualTo(PdkValidationModelVersion.of(second));
        second.getNameFieldMap().get("value").defaultFunction("_CURRENT_TIMESTAMP");
        assertThat(PdkValidationModelVersion.of(first)).isNotEqualTo(PdkValidationModelVersion.of(second));
    }
    @Test void documentRuntimeStatisticsDoNotBecomeSchemaButOtherAttributesStillDo(){
        var artifact=new ConnectorRef(List.of(),"io.tapdata.mongodb.MongodbConnector","2.0.8",null,null,"a".repeat(64),false);
        var first=table();var second=table();
        first.setTableAttr(Map.of("storageSize",4096L,"indexSize",4096L,"size",100L,"avgObjSize",50.0,"schema-choice",1));
        second.setTableAttr(Map.of("storageSize",32768L,"indexSize",8192L,"size",200L,"avgObjSize",100.0,"schema-choice",1));
        assertThat(PdkValidationModelVersion.of(first,artifact)).isEqualTo(PdkValidationModelVersion.of(second,artifact));
        assertThat(PdkValidationModelVersion.of(first)).isNotEqualTo(PdkValidationModelVersion.of(second));
        second.setTableAttr(Map.of("storageSize",new Object()));
        assertThatThrownBy(()->PdkValidationModelVersion.of(second,artifact)).isInstanceOf(TapstateException.class);
    }
    private static final class ExoticNumber extends TapNumber {}
}
