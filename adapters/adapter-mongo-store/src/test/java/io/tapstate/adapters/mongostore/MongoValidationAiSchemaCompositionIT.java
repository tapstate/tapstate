package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.util.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;

/** Real independent CONFIG/CACHE layouts; component loss never confers the other component's creation right. */
@RequiresDocker
class MongoValidationAiSchemaCompositionIT {
    @Container static final MongoDBContainer CONFIG=MongoValidationSchemaIT.CONFIG;
    @Container static final MongoDBContainer CACHE=MongoValidationSchemaIT.CACHE;
    private static final String OWNER=JsonWriter.write(List.of("cloud","organization","cluster"));
    private static final AiProviderSettingsStore.Settings SETTING=new AiProviderSettingsStore.Settings("openai-compatible","https://api.example/v1","model","private-fixture-key");

    @Test void originalAiInstallationAndControlledValidationPreparationComposeWithoutChangingTheEncryptedRow(){
        try(var f=new MongoValidationSchemaIT.Fixture(0,StorageBinding.Mode.CLOUD,true)){
            assertThat(f.configSchema().getInteger("installedVersion")).isEqualTo(2);
            assertThatThrownBy(f.configuration::validationPreparations).isInstanceOf(TapstateException.class);
            f.configuration.aiProviderSettings().put(OWNER,SETTING);var row=SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(f.configDb).find().first();
            var original=f.configuration.bindings().cachePreparation().orElseThrow();
            var receipt=f.configuration.prepareInitialValidationSchema(original,f.cache,()->{});assertThat(receipt.ready()).isTrue();
            assertThat(f.configSchema().getInteger("installedVersion")).isEqualTo(3);assertThat(f.cacheSchema().getInteger("installedVersion")).isEqualTo(2);
            assertThat(SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(f.configDb).find().first()).isEqualTo(row);
            try(var reopened=MongoConfigurationDomain.open(f.settings,"cluster")){
                assertThat(reopened.schemaVersion()).isEqualTo(3);assertThat(reopened.aiProviderSettings().get(OWNER)).contains(SETTING);
                assertThat(reopened.validationPreparations()).isNotNull();
                assertThat(reopened.prepareInitialValidationSchema(original,f.cache,()->{})).isEqualTo(receipt);
            }
            var before=f.configSchema();SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(f.configDb).drop();
            assertThatThrownBy(()->MongoConfigurationDomain.open(f.settings,"cluster")).isInstanceOf(TapstateException.class);
            assertThatThrownBy(()->f.configuration.aiProviderSettings().get(OWNER)).isInstanceOf(TapstateException.class);
            assertThat(f.configSchema()).isEqualTo(before);assertThat(f.configDb.listCollectionNames()).doesNotContain("ai_provider_settings");
        }
    }

    @Test void originalValidationVersionTwoCannotFallBackToAiAfterMarkerLoss(){
        try(var f=new MongoValidationSchemaIT.Fixture(1)){
            f.upgrade();assertThat(f.configSchema().getInteger("installedVersion")).isEqualTo(2);
            assertThatThrownBy(()->f.configuration.aiProviderSettings().get(OWNER)).isInstanceOfSatisfying(TapstateException.class,error->assertThat(error.code()).isEqualTo(StoreError.AI_PROVIDER_SCHEMA_UNSUPPORTED));
            SystemCollections.SCHEMA_MIGRATIONS.on(f.configDb).updateOne(new Document("_id","config"),new Document("$unset",new Document("validation","")));
            var version=SystemCollections.CONFIGURATION_VERSION.on(f.configDb).find().first();
            assertThatThrownBy(()->MongoConfigurationDomain.open(f.settings,"cluster")).isInstanceOf(TapstateException.class);
            assertThatThrownBy(()->f.configuration.artifacts().create(source())).isInstanceOf(TapstateException.class);
            assertThat(SystemCollections.CONFIGURATION_VERSION.on(f.configDb).find().first()).isEqualTo(version);
            assertThat(f.configDb.listCollectionNames()).doesNotContain("ai_provider_settings");
        }
    }

    @Test void residualValidationStructuresInAiVersionTwoNeverAcquirePreparationOrAuthoringRights(){
        try(var f=new MongoValidationSchemaIT.Fixture(0,StorageBinding.Mode.CLOUD,true)){
            var before=f.configSchema();var version=SystemCollections.CONFIGURATION_VERSION.on(f.configDb).find().first();
            f.configDb.createCollection("validation_tasks");
            assertThatThrownBy(()->MongoConfigurationDomain.open(f.settings,"cluster")).isInstanceOf(TapstateException.class);
            assertThatThrownBy(()->f.configuration.artifacts().create(source())).isInstanceOf(TapstateException.class);
            assertThatThrownBy(()->f.configuration.prepareInitialValidationSchema(f.configuration.bindings().cachePreparation().orElseThrow(),f.cache,()->{})).isInstanceOf(TapstateException.class);
            assertThat(f.configSchema()).isEqualTo(before);assertThat(SystemCollections.CONFIGURATION_VERSION.on(f.configDb).find().first()).isEqualTo(version);
            assertThat(f.configDb.listCollectionNames()).doesNotContain("validation_reports","validation_differences");
        }
    }

    @Test void historicalValidationTwoAlsoRefusesRetainedHandleWritesAfterStructureLoss(){
        try(var f=new MongoValidationSchemaIT.Fixture(1)){
            f.upgrade();assertThat(f.configuration.artifacts().create(new SourceResource("healthy",null,"mongodb",Map.of("uri","mongodb://business.invalid/healthy"),null,null,null,null))).isEqualTo(ArtifactMutation.CREATED);
            SystemCollections.VALIDATION_REPORTS.on(f.configDb).drop();var before=SystemCollections.CONFIGURATION_VERSION.on(f.configDb).find().first();
            assertThatThrownBy(()->f.configuration.artifacts().create(source())).isInstanceOf(TapstateException.class);
            assertThat(f.configuration.artifacts().get("unexpected")).isEmpty();
            assertThat(SystemCollections.CONFIGURATION_VERSION.on(f.configDb).find().first()).isEqualTo(before);
            assertThat(f.configDb.listCollectionNames()).doesNotContain("validation_reports");
        }
    }

    @Test void composedMaintenanceBlocksOrdinaryWritesAndOnlyItsOriginalOperationFinishes(){
        try(var f=new MongoValidationSchemaIT.Fixture(1,StorageBinding.Mode.CLOUD,true)){
            f.configuration.aiProviderSettings().put(OWNER,SETTING);var row=SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(f.configDb).find().first();var operation=UUID.randomUUID();
            try(var preparing=f.prepare(operation)){
                assertThat(f.configSchema().getInteger("installedVersion")).isEqualTo(3);
                assertThat(preparing.advance(1).ready()).isFalse();var version=SystemCollections.CONFIGURATION_VERSION.on(f.configDb).find().first();
                assertThatThrownBy(()->f.configuration.artifacts().create(source())).isInstanceOf(TapstateException.class);
                assertThatThrownBy(()->f.configuration.aiProviderSettings().put(OWNER,SETTING)).isInstanceOf(TapstateException.class);
                assertThatThrownBy(()->MongoConfigurationDomain.open(f.settings,"cluster")).isInstanceOf(TapstateException.class);
                assertThatThrownBy(()->f.resume(UUID.randomUUID())).isInstanceOf(TapstateException.class);
                assertThat(SystemCollections.CONFIGURATION_VERSION.on(f.configDb).find().first()).isEqualTo(version);
                preparing.prepareCache(f.cache);while(!preparing.advance(20).ready()){}
            }
            try(var reopened=MongoConfigurationDomain.open(f.settings,"cluster")){assertThat(reopened.aiProviderSettings().get(OWNER)).contains(SETTING);}
            assertThat(SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(f.configDb).find().first()).isEqualTo(row);
            SystemCollections.VALIDATION_REPORTS.on(f.configDb).drop();var version=SystemCollections.CONFIGURATION_VERSION.on(f.configDb).find().first();
            assertThatThrownBy(()->MongoConfigurationDomain.open(f.settings,"cluster")).isInstanceOf(TapstateException.class);
            assertThatThrownBy(()->f.configuration.artifacts().create(source())).isInstanceOf(TapstateException.class);
            assertThatThrownBy(()->f.configuration.aiProviderSettings().get(OWNER)).isInstanceOf(TapstateException.class);
            assertThat(SystemCollections.CONFIGURATION_VERSION.on(f.configDb).find().first()).isEqualTo(version);
            assertThat(f.configDb.listCollectionNames()).doesNotContain("validation_reports");
        }
    }
    private static SourceResource source(){return new SourceResource("unexpected",null,"mongodb",Map.of("uri","mongodb://business.invalid/data"),null,null,null,null);}
}
