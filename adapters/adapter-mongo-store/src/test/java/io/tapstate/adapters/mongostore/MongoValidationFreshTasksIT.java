package io.tapstate.adapters.mongostore;

import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.util.*;
import java.util.concurrent.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import static org.assertj.core.api.Assertions.*;

/** Server-assigned task lifetimes use the existing independent parent identity and CONFIG write conflict domain. */
@RequiresDocker
class MongoValidationFreshTasksIT {
    @Container static final MongoDBContainer CONFIG=MongoValidationTaskStoreIT.CONFIG;
    @Container static final MongoDBContainer CACHE=MongoValidationTaskStoreIT.CACHE;
    @Test void concurrentCreatesAndLostReplyRetriesReturnOneOriginalServerGeneration()throws Exception{
        try(var f=new MongoValidationTaskStoreIT.Fixture();var callers=Executors.newFixedThreadPool(4)){
            var proposed=f.create("flow");var declarations=f.tasks.declarations(new ValidationTaskStore.Selection("flow",List.of("out")));
            var parentBefore=SystemCollections.OPERATOR_GENERATIONS.on(f.configDb).find(new Document("_id","flow")).first().get("validationPreparation",Document.class);
            assertThat(parentBefore.getString("phase")).isEqualTo("UNASSIGNED");
            var request=new ValidationTaskStore.FreshCreate("flow","fresh",declarations.parentGeneration(),proposed.comparison(),proposed.settings(),UUID.randomUUID());
            var requests=new ArrayList<Future<ValidationTaskStore.Created>>();
            for(int index=0;index<4;index++)requests.add(callers.submit(()->f.tasks.createFresh(request)));
            var results=new ArrayList<ValidationTaskStore.Created>();for(var response:requests)results.add(response.get(10,TimeUnit.SECONDS));
            assertThat(results).filteredOn(value->value.outcome()==ValidationTaskStore.Write.APPLIED).hasSize(1);
            var generations=results.stream().map(ValidationTaskStore.Created::generation).collect(java.util.stream.Collectors.toSet());assertThat(generations).hasSize(1);
            var generation=generations.iterator().next();var replay=f.tasks.createFresh(request);assertThat(replay.outcome()).isEqualTo(ValidationTaskStore.Write.DUPLICATE);assertThat(replay.generation()).isEqualTo(generation);
            var changedSettings=new ValidationTaskStore.Settings(new ValidationTaskStore.Full(proposed.settings().full().method(),1,null,false,9),proposed.settings().incremental());
            assertThat(f.tasks.createFresh(new ValidationTaskStore.FreshCreate("flow","fresh",request.parentGeneration(),proposed.comparison(),changedSettings,request.commandId())))
                    .isEqualTo(new ValidationTaskStore.Created(ValidationTaskStore.Write.CONFLICT,null));
            var saved=f.tasks.read("flow","fresh").orElseThrow();assertThat(saved.task().generation()).isEqualTo(generation);assertThat(saved.full().permission()).isNull();
            assertThat(saved.full().nextStartSequence()).isEqualTo(1);
            var parentAfter=SystemCollections.OPERATOR_GENERATIONS.on(f.configDb).find(new Document("_id","flow")).first().get("validationPreparation",Document.class);
            assertThat(parentAfter.getString("phase")).isEqualTo("UNASSIGNED");assertThat(parentAfter.getString("generation")).isEqualTo(request.parentGeneration().toString());
            assertThat(parentAfter.getLong("revision")).isGreaterThan(parentBefore.getLong("revision"));
        }
    }
    @Test void anOldParentCannotInsertOrDeduplicateAgainstAnIdenticalRecreatedParent(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var proposal=f.create("flow");var old=f.tasks.declarations(new ValidationTaskStore.Selection("flow",List.of("out")));
            var request=new ValidationTaskStore.FreshCreate("flow","fresh",old.parentGeneration(),proposal.comparison(),proposal.settings(),UUID.randomUUID());
            var replacement=UUID.randomUUID();var parents=SystemCollections.OPERATOR_GENERATIONS.on(f.configDb);
            parents.updateOne(new Document("_id","flow"),new Document("$set",new Document("validationPreparation.generation",replacement.toString())));
            assertThat(f.tasks.createFresh(request)).isEqualTo(new ValidationTaskStore.Created(ValidationTaskStore.Write.CONFLICT,null));assertThat(f.tasks.read("flow","fresh")).isEmpty();
            var current=f.tasks.declarations(old.selection());assertThat(current.dependencyHashes()).isEqualTo(old.dependencyHashes());assertThat(current.parentGeneration()).isEqualTo(replacement);
            var next=f.tasks.createFresh(new ValidationTaskStore.FreshCreate("flow","fresh",replacement,proposal.comparison(),proposal.settings(),request.commandId()));
            assertThat(next.outcome()).isEqualTo(ValidationTaskStore.Write.APPLIED);
            assertThat(f.tasks.createFresh(request)).isEqualTo(new ValidationTaskStore.Created(ValidationTaskStore.Write.CONFLICT,null));
            assertThat(f.tasks.read("flow","fresh").orElseThrow().task().generation()).isEqualTo(next.generation());
        }
    }
    @Test void aClosingParentNeverCreatesFreshConfiguration(){
        try(var f=new MongoValidationTaskStoreIT.Fixture()){
            var proposal=f.create("flow");var input=f.tasks.declarations(new ValidationTaskStore.Selection("flow",List.of("out")));
            SystemCollections.OPERATOR_GENERATIONS.on(f.configDb).updateOne(new Document("_id","flow"),new Document("$set",new Document("validationPreparation.closingOperationId",UUID.randomUUID().toString())));
            assertThatThrownBy(()->f.tasks.createFresh(new ValidationTaskStore.FreshCreate("flow","fresh",input.parentGeneration(),proposal.comparison(),proposal.settings(),UUID.randomUUID())))
                    .isInstanceOf(io.tapstate.core.common.TapstateException.class);
            assertThat(f.tasks.read("flow","fresh")).isEmpty();
        }
    }
}
