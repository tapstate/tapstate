package io.tapstate.adapters.mongostore;

import com.mongodb.client.*;
import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertConfiguration.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.alerting.ReceiverSecretMaterial;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.function.Function;
import org.bson.Document;

/** Composes the three alert fragments in the original CONFIG transaction and rechecks current authority. */
final class MongoAlertCoordinator implements AlertStore {
    private final MongoConfigurationDomain domain;private final MongoDatabase db;private final SourceConfigCipherProvider keys;
    private final MongoConfigurationArtifactStore artifacts;private final MongoConfigurationSecurityStores security;
    private final MongoStorageBindingStore bindings;private final Host host;private final Clock clock;
    private final MongoAlertConfigurationStore definitions;private final MongoAlertStateStore states;private final MongoAlertNotificationStore notifications;
    private final MongoAlertNodeLifecycleStore nodes;
    MongoAlertCoordinator(MongoConfigurationDomain domain,MongoDatabase db,SourceConfigCipherProvider keys,MongoConfigurationArtifactStore artifacts,
            MongoConfigurationSecurityStores security,MongoStorageBindingStore bindings,Host host,AlarmTypeCatalog catalog,Clock clock) {
        this.domain=domain;this.db=db;this.keys=keys;this.artifacts=artifacts;this.security=security;this.bindings=bindings;this.host=host;this.clock=clock;
        this.definitions=domain.alertDefinitions(host.scope(),catalog);this.states=new MongoAlertStateStore(db,host.scope(),host.locallyActive());
        if(host.deployment()==AlertChannelPolicy.Deployment.ON_PREM&&!host.scope().authorityId().equals(host.setup().installationId()))throw stale("installation-authority");
        this.notifications=new MongoAlertNotificationStore(db,host.scope(),host.deployment(),host.locallyActive(),clock);
        this.nodes=new MongoAlertNodeLifecycleStore(domain,db,keys,bindings,host.scope(),host.setup(),host.locallyActive());
    }
    private <T>T tx(Access access,boolean write,boolean authoring,Function<ClientSession,T> action) {
        if(access!=null){access.revalidate().run();access.locallyActive().run();}
        host.locallyActive().run(); if(domain.alertSchemaVersion()!=MongoAlertStorageSchema.VERSION)throw unreadable("schema-capability");
        return StoreIo.call(()->domain.alertTransaction(new MongoReadBudget(Duration.ofSeconds(10)),session->{
            currentHost(session);if(access!=null){currentActor(session,access.actor(),access);if(write&&access.actor()==null&&!Set.of("WRITE","ADMIN").contains(access.cloudGrantedScope()))throw stale("cloud-write-permission");}
            if(write){
                if(!keys.fenceWrite(new SourceConfigWriteScope(session),keys.currentForWrite().activeKeyId()))throw stale("configuration-key");
                if(authoring)ConfigurationVersion.advance(session,SystemCollections.CONFIGURATION_VERSION.on(db));
                else ConfigurationVersion.advanceAlert(session,SystemCollections.CONFIGURATION_VERSION.on(db));
            }
            T result=action.apply(session);host.locallyActive().run();if(access!=null)access.locallyActive().run();return result;
        }));
    }
    private void currentHost(ClientSession session) {
        host.locallyActive().run();var scope=host.setup();
        Document setup=SystemCollections.STORAGE_SETUP.on(db).find(session,new Document("_id","cluster")).first();
        if(setup==null||!scope.clusterId().equals(setup.get("clusterId"))||!scope.installationId().equals(setup.get("installationId"))
                ||!scope.operationId().equals(setup.get("operationId"))||!Long.valueOf(scope.generation()).equals(setup.get("generation")))throw stale("host-installation");
        var binding=bindings.loadForTransaction(session);
        if(binding.isPresent()&&(binding.get().mode()==StorageBinding.Mode.OP)!=(host.deployment()==AlertChannelPolicy.Deployment.ON_PREM))throw stale("deployment");
        Document schema=SystemCollections.SCHEMA_MIGRATIONS.on(db).find(session,new Document("_id","config")).first();
        Document version=SystemCollections.CONFIGURATION_VERSION.on(db).find(session,new Document("_id",ConfigurationVersion.ID)).first();
        MongoAlertStorageSchema.verifyAuthorityState(schema,version,scope.clusterId(),keys.currentForWrite().activeKeyId());
    }
    private void currentActor(ClientSession session,AlertTestAuthority actor,Access access) {
        if(access.actor()==null){
            if(host.deployment()!=AlertChannelPolicy.Deployment.CLOUD_MANAGED||!host.scope().authorityId().equals(access.cloudOrganization())
                    ||!Set.of("READ","WRITE","ADMIN").contains(access.cloudGrantedScope()))throw stale("cloud-caller");return;
        }
        if(host.deployment()!=AlertChannelPolicy.Deployment.ON_PREM||!actor.realm().equals(host.setup().installationId())||!actor.grantedScope().equals("ADMIN"))throw stale("actor-permission");
        if(actor.mechanism()==AlertTestAuthority.Mechanism.MACHINE_TOKEN) {
            var token=security.tokenInTransaction(session,actor.subject()).orElseThrow(()->stale("actor-token"));
            if(token.revoked()||!token.scope().equals(actor.grantedScope())||!token.authenticationBinding().equals(actor.credentialBinding()))throw stale("actor-token");
        } else {
            if(!clock.instant().isBefore(actor.expiresAt())||!actor.credentialBinding().equals(access.currentVerifierBinding().get()))throw stale("actor-jwt");
            Document user=SystemCollections.CONFIG_USERS.on(db).find(session,new Document("_id",actor.subject())).first();
            if(user==null||!MongoFirstAdminStore.decodeUser(user).role().equalsIgnoreCase("admin"))throw stale("actor-user-permission");
        }
    }
    @Override public Optional<Stored<AlertStateRecord.Head>> state(AlarmKey key,ReadAccess access) {
        visible(key,access);var result=tx(null,false,false,s->states.loadEnvelope(s,key).map(v->new Stored<>(v.revision(),v.value())));access.requireActive().run();return result;
    }
    @Override public Optional<Stored<AlertIntent.Record>> test(AlertTestCommand.Key key,Access access){
        Objects.requireNonNull(access);scope(key.scope());if(access.actor()==null||!key.actorPrincipal().equals(access.actor().principalKey()))throw stale("test-query-actor");
        return tx(access,false,false,s->notifications.testIntent(s,key).map(v->new Stored<>(v.recordRevision(),v.value())));
    }
    @Override public Optional<Stored<AlertIntent.Record>> intent(AlertIntent.Identity key,Access access) {
        scope(key.scope());return tx(access,false,false,s->notifications.intent(s,key).map(v->new Stored<>(v.recordRevision(),v.value())));
    }
    @Override public Optional<Rule> originalRule(AlertRule binding,ReadAccess access){
        visible(binding.key(),access);
        var result=tx(null,false,false,s->{var head=states.loadEnvelope(s,binding.key());
            if(head.isEmpty()||head.get().value().binding()==null)return Optional.<Rule>empty();
            var identity=head.get().value().binding().ruleIdentity();
            if(!AlertOperationDomain.belongs(identity.incarnation(),binding.key()))return Optional.<Rule>empty();
            return definitions.load(s,Kind.RULE,identity.id()).map(MongoAlertConfigurationCodec.Stored::definition)
                    .filter(Rule.class::isInstance).map(Rule.class::cast)
                    .filter(rule->rule.header().identity().equals(identity)&&rule.header().revision()==binding.revision());
        });access.requireActive().run();return result;
    }
    @Override public Optional<Definition> definition(Kind kind,String id,Access access) {
        return tx(access,false,false,s->definitions.load(s,kind,id).map(MongoAlertConfigurationCodec.Stored::definition));
    }
    @Override public DefinitionPage definitions(Kind kind,String after,int limit,Access access){return tx(access,false,false,s->new MongoAlertReads(db,host.scope(),states).definitions(s,kind,after,limit));}
    @Override public ResourcePage resources(String kind,String after,int limit,Access access){
        if(limit<1||limit>100)throw stale("resource-page-limit");
        if(!Set.of("source","pipeline").contains(kind))return new ResourcePage(List.of(),null);
        return tx(access,false,false,s->{
            var collection=kind.equals("source")?SystemCollections.SOURCES:SystemCollections.PIPELINES;
            var filter=after==null?new Document():new Document("_id",new Document("$gt",after));
            var raw=collection.on(db).find(s,filter).hintString("_id_").sort(new Document("_id",1)).limit(limit+1).into(new ArrayList<>());
            boolean more=raw.size()>limit;if(more)raw.removeLast();var rows=new ArrayList<AlarmKey.Resource>();
            for(var row:raw){String id=row.getString("_id");if(kind.equals("source"))rows.add(new AlarmKey.Resource(kind,id,MongoSourceIdentity.read(row).binding().incarnation()));
                else{var state=MongoPipelineStateMappings.decode(id,SystemCollections.OPERATOR_GENERATIONS.on(db).find(s,new Document("_id",id)).first());
                    if(state.phase()==PipelineStateMapping.Phase.ACTIVE)rows.add(new AlarmKey.Resource(kind,id,Long.toString(state.stateGeneration())));}}
            return new ResourcePage(rows,more?raw.getLast().getString("_id"):null);
        });
    }
    @Override public DefinitionPage evaluationRules(String after,int limit){return tx(null,false,false,s->new MongoAlertReads(db,host.scope(),states).definitions(s,Kind.RULE,after,limit));}
    @Override public EvaluationPage evaluationStates(AlarmKey after,int limit){return tx(null,false,false,s->{
        var page=new MongoAlertReads(db,host.scope(),states).evaluationStates(s,after,limit);var removed=new HashSet<AlarmKey.Resource>();var superseded=new HashSet<AlarmKey>();
        for(var row:page.rows()){var resource=row.value().snapshot().rule().key().resource();if(resourceRemoved(s,resource))removed.add(resource);else if(sourceBindingChanged(s,row.value()))superseded.add(row.value().snapshot().rule().key());}
        return new EvaluationPage(page.rows(),page.next(),removed,superseded);
    });}
    @Override public Outcome saveDefinition(Definition before,Definition after,Access access) {
        Objects.requireNonNull(access);management(after,access);
        try{return tx(access,true,true,s->{validateDefinitionReferences(s,after);return outcome(definitions.stage(s,before,after));});}catch(Conflict conflict){return Outcome.CONFLICT;}
    }
    @Override public Outcome saveReceiver(AlertReceiver before,AlertReceiver after,PrivateMaterial replacement,boolean retain,Access access) {
        Objects.requireNonNull(access);management(after,access);
        MongoAlertReceiverSecrets.Prepared prepared;
        if(retain){var captured=tx(access,false,false,s->definitions.captureReceiverForManagement(s,after.header().identity()));prepared=definitions.retainReceiverMaterial(after,captured);}
        else{try(var material=material(replacement)){prepared=definitions.prepareReceiver(after,material);}}
        try{return tx(access,true,true,s->outcome(definitions.stageReceiver(s,before,prepared)));}catch(Conflict conflict){return Outcome.CONFLICT;}
    }
    private void management(Definition after,Access access){scope(after.header().identity().scope());if(after.header().deployment()!=host.deployment()
            ||!after.header().updatedBy().equals(access.subject()))throw stale("configuration-author");
        if(access.actor()==null&&(!Set.of("WRITE","ADMIN").contains(access.cloudGrantedScope())||after instanceof AlertReceiver||after instanceof Subscription))throw stale("cloud-external-configuration");}
    private void validateDefinitionReferences(ClientSession s,Definition value) {
        if(value instanceof Rule rule)for(var resource:rule.resources().selected())resource(s,resource,0,false);
        if(value instanceof Subscription route)for(var channel:route.channels())for(var receiver:channel.receivers()) {
            var current=definitions.captureReceiver(s,receiver).receiver();if(!channel.channelType().equals(current.transport().channelType()))throw stale("route-channel");
        }
        if(value instanceof Silence silence)for(var resource:silence.window().resources())resource(s,resource,0,false);
    }
    @Override public PreparedCandidate prepareCandidate(AlertTestCommand.Key key,AlertTestCommand.Parameters form,AlertReceiver.Transport transport,
            Identity existing,long revision,PrivateMaterial material,Set<PrivateField> retain,Access access) {
        Objects.requireNonNull(access);scope(key.scope());if(access.actor()==null||!key.actorPrincipal().equals(access.actor().principalKey()))throw stale("test-actor");
        if(!form.channelType().equals(transport.channelType()))throw stale("candidate-channel");
        Set<PrivateField> retained=Set.copyOf(retain);
        var captured=tx(access,false,false,s->{
            if(existing==null){if(form.receiver()!=null||!retained.isEmpty())throw stale("candidate-original-receiver");return null;}
            var receiver=definitions.captureReceiver(s,existing);
            if(form.receiver()==null||!existing.incarnation().equals(form.receiver().incarnation())||receiver.receiver().header().revision()!=form.receiver().expectedRevision()
                    ||receiver.receiver().bindingRevision()!=revision)throw stale("candidate-edit-baseline");return receiver;
        });
        var cipher=keys.refresh(new MongoReadBudget(Duration.ofSeconds(5)));
        try(var secret=retained.isEmpty()?material(material):retainedMaterial(captured,transport,material,retained,cipher)){
            var p=MongoAlertCandidateSecrets.prepare(key,form,transport,existing,revision,secret,cipher);
            return new PreparedCandidate(p.parameters(),p.target(),p.encrypted());
        }
    }
    @Override public Stored<AlertIntent.Record> admitTest(AlertIntent.Record candidate,Access access) {
        Objects.requireNonNull(access);if(access.actor()==null||!(candidate.frozen().identity() instanceof AlertIntent.Test test)
                ||!access.actor().equals(candidate.frozen().testAuthority()))throw stale("test-authority");
        var replay=tx(access,false,false,s->notifications.testIntent(s,test.command().key()));
        if(replay.isPresent()){
            var old=replay.get();AlertTestCommand.admit(host.deployment(),candidate.frozen().channel(),test.command().key(),test.command().parameters(),
                    ((AlertIntent.Test)old.value().frozen().identity()).command(),clock.instant());return new Stored<>(old.recordRevision(),old.value());
        }
        if(!(candidate.frozen().target() instanceof AlertIntent.CandidateTarget candidateTarget))throw stale("test-candidate");
        try(var verified=MongoAlertCandidateSecrets.resolve(test.command(),candidateTarget,candidate.state().candidate(),keys.refresh(new MongoReadBudget(Duration.ofSeconds(5))))){ }
        return tx(access,true,false,s->{
            var old=notifications.testIntent(s,test.command().key());
            var admission=AlertTestCommand.admit(host.deployment(),candidate.frozen().channel(),test.command().key(),test.command().parameters(),
                    old.isEmpty()?null:((AlertIntent.Test)old.get().value().frozen().identity()).command(),test.command().acceptedAt());
            if(admission.replay()){var v=old.orElseThrow();return new Stored<>(v.recordRevision(),v.value());}
            if(!admission.command().equals(test.command())||test.command().acceptedAt().isAfter(clock.instant())
                    ||Duration.between(test.command().acceptedAt(),clock.instant()).compareTo(Duration.ofSeconds(30))>0)throw stale("test-admission-time");
            validateIntent(s,candidate.frozen(),true,access);
            var minute=AlertTestQuota.minute(test.command().acceptedAt());var quota=notifications.quota(s,minute);
            List<AlertIntent.Record> recent=new MongoAlertReads(db,host.scope(),states).testWindow(s,clock.instant());
            if(quota.isEmpty()&&recent.stream().anyMatch(v->AlertTestQuota.minute(v.frozen().createdAt()).equals(minute)))throw unreadable("missing-test-quota");
            var target=target(candidate.frozen());
            for(var v:recent)if(v.state().retained().stoppedAt()==null&&clock.instant().isBefore(v.frozen().deadline())) {
                boolean receiver=candidate.frozen().target() instanceof AlertIntent.CandidateTarget c&&c.existingReceiver()!=null
                        &&v.frozen().target() instanceof AlertIntent.CandidateTarget o&&c.existingReceiver().equals(o.existingReceiver());
                if(receiver||target(v.frozen()).equals(target))throw stale("test-target-busy");
            }
            var bucket=quota.map(MongoAlertNotificationCodec.Quota::value).orElseGet(()->AlertTestQuota.Bucket.empty(host.scope(),test.command().acceptedAt()));
            outcome(notifications.stageQuota(s,quota.orElse(null),bucket.accepted(access.actor().principalKey())));
            outcome(notifications.stageIntent(s,null,candidate));return new Stored<>(1,candidate);
        });
    }
    @Override public Outcome commit(Commit candidate){return commit(candidate,null);}
    @Override public Outcome commitAsCaller(Commit candidate,Access access){Objects.requireNonNull(access);return commit(candidate,access);}
    private Outcome commit(Commit c,Access access) {
        try{return tx(access,true,false,s->{
            boolean changed=false;
            if(c.version()!=null){
                var version=SystemCollections.CONFIGURATION_VERSION.on(db).find(s,new Document("_id",ConfigurationVersion.ID)).first();
                if(version==null||!Long.valueOf(c.version().configurationRevision()).equals(version.get("revision"))
                        ||!Long.valueOf(Math.incrementExact(c.version().alertFence())).equals(version.get(ConfigurationVersion.ALERT_FENCE)))throw new Conflict();
            }
            if(c.state()!=null){var v=c.state();scope(v.after().snapshot().rule().key().scope());
                if(v.proof()!=null){
                    v.proof().requireCurrent().run();var original=v.proof().pipeline();var key=v.after().snapshot().rule().key();
                    if(original==null) {
                        if(!Set.of("source","cluster-storage").contains(key.resource().kind()))throw stale("operational-proof-resource");
                    }else if(!key.resource().kind().equals("pipeline")||!key.resource().id().equals(original.scope().pipelineId())
                            ||!key.resource().incarnation().equals(Long.toString(original.scope().stateGeneration()))
                            ||!((MongoPipelineIntentStore)domain.intents()).loadInTransaction(s,original.scope().pipelineId()).filter(original::equals).isPresent())throw stale("pipeline-producer-intent");
                }
                if(v.diagnosticSignal()!=null && v.after().operationalEvidence()!=null && v.after().operationalEvidence().version()==2
                        && v.proof()==null)throw stale("missing-operational-local-proof");
                if(c.kind()!=CommitKind.STATE_AND_PLAN&&c.kind()!=CommitKind.END)throw stale("state-command");
                if(c.kind()==CommitKind.END){
                    if(v.after().snapshot().enabled()||v.after().snapshot().status()==AlertSnapshot.Status.FIRING||v.before()==null||v.before().binding()==null)throw stale("administrative-end");
                    if(v.after().retiredAt()==null){
                        var previous=v.before().binding();var current=definitions.load(s,Kind.RULE,previous.ruleIdentity().id());
                        if(current.isPresent()&&current.get().definition() instanceof Rule rule&&rule.header().identity().equals(previous.ruleIdentity())
                                &&rule.matches(v.before().snapshot().rule().key()))throw stale("rule-still-enabled");
                    }else{
                        var previous=v.before().binding();var rule=definitions.load(s,Kind.RULE,previous.ruleIdentity().id());
                        boolean retiredRule=rule.isEmpty()||!rule.get().definition().header().identity().equals(previous.ruleIdentity())||!rule.get().definition().header().active();
                        if(!retiredRule && !(sourceBindingSuperseded(s,v.before())))requireResourceRemoved(s,v.before().snapshot().rule().key().resource());
                    }
                }
                else {
                    if(v.after().binding()==null)throw stale("state-rule-binding");
                    validateMember(s,v.after().binding(),true);
                }
                if(v.before()==null && v.after().binding()!=null)requireDomainCapacity(s,v.after());
                changed|=outcome(states.stageEnvelope(s,v.revision(),v.before(),v.after(),v.transitions(),v.diagnosticSignal(),clock))==Outcome.STAGED;
                if(v.proof()!=null)v.proof().requireCurrent().run();
            }
            for(var change:c.intents()){
                var before=change.before();var after=change.after();scope(after.frozen().identity().scope());
                if(before==null&&!(after.frozen().identity() instanceof AlertIntent.Business))throw stale("test-requires-admission");
                int prior=before==null?0:before.value().state().attemptsConsumed();
                boolean grants=after.state().attemptsConsumed()>prior;
                if(grants&&c.kind()!=CommitKind.PERMIT||c.kind()==CommitKind.PERMIT&&before==null)throw stale("permit-command");
                if(before!=null&&after.state().manualCycles()!=before.value().state().manualCycles()&&access==null)throw stale("manual-retry-actor");
                if(before==null||grants||before.value().state().manualCycles()!=after.state().manualCycles())validateIntent(s,after.frozen(),grants||before!=null&&before.value().state().manualCycles()!=after.state().manualCycles(),access);
                if(grants&&host.deployment()==AlertChannelPolicy.Deployment.ON_PREM){var limit=notifications.limit(s,target(after.frozen()));if(limit.isPresent()&&!limit.get().value().restrictionElapsed(clock.instant()))throw stale("target-not-before");}
                if(c.kind()==CommitKind.END&&(before==null||grants||!after.state().permitsStopped()))throw stale("administrative-stop");
                if(c.kind()==CommitKind.RESULT&&before!=null&&!before.value().state().owner().equals(after.state().owner()))throw stale("result-owner");
                Outcome staged=outcome(notifications.stageIntent(s,before==null?null:new MongoAlertDeliveryCodec.Stored(before.revision(),before.value()),after));changed|=staged==Outcome.STAGED;
                if(before!=null&&host.deployment()==AlertChannelPolicy.Deployment.ON_PREM)for(var attempt:after.state().attempts())if(attempt.protocol()!=null&&attempt.protocol().retryAfter()!=null){
                    var d=target(after.frozen());var current=notifications.limit(s,d);var value=current.isEmpty()?new AlertTargetLimit(d,attempt.protocol().retryAfter()):current.get().value().observe(d,attempt.protocol().retryAfter());
                    outcome(notifications.stageLimit(s,current.orElse(null),value));
                }
            }
            for(var v:c.schedules())changed|=outcome(notifications.stageSchedule(s,v.before()==null?null:new MongoAlertScheduleCodec.Stored(v.before().revision(),v.before().value()),v.after()))==Outcome.STAGED;
            return changed?Outcome.STAGED:Outcome.UNCHANGED;
        });}catch(Conflict conflict){return Outcome.CONFLICT;}
    }
    @Override public Planning planning(){return tx(null,false,false,s->{
        var version=SystemCollections.CONFIGURATION_VERSION.on(db).find(s,new Document("_id",ConfigurationVersion.ID)).first();
        ConfigurationVersion.validateAlertState(version,true);var budget=new PlanningBudget();
        var defs=new ArrayList<Definition>();for(var kind:Kind.values()){defs.addAll(definitionSnapshot(s,kind));if(defs.size()>1000)throw unreadable("planning-definitions-budget");}
        for(var definition:defs)budget.charge(MongoAlertConfigurationCodec.write(definition,0));
        var heads=new ArrayList<Stored<AlertStateRecord.Head>>();AlarmKey cursor=null;
        do{var page=new MongoAlertReads(db,host.scope(),states).evaluationStates(s,cursor,100);heads.addAll(page.rows());cursor=page.next();
            if(heads.size()>1000||heads.size()==1000&&cursor!=null)throw unreadable("planning-states-budget");}while(cursor!=null);
        for(var head:heads){budget.charge(MongoAlertStateEnvelope.head(head.value(),head.revision()));if(head.value().episode()!=null)budget.charge(MongoAlertStateEnvelope.episode(head.value().episode()));}
        var schedules=new LinkedHashMap<AlertSchedule.Key,Stored<AlertSchedule.Record>>();boolean isolated=false;
        var low=MongoAlertDeliveryCodec.scope(host.scope()).append("subjectKind",new org.bson.types.MinKey());
        var high=MongoAlertDeliveryCodec.scope(host.scope()).append("subjectKind",new org.bson.types.MaxKey());
        var rows=SystemCollections.ALERT_NOTIFICATION_SCHEDULES.on(db).find(s,new Document("_id",new Document("$gte",low).append("$lte",high))
                .append("batch",null).append("retiredAt",null)).hintString("_id_").limit(1001);
        int roots=0;try(var iterator=rows.iterator()){while(iterator.hasNext()){var row=iterator.next();if(++roots>1000)throw unreadable("planning-schedules-budget");budget.charge(row);
            try{var id=row.get("_id",Document.class);var key=MongoAlertReads.scheduleKey(id);var v=MongoAlertScheduleCodec.read(key,row);schedules.put(key,new Stored<>(v.recordRevision(),v.value()));}
            catch(TapstateException failure){if(failure.code()!=IoError.DOCUMENT_UNREADABLE)throw failure;isolated=true;}
            catch(IllegalArgumentException|NullPointerException malformed){isolated=true;}
        }}
        var intents=new LinkedHashMap<DeliveryKey,Stored<AlertIntent.Record>>();var episodes=new HashMap<FrozenGroupPolicy.Member,AlertStateRecord.Episode>();
        for(var row:List.copyOf(schedules.values())){try{
            for(var member:row.value().members())if(!episodes.containsKey(member.episodeMember())){var episode=states.episodeEnvelope(s,member.evaluation().key(),member.episode()).orElseThrow(()->unreadable("planning-episode"));budget.charge(MongoAlertStateEnvelope.episode(episode));episodes.put(member.episodeMember(),episode);}
            for(var key:row.value().protectedIntents()){
                planningIntent(s,key,intents,budget);
                if(key.notification().kind()==DeliveryKey.Kind.FAULT||key.notification().kind()==DeliveryKey.Kind.REMINDER){
                    var batchKey=AlertSchedule.Key.forDelivery(AlertSchedule.SubjectKind.GROUP,key);var batch=notifications.schedule(s,batchKey);
                    if(batch.isEmpty())throw unreadable("planning-batch");
                    if(batch.isPresent()&&!schedules.containsKey(batchKey)){
                        budget.charge(MongoAlertScheduleCodec.write(batch.get().value(),batch.get().recordRevision()));schedules.put(batchKey,new Stored<>(batch.get().recordRevision(),batch.get().value()));
                        var disposition=batch.get().value().disposition();if(disposition!=null&&disposition.successor()!=null)planningIntent(s,disposition.successor(),intents,budget);
                    }
                }
            }
        }catch(TapstateException failure){if(failure.code()!=IoError.DOCUMENT_UNREADABLE)throw failure;isolated=true;schedules.remove(row.value().key());}}
        for(var head:heads)if(head.value().episode()!=null)episodes.put(new FrozenGroupPolicy.Member(head.value().snapshot().rule().key(),head.value().episode().value().number()),head.value().episode());
        var limits=new HashMap<AlertTargetLimit.Domain,AlertTargetLimit>();
        for(var d:defs)if(d instanceof AlertReceiver receiver&&receiver.header().active()&&receiver.enabled())for(var target:AlertTargetIdentity.domains(host.scope(),receiver.transport()).values())notifications.limit(s,target).ifPresent(v->limits.put(target,v.value()));
        var dependencies=new HashSet<Dependency>();var active=heads.stream().map(v->v.value().snapshot()).filter(v->v.status()==AlertSnapshot.Status.FIRING).toList();
        var edges=new HashSet<AlertInhibitionPolicy.Edge>();for(var def:defs)if(def instanceof Subscription route)edges.addAll(route.inhibition());
        int pairs=0;for(var root:active)for(var dependent:active)if(!root.rule().key().typeId().equals(dependent.rule().key().typeId())
                &&edges.contains(new AlertInhibitionPolicy.Edge(root.rule().key().typeId(),dependent.rule().key().typeId()))){
            if(++pairs>10000)throw unreadable("planning-dependency-budget");
            if(dependency(s,root.rule().key(),dependent.rule().key(),false))dependencies.add(new Dependency(root.rule().key(),dependent.rule().key()));
        }
        return new Planning(new PlanVersion(version.getLong("revision"),version.getLong(ConfigurationVersion.ALERT_FENCE)),host,defs,heads,List.copyOf(schedules.values()),intents,episodes,limits,dependencies,isolated);
    });}
    private void planningIntent(ClientSession s,DeliveryKey key,Map<DeliveryKey,Stored<AlertIntent.Record>> intents,PlanningBudget budget){
        if(intents.containsKey(key))return;if(intents.size()>=1000)throw unreadable("planning-intents-budget");
        var value=notifications.requiredIntent(s,new AlertIntent.Business(key),1);budget.charge(MongoAlertDeliveryCodec.write(value.value(),value.recordRevision()));
        intents.put(key,new Stored<>(value.recordRevision(),value.value()));
    }
    private static final class PlanningBudget {
        private long bytes;
        void charge(Document row){bytes+=MongoAlertConfigurationCodec.size(row);if(bytes>16*1024*1024)throw unreadable("planning-bytes-budget");}
    }
    private void requireResourceRemoved(ClientSession s,AlarmKey.Resource resource){
        if(!resourceRemoved(s,resource))throw stale("resource-still-active-or-unproven");
    }
    private boolean sourceBindingSuperseded(ClientSession session,AlertStateRecord.Head head) {
        return head.operationalEvidence()!=null && head.operationalEvidence().version()==2
                && AlertOperationDomain.supported(head.snapshot().rule().key()) && sourceBindingChanged(session,head);
    }
    private boolean sourceBindingChanged(ClientSession session,AlertStateRecord.Head head) {
        var resource=head.snapshot().rule().key().resource();
        if(!resource.kind().equals("source") || head.binding()==null || head.binding().sourceBindingRevision()<1)return false;
        var row=SystemCollections.SOURCES.on(db).find(session,new Document("_id",resource.id())).first();
        if(row==null)return false;
        var actual=MongoSourceIdentity.read(row).binding();
        if(!actual.incarnation().equals(resource.incarnation()))return false;
        if(actual.bindingRevision()<head.binding().sourceBindingRevision())throw unreadable("source-binding-went-backwards");
        return actual.bindingRevision()>head.binding().sourceBindingRevision();
    }
    private boolean resourceRemoved(ClientSession s,AlarmKey.Resource resource){
        if(resource.kind().equals("pipeline")){
            var row=SystemCollections.OPERATOR_GENERATIONS.on(db).find(s,new Document("_id",resource.id())).first();
            if(row==null)throw unreadable("missing-retained-pipeline-identity");
            var mapping=MongoPipelineStateMappings.decode(resource.id(),row);
            return mapping.phase()!=PipelineStateMapping.Phase.ACTIVE||!Long.toString(mapping.stateGeneration()).equals(resource.incarnation());
        }else if(resource.kind().equals("source")){
            var row=SystemCollections.SOURCES.on(db).find(s,new Document("_id",resource.id())).first();
            return row==null||!MongoSourceIdentity.read(row).binding().incarnation().equals(resource.incarnation());
        }else if(resource.kind().equals("cluster-storage")){
            if(resource.id().equals("cache")) {
                var current=bindings.cachePreparationForTransaction(s).orElseThrow(()->stale("cache-binding"));
                return bindingRemoved(resource.incarnation(),current.authScope().cacheIncarnation(),current.authScope().bindingVersion());
            }
            var current=bindings.loadForTransaction(s).orElseThrow(()->stale("master-binding"));
            if(!current.defaultSourceId().equals(resource.id()))return true;
            var row=SystemCollections.SOURCES.on(db).find(s,new Document("_id",resource.id())).first();
            if(row==null)return true;
            var actual=MongoSourceIdentity.read(row).binding();
            return bindingRemoved(resource.incarnation(),actual.incarnation(),actual.bindingRevision());
        }else if(resource.kind().equals("cluster-node")){
            var proof=host.resourceEvidence().apply(resource).orElse(null);
            if(proof instanceof NodeEvidence node){nodeResource(s,resource,node);return !node.expected();}
            throw stale("node-lifecycle-unavailable");
        }else return false;
    }
    private static boolean bindingRemoved(String recorded,String incarnation,long revision) {
        String prefix=incarnation+":";if(!recorded.startsWith(prefix))return true;
        long previous;
        try{previous=Long.parseLong(recorded.substring(prefix.length()));}
        catch(NumberFormatException invalid){throw unreadable("storage-binding-revision");}
        if(previous<1 || revision<previous)throw unreadable("storage-binding-went-backwards");
        return revision>previous;
    }
    private void validateIntent(ClientSession s,AlertIntent.Frozen f,boolean grant,Access access) {
        scope(f.identity().scope());if(f.retention().deployment()!=host.deployment())throw stale("intent-profile");
        if(f.route() instanceof AlertIntent.ConfiguredRoute r&&!definitions.fenceSubscriptionBinding(s,r.identity(),r.bindingRevision()))throw stale("subscription-binding");
        if(f.target() instanceof AlertIntent.ReceiverTarget r){var captured=definitions.captureReceiver(s,r.identity());if(!AlertIntent.ReceiverTarget.capture(captured.receiver()).equals(r)||!definitions.fenceReceiverUse(s,captured))throw stale("receiver-binding");}
        else if(f.target() instanceof AlertIntent.CandidateTarget c){
            if(f.testAuthority()==null||access==null||!f.testAuthority().equals(access.actor()))throw stale("test-caller-continuation");
            if(c.existingReceiver()!=null){var saved=definitions.captureReceiver(s,c.existingReceiver());if(saved.receiver().bindingRevision()!=c.receiverBindingRevision()
                    ||((AlertIntent.Test)f.identity()).command().parameters().receiver().expectedRevision()!=saved.receiver().header().revision()
                    ||!definitions.fenceReceiverUse(s,saved))throw stale("candidate-receiver-binding");}
        } else if(!f.route().equals(host.managedRoute())||!f.target().equals(host.managedTarget())||host.deployment()!=AlertChannelPolicy.Deployment.CLOUD_MANAGED)throw stale("managed-sdk-profile");
        for(var member:f.members()){
            validateMember(s,member,grant);
            if(member.episode()>0){var episode=states.episodeEnvelope(s,member.evaluation().key(),member.episode()).orElseThrow(()->stale("episode"));
                if(!episode.originalBinding().equals(member.evaluation()))throw stale("episode-binding");
                if(grant&&f.identity() instanceof AlertIntent.Business business){
                    var kind=business.key().notification().kind();
                    if(kind==DeliveryKey.Kind.FAULT||kind==DeliveryKey.Kind.REMINDER){var head=states.loadEnvelope(s,member.evaluation().key()).orElseThrow(()->stale("current-episode")).value();
                        if(!head.snapshot().enabled()||head.snapshot().status()!=AlertSnapshot.Status.FIRING||head.snapshot().episode()==null
                                ||head.snapshot().episode().number()!=member.episode()||!head.snapshot().rule().equals(member.evaluation()))throw stale("episode-no-longer-active");
                    }else if(kind==DeliveryKey.Kind.RECOVERY&&episode.value().endReason()!=AlertSnapshot.EndReason.RECOVERED)throw stale("recovery-evidence");
                }
            }
        }
        if(grant&&f.identity() instanceof AlertIntent.Business)requireNotificationEligibility(s,f);
        if(host.deployment()==AlertChannelPolicy.Deployment.ON_PREM)ensureTargetHead(s,f);
        if(grant&&!host.availableChannels().contains(f.channel()))throw new TapstateException(AlertError.CHANNEL_UNAVAILABLE,Map.of("channel",f.channel().typeId(),"phase","permission"),null);
        if(grant&&!clock.instant().isBefore(f.deadline()))throw stale("intent-deadline");
        target(f);
    }
    private void validateMember(ClientSession s,AlertIntent.Member member,boolean fence) {
        var current=definitions.load(s,Kind.RULE,member.ruleIdentity().id()).orElseThrow(()->stale("rule"));
        if(!(current.definition() instanceof Rule r)||!r.header().identity().equals(member.ruleIdentity())||!r.header().active()||!r.enabled()
                ||!AlertOperationDomain.belongs(member.ruleIdentity().incarnation(),member.evaluation().key())||r.header().revision()!=member.evaluation().revision()||!r.matches(member.evaluation().key())||!definitions.fence(s,r))throw stale("rule-binding");
        resource(s,member.evaluation().key().resource(),member.sourceBindingRevision(),fence);
        if(member.evaluation().key().resource().kind().equals("cluster-node")){var proof=host.resourceEvidence().apply(member.evaluation().key().resource()).orElseThrow(()->stale("member-proof"));
            if(!(proof instanceof NodeEvidence n)||!n.producerIncarnation().equals(member.evaluation().producerIncarnation()))throw stale("member-producer");nodeResource(s,member.evaluation().key().resource(),n);}
    }
    private void requireDomainCapacity(ClientSession session,AlertStateRecord.Head head) {
        var key=head.snapshot().rule().key();String rule=head.binding().ruleIdentity().incarnation();
        if(key.qualifier().equals(rule))return;
        if(!AlertOperationDomain.belongs(rule,key) || head.operationalEvidence()==null || head.operationalEvidence().version()!=2)throw stale("operation-domain-key");
        var lower=MongoAlertStateCodec.key(key);lower.put("qualifier",rule);
        var upper=new Document(lower);upper.put("qualifier",rule+"\uffff");
        long retained=SystemCollections.ALERT_INSTANCES.on(db).countDocuments(session,new Document("_id",new Document("$gte",lower).append("$lt",upper)),
                new com.mongodb.client.model.CountOptions().limit(AlertOperationDomain.MAX_RETAINED+1));
        if(retained>=AlertOperationDomain.MAX_RETAINED)throw new TapstateException(AlertError.INVALID_CONFIGURATION,
                Map.of("field","operationDomains","reason","capacity"),null);
    }
    private void ensureTargetHead(ClientSession s,AlertIntent.Frozen value){
        var d=target(value);if(notifications.limit(s,d).isPresent())return;
        for(var original:new MongoAlertReads(db,host.scope(),states).intentSnapshot(s,1000))if(target(original.frozen()).equals(d))throw unreadable("missing-original-target-limit");
        outcome(notifications.stageLimit(s,null,new AlertTargetLimit(d,value.createdAt())));
    }
    private AlertTargetLimit.Domain target(AlertIntent.Frozen f) {
        AlertReceiver.Transport transport=f.target() instanceof AlertIntent.ReceiverTarget r?r.transport():f.target() instanceof AlertIntent.CandidateTarget c?c.transport():null;
        if(transport!=null){var d=AlertTargetIdentity.domains(host.scope(),transport).get(f.identity().targetId());if(d==null)throw stale("semantic-target");return d;}
        var sdk=(AlertIntent.SdkTarget)f.target();if(!sdk.logicalTargetId().equals(f.identity().targetId()))throw stale("sdk-target");
        return new AlertTargetLimit.Domain(host.scope(),f.channel().typeId(),sdk.logicalTargetId(),sdk.clusterBinding());
    }
    private void resource(ClientSession s,AlarmKey.Resource r,long bindingRevision,boolean fence) {
        if(r.kind().equals("source")){
            Document row=SystemCollections.SOURCES.on(db).find(s,new Document("_id",r.id())).first();if(row==null)throw stale("source");
            var id=MongoSourceIdentity.read(row).binding();if(!id.incarnation().equals(r.incarnation())||bindingRevision>0&&id.bindingRevision()!=bindingRevision)throw stale("source-incarnation");
            if(fence&&!artifacts.fenceSourceBinding(s,id))throw stale("source-binding");
        } else if(r.kind().equals("pipeline")){
            var state=MongoPipelineStateMappings.decode(r.id(),SystemCollections.OPERATOR_GENERATIONS.on(db).find(s,new Document("_id",r.id())).first());
            if(state.phase()!=PipelineStateMapping.Phase.ACTIVE||!Long.toString(state.stateGeneration()).equals(r.incarnation())||artifacts.getInTransaction(s,r.id()).isEmpty())throw stale("pipeline-incarnation");
        } else if(r.kind().equals("cluster")){if(!r.id().equals(host.scope().clusterId())||!r.incarnation().equals(host.setup().installationId()))throw stale("cluster-incarnation");}
        else if(r.kind().equals("cluster-node")||r.kind().equals("cluster-storage")) {
            var proof=host.resourceEvidence().apply(r).orElseThrow(()->stale("resource-capability-unavailable"));if(!r.equals(proof.resource()))throw stale("foreign-resource-proof");
            if(proof instanceof NodeEvidence n){nodeResource(s,r,n);if(!n.expected())throw stale("retired-member");}
            else if(proof instanceof StorageEvidence storage){
                if(storage.role()==StorageRole.CACHE){var prepared=bindings.cachePreparationForTransaction(s).orElseThrow(()->stale("cache-binding"));
                    if(!storage.cache().equals(prepared.authScope())||!r.id().equals("cache")||!r.incarnation().equals(storage.cache().cacheIncarnation()+":"+storage.cache().bindingVersion()))throw stale("cache-binding");}
                else {var actual=bindings.loadForTransaction(s).orElseThrow(()->stale("master-binding"));var id=storage.master();
                    if(!actual.defaultSourceId().equals(id.sourceId())||!r.id().equals(id.sourceId())||!r.incarnation().equals(id.incarnation()+":"+id.bindingRevision())
                            ||!artifacts.fenceSourceBinding(s,id))throw stale("master-binding");}
            } else throw stale("unsupported-resource-proof");
        }
        else throw stale("unsupported-resource-proof");
    }
    private void nodeResource(ClientSession session,AlarmKey.Resource resource,NodeEvidence evidence) {
        evidence.requireCurrent().run();
        var row=nodes.load(session,new AlertNodeLifecycleStore.Key(host.scope(),resource.id(),resource.incarnation()))
                .orElseThrow(()->stale("missing-node-lifecycle"));
        var registration=row.registration();
        if(evidence.lifecycleRevision()!=row.revision() || !evidence.producerIncarnation().equals(registration.producerIncarnation())
                || !nodes.currentRegistration(session,registration))throw stale("node-lifecycle-binding");
        if(evidence.expected()) {
            if(nodes.successor(session,row).isPresent())throw stale("superseded-node");
        }else if(row.phase()!=AlertNodeLifecycleStore.Phase.RETIRING) {
            if(evidence.supersedingBootId()==null)throw stale("unproven-node-retirement");
            var successor=nodes.load(session,new AlertNodeLifecycleStore.Key(host.scope(),resource.id(),evidence.supersedingBootId()))
                    .orElseThrow(()->stale("missing-successor-node"));
            if(!successor.registration().setup().equals(registration.setup()) || !successor.registration().cache().equals(registration.cache())
                    || successor.registration().session().claimGeneration()<=registration.session().claimGeneration())throw stale("unproven-successor-node");
        }
        evidence.requireCurrent().run();
    }
    @Override public PrivateMaterial resolvePermittedMaterial(AlertIntent.Identity id,DeliveryAttempt.Permit permit,Access access) {
        var value=tx(access,true,false,s->{var stored=notifications.requiredIntent(s,id,1);var v=stored.value();
            if(!clock.instant().isBefore(v.frozen().deadline())||!clock.instant().isBefore(permit.deadline())||!v.state().owner().owns(permit,clock.instant())
                    ||v.state().attempts().stream().noneMatch(a->a.permit().equals(permit)&&a.endedAt()==null))throw stale("original-permit");
            validateIntent(s,v.frozen(),true,access);return v;});
        var cipher=keys.refresh(new MongoReadBudget(Duration.ofSeconds(5)));
        materialTime(value,permit,access);
        if(value.frozen().target() instanceof AlertIntent.CandidateTarget c){try(var secret=MongoAlertCandidateSecrets.resolve(((AlertIntent.Test)id).command(),c,value.state().candidate(),cipher)){materialTime(value,permit,access);return copy(secret);}}
        if(value.frozen().target() instanceof AlertIntent.ReceiverTarget r){var capture=tx(access,true,false,s->{validateIntent(s,value.frozen(),true,access);return definitions.captureReceiver(s,r.identity());});materialTime(value,permit,access);try(var secret=MongoAlertReceiverSecrets.resolve(capture.receiver(),capture.privateSlot(),cipher)){materialTime(value,permit,access);return copy(secret);}}
        return new PrivateMaterial(null,null,null);
    }
    private void materialTime(AlertIntent.Record value,DeliveryAttempt.Permit permit,Access access){
        host.locallyActive().run();if(access!=null)access.locallyActive().run();
        if(!clock.instant().isBefore(value.frozen().deadline())||!clock.instant().isBefore(permit.deadline())||!value.state().owner().owns(permit,clock.instant()))throw stale("material-permit-cutoff");
    }
    private void requireNotificationEligibility(ClientSession s,AlertIntent.Frozen value){
        for(var definition:definitionSnapshot(s,Kind.SILENCE)){
            var silence=(Silence)definition;for(var member:value.members())if(silence.matches(member.evaluation().key(),clock.instant()))throw stale("notification-silenced");
        }
        if(value.route() instanceof AlertIntent.ConfiguredRoute r){var subscription=(Subscription)definitions.load(s,Kind.SUBSCRIPTION,r.identity().id()).orElseThrow(()->stale("subscription")).definition();
            for(var member:value.members()){
                var episode=states.episodeEnvelope(s,member.evaluation().key(),member.episode());
                AlarmSeverity severity=episode.isEmpty()?AlarmSeverity.INDETERMINATE:episode.get().value().severity();
                if(!subscription.matches(member.evaluation().key(),severity))throw stale("subscription-no-longer-matches");
            }
            if(!subscription.inhibition().isEmpty()){
                var active=new MongoAlertReads(db,host.scope(),states).activeSnapshot(s,1000);
                var policy=new AlertInhibitionPolicy(host.scope(),subscription.inhibition());
                for(var member:value.members()){
                    var dependent=states.loadEnvelope(s,member.evaluation().key()).orElseThrow(()->stale("dependent-state")).value().snapshot();
                    for(var root:active)if(policy.suppresses(root.snapshot(),dependent,dependency(s,root.snapshot().rule().key(),dependent.rule().key())))throw stale("notification-inhibited");
                }
            }
        }
    }
    private List<Definition> definitionSnapshot(ClientSession s,Kind kind){
        var values=new ArrayList<Definition>();String cursor=null;
        do{var page=new MongoAlertReads(db,host.scope(),states).definitions(s,kind,cursor,100);values.addAll(page.rows());cursor=page.nextId();
            if(values.size()>1000||values.size()==1000&&cursor!=null)throw unreadable("policy-read-budget");}while(cursor!=null);return List.copyOf(values);
    }
    private boolean dependency(ClientSession s,AlarmKey root,AlarmKey dependent){return dependency(s,root,dependent,true);}
    private boolean dependency(ClientSession s,AlarmKey root,AlarmKey dependent,boolean fence){
        if(root.resource().equals(dependent.resource()))return true;
        if(root.resource().kind().equals("source")&&dependent.resource().kind().equals("pipeline")){
            var resource=artifacts.getInTransaction(s,dependent.resource().id());
            if(resource.isPresent()&&resource.get() instanceof io.tapstate.core.model.PipelineResource pipeline&&pipeline.sourceIds().contains(root.resource().id())){
                resource(s,root.resource(),0,fence);resource(s,dependent.resource(),0,false);return true;
            }
        }
        return false;
    }
    @Override public EndedPage ended(EpisodeCursor after,int limit,ReadAccess access,AlertSnapshot.EndReason reason){access.requireActive().run();var result=tx(null,false,false,s->new MongoAlertReads(db,host.scope(),states).ended(s,after,limit,access,reason));access.requireActive().run();return result;}
    @Override public DeliveryPage deliveries(String after,int limit, String channel,DeliveryEvidence.Outcome outcome,ReadAccess access){access.requireActive().run();var result=tx(null,false,false,s->new MongoAlertReads(db,host.scope(),states).deliveries(s,after,limit,channel,outcome,access));access.requireActive().run();return result;}
    @Override public Optional<Stored<AlertIntent.Record>> delivery(String id,ReadAccess access){access.requireActive().run();var result=tx(null,false,false,s->new MongoAlertReads(db,host.scope(),states).delivery(s,id,access));access.requireActive().run();return result;}
    @Override public ActivePage active(ActiveCursor after,int limit,ReadAccess access){access.requireActive().run();return tx(null,false,false,s->new MongoAlertReads(db,host.scope(),states).active(s,after,limit,access));}
    @Override public ActivePage activeFiltered(ActiveCursor after,int limit,ReadAccess access,AlarmFilter filter){access.requireActive().run();return tx(null,false,false,s->new MongoAlertReads(db,host.scope(),states).active(s,after,limit,access,filter));}
    @Override public HistoryPage history(AlarmKey key,Long after,int limit,ReadAccess access){visible(key,access);var result=tx(null,false,false,s->new MongoAlertReads(db,host.scope(),states).history(s,key,after,limit));access.requireActive().run();return result;}
    @Override public TransitionPage transitions(AlarmKey key,TransitionCursor after,int limit,ReadAccess access){visible(key,access);var result=tx(null,false,false,s->new MongoAlertReads(db,host.scope(),states).transitions(s,key,after,limit));access.requireActive().run();return result;}
    @Override public DuePage dueIntents(Instant through,DueCursor after,int limit){return tx(null,false,false,s->new MongoAlertReads(db,host.scope(),states).due(s,through,after,limit));}
    @Override public SchedulePage dueSchedules(Instant through,ScheduleCursor after,int limit){return tx(null,false,false,s->new MongoAlertReads(db,host.scope(),states).schedules(s,through,after,limit));}
    private void visible(AlarmKey key,ReadAccess access){scope(key.scope());access.requireActive().run();if(!access.permits(key))throw stale("resource-visibility");}
    private void scope(AlarmKey.Scope scope){if(!host.scope().equals(scope))throw stale("scope");}
    private static ReceiverSecretMaterial retainedMaterial(MongoAlertConfigurationStore.ReceiverCapture captured,AlertReceiver.Transport transport,
            PrivateMaterial replacement,Set<PrivateField> retain,SourceConfigCipher cipher){
        if(captured==null)throw stale("candidate-original-receiver");
        try(var original=MongoAlertReceiverSecrets.resolve(captured.receiver(),captured.privateSlot(),cipher)){
            byte[] a=replacement==null?null:replacement.authentication(),c=replacement==null?null:replacement.privateCa();
            try{
                if(retain.contains(PrivateField.AUTHENTICATION)){
                    if(a!=null||!AlertReceiver.authenticationIdentity(captured.receiver().transport()).equals(AlertReceiver.authenticationIdentity(transport)))throw stale("retained-authentication-binding");
                    a=original.authentication();
                }
                if(retain.contains(PrivateField.PRIVATE_CA)){
                    if(c!=null||captured.receiver().transport().trust()!=AlertReceiver.Trust.PRIVATE_CA||transport.trust()!=AlertReceiver.Trust.PRIVATE_CA)throw stale("retained-trust-binding");c=original.privateCa();
                }
                if(retain.contains(PrivateField.ENDPOINT) && (replacement!=null&&replacement.endpoint()!=null
                        || !(transport instanceof AlertReceiver.Webhook webhook) || original.webhookEndpoint()==null
                        || !webhook.matchesEndpoint(original.webhookEndpoint()))) throw stale("retained-endpoint-binding");
                var endpoint=replacement!=null&&replacement.endpoint()!=null?replacement.endpoint():original.webhookEndpoint();
                return new ReceiverSecretMaterial(a,c,endpoint);
            }finally{if(a!=null)Arrays.fill(a,(byte)0);if(c!=null)Arrays.fill(c,(byte)0);}
        }
    }
    private static ReceiverSecretMaterial material(PrivateMaterial value){if(value==null)return null;byte[] a=value.authentication(),c=value.privateCa();try{return new ReceiverSecretMaterial(a,c,value.endpoint());}finally{if(a!=null)Arrays.fill(a,(byte)0);if(c!=null)Arrays.fill(c,(byte)0);}}
    private static PrivateMaterial copy(ReceiverSecretMaterial value){byte[] a=value.authentication(),c=value.privateCa();try{return new PrivateMaterial(a,c,value.webhookEndpoint());}finally{if(a!=null)Arrays.fill(a,(byte)0);if(c!=null)Arrays.fill(c,(byte)0);}}
    private static Outcome outcome(Enum<?> value){if(value.name().equals("CONFLICT"))throw new Conflict();return Outcome.valueOf(value.name());}
    private static final class Conflict extends RuntimeException { }
    private static TapstateException stale(String action){return new TapstateException(AlertError.STALE_ACTION,Map.of("action",action),null);}
    private static TapstateException unreadable(String field){return new TapstateException(IoError.DOCUMENT_UNREADABLE,Map.of("id","alert-store","field",field),null);}
}
