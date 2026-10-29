package io.tapstate.app;

import io.tapstate.core.alerting.*;
import java.math.BigDecimal;
import java.time.*;
import java.util.*;
import static io.tapstate.app.AlertHttpModel.*;

/** Pure virtual-clock examples over classified synthetic conclusions; never a source probe or publication. */
final class AlertRulePreview {
    private AlertRulePreview() { }
    static Map<String,Object> evaluate(Map<String,Object> input,AlarmKey.Scope scope,AlarmTypeCatalog catalog,
            AlertChannelPolicy.Deployment deployment,String actor,Instant now) {
        fields(input,"form","sample");var form=object(input.get("form"));String sample=text(input,"sample");
        if(!Set.of("BAD","GOOD","MISSING").contains(sample))throw invalid("sample");
        var parameters=new LinkedHashMap<String,BigDecimal>();object(form.getOrDefault("parameters",Map.of())).forEach((key,value)->{
            try{if(value==null)throw AlertHttpModel.invalid("parameters");parameters.put(key,new BigDecimal(value.toString()));}catch(NumberFormatException invalid){throw AlertHttpModel.invalid("parameters");}
        });
        var header=AlertConfiguration.Header.create(AlertConfiguration.Identity.create(AlertConfiguration.Kind.RULE,scope,"synthetic-preview"),deployment,now,actor);
        var severity=form.get("severity")==null||"DEFAULT".equals(form.get("severity"))?null:value(AlarmSeverity.class,text(form,"severity"));
        var definition=new AlertConfiguration.Rule(header,"Synthetic example",text(form,"typeId"),integer(form,"typeVersion",1),AlertAuthoring.resources(form),parameters,severity,true,catalog.metric(text(form,"typeId"),parameters)).validated(catalog);
        var key=new AlarmKey(scope,new AlarmKey.Resource(definition.resources().kinds().stream().sorted().findFirst().orElseThrow(),"synthetic-example","synthetic-example"),definition.typeId(),header.identity().incarnation());
        long trigger=definition.parameters().getOrDefault("trigger-for",BigDecimal.ZERO).longValueExact();long recover=definition.parameters().getOrDefault("resolve-for",BigDecimal.ZERO).longValueExact();
        var rule=new AlertRule(key,1,1,"synthetic-example",Duration.ofSeconds(trigger),Duration.ofSeconds(recover),Duration.ofSeconds(15),Duration.ofSeconds(5));
        var effective=definition.effectiveSeverity(catalog);var state=AlertSnapshot.initial(rule);AlertSnapshot.Status first=null;
        for(long second=0;second<=trigger;second++){var at=now.plusSeconds(second);state=AlertLifecycle.evaluate(state,new AlertSignal(key,1,1,"synthetic-example",second,at,AlertSignal.Condition.BAD,effective),at).snapshot();if(first==null)first=state.status();}
        long elapsed=trigger;
        if(sample.equals("GOOD")){for(long second=0;second<=recover;second++){var at=now.plusSeconds(trigger+1+second);state=AlertLifecycle.evaluate(state,new AlertSignal(key,1,1,"synthetic-example",state.lastSequence()+1,at,AlertSignal.Condition.GOOD,effective),at).snapshot();}elapsed=trigger+1+recover;}
        else if(sample.equals("MISSING")){elapsed=trigger+1;var at=now.plusSeconds(elapsed);state=AlertLifecycle.evaluate(state,new AlertSignal(key,1,1,"synthetic-example",state.lastSequence()+1,at,AlertSignal.Condition.UNKNOWN,effective),at).snapshot();}
        return map("synthetic",true,"sourceVerified",false,"persisted",false,"sent",false,"initialStatus",first.name(),"status",state.status().name(),"quality",state.quality().name(),"severity",effective.name(),"virtualElapsedSeconds",elapsed);
    }
}
