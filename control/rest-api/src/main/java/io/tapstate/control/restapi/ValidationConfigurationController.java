package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.ValidationConfigurationService;
import io.tapstate.control.core.ValidationExecutionService;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import tools.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;

/** Authenticated direct FULL authoring. The metadata lease remains charged through serialization. */
@RestController
class ValidationConfigurationController {
    private static final int MAX_REQUEST_BYTES=256*1024;
    private final ObjectProvider<ValidationConfigurationService> services;private final ObjectProvider<ValidationExecutionService> executions;private final ObjectMapper json;private final ObjectMapper requests;
    ValidationConfigurationController(ObjectProvider<ValidationConfigurationService> services,ObjectProvider<ValidationExecutionService> executions,ObjectMapper json){this.services=services;this.executions=executions;this.json=json;
        requests=json.rebuild().enable(tools.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                    tools.jackson.databind.DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
                    tools.jackson.databind.DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
                .withCoercionConfig(tools.jackson.databind.type.LogicalType.Textual,config->{
                    for(var shape:java.util.List.of(tools.jackson.databind.cfg.CoercionInputShape.Integer,tools.jackson.databind.cfg.CoercionInputShape.Float,
                            tools.jackson.databind.cfg.CoercionInputShape.Boolean))config.setCoercion(shape,tools.jackson.databind.cfg.CoercionAction.Fail);
                }).withCoercionConfig(tools.jackson.databind.type.LogicalType.Integer,config->{
                    for(var shape:java.util.List.of(tools.jackson.databind.cfg.CoercionInputShape.Float,tools.jackson.databind.cfg.CoercionInputShape.String,
                            tools.jackson.databind.cfg.CoercionInputShape.Boolean))config.setCoercion(shape,tools.jackson.databind.cfg.CoercionAction.Fail);
                }).withCoercionConfig(tools.jackson.databind.type.LogicalType.Boolean,config->{
                    for(var shape:java.util.List.of(tools.jackson.databind.cfg.CoercionInputShape.Integer,tools.jackson.databind.cfg.CoercionInputShape.Float,
                            tools.jackson.databind.cfg.CoercionInputShape.String))config.setCoercion(shape,tools.jackson.databind.cfg.CoercionAction.Fail);
                }).build();
    }
    @Verb("validation.evaluate") @PostMapping(value="/validation:evaluate",consumes="application/json",produces="application/json")
    void evaluate(HttpServletRequest request,HttpServletResponse response)throws IOException{
        var input=body(request,ValidationConfigurationService.Input.class);
        try(var result=input(()->service().evaluate(input))){
            response.setHeader(HttpHeaders.CACHE_CONTROL,"no-store");response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            json.writeValue(response.getOutputStream(),result.view());
        }
    }
    @Verb("validation.save") @PostMapping(value="/validation:save",consumes="application/json",produces="application/json")
    ResponseEntity<ValidationConfigurationService.Saved> save(HttpServletRequest request)throws IOException{
        var command=body(request,ValidationConfigurationService.Save.class);
        var result=input(()->service().save(AuthenticatedCaller.subject(),command));
        return ResponseEntity.status(switch(result.outcome()){case "APPLIED","DUPLICATE"->HttpStatus.OK;default->HttpStatus.CONFLICT;})
                .cacheControl(CacheControl.noStore()).body(result);
    }
    @Verb("validation.get") @GetMapping("/pipelines/{pipeline}/validation-tasks/{task}")
    ResponseEntity<ValidationConfigurationService.Task> read(@PathVariable("pipeline") String pipeline,@PathVariable("task") String task){
        return service().read(pipeline,task).map(value->ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(value))
                .orElseGet(()->ResponseEntity.notFound().header(HttpHeaders.CACHE_CONTROL,"no-store").build());
    }
    record PreviewRequest(ValidationConfigurationService.Schedule schedule,String inputRevision){}
    @Verb("validation.schedule-preview") @PostMapping(value="/validation:schedule-preview",consumes="application/json",produces="application/json")
    ResponseEntity<ValidationConfigurationService.Preview> preview(HttpServletRequest request)throws IOException{
        var input=body(request,PreviewRequest.class);
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(input(()->service().preview(input.schedule(),input.inputRevision())));
    }
    @Verb("validation.schedule-disable") @PostMapping(value="/validation:schedule-disable",consumes="application/json",produces="application/json")
    ResponseEntity<ValidationConfigurationService.Disabled> disable(HttpServletRequest request)throws IOException{
        var command=body(request,ValidationConfigurationService.Disable.class);var result=input(()->service().disable(AuthenticatedCaller.subject(),command));
        return ResponseEntity.status(switch(result.outcome()){case "APPLIED","DUPLICATE"->HttpStatus.OK;default->HttpStatus.CONFLICT;}).cacheControl(CacheControl.noStore()).body(result);
    }
    @Verb("validation.start") @PostMapping(value="/validation:start",consumes="application/json",produces="application/json")
    void start(HttpServletRequest request,HttpServletResponse response)throws IOException{
        var original=body(request,ValidationExecutionService.Input.class);input(()->{ValidationExecutionService.validate(original);return null;});
        try(var result=execution().start(AuthenticatedCaller.subject(),original)){write(response,result.view());}
    }
    @Verb("validation.progress") @PostMapping(value="/validation:progress",consumes="application/json",produces="application/json")
    void progress(HttpServletRequest request,HttpServletResponse response)throws IOException{
        var original=body(request,ValidationExecutionService.Input.class);input(()->{ValidationExecutionService.validate(original);return null;});
        try(var result=execution().read(original)){write(response,result.view());}
    }
    @Verb("validation.stop") @PostMapping(value="/validation:stop",consumes="application/json",produces="application/json")
    void stop(HttpServletRequest request,HttpServletResponse response)throws IOException{
        var original=body(request,ValidationExecutionService.Input.class);input(()->{ValidationExecutionService.validate(original);return null;});
        try(var result=execution().stop(AuthenticatedCaller.subject(),original)){write(response,result.view());}
    }
    private void write(HttpServletResponse response,ValidationExecutionService.View value)throws IOException{
        response.setHeader(HttpHeaders.CACHE_CONTROL,"no-store");response.setContentType(MediaType.APPLICATION_JSON_VALUE);json.writeValue(response.getOutputStream(),value);
    }
    private ValidationExecutionService execution(){
        try{var service=executions.getIfAvailable();if(service==null)throw new TapstateException(ControlError.STORAGE_UNAVAILABLE,Map.of(),null);return service;}
        catch(org.springframework.beans.BeansException failed){for(Throwable cause=failed;cause!=null;cause=cause.getCause())if(cause instanceof TapstateException coded)throw coded;throw failed;}
    }
    private <T>T body(HttpServletRequest request,Class<T> type)throws IOException{
        if(request.getContentLengthLong()>MAX_REQUEST_BYTES)throw MalformedRequest.rejecting("validation request exceeds 256 KiB",null);
        byte[] bytes=request.getInputStream().readNBytes(MAX_REQUEST_BYTES+1);
        if(bytes.length>MAX_REQUEST_BYTES)throw MalformedRequest.rejecting("validation request exceeds 256 KiB",null);
        try{var value=requests.treeToValue(requests.readTree(bytes),type);if(value==null)throw MalformedRequest.rejecting("validation request is null",null);return value;}
        catch(tools.jackson.core.JacksonException bad){throw MalformedRequest.rejecting("validation request does not match its shape",bad);}
    }
    private static <T>T input(java.util.function.Supplier<T> action){
        try{return action.get();}catch(ValidationConfigurationService.StaleDefinition stale){throw stale;}
        catch(TapstateException failed){
            if(failed.code()==ValidationError.INVALID_CONTRACT||failed.code()==ValidationError.RULE_UNSUPPORTED
                    ||failed.code()==ValidationError.SCHEDULE_INVALID||failed.code()==ValidationError.SCHEDULE_FIELD_COUNT
                    ||failed.code()==ValidationError.SCHEDULE_NO_DATE)throw new BadRequestCodedException(failed);
            throw failed;
        }
    }
    private ValidationConfigurationService service(){
        try{var service=services.getIfAvailable();if(service==null)throw new TapstateException(ControlError.STORAGE_UNAVAILABLE,Map.of(),null);return service;}
        catch(org.springframework.beans.BeansException failed){
            for(Throwable cause=failed;cause!=null;cause=cause.getCause())if(cause instanceof TapstateException coded)throw coded;
            throw failed;
        }
    }
}
