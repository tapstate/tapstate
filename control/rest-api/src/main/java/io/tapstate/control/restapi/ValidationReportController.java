package io.tapstate.control.restapi;

import io.tapstate.control.core.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import tools.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;

/** Pure CONFIG report face. Its response frame remains charged until serialization actually ends. */
@RestController
class ValidationReportController {
    private final ObjectProvider<ValidationReportService> services;private final ObjectMapper json;
    ValidationReportController(ObjectProvider<ValidationReportService> services,ObjectMapper json){this.services=services;this.json=json;}
    @Verb("validation.history") @GetMapping("/pipelines/{pipeline}/validation-tasks/{task}/reports")
    void history(@PathVariable("pipeline") String pipeline,@PathVariable("task") String task,
            @RequestParam(value="offset",defaultValue="0") String offset,@RequestParam(value="limit",defaultValue="10") String limit,HttpServletResponse response)throws IOException{
        try(var report=input(()->service().history(pipeline,task,offset,limit))){write(response,report.value());}
    }
    @Verb("validation.report") @GetMapping("/pipelines/{pipeline}/validation-tasks/{task}/reports/{run}")
    void report(@PathVariable("pipeline") String pipeline,@PathVariable("task") String task,@PathVariable("run") String run,
            @RequestParam(value="generation",required=false) String generation,@RequestParam(value="sequence",required=false) String sequence,HttpServletResponse response)throws IOException{
        try(var report=input(()->service().report(new ValidationReportService.Reference(pipeline,task,generation,sequence,run)))){write(response,report.value());}
    }
    @Verb("validation.roots") @GetMapping("/pipelines/{pipeline}/validation-tasks/{task}/reports/{run}/roots")
    void roots(@PathVariable("pipeline") String pipeline,@PathVariable("task") String task,@PathVariable("run") String run,
            @RequestParam(value="generation",required=false) String generation,@RequestParam(value="sequence",required=false) String sequence,
            @RequestParam(value="offset",defaultValue="0") String offset,@RequestParam(value="limit",defaultValue="10") String limit,HttpServletResponse response)throws IOException{
        try(var report=input(()->service().roots(new ValidationReportService.Reference(pipeline,task,generation,sequence,run),offset,limit))){write(response,report.value());}
    }
    @Verb("validation.root") @GetMapping("/pipelines/{pipeline}/validation-tasks/{task}/reports/{run}/roots/{root}")
    void root(@PathVariable("pipeline") String pipeline,@PathVariable("task") String task,@PathVariable("run") String run,@PathVariable("root") String root,
            @RequestParam(value="generation",required=false) String generation,@RequestParam(value="sequence",required=false) String sequence,HttpServletResponse response)throws IOException{
        try(var report=input(()->service().root(new ValidationReportService.Reference(pipeline,task,generation,sequence,run),root))){write(response,report.value());}
    }
    private void write(HttpServletResponse response,Object value)throws IOException{
        response.setHeader(HttpHeaders.CACHE_CONTROL,"no-store");
        if(value==null){response.setStatus(404);return;}response.setContentType(MediaType.APPLICATION_JSON_VALUE);json.writeValue(response.getOutputStream(),value);
    }
    private static <T>T input(java.util.function.Supplier<T> query){
        try{return query.get();}catch(TapstateException failed){
            if(failed.code()==ValidationError.INVALID_CONTRACT&&String.valueOf(failed.args().get("field")).startsWith("report.request."))throw new BadRequestCodedException(failed);throw failed;
        }
    }
    private ValidationReportService service(){
        try{var service=services.getIfAvailable();if(service==null)throw new TapstateException(ControlError.STORAGE_UNAVAILABLE,Map.of(),null);return service;}
        catch(org.springframework.beans.BeansException failed){for(Throwable cause=failed;cause!=null;cause=cause.getCause())if(cause instanceof TapstateException coded)throw coded;throw failed;}
    }
}
