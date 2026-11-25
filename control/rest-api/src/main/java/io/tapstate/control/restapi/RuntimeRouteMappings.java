package io.tapstate.control.restapi;

import io.tapstate.control.core.OperationRegistry;
import jakarta.servlet.http.HttpServletRequest;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.springframework.context.annotation.Import;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.method.HandlerTypePredicate;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;
import org.springframework.web.util.ServletRequestPathUtils;
import org.springframework.web.util.pattern.PathPattern;
import org.springframework.web.util.pattern.PathPatternParser;

/** Declaration-only matching for unavailable runtimes; no controller instance is created or invoked. */
final class RuntimeRouteMappings extends RequestMappingHandlerMapping {
    private final OperationRegistry operations;
    private final List<PathPattern> streams = List.of(
            PathPatternParser.defaultInstance.parse(PipelineStreamConfiguration.STATUS_WATCH_PATH),
            PathPatternParser.defaultInstance.parse(PipelineStreamConfiguration.LOGS_FOLLOW_PATH),
            PathPatternParser.defaultInstance.parse(DataBrowserStreamConfiguration.TAIL_PATH));

    RuntimeRouteMappings(OperationRegistry operations) {
        this.operations = operations;
        setPathPrefixes(Map.of("/api", HandlerTypePredicate.forAnnotation(RestController.class)));
        afterPropertiesSet();
    }

    @Override
    protected void initHandlerMethods() {
        for (Class<?> type : ControlHttpFace.class.getAnnotation(Import.class).value()) {
            if (!AnnotatedElementUtils.hasAnnotation(type, RestController.class)) continue;
            Map<Method, RequestMappingInfo> mappings = MethodIntrospector.selectMethods(type,
                    (MethodIntrospector.MetadataLookup<RequestMappingInfo>) method -> getMappingForMethod(method, type));
            mappings.forEach((method, mapping) -> {
                var verb = AnnotatedElementUtils.findMergedAnnotation(method, Verb.class);
                if (verb == null) throw new IllegalStateException("an API route must declare its operation: " + method);
                operations.resolve(verb.value());
                // The type is only a metadata holder. These HandlerMethods never enter the dispatcher.
                registerMapping(mapping, type, method);
            });
        }
    }

    boolean matches(HttpServletRequest request) throws Exception {
        if (super.getHandlerInternal(request) != null) return true;
        var path = ServletRequestPathUtils.getParsedRequestPath(request).pathWithinApplication();
        return streams.stream().anyMatch(pattern -> pattern.matches(path));
    }
}
