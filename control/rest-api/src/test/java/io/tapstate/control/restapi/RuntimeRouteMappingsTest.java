package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlOperations;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.support.GenericWebApplicationContext;
import org.springframework.web.method.HandlerTypePredicate;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import static org.assertj.core.api.Assertions.assertThat;

class RuntimeRouteMappingsTest {
    @Test void declarationsMatchTheRealMvcRegistrationsWithoutConstructingControllers() {
        try (var context = new GenericWebApplicationContext()) {
            for (Class<?> type : ControlHttpFace.class.getAnnotation(Import.class).value()) {
                if (!AnnotatedElementUtils.hasAnnotation(type, RestController.class)) continue;
                context.registerBean(type.getName(), type, () -> {
                    throw new AssertionError("route discovery must not construct a controller: " + type.getName());
                }, definition -> definition.setLazyInit(true));
            }
            context.registerBean(RequestMappingHandlerMapping.class, () -> {
                var mapping = new RequestMappingHandlerMapping();
                mapping.setPathPrefixes(Map.of("/api", HandlerTypePredicate.forAnnotation(RestController.class)));
                return mapping;
            });
            context.refresh();
            var real = context.getBean(RequestMappingHandlerMapping.class).getHandlerMethods();
            var declared = new RuntimeRouteMappings(ControlOperations.registry()).getHandlerMethods();
            assertThat(declared.keySet()).containsExactlyInAnyOrderElementsOf(real.keySet());
            declared.forEach((mapping, method) -> assertThat(method.getMethod()).isEqualTo(real.get(mapping).getMethod()));
            assertThat(Arrays.stream(context.getBeanFactory().getSingletonNames())
                    .filter(name -> name.startsWith("io.tapstate.control.restapi."))).isEmpty();
        }
    }
}
