package io.tapstate.control.restapi;

import io.tapstate.control.core.Operation;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.TapstatePrincipal;
import io.tapstate.control.restapi.OperationRequestMappingHandlerMapping.OptionsHandler;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.server.RequestPath;
import org.springframework.security.authorization.AuthorizationDecision;
import org.springframework.security.authorization.AuthorizationManager;
import org.springframework.security.authorization.AuthorizationResult;
import org.springframework.security.core.Authentication;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.security.web.access.intercept.RequestAuthorizationContext;
import org.springframework.web.servlet.handler.HandlerMappingIntrospector;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerExecutionChain;
import org.springframework.web.util.ServletRequestPathUtils;
import org.springframework.web.util.UrlPathHelper;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Authorizes a mapped REST verb from its {@link Verb} id and the central operation registry.
 *
 * <p>No route-to-scope table lives here: MVC resolves the handler, the handler declares the operation id,
 * and the registry supplies the required scope. An unmapped route abstains so MVC can render its normal
 * 404/405 response, while a mapped handler must declare a registered operation.
 */
final class OperationAuthorizationManager implements AuthorizationManager<RequestAuthorizationContext> {

    static final String OPERATION_ATTRIBUTE = OperationAuthorizationManager.class.getName() + ".operation";

    private final ObjectProvider<HandlerMappingIntrospector> handlers;
    private final OperationRegistry registry;

    OperationAuthorizationManager(ObjectProvider<HandlerMappingIntrospector> handlers, OperationRegistry registry) {
        this.handlers = Objects.requireNonNull(handlers, "handlers");
        this.registry = Objects.requireNonNull(registry, "registry");
    }

    @Override
    public AuthorizationResult authorize(Supplier<? extends Authentication> authentication,
            RequestAuthorizationContext context) {
        HttpServletRequest request = context.getRequest();
        HandlerMethod handler = handlerFor(request);
        if (handler == null) {
            OperationRequestMappingHandlerMapping.recordOptionsLookup(request, null);
            return null;
        }
        if (handler instanceof OptionsHandler options) {
            // Resolve every mapping before granting discovery and report a stable refusal for mixed scopes.
            var operations = options.contributors().stream().map(this::operationFor)
                    .distinct().sorted(Comparator.comparing(Operation::id)).toList();
            Authentication current = authentication.get();
            for (Operation operation : operations) {
                if (!permits(current, operation)) {
                    request.setAttribute(OPERATION_ATTRIBUTE, operation);
                    return new AuthorizationDecision(false);
                }
            }
            OperationRequestMappingHandlerMapping.recordOptionsLookup(request, handler);
            return new AuthorizationDecision(true);
        }
        Operation operation = operationFor(handler);
        request.setAttribute(OPERATION_ATTRIBUTE, operation);
        boolean granted = permits(authentication.get(), operation);
        if (granted) {
            OperationRequestMappingHandlerMapping.recordOptionsLookup(request, handler);
        }
        return new AuthorizationDecision(granted);
    }

    private static boolean permits(Authentication current, Operation operation) {
        return current != null && current.isAuthenticated() && current.getPrincipal() instanceof TapstatePrincipal principal
                && principal.permits(operation.scope());
    }

    private Operation operationFor(HandlerMethod handler) {
        Verb verb = handler.getMethodAnnotation(Verb.class);
        if (verb == null) {
            throw new IllegalStateException("an /api handler carries no @Verb: " + handler);
        }
        return registry.resolve(verb.value());
    }

    private HandlerMethod handlerFor(HttpServletRequest request) {
        HandlerMappingIntrospector introspector = handlers.getObject();
        List<HandlerMapping> mappings = introspector.getHandlerMappings();
        boolean parsePath = mappings.stream().anyMatch(HandlerMapping::usesPathPatterns);
        RequestPath previousPath = (RequestPath) request.getAttribute(ServletRequestPathUtils.PATH_ATTRIBUTE);
        String previousLookupPath = (String) request.getAttribute(UrlPathHelper.PATH_ATTRIBUTE);
        try {
            if (parsePath) {
                ServletRequestPathUtils.parseAndCache(request);
            }
            for (HandlerMapping mapping : mappings) {
                HandlerExecutionChain chain;
                try {
                    chain = mapping.getHandler(request);
                } catch (Exception mappingFailure) {
                    return null;
                }
                if (chain == null) {
                    continue;
                }
                if (!(chain.getHandler() instanceof HandlerMethod handler)) {
                    continue;
                }
                return handler;
            }
            return null;
        } finally {
            ServletRequestPathUtils.setParsedRequestPath(previousPath, request);
            restoreLookupPath(request, previousLookupPath);
        }
    }

    private static void restoreLookupPath(HttpServletRequest request, String previousLookupPath) {
        if (previousLookupPath != null) {
            request.setAttribute(UrlPathHelper.PATH_ATTRIBUTE, previousLookupPath);
        } else {
            request.removeAttribute(UrlPathHelper.PATH_ATTRIBUTE);
        }
    }
}
