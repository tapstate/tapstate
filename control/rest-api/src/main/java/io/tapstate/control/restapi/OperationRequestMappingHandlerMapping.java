package io.tapstate.control.restapi;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpMethod;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.cors.CorsUtils;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Retains the mappings that contribute to Spring MVC's automatic OPTIONS response. */
public final class OperationRequestMappingHandlerMapping extends RequestMappingHandlerMapping {

    private static final String OPTIONS_LOOKUP = OperationRequestMappingHandlerMapping.class.getName() + ".optionsLookup";

    @Override
    protected HandlerMethod handleNoMatch(Set<RequestMappingInfo> mappings, String lookupPath,
            HttpServletRequest request) throws ServletException {
        HandlerMethod handler = super.handleNoMatch(mappings, lookupPath, request);
        if (handler == null || !isMethodDiscovery(request)) {
            return handler;
        }

        // For OPTIONS, the method-mismatch fallback uses every mapping whose path matches.
        Map<RequestMappingInfo, HandlerMethod> registered = getHandlerMethods();
        Map<RequestMappingInfo, HandlerMethod> contributors = new LinkedHashMap<>();
        for (RequestMappingInfo mapping : mappings) {
            if (mapping.getActivePatternsCondition().getMatchingCondition(request) != null) {
                contributors.put(mapping, registered.get(mapping));
            }
        }
        return new OptionsHandler(handler, this, lookupPath, contributors);
    }

    private static boolean isMethodDiscovery(HttpServletRequest request) {
        return HttpMethod.OPTIONS.matches(request.getMethod()) && !CorsUtils.isPreFlightRequest(request);
    }

    static void recordOptionsLookup(HttpServletRequest request, HandlerMethod handler) {
        if (isMethodDiscovery(request)) {
            request.setAttribute(OPTIONS_LOOKUP, new OptionsLookup(handler));
        }
    }

    static void requireAuthorizedDispatch(HttpServletRequest request, Object handler) {
        // The runtime may be withdrawn between authorization and MVC dispatch.
        if (handler instanceof StorageHttpFace.UnavailableHandler) {
            return;
        }
        Object lookup = request.getAttribute(OPTIONS_LOOKUP);
        if (lookup instanceof OptionsLookup authorized
                ? dispatchMatches(authorized.handler(), handler)
                : !(handler instanceof OptionsHandler)) {
            return;
        }
        throw new AccessDeniedException("The OPTIONS mapping changed before dispatch");
    }

    private static boolean dispatchMatches(HandlerMethod expected, Object actual) {
        if (expected instanceof OptionsHandler options) {
            return actual instanceof OptionsHandler target && options.sameMappingsAs(target);
        }
        if (actual instanceof HandlerMethod target) {
            return expected != null && target.getBean() == expected.getBean() && target.getMethod().equals(expected.getMethod());
        }
        return expected == null;
    }

    private record OptionsLookup(HandlerMethod handler) {}

    static final class OptionsHandler extends HandlerMethod {
        private final OperationRequestMappingHandlerMapping mapping;
        private final String lookupPath;
        private final Map<RequestMappingInfo, HandlerMethod> contributors;

        private OptionsHandler(HandlerMethod handler, OperationRequestMappingHandlerMapping mapping,
                String lookupPath, Map<RequestMappingInfo, HandlerMethod> contributors) {
            super(handler);
            if (contributors.isEmpty()) {
                throw new IllegalStateException("An automatic OPTIONS handler must have a matching route");
            }
            this.mapping = mapping;
            this.lookupPath = lookupPath;
            this.contributors = Map.copyOf(contributors);
        }

        Collection<HandlerMethod> contributors() {
            return contributors.values();
        }

        private boolean sameMappingsAs(OptionsHandler other) {
            // Spring creates a fresh synthetic handler on each lookup.
            return mapping == other.mapping && lookupPath.equals(other.lookupPath) && contributors.equals(other.contributors);
        }
    }
}
