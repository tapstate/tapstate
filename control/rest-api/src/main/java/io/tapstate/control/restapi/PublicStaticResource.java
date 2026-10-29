package io.tapstate.control.restapi;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.servlet.HandlerExecutionChain;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.handler.HandlerMappingIntrospector;
import org.springframework.web.servlet.resource.ResourceHttpRequestHandler;
import org.springframework.web.util.ServletRequestPathUtils;
import org.springframework.web.util.UrlPathHelper;

/** Allows only requests resolved by MVC to its static resource handler. */
final class PublicStaticResource {
    private PublicStaticResource() { }

    static boolean matches(HttpServletRequest request, HandlerMappingIntrospector introspector) {
        if (!"GET".equals(request.getMethod()) && !"HEAD".equals(request.getMethod())) {
            return false;
        }
        var previous = ServletRequestPathUtils.getCachedPath(request);
        var previousLookup = request.getAttribute(UrlPathHelper.PATH_ATTRIBUTE);
        try {
            ServletRequestPathUtils.parseAndCache(request);
            for (HandlerMapping mapping : introspector.getHandlerMappings()) {
                HandlerExecutionChain chain = mapping.getHandler(request);
                if (chain != null) {
                    if (!(chain.getHandler() instanceof ResourceHttpRequestHandler resourceHandler)) {
                        return false;
                    }
                    String path = request.getServletPath();
                    if (path == null || !path.matches("/[A-Za-z0-9._-]+(?i:\\.(svg|png|ico|jpg|jpeg|webp|gif|avif|bmp))")) {
                        return false;
                    }
                    for (var location : resourceHandler.getLocations()) {
                        if (location.createRelative(path.substring(1)).isReadable()) {
                            return true;
                        }
                    }
                    return false;
                }
            }
            return false;
        } catch (Exception failure) {
            return false;
        } finally {
            if (previousLookup == null) {
                request.removeAttribute(UrlPathHelper.PATH_ATTRIBUTE);
            } else {
                request.setAttribute(UrlPathHelper.PATH_ATTRIBUTE, previousLookup);
            }
            if (previous instanceof org.springframework.http.server.RequestPath path) {
                ServletRequestPathUtils.setParsedRequestPath(path, request);
            } else {
                ServletRequestPathUtils.clearParsedRequestPath(request);
            }
        }
    }
}
