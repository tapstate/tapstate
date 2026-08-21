package io.tapstate.control.restapi;

import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.TokenService;
import io.tapstate.control.core.TokenSigner;
import io.tapstate.messages.MessageCatalog;
import jakarta.servlet.DispatcherType;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.AuthenticationFilter;
import org.springframework.security.web.context.SecurityContextHolderFilter;
import org.springframework.security.web.servlet.util.matcher.PathPatternRequestMatcher;
import org.springframework.security.web.util.matcher.DispatcherTypeRequestMatcher;
import org.springframework.security.web.util.matcher.NegatedRequestMatcher;
import org.springframework.web.servlet.handler.HandlerMappingIntrospector;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

/**
 * Spring Security ownership of the HTTP control surface.
 *
 * <p>The API chain accepts only Tapstate bearer credentials and delegates scopes to the operation registry.
 * The root chain exposes the narrow bootstrap/login/probe allow-list and refuses every other root endpoint.
 * Both chains are stateless: Tapstate does not create, read, or refresh a servlet session in this phase.
 */
@Configuration
class RestApiSecurityConfiguration {

    @Bean
    ApiSecurityErrorWriter apiSecurityErrorWriter(MessageCatalog catalog, ObjectMapper json) {
        return new ApiSecurityErrorWriter(catalog, json);
    }

    @Bean
    AuthenticationManager tapstateAuthenticationManager(TokenService tokens, TokenSigner signer) {
        return new ProviderManager(List.of(
                new MachineTokenAuthenticationProvider(tokens), new HumanJwtAuthenticationProvider(signer)));
    }

    @Bean
    @Order(1)
    SecurityFilterChain apiSecurityFilterChain(
            HttpSecurity http,
            ObjectProvider<HandlerMappingIntrospector> handlers,
            OperationRegistry registry,
            AuthenticationManager authenticationManager,
            ApiSecurityErrorWriter errors) throws Exception {
        CodedAuthenticationEntryPoint entryPoint = new CodedAuthenticationEntryPoint(errors);
        AuthenticationFilter bearer = bearerFilter(authenticationManager, entryPoint);
        OperationAuthorizationManager authorization = new OperationAuthorizationManager(handlers, registry);

        http
                .securityMatcher(PathPatternRequestMatcher.pathPattern("/api/**"))
                .csrf(AbstractHttpConfigurer::disable)
                .formLogin(AbstractHttpConfigurer::disable)
                .httpBasic(AbstractHttpConfigurer::disable)
                .logout(AbstractHttpConfigurer::disable)
                .requestCache(AbstractHttpConfigurer::disable)
                .sessionManagement(sessions -> sessions.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .exceptionHandling(exceptions -> exceptions
                        .authenticationEntryPoint(entryPoint)
                        .accessDeniedHandler(new CodedAccessDeniedHandler(errors)))
                .addFilterAfter(bearer, SecurityContextHolderFilter.class)
                .authorizeHttpRequests(authorize -> authorize
                        .dispatcherTypeMatchers(DispatcherType.ERROR).permitAll()
                        // WebSocket upgrades retain their own handshake gate because they are not MVC verbs.
                        .requestMatchers(
                                "/api/pipelines/*/status/watch",
                                "/api/pipelines/*/logs/follow",
                                "/api/data-browser/*/*/tail")
                        .permitAll()
                        .anyRequest().access(authorization));
        return http.build();
    }

    @Bean
    @Order(2)
    SecurityFilterChain rootSecurityFilterChain(HttpSecurity http, ApiSecurityErrorWriter errors) throws Exception {
        CodedAuthenticationEntryPoint entryPoint = new CodedAuthenticationEntryPoint(errors);
        http
                .securityMatcher(new NegatedRequestMatcher(PathPatternRequestMatcher.pathPattern("/api/**")))
                .csrf(AbstractHttpConfigurer::disable)
                .formLogin(AbstractHttpConfigurer::disable)
                .httpBasic(AbstractHttpConfigurer::disable)
                .logout(AbstractHttpConfigurer::disable)
                .requestCache(AbstractHttpConfigurer::disable)
                .sessionManagement(sessions -> sessions.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .exceptionHandling(exceptions -> exceptions.authenticationEntryPoint(entryPoint))
                .authorizeHttpRequests(authorize -> authorize
                        .requestMatchers("/healthz", AuthWire.DISCOVERY_PATH, AuthWire.LOGIN_PATH,
                                "/auth/bootstrap", "/error").permitAll()
                        .anyRequest().denyAll());
        return http.build();
    }

    private static AuthenticationFilter bearerFilter(
            AuthenticationManager manager, CodedAuthenticationEntryPoint entryPoint) {
        AuthenticationFilter filter = new AuthenticationFilter(manager, new StrictBearerAuthenticationConverter());
        filter.setRequestMatcher(new DispatcherTypeRequestMatcher(DispatcherType.REQUEST));
        filter.setSuccessHandler(new ContinuingAuthenticationSuccessHandler());
        filter.setFailureHandler((request, response, failure) -> entryPoint.commence(request, response, failure));
        return filter;
    }
}
