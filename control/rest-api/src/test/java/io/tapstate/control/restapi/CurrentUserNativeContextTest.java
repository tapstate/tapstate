package io.tapstate.control.restapi;

import io.tapstate.control.core.*;
import io.tapstate.core.common.TapstateException;
import jakarta.servlet.http.Cookie;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.core.context.SecurityContextHolder;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class CurrentUserNativeContextTest {
    @AfterEach void clearIdentity(){SecurityContextHolder.clearContext();}
    @Test void emailComesFromTheCapturedNativeSessionRatherThanTheLegacyService(){
        var nativeService=mock(CloudAuthenticationService.class);var legacy=mock(CloudAuthenticationService.class);var captures=new AtomicInteger();
        when(nativeService.userEmail("cookie")).thenReturn(Optional.of("verified@example.test"));
        var controller=controller(legacy,()->{captures.incrementAndGet();return new ManagedAuthenticationContext(nativeService,()->{});});
        var response=controller.current(request());assertThat(response.getBody().email()).isEqualTo("verified@example.test");
        assertThat(response.getBody().principal()).isEqualTo("original-user");assertThat(response.getBody().username()).isNull();
        assertThat(captures).hasValue(1);verifyNoInteractions(legacy);
    }
    @Test void replacingTheContextDuringEmailReadCannotPublishOrAdoptTheSuccessor(){
        var nativeService=mock(CloudAuthenticationService.class);var legacy=mock(CloudAuthenticationService.class);var successor=mock(CloudAuthenticationService.class);
        var live=new AtomicBoolean(true);var captures=new AtomicInteger();
        var selected=new AtomicReference<>(new ManagedAuthenticationContext(nativeService,()->{if(!live.get())throw new TapstateException(ControlError.UNAUTHENTICATED,Map.of(),null);}));
        when(nativeService.userEmail("cookie")).thenAnswer(ignored->{live.set(false);selected.set(new ManagedAuthenticationContext(successor,()->{}));return Optional.of("private@example.test");});
        var controller=controller(legacy,()->{captures.incrementAndGet();return selected.get();});
        assertThatThrownBy(()->controller.current(request())).isInstanceOfSatisfying(TapstateException.class,error->assertThat(error.code()).isEqualTo(ControlError.UNAUTHENTICATED));
        assertThat(captures).hasValue(1);verifyNoInteractions(legacy,successor);
    }
    @Test void anUnavailableNativeContextDoesNotFallBackToTheLegacyService(){
        var legacy=mock(CloudAuthenticationService.class);var controller=controller(legacy,()->null);
        assertThatThrownBy(()->controller.current(request())).isInstanceOfSatisfying(TapstateException.class,error->assertThat(error.code()).isEqualTo(CloudAuthenticationService.unavailable().code()));
        verifyNoInteractions(legacy);
    }
    private static CurrentUserController controller(CloudAuthenticationService legacy,ManagedAuthenticationContext.Provider contexts){
        var beans=new DefaultListableBeanFactory();beans.registerSingleton("legacy",legacy);beans.registerSingleton("native",contexts);
        SecurityContextHolder.getContext().setAuthentication(new TapstateAuthentication(TapstatePrincipal.cloudSession(new VerifiedToken("original-user",Scope.READ))));
        return new CurrentUserController(new CurrentUserQueryService(AuthenticationMode.CLOUD),beans.getBeanProvider(CloudAuthenticationService.class),beans.getBeanProvider(ManagedAuthenticationContext.Provider.class));
    }
    private static MockHttpServletRequest request(){var request=new MockHttpServletRequest();request.setCookies(new Cookie(CloudSessionCookies.NAME,"cookie"));return request;}
}
