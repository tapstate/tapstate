package io.tapstate.control.restapi;

import io.tapstate.control.core.*;
import java.util.*;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/** Isolated HTTP fixture; the supplied dispatcher uses its test's real prepared CONFIG store. */
@SpringBootConfiguration
@EnableAutoConfiguration
@Import({RestApiConfiguration.class,RestApiSecurityConfiguration.class,ApiExceptionHandler.class,AlertController.class})
public class AlertTestHttpFace {
    @Bean OperationRegistry registry(){return ControlOperations.registry();}
    @Bean CredentialAuthenticator credentials(){return CredentialAuthenticator.scoped(p->"cyxt_alert-browser.fixture".equals(p)
            ?Optional.of(new CredentialAuthentication(new VerifiedToken("operator",Scope.ADMIN),Optional.empty(),Optional.empty(),()->{})):Optional.empty());}
}
