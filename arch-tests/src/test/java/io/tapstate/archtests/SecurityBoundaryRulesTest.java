package io.tapstate.archtests;

import com.tngtech.archunit.core.domain.Dependency;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.TapstatePrincipal;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.security.authorization.AuthorizationManager;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Keeps bearer processing at the security boundary rather than letting a controller recreate an
 * authentication or authorization path beside it.
 */
class SecurityBoundaryRulesTest {

    private static final String REST_API_PACKAGE = "io.tapstate.control.restapi";
    private static final String AUTHORIZATION_SEAM = REST_API_PACKAGE + ".OperationAuthorizationManager";

    private static JavaClasses restApi;
    private static JavaClasses controlCore;

    @BeforeAll
    static void importProductionClasses() {
        ClassFileImporter importer = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS);
        restApi = importer.importPackages(REST_API_PACKAGE);
        controlCore = importer.importPackages("io.tapstate.control.core");
    }

    private static Set<String> bearerAuthorityTypes() {
        JavaClass authenticator = controlCore.get(CredentialAuthenticator.class.getName());
        Set<String> types = authenticator.getDirectDependenciesFromSelf().stream()
                .map(Dependency::getTargetClass)
                .filter(type -> type.getPackageName().equals(CredentialAuthenticator.class.getPackageName()))
                .map(JavaClass::getName)
                .collect(java.util.stream.Collectors.toCollection(TreeSet::new));
        types.add(authenticator.getName());
        types.add(TapstatePrincipal.class.getName());
        assertThat(types)
                .as("the security boundary must derive at least one real credential authority type")
                .isNotEmpty();
        return types;
    }

    @Test
    @DisplayName("controllers do not parse bearer credentials or call credential authorities")
    void controllersDoNotReachIntoBearerAuthentication() {
        Set<String> bearerAuthorityTypes = bearerAuthorityTypes();
        Set<String> forbiddenDependencies = restApi.stream()
                .filter(type -> type.getSimpleName().endsWith("Controller"))
                .flatMap(type -> type.getDirectDependenciesFromSelf().stream())
                .map(Dependency::getTargetClass)
                .map(JavaClass::getName)
                .filter(bearerAuthorityTypes::contains)
                .collect(java.util.stream.Collectors.toCollection(TreeSet::new));

        assertThat(forbiddenDependencies)
                .as("controllers receive the authenticated caller from the security context; they must not "
                        + "parse bearer credentials or access their signer/token authority directly")
                .isEmpty();
    }

    @Test
    @DisplayName("the registry-backed authorization manager is the only REST authorization seam")
    void registryBackedAuthorizationHasOneSeam() {
        List<String> authorizationManagers = restApi.stream()
                .filter(type -> type.isAssignableTo(AuthorizationManager.class))
                .map(JavaClass::getName)
                .sorted()
                .toList();

        assertThat(authorizationManagers)
                .as("a second authorization manager would reintroduce an independently maintained policy path")
                .containsExactly(AUTHORIZATION_SEAM);

        JavaClass seam = restApi.get(AUTHORIZATION_SEAM);
        assertThat(seam.getDirectDependenciesFromSelf())
                .extracting(Dependency::getTargetClass)
                .extracting(JavaClass::getName)
                .as("the REST authorization seam must derive required scopes from the central registry")
                .contains(OperationRegistry.class.getName());
    }

    @Test
    @DisplayName("the retired interceptor and duplicate control scope enums stay absent")
    void retiredInterceptorAndDuplicateScopeStayAbsent() {
        assertThat(restApi.stream().map(JavaClass::getName))
                .as("the retired interceptor must not become a second authentication path")
                .doesNotContain("io.tapstate.control.restapi.AuthInterceptor");

        List<String> scopes = controlCore.stream()
                .filter(JavaClass::isEnum)
                .filter(type -> type.getSimpleName().equals("Scope"))
                .map(JavaClass::getName)
                .sorted()
                .toList();
        assertThat(scopes)
                .as("credential and operation authorization must share the one control scope vocabulary")
                .containsExactly("io.tapstate.control.core.Scope");
    }
}
