package io.tapstate.testsupport;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.annotation.ElementType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("interface skip control")
interface CiDisabledInterface {}

@Disabled("a direct superclass annotation is not inherited")
class CiDisabledBase implements CiDisabledInterface {
    @Test
    void inheritedEnabled() {
        org.assertj.core.api.Assertions.assertThat(getClass()).isEqualTo(CiInjectedEvidenceTest.class);
    }
}

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Disabled("composed skip control")
@interface CiDisabled {}

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
@CiDisabled
@interface CiInheritedDisabled {}

@Retention(RetentionPolicy.CLASS)
@Target(ElementType.METHOD)
@Disabled("class-retained annotations cannot disable runtime tests")
@interface CiSourceOnlyDisabled {}
