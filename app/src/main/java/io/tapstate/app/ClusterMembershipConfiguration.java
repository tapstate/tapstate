package io.tapstate.app;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The committed-membership gate, and the cluster properties it is cut from, in a configuration of their
 * own so that everything which needs them can import them.
 *
 * <p>They used to sit beside the engine member, which is where they are used most but not where they are
 * needed most: the store bridge fences its claims through this gate, and the topology read face reads the
 * cluster id, and neither of those needs an engine member to exist. Every assembly that brought up one
 * without the other failed at startup on a bean it could not see a reason to want -- and because the
 * assemblies in question are integration tests, which a plain test run does not execute, it failed where
 * nothing was looking.
 *
 * <p>The gate itself is a plain object over the configured properties with nothing of the engine in it.
 * Importing this configuration from several places is safe and is the point: a configuration class is
 * registered once however many times it is imported.
 */
@Configuration
@EnableConfigurationProperties(ClusterProperties.class)
class ClusterMembershipConfiguration {

    @Bean
    @org.springframework.context.annotation.Conditional(BusinessScope.class)
    ClusterMembershipGate clusterMembershipGate(ClusterProperties properties) {
        return new ClusterMembershipGate(properties);
    }

    /** The minimum CONFIG context owns no business gate; an admitted child owns exactly one. */
    static final class BusinessScope implements org.springframework.context.annotation.Condition {
        @Override public boolean matches(org.springframework.context.annotation.ConditionContext context,
                org.springframework.core.type.AnnotatedTypeMetadata metadata) {
            return !context.getRegistry().containsBeanDefinition("storageRootConfiguration");
        }
    }

    /**
     * This start of the process, which its member joins the cluster under and its store client is named for. Here
     * rather than beside either of them because both need the same one, and neither needs the other to exist.
     */
    @Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean(BootId.class)
    BootId bootId() {
        return BootId.fresh();
    }
}
