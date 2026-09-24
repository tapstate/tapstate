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
    ClusterMembershipGate clusterMembershipGate(ClusterProperties properties) {
        return new ClusterMembershipGate(properties);
    }
}
