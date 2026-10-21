package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ClusterCapacityLimits;
import io.tapstate.core.lifecycle.ParallelismBudget;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.ObjectProvider;

import java.util.Map;

/** Immutable profile inputs and the one aggregate-capacity configuration used by actuation. */
@Configuration
@EnableConfigurationProperties({ClusterCapacityProperties.class, ExecutionProfileProperties.class})
class ExecutionProfileConfiguration {
    @Bean
    ClusterCapacityLimits clusterCapacityLimits(ClusterCapacityProperties properties) {
        try {
            return properties.limits();
        } catch (IllegalArgumentException invalid) {
            throw new TapstateException(BootError.EXECUTION_PROFILE_INVALID,
                    Map.of("detail", "aggregate capacity ceilings must be positive"), invalid);
        }
    }

    @Bean
    ExecutionProfileFactory executionProfileFactory(HazelcastProperties hazelcast, ClusterProperties cluster,
            ExecutionProfileProperties properties, ObjectProvider<ParallelismBudget> budget, ClusterCapacityLimits limits) {
        return new ExecutionProfileFactory(hazelcast, cluster, properties,
                budget.getIfAvailable(() -> ParallelismBudget.DEFAULTS), limits);
    }
}
