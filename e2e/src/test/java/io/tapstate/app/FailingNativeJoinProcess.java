package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.JsonWriter;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.StandardEnvironment;

/** A real joining candidate fails only after its parent observed its admission to the shared membership. */
public final class FailingNativeJoinProcess {
    private FailingNativeJoinProcess() {}
    public static void main(String[] args) throws Exception {
        var environment=new StandardEnvironment();
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        var owner=new AtomicReference<NativeBusinessContext>(); var child=new AtomicReference<ConfigurableApplicationContext>();
        var member=new AtomicReference<HazelcastInstance>();
        var arguments=new java.util.ArrayList<>(Arrays.asList(args).subList(2,args.length));
        arguments.addAll(java.util.List.of("--server.address=0.0.0.0","--logging.level.root=ERROR","--tapstate.store.mongo.uri="+args[0],
                "--tapstate.store.mongo.server-selection-timeout=5s","--tapstate.connectors.plugins-dir="+args[1],
                "--tapstate.connectors.seed-dir="+Path.of(args[1]).resolve("no-seeds"),"--tapstate.connectors.also-accept-ids=e2e_file",
                "--tapstate.hz.jet.cooperative-thread-count=2"));
        try (var application=new SpringApplicationBuilder(Bootstrap.class).environment(environment).initializers(parent -> {
            var definition=new RootBeanDefinition(StorageBusinessFactory.class,() -> (StorageBusinessFactory)bound -> {
                var nativeOwner=new NativeBusinessContext(parent.getBean(StorageRoot.class),bound,parent); owner.set(nativeOwner);
                return new StorageRuntime.Business() {
                    @Override public void start() {
                        nativeOwner.start();
                        try {
                            var field=NativeBusinessContext.class.getDeclaredField("context"); field.setAccessible(true);
                            var actual=(ConfigurableApplicationContext)field.get(nativeOwner); child.set(actual);
                            member.set(actual.getBean(HazelcastInstance.class));
                            if (!member.get().getLifecycleService().isRunning()) throw new AssertionError("the candidate did not start its real member");
                            var node=actual.getBean(NodeSessionLease.class).currentFence().orElseThrow();
                            System.out.println("JOIN_CANDIDATE_HELD "+JsonWriter.write(Map.of("nodeId",node.owner().nodeId(),"bootId",node.owner().bootId(),
                                    "claimGeneration",node.claimGeneration(),"pid",ProcessHandle.current().pid()))); System.out.flush();
                            if (System.in.read()!='\n') throw new AssertionError("the test did not release its admitted candidate");
                        } catch (ReflectiveOperationException | java.io.IOException failure) { throw new AssertionError(failure); }
                        throw StorageRuntime.unavailable();
                    }
                    @Override public void close() { nativeOwner.close(); }
                };
            }); definition.setPrimary(true);
            ((BeanDefinitionRegistry)parent.getBeanFactory()).registerBeanDefinition("failingNativeJoin",definition);
        }).run(arguments.toArray(String[]::new))) {
            if (child.get()==null || child.get().isActive() || member.get()==null || member.get().getLifecycleService().isRunning()
                    || application.getBean(StorageRoot.class).runtime().business().isPresent()
                    || application.getBean(StorageRoot.class).runtime().cleanupRequired())
                throw new AssertionError("the failed candidate did not leave a clean retained root");
            System.out.println("JOIN_FAILED_CLEANED"); System.out.flush();
            while (System.in.read()!=-1) { }
        } finally { if (owner.get()!=null) owner.get().close(); }
    }
}
