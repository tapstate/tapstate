package io.tapstate.testsupport;

import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;

/** Test invocations may supply creation metadata without coupling fixtures to a local coordinator. */
public final class ContainerLabels {
    private static final java.util.Set<GenericContainer<?>> CONFIGURED=java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    private ContainerLabels() { }

    public static Map<String,String> runtime() {
        String supplied=System.getenv("TAPSTATE_TEST_CONTAINER_LABELS");
        if(supplied==null || supplied.isBlank())return Map.of();
        var labels=new LinkedHashMap<String,String>();
        for(String entry:supplied.split(",")){
            int split=entry.indexOf('=');
            if(split<1)throw new IllegalArgumentException("invalid test container label");
            labels.put(entry.substring(0,split),entry.substring(split+1));
        }
        return Map.copyOf(labels);
    }

    static void configure(Class<?> test) {
        var labels=runtime();if(labels.isEmpty())return;
        for(Class<?> type=test;type!=null;type=type.getSuperclass())for(var field:type.getDeclaredFields()){
            if(!Modifier.isStatic(field.getModifiers()) || !field.isAnnotationPresent(org.testcontainers.junit.jupiter.Container.class)
                    || !GenericContainer.class.isAssignableFrom(field.getType()))continue;
            try {
                field.setAccessible(true);
                var container=(GenericContainer<?>)field.get(null);
                if(container!=null)configure(container);
            }catch(IllegalAccessException failure){throw new IllegalStateException("cannot configure owned test container",failure);}
        }
    }

    /** Anonymous volumes preserve image mounts and remain eligible for scoped JVM-exit cleanup. */
    public static synchronized void configure(GenericContainer<?> container) {
        var labels=runtime();
        if(labels.isEmpty() || container.isRunning() || !CONFIGURED.add(container))return;
        container.withLabels(labels).withCreateContainerCmdModifier(command->{
            String memory=System.getenv("TAPSTATE_TEST_CONTAINER_MEMORY_BYTES");
            if(memory!=null && !memory.isBlank())command.getHostConfig().withMemory(Long.parseLong(memory));
            var docker=org.testcontainers.DockerClientFactory.instance().client();
            var volumes=docker.inspectImageCmd(container.getDockerImageName()).exec().getConfig().getVolumes();
            if(volumes==null)return;
            var host=command.getHostConfig();
            var mounts=new java.util.ArrayList<com.github.dockerjava.api.model.Mount>();
            if(host.getMounts()!=null)mounts.addAll(host.getMounts());
            for(var path:volumes.keySet()){
                if(host.getTmpFs()!=null && host.getTmpFs().containsKey(path))continue;
                if(host.getBinds()!=null && java.util.Arrays.stream(host.getBinds()).anyMatch(bind->path.equals(bind.getVolume().getPath())))continue;
                if(mounts.stream().anyMatch(mount->path.equals(mount.getTarget())))continue;
                var reaper=org.testcontainers.utility.ResourceReaper.instance();
                var volumeLabels=new LinkedHashMap<>(reaper.getLabels());volumeLabels.putAll(labels);
                reaper.registerLabelsFilterForCleanup(volumeLabels);
                String name=docker.createVolumeCmd().withLabels(volumeLabels).exec().getName();
                mounts.add(new com.github.dockerjava.api.model.Mount().withType(com.github.dockerjava.api.model.MountType.VOLUME)
                        .withSource(name).withTarget(path));
            }
            host.withMounts(mounts);
        });
    }
}
