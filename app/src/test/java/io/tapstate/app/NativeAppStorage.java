package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationBootstrap;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.util.Map;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.MongoDBContainer;

/** Real native installation facts for assembly tests; the class container owns the isolated databases. */
record NativeAppStorage(String configurationUri,String configurationDatabase,String cacheUri,String cacheDatabase,
        String masterUri,String masterDatabase,String clusterId,CachePreparation preparation,int schemaVersion) {
    static final String USERNAME="fixture-admin";
    static final String PASSWORD="fixture-native-password";

    static NativeAppStorage create(MongoDBContainer mongo,String prefix,StorageBinding.Mode mode,String clusterId) {
        var suffix=Long.toUnsignedString(System.nanoTime(),16);
        return prepare(mongo.getReplicaSetUrl(prefix+"_config_"+suffix),mongo.getReplicaSetUrl(prefix+"_cache_"+suffix),
                mongo.getReplicaSetUrl(prefix+"_master_"+suffix),mode,clusterId);
    }

    static NativeAppStorage prepare(String configUri,String cacheUri,String masterUri,StorageBinding.Mode mode,String clusterId) {
        var configDb=new com.mongodb.ConnectionString(configUri).getDatabase();
        var cacheDb=new com.mongodb.ConnectionString(cacheUri).getDatabase();
        var masterDb=new com.mongodb.ConnectionString(masterUri).getDatabase();
        var setup=new StorageSetupScope(clusterId,"fixture-installation","fixture-setup",1);
        var cache=new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,cacheUri,cacheDb,null);
        try(var configuration=MongoConfigurationBootstrap.prepare(new MongoConnectionSettings(configUri,null,Duration.ofSeconds(5)),
                setup.clusterId(),setup.installationId(),setup.operationId(),1)) {
            var target=new SourceResource(StorageBinding.defaultSourceId(mode),null,"mongodb",
                    Map.of("isUri",true,"uri",masterUri),null,null,null,null);
            var initial=configuration.bindings().commitWithFirstCache(setup,mode,cache,target);
            try(var domain=MongoCacheDomain.connect(cache,Duration.ofSeconds(5),"native-app-fixture")) {
                domain.prepareFirstAuth(initial.firstCreation().orElseThrow());
                domain.prepareInitialBusiness(initial.preparation(),() -> {});
            }
            configuration.bindings().complete(setup);
            if(mode==StorageBinding.Mode.OP)configuration.firstAdmin().create(configuration.firstAdmin().load().orElseThrow().scope(),
                    new User(USERNAME,new BCryptPasswordHasher().hash(PASSWORD),"admin"));
            return new NativeAppStorage(configUri,configDb,cacheUri,cacheDb,masterUri,masterDb,clusterId,
                    initial.preparation(),configuration.schemaVersion());
        }
    }

    static ConfigurableApplicationContext business(ConfigurableApplicationContext root) {
        var storage=root.getBean(StorageRoot.class);
        var owner=storage.runtime().business().orElseThrow(() -> new AssertionError("business unavailable: "+storage.phase()));
        if(!(owner instanceof NativeBusinessContext nativeBusiness))throw new AssertionError("native business owner missing");
        return (ConfigurableApplicationContext)ReflectionTestUtils.getField(nativeBusiness,"context");
    }
}
