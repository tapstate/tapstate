package io.tapstate.app;

import java.net.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

/** Opt-in local peers for actual browser checks; no application authority or delivery behavior is mocked. */
@EnabledIfSystemProperty(named="tapstate.alert.browser-peers",matches=".+")
class AlertBrowserPeersIT {
    @TempDir Path work;
    @Test void browserPeers() throws Exception {
        var root=Path.of(System.getProperty("tapstate.alert.browser-peers"));Files.createDirectories(root);
        AlertDeliveryNetworkIT.temp=work;AlertDeliveryNetworkIT.tls();
        try(var smtp=new AlertDeliveryNetworkIT.SmtpPeer(true,true)){
            var server=com.sun.net.httpserver.HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(),0),0);
            server.setHttpsConfigurator(new com.sun.net.httpserver.HttpsConfigurator(AlertDeliveryNetworkIT.serverTls));var executor=Executors.newFixedThreadPool(2);server.setExecutor(executor);
            var accepted=new java.util.concurrent.atomic.AtomicInteger();
            server.createContext("/",exchange->{try(exchange){exchange.getRequestBody().readNBytes(65537);String path=exchange.getRequestURI().getPath();if(path.equals("/unknown"))return;int status=path.equals("/rejected")?401:200;if(status==200)accepted.incrementAndGet();exchange.sendResponseHeaders(status,-1);}});
            server.start();
            try{Files.write(root.resolve("ca.pem"),AlertDeliveryNetworkIT.pem);Files.writeString(root.resolve("ready.json"),io.tapstate.core.common.JsonWriter.write(Map.of("smtpUnknownPort",smtp.port(),"httpsPort",server.getAddress().getPort(),"host","localhost")));
                long until=System.nanoTime()+Duration.ofMinutes(30).toNanos();while(!Files.exists(root.resolve("stop"))&&System.nanoTime()<until)Thread.sleep(100);
                Files.writeString(root.resolve("receipts.json"),io.tapstate.core.common.JsonWriter.write(Map.of("smtpDataBodies",smtp.bodies.size(),"httpsAccepted",accepted.get())));
            }finally{server.stop(0);executor.shutdownNow();}
        }
    }
}
