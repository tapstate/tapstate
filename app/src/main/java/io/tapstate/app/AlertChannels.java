package io.tapstate.app;

import io.tapstate.adapters.alerting.*;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.alerting.NotificationChannel;
import io.tapstate.spi.store.AlertStore;
import java.io.ByteArrayInputStream;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.cert.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import javax.net.ssl.*;

/** Static deployment assembly. DNS is bounded, refreshed for each attempt and pinned through the call. */
final class AlertChannels implements AlertDeliveryRuntime.Channels {
    interface Resolver { InetAddress[] resolve(String host) throws UnknownHostException; }
    private final AlertChannelPolicy.Deployment deployment;private final CloudRuntimeSettings cloud;
    private final Clock clock;private final Resolver resolver;private final Runnable active;private final String expectedOrg;private final SSLContext systemTls;private final boolean outboundEnabled;
    private final ThreadPoolExecutor dns=new ThreadPoolExecutor(2,2,0,TimeUnit.SECONDS,new SynchronousQueue<>(),r->{var t=new Thread(r,"alert-dns");t.setDaemon(true);return t;});
    AlertChannels(AlertChannelPolicy.Deployment deployment,CloudRuntimeSettings cloud,Clock clock,Runnable active,String expectedOrg){
        this(deployment,cloud,clock,active,expectedOrg,InetAddress::getAllByName);
    }
    AlertChannels(AlertChannelPolicy.Deployment deployment,CloudRuntimeSettings cloud,Clock clock,Runnable active,String expectedOrg,boolean outboundEnabled){
        this(deployment,cloud,clock,active,expectedOrg,InetAddress::getAllByName,systemTrust(),outboundEnabled);
    }
    AlertChannels(AlertChannelPolicy.Deployment deployment,CloudRuntimeSettings cloud,Clock clock,Runnable active,String expectedOrg,Resolver resolver){
        this(deployment,cloud,clock,active,expectedOrg,resolver,systemTrust());
    }
    AlertChannels(AlertChannelPolicy.Deployment deployment,CloudRuntimeSettings cloud,Clock clock,Runnable active,String expectedOrg,Resolver resolver,SSLContext systemTls){
        this(deployment,cloud,clock,active,expectedOrg,resolver,systemTls,true);
    }
    private AlertChannels(AlertChannelPolicy.Deployment deployment,CloudRuntimeSettings cloud,Clock clock,Runnable active,String expectedOrg,Resolver resolver,SSLContext systemTls,boolean outboundEnabled){
        this.deployment=deployment;this.cloud=cloud;this.clock=clock;this.active=active;this.expectedOrg=expectedOrg;this.resolver=resolver;this.systemTls=systemTls;this.outboundEnabled=outboundEnabled;
    }
    static boolean outboundEnabled(String value){
        if(value==null || value.equalsIgnoreCase("true"))return true;
        if(value.equalsIgnoreCase("false"))return false;
        throw invalid("outbound-enabled");
    }
    static Set<AlertChannelPolicy.Capability> capabilities(AlertChannelPolicy.Deployment deployment,CloudRuntimeSettings cloud,boolean enabled){
        return enabled?capabilities(deployment,cloud):Set.of();
    }
    static Set<AlertChannelPolicy.Capability> capabilities(AlertChannelPolicy.Deployment deployment,CloudRuntimeSettings cloud) {
        if(deployment==AlertChannelPolicy.Deployment.ON_PREM)return Set.of(new AlertChannelPolicy.Capability("email",1,AlertChannelPolicy.Family.OP_EXTERNAL),new AlertChannelPolicy.Capability("webhook",1,AlertChannelPolicy.Family.OP_EXTERNAL));
        if(deployment!=AlertChannelPolicy.Deployment.CLOUD_MANAGED || cloud==null || !cloud.cloud())return Set.of();
        var origin=cloud.baseUrl();var token=cloud.token();
        if(origin==null || !"https".equalsIgnoreCase(origin.getScheme()) || origin.getHost()==null || origin.getRawUserInfo()!=null
                || origin.getRawQuery()!=null || origin.getRawFragment()!=null || !(origin.getPath().isEmpty()||origin.getPath().equals("/"))
                || token==null || token.isBlank() || token.length()>4096 || token.codePoints().anyMatch(Character::isISOControl)
                || !cloud.clusterId().matches("[a-fA-F0-9]{24}"))return Set.of();
        return Set.of(new AlertChannelPolicy.Capability("cloud-sdk",2,AlertChannelPolicy.Family.CLOUD_SDK));
    }
    @Override public boolean supports(AlertIntent.Frozen frozen){return outboundEnabled && frozen.retention().deployment()==deployment
            && capabilities(deployment,cloud).contains(frozen.channel())
            && (deployment!=AlertChannelPolicy.Deployment.CLOUD_MANAGED || expectedOrg!=null && expectedOrg.matches("[a-fA-F0-9]{24}")
                    && expectedOrg.equals(frozen.identity().scope().authorityId()) && cloud.clusterId().equals(frozen.identity().scope().clusterId()));}
    @Override public Object prepare(AlertIntent.Frozen frozen)throws InterruptedException {
        active.run();if(!supports(frozen))throw invalid("capability");
        String host;
        if(frozen.target() instanceof AlertIntent.SdkTarget)host=cloud.baseUrl().getHost();
        else {var transport=transport(frozen);host=transport instanceof AlertReceiver.Email email?email.host():((AlertReceiver.Webhook)transport).origin().getHost();}
        Future<InetAddress[]> lookup;
        try{lookup=dns.submit(()->resolver.resolve(host));}catch(RejectedExecutionException busy){return new NotificationChannel.Result(DeliveryEvidence.Outcome.NOT_ACCEPTED,NotificationChannel.Action.RETRY,null,NotificationChannel.Phase.CONNECT,0,"dns-capacity");}
        try{
            var addresses=lookup.get(2,TimeUnit.SECONDS);active.run();
            if(addresses==null||addresses.length==0||addresses.length>8||Arrays.stream(addresses).anyMatch(a->a==null||a.isAnyLocalAddress()||a.isMulticastAddress()||a.isLinkLocalAddress()))throw invalid("resolved-address");
            return addresses.clone();
        }catch(TimeoutException unavailable){lookup.cancel(true);return new NotificationChannel.Result(DeliveryEvidence.Outcome.NOT_ACCEPTED,NotificationChannel.Action.RETRY,null,NotificationChannel.Phase.CONNECT,0,"dns-timeout");}
        catch(ExecutionException unavailable){
            if(unavailable.getCause() instanceof UnknownHostException)return new NotificationChannel.Result(DeliveryEvidence.Outcome.NOT_ACCEPTED,NotificationChannel.Action.RETRY,null,NotificationChannel.Phase.CONNECT,0,"dns-unavailable");
            if(unavailable.getCause() instanceof RuntimeException bug)throw bug;
            throw new IllegalStateException(unavailable.getCause());
        }finally{if(!lookup.isDone())lookup.cancel(true);}
    }
    @Override public NotificationChannel open(AlertIntent.Frozen frozen,AlertStore.PrivateMaterial material,Object prepared){
        active.run();var addresses=(InetAddress[])prepared;var destination=AlertDeliveryRuntime.destination(frozen);
        if(frozen.target() instanceof AlertIntent.SdkTarget)return new CloudAlertSdkChannel(deployment,destination,cloud.baseUrl(),cloud.token(),expectedOrg,addresses,systemTls,clock);
        var transport=transport(frozen);byte[] authentication=material.authentication(),ca=material.privateCa();char[] password=null;
        try{
            SSLContext ssl=transport.trust()==AlertReceiver.Trust.SYSTEM?systemTls:trust(ca);
            var binding=new TargetBinding(destination.scope(),destination.receiverIncarnation(),destination.targetId(),destination.bindingId());
            if(transport instanceof AlertReceiver.Email email){
                if(authentication!=null)password=new String(authentication,StandardCharsets.UTF_8).toCharArray();
                return new SmtpChannel(deployment,binding,email.host(),new InetSocketAddress(addresses[0],email.port()),
                        SmtpChannel.Encryption.valueOf(email.encryption().name()),ssl,SmtpChannel.Authentication.valueOf(email.authentication().name()),
                        email.username(),password,email.from(),frozen.identity().targetId(),clock);
            }
            var webhook=(AlertReceiver.Webhook)transport;var endpoint=material.endpoint();
            if(endpoint==null||!webhook.matchesEndpoint(endpoint))throw invalid("frozen-endpoint");
            return new WebhookChannel(deployment,binding,endpoint,Set.of(new WebhookChannel.Origin(endpoint.getHost(),endpoint.getPort()<0?443:endpoint.getPort())),ssl,
                    webhook.authentication()==AlertReceiver.Authentication.BEARER?new String(Objects.requireNonNull(authentication),StandardCharsets.UTF_8):null,
                    webhook.authentication()==AlertReceiver.Authentication.HMAC_SHA256?authentication:null,clock,addresses);
        }finally{if(authentication!=null)Arrays.fill(authentication,(byte)0);if(ca!=null)Arrays.fill(ca,(byte)0);if(password!=null)Arrays.fill(password,'\0');}
    }
    private static AlertReceiver.Transport transport(AlertIntent.Frozen f){return f.target() instanceof AlertIntent.ReceiverTarget r?r.transport():((AlertIntent.CandidateTarget)f.target()).transport();}
    private static SSLContext systemTrust(){try{return SSLContext.getDefault();}catch(NoSuchAlgorithmException unavailable){throw new IllegalStateException(unavailable);}}
    static SSLContext trust(byte[] pem){
        if(pem==null||pem.length==0||pem.length>8192)throw invalid("private-ca");
        try{
            var certificates=CertificateFactory.getInstance("X.509").generateCertificates(new ByteArrayInputStream(pem));
            if(certificates.isEmpty()||certificates.size()>16)throw invalid("private-ca");
            KeyStore store=KeyStore.getInstance(KeyStore.getDefaultType());store.load(null,null);int index=0;
            for(var certificate:certificates){if(!(certificate instanceof X509Certificate x)||x.getBasicConstraints()<0)throw invalid("private-ca");store.setCertificateEntry("ca-"+index++,certificate);}
            var managers=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());managers.init(store);
            var ssl=SSLContext.getInstance("TLS");ssl.init(null,managers.getTrustManagers(),null);return ssl;
        }catch(GeneralSecurityException|java.io.IOException invalid){throw invalid("private-ca");}
    }
    @Override public void close(){dns.shutdownNow();}
    private static TapstateException invalid(String field){return new TapstateException(AlertError.INVALID_CONFIGURATION,Map.of("field",field,"reason","unavailable-or-untrusted-channel-binding"),null);}
}
