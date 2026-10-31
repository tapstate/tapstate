package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.adapters.alerting.TargetBinding;
import io.tapstate.spi.alerting.NotificationChannel;
import java.net.*;
import java.time.Clock;
import java.util.*;
import javax.net.ssl.SSLContext;

/** Static host registrations. Extension adapters consume the same frozen HTTPS/JSON envelope and permission boundary. */
final class AlertChannelRegistry {
    interface Factory { NotificationChannel open(Context context); }
    /** Borrowed construction inputs; a channel copies any material it retains after its factory returns. */
    static final class Context {
        private final AlertChannelPolicy.Deployment deployment;
        private final AlertChannelPolicy.Capability capability;
        private final TargetBinding binding;
        private final AlertReceiver.HttpsJson transport;
        private final URI endpoint;
        private final SSLContext tls;
        private final String bearer;
        private final byte[] signingKey;
        private final Clock clock;
        private final InetAddress[] addresses;
        Context(AlertChannelPolicy.Deployment deployment,AlertChannelPolicy.Capability capability,TargetBinding binding,
                AlertReceiver.HttpsJson transport,URI endpoint,SSLContext tls,String bearer,byte[] signingKey,Clock clock,InetAddress[] addresses) {
            this.deployment=deployment;this.capability=capability;this.binding=binding;this.transport=transport;
            this.endpoint=endpoint;this.tls=tls;this.bearer=bearer;this.signingKey=signingKey;this.clock=clock;this.addresses=addresses;
        }
        public AlertChannelPolicy.Deployment deployment(){return deployment;}
        public AlertChannelPolicy.Capability capability(){return capability;}
        public TargetBinding binding(){return binding;}
        public AlertReceiver.HttpsJson transport(){return transport;}
        public URI endpoint(){return endpoint;}
        public SSLContext tls(){return tls;}
        public String bearer(){return bearer;}
        public byte[] signingKey(){return signingKey;}
        public Clock clock(){return clock;}
        public InetAddress[] addresses(){return addresses;}
    }
    record Registration(AlertChannelCatalog.Descriptor descriptor,Factory factory) {
        Registration { Objects.requireNonNull(descriptor);Objects.requireNonNull(factory);
            if(descriptor.transportFamily()!=AlertChannelCatalog.TransportFamily.HTTPS_JSON||descriptor.capability().version()!=1
                    ||Set.of("email","webhook","cloud-sdk").contains(descriptor.capability().typeId()))throw new IllegalArgumentException("invalid static HTTPS registration"); }
    }
    private final Map<String,Registration> extensions;
    private final AlertChannelCatalog catalog;
    AlertChannelRegistry(List<Registration> registrations){
        var map=new LinkedHashMap<String,Registration>();for(var entry:registrations)if(map.putIfAbsent(entry.descriptor().capability().typeId(),entry)!=null)throw new IllegalArgumentException("duplicate channel registration");
        extensions=Map.copyOf(map);catalog=AlertChannelCatalog.defaults().with(registrations.stream().map(Registration::descriptor).toList());
    }
    static AlertChannelRegistry defaults(){return new AlertChannelRegistry(List.of());}
    AlertChannelCatalog catalog(){return catalog;}
    Registration require(String type){var value=extensions.get(type);if(value==null)throw new io.tapstate.core.common.TapstateException(AlertError.CHANNEL_UNAVAILABLE,Map.of("channel",type,"phase","registration"),null);return value;}
}
