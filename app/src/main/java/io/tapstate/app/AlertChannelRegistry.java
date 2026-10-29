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
    record Context(AlertChannelPolicy.Deployment deployment,AlertChannelPolicy.Capability capability,TargetBinding binding,
            AlertReceiver.HttpsJson transport,URI endpoint,SSLContext tls,String bearer,byte[] signingKey,Clock clock,InetAddress[] addresses) { }
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
