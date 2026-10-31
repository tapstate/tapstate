package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.AlertConfiguration.Header;
import io.tapstate.core.alerting.AlertReceiver;
import io.tapstate.core.alerting.AlertReceiver.*;
import java.net.URI;
import java.util.Arrays;
import java.util.Set;
import org.bson.Document;

/** Explicit public receiver fields; this codec never returns the private slot. */
final class MongoAlertReceiverCodec {
    private MongoAlertReceiverCodec() { }
    static Document body(AlertReceiver receiver) {
        Document credentials = credentials(receiver.credentials());
        return new Document("name", receiver.name()).append("enabled", receiver.enabled()).append("bindingRevision", receiver.bindingRevision())
                .append("transport", transport(receiver.transport())).append("credentials", credentials);
    }
    static Document transport(Transport value) {
        return switch (value) {
            case Email email -> new Document("channelType", "email").append("host", email.host()).append("port", email.port())
                    .append("encryption", email.encryption().name()).append("authentication", email.authentication().name())
                    .append("username", email.username()).append("from", email.from()).append("recipients", email.recipients()).append("trust", email.trust().name());
            case Webhook webhook -> new Document("channelType", "webhook").append("origin", webhook.origin().toASCIIString())
                    .append("endpointDigest", webhook.endpointDigest()).append("authentication", webhook.authentication().name())
                    .append("principal", webhook.principal()).append("trust", webhook.trust().name());
            case HttpsJson https -> generic(https);
        };
    }
    private static Document generic(HttpsJson value){
        var parameters=new Document();value.parameters().forEach(parameters::append);transportBudget(MongoAlertConfigurationCodec.size(parameters)<=4096,"parameters");
        var encoded=new Document("channelType",value.channelType()).append("family","HTTPS_JSON").append("transportVersion",value.transportVersion())
                .append("origin",value.origin().toASCIIString()).append("endpointDigest",value.endpointDigest()).append("authentication",value.authentication().name())
                .append("principal",value.principal()).append("trust",value.trust().name()).append("parameters",parameters);
        transportBudget(MongoAlertConfigurationCodec.size(encoded)<=5120,"transport");return encoded;
    }
    private static void transportBudget(boolean allowed,String field){if(!allowed)throw new io.tapstate.core.common.TapstateException(io.tapstate.core.alerting.AlertError.INVALID_CONFIGURATION,
            java.util.Map.of("field",field,"reason","encoded-record-too-large"),null);}
    static Document credentials(CredentialReference value) {
        return value == null ? null : new Document("credentialId", value.credentialId()).append("authenticationIdentity", value.authenticationIdentity());
    }
    static AlertReceiver read(Header header, Document body) {
        fields(body, Set.of("name", "enabled", "bindingRevision", "transport", "credentials"));
        require(body.get("enabled") instanceof Boolean && body.get("bindingRevision") instanceof Long);
        Transport transport = readTransport(document(body.get("transport")));
        return new AlertReceiver(header, text(body, "name"), body.getBoolean("enabled"), body.getLong("bindingRevision"), transport,
                body.get("credentials") == null ? null : readCredentials(header.identity(), document(body.get("credentials"))));
    }
    static Transport readTransport(Document t) {
        if(t.containsKey("family")){
            fields(t,Set.of("channelType","family","transportVersion","origin","endpointDigest","authentication","principal","trust","parameters"));
            require("HTTPS_JSON".equals(t.get("family"))&&Integer.valueOf(1).equals(t.get("transportVersion"))&&MongoAlertConfigurationCodec.size(t)<=5120);
            var raw=document(t.get("parameters"));require(MongoAlertConfigurationCodec.size(raw)<=4096);var parameters=new java.util.TreeMap<String,String>();
            raw.forEach((key,value)->{require(value instanceof String);parameters.put(key,(String)value);});
            return new HttpsJson(text(t,"channelType"),1,URI.create(text(t,"origin")),text(t,"endpointDigest"),enumValue(Authentication.values(),text(t,"authentication")),
                    nullableText(t,"principal"),enumValue(Trust.values(),text(t,"trust")),parameters);
        }
        return switch (text(t, "channelType")) {
            case "email" -> {
                fields(t, Set.of("channelType", "host", "port", "encryption", "authentication", "username", "from", "recipients", "trust"));
                require(t.get("port") instanceof Integer && t.get("recipients") instanceof java.util.List<?>);
                require(t.getList("recipients", Object.class).size() <= 32 && t.getList("recipients", Object.class).stream().allMatch(String.class::isInstance));
                yield new Email(text(t, "host"), t.getInteger("port"), enumValue(Encryption.values(), text(t, "encryption")),
                        enumValue(Authentication.values(), text(t, "authentication")), nullableText(t, "username"), text(t, "from"),
                        t.getList("recipients", String.class), enumValue(Trust.values(), text(t, "trust")));
            }
            case "webhook" -> {
                fields(t, Set.of("channelType", "origin", "endpointDigest", "authentication", "principal", "trust"));
                yield new Webhook(URI.create(text(t, "origin")), text(t, "endpointDigest"), enumValue(Authentication.values(), text(t, "authentication")),
                        nullableText(t, "principal"), enumValue(Trust.values(), text(t, "trust")));
            }
            default -> throw new IllegalArgumentException("unknown receiver transport");
        };
    }
    static CredentialReference readCredentials(io.tapstate.core.alerting.AlertConfiguration.Identity identity, Document value) {
        fields(value, Set.of("credentialId", "authenticationIdentity"));
        return new CredentialReference(identity, text(value, "credentialId"), text(value, "authenticationIdentity"));
    }
    private static String text(Document doc, String name) { require(doc.get(name) instanceof String); return doc.getString(name); }
    private static String nullableText(Document doc, String name) { return doc.get(name) == null ? null : text(doc, name); }
    private static Document document(Object value) { require(value instanceof Document); return (Document) value; }
    private static void fields(Document value, Set<String> expected) { require(value.keySet().equals(expected)); }
    private static void require(boolean valid) { if (!valid) { throw new IllegalArgumentException("invalid receiver encoding"); } }
    private static <E extends Enum<E>> E enumValue(E[] known, String value) {
        return Arrays.stream(known).filter(item -> item.name().equals(value)).findFirst().orElseThrow(IllegalArgumentException::new);
    }
}
