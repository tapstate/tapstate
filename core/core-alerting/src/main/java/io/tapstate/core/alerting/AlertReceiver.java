package io.tapstate.core.alerting;

import io.tapstate.core.common.JsonWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.Objects;
import java.util.UUID;

/** Public receiver definition. Credential and sensitive endpoint material is never part of this value. */
public record AlertReceiver(AlertConfiguration.Header header, String name, boolean enabled, long bindingRevision,
        Transport transport, CredentialReference credentials) implements AlertConfiguration.Definition {
    public enum Encryption { IMPLICIT_TLS, STARTTLS_REQUIRED }
    public enum Authentication { NONE, PLAIN, LOGIN, BEARER, HMAC_SHA256 }
    public enum Trust { SYSTEM, PRIVATE_CA }
    public sealed interface Transport permits Email, Webhook, HttpsJson {
        String channelType();
        Authentication authentication();
        Trust trust();
    }
    /** A closed HTTPS transport envelope; its public text fields are further constrained by static registration. */
    public record HttpsJson(String channelType, int transportVersion, URI origin, String endpointDigest,
            Authentication authentication, String principal, Trust trust, Map<String,String> parameters) implements Transport {
        public HttpsJson {
            new AlertChannelPolicy.Capability(channelType,1,AlertChannelPolicy.Family.OP_EXTERNAL);
            AlertValidation.require(!java.util.Set.of("email","webhook","cloud-sdk").contains(channelType),"channelType","reserved-id");
            AlertValidation.require(transportVersion==1,"transportVersion","unsupported");
            new Webhook(origin,endpointDigest,authentication,principal,trust);
            AlertValidation.require(parameters!=null&&parameters.size()<=16,"parameters","too-many");
            var normalized=new TreeMap<String,String>();
            parameters.forEach((key,value)->{
                AlertValidation.require(key!=null&&key.matches("[a-z][a-z0-9-]{0,63}")&&value!=null
                        &&value.getBytes(StandardCharsets.UTF_8).length<=256&&value.codePoints().noneMatch(Character::isISOControl),"parameters","invalid-text-field");
                normalized.put(key,value);
            });
            parameters=java.util.Collections.unmodifiableMap(normalized);
            AlertValidation.require(JsonWriter.write(parameters).getBytes(StandardCharsets.UTF_8).length<=4096,"parameters","encoded-budget");
            var envelope=new TreeMap<String,Object>();envelope.put("channelType",channelType);envelope.put("family","HTTPS_JSON");envelope.put("transportVersion",transportVersion);
            envelope.put("origin",origin.toASCIIString());envelope.put("endpointDigest",endpointDigest);envelope.put("authentication",authentication.name());
            envelope.put("principal",principal);envelope.put("trust",trust.name());envelope.put("parameters",parameters);
            AlertValidation.require(JsonWriter.write(envelope).getBytes(StandardCharsets.UTF_8).length<=5120,"transport","encoded-budget");
        }
        public static HttpsJson fromEndpoint(String type,int version,URI endpoint,Authentication auth,String principal,Trust trust,Map<String,String> parameters){
            var original=Webhook.fromEndpoint(endpoint,auth,principal,trust);
            return new HttpsJson(type,version,original.origin(),original.endpointDigest(),auth,principal,trust,parameters);
        }
    }
    public static Webhook https(Transport transport){
        if(transport instanceof Webhook webhook)return webhook;
        var value=(HttpsJson)transport;
        return new Webhook(value.origin(),value.endpointDigest(),value.authentication(),value.principal(),value.trust());
    }
    public static String httpsTargetId(Transport transport){
        var value=https(transport);
        return transport instanceof Webhook?"webhook-"+value.endpointDigest():"https-"+DeliveryKey.fingerprint(java.util.List.of(transport.channelType(),value.endpointDigest()));
    }
    public record Email(String host, int port, Encryption encryption, Authentication authentication,
            String username, String from, List<String> recipients, Trust trust) implements Transport {
        public Email {
            host = AlertValidation.text(host, "smtpHost", 253).toLowerCase(Locale.ROOT);
            AlertValidation.require(host.matches("[a-z0-9._:-]+"), "smtpHost", "invalid-host");
            AlertValidation.require(port > 0 && port <= 65535, "smtpPort", "invalid-port");
            Objects.requireNonNull(encryption, "encryption"); Objects.requireNonNull(trust, "trust");
            AlertValidation.require(authentication == Authentication.NONE || authentication == Authentication.PLAIN
                    || authentication == Authentication.LOGIN, "smtpAuthentication", "unsupported");
            if (authentication == Authentication.NONE) { AlertValidation.require(username == null, "smtpUsername", "unexpected"); }
            else { username = AlertValidation.text(username, "smtpUsername", 256); }
            from = mailbox(from);
            AlertValidation.require(recipients != null && !recipients.isEmpty() && recipients.size() <= 32, "recipients", "outside-1-32");
            recipients = recipients.stream().map(AlertReceiver::mailbox).sorted().toList();
            AlertValidation.require(recipients.stream().distinct().count() == recipients.size(), "recipients", "duplicate-mailbox");
        }
        @Override public String channelType() { return "email"; }
    }
    /** The full endpoint, including secret path/query components, stays in the private encrypted slot. */
    public record Webhook(URI origin, String endpointDigest, Authentication authentication, String principal, Trust trust) implements Transport {
        public Webhook {
            Objects.requireNonNull(origin, "origin"); Objects.requireNonNull(trust, "trust");
            AlertValidation.require(origin.equals(originOf(origin)), "webhookOrigin", "origin-only-required");
            AlertValidation.require(endpointDigest != null && endpointDigest.matches("[a-f0-9]{64}"), "endpointDigest", "invalid-digest");
            AlertValidation.require(authentication == Authentication.NONE || authentication == Authentication.BEARER
                    || authentication == Authentication.HMAC_SHA256, "webhookAuthentication", "unsupported");
            if (authentication == Authentication.NONE) { AlertValidation.require(principal == null, "webhookPrincipal", "unexpected"); }
            else { principal = AlertValidation.text(principal, "webhookPrincipal", 256); }
        }
        @Override public String channelType() { return "webhook"; }
        public static Webhook fromEndpoint(URI endpoint, Authentication authentication, String principal, Trust trust) {
            return new Webhook(originOf(endpoint), endpointDigest(endpoint), authentication, principal, trust);
        }
        public boolean matchesEndpoint(URI endpoint) { return origin.equals(originOf(endpoint)) && endpointDigest.equals(endpointDigest(endpoint)); }
        private static String endpointDigest(URI endpoint) {
            return DeliveryKey.fingerprint(List.of("webhook-endpoint-v1", canonicalEndpoint(endpoint)));
        }
    }
    public record CredentialReference(AlertConfiguration.Identity receiver, String credentialId, String authenticationIdentity) {
        public CredentialReference {
            Objects.requireNonNull(receiver, "receiver");
            AlertValidation.require(receiver.kind() == AlertConfiguration.Kind.RECEIVER, "credentialReceiver", "wrong-kind");
            AlertValidation.require(credentialId != null, "credentialId", "required");
            try {
                UUID id = UUID.fromString(credentialId);
                AlertValidation.require(id.version() == 4 && id.variant() == 2 && id.toString().equals(credentialId), "credentialId", "invalid-uuid");
            } catch (IllegalArgumentException invalid) { AlertValidation.require(false, "credentialId", "invalid-uuid"); }
            authenticationIdentity = AlertValidation.text(authenticationIdentity, "authenticationIdentity", 1024);
        }
        public static CredentialReference create(AlertConfiguration.Identity receiver, String authenticationIdentity) {
            return new CredentialReference(receiver, UUID.randomUUID().toString(), authenticationIdentity);
        }
        public String binding() {
            return JsonWriter.write(List.of(receiver.scope().authorityId(), receiver.scope().clusterId(), receiver.id(),
                    receiver.incarnation(), credentialId, authenticationIdentity));
        }
    }
    public AlertReceiver {
        Objects.requireNonNull(header, "header"); Objects.requireNonNull(transport, "transport");
        AlertValidation.require(header.identity().kind() == AlertConfiguration.Kind.RECEIVER, "receiver", "wrong-kind");
        AlertValidation.require(header.deployment() == AlertChannelPolicy.Deployment.ON_PREM, "deployment", "receiver-not-permitted");
        name = AlertValidation.text(name, "receiverName", 128);
        AlertValidation.require(bindingRevision > 0 && bindingRevision <= header.revision(), "bindingRevision", "invalid-revision");
        boolean privateSlot = !(transport instanceof Email) || transport.authentication() != Authentication.NONE || transport.trust() == Trust.PRIVATE_CA;
        AlertValidation.require(privateSlot == (credentials != null), "credentials", "private-slot-required-or-unexpected");
        if (credentials != null) {
            AlertValidation.require(credentials.receiver().equals(header.identity()), "credentials", "wrong-receiver");
            AlertValidation.require(credentials.authenticationIdentity().equals(authenticationIdentity(transport)), "credentials", "authentication-identity-mismatch");
        }
    }
    public AlertReceiver edit(String name, boolean enabled, Transport transport, CredentialReference credentials, Instant at, String actor) {
        boolean changed = enabled != this.enabled || !transport.equals(this.transport) || !Objects.equals(credentials, this.credentials);
        AlertValidation.require(!changed || bindingRevision < Long.MAX_VALUE, "bindingRevision", "exhausted");
        return new AlertReceiver(header.next(at, actor), name, enabled, bindingRevision + (changed ? 1 : 0), transport, credentials);
    }
    public AlertReceiver retire(Instant at, String actor) {
        return new AlertReceiver(header.retire(at, actor), name, enabled, bindingRevision, transport, credentials);
    }
    public boolean succeeds(AlertReceiver before) {
        if (!header.succeeds(before.header())) { return false; }
        boolean changed = enabled != before.enabled || !transport.equals(before.transport) || !Objects.equals(credentials, before.credentials);
        return bindingRevision == before.bindingRevision + (changed ? 1 : 0)
                && (header.active() || !changed && name.equals(before.name));
    }
    public static String authenticationIdentity(Transport transport) {
        return transport instanceof Email email ? JsonWriter.write(List.of("smtp", email.authentication().name(), email.username() == null ? "" : email.username()))
                : JsonWriter.write(List.of(transport.channelType(), transport.authentication().name(), https(transport).principal() == null ? "" : https(transport).principal()));
    }
    public static URI originOf(URI endpoint) {
        canonicalEndpoint(endpoint);
        try { return new URI("https", null, endpoint.getHost().toLowerCase(Locale.ROOT), endpoint.getPort() == 443 ? -1 : endpoint.getPort(), null, null, null); }
        catch (java.net.URISyntaxException invalid) { throw new IllegalStateException("validated origin could not be represented", invalid); }
    }
    public static String canonicalEndpoint(URI endpoint) {
        AlertValidation.require(endpoint != null && "https".equalsIgnoreCase(endpoint.getScheme()) && endpoint.getHost() != null
                && endpoint.getUserInfo() == null && endpoint.getFragment() == null && endpoint.getPort() >= -1 && endpoint.getPort() != 0
                && endpoint.getPort() <= 65535 && endpoint.toASCIIString().getBytes(StandardCharsets.UTF_8).length <= 2048,
                "webhookEndpoint", "invalid-https-endpoint");
        try {
            String authority = new URI("https", null, endpoint.getHost().toLowerCase(Locale.ROOT),
                    endpoint.getPort() == 443 ? -1 : endpoint.getPort(), null, null, null).toASCIIString();
            return authority + (endpoint.getRawPath() == null || endpoint.getRawPath().isEmpty() ? "/" : endpoint.getRawPath())
                    + (endpoint.getRawQuery() == null ? "" : "?" + endpoint.getRawQuery());
        } catch (java.net.URISyntaxException invalid) { throw new IllegalStateException("validated endpoint could not be represented", invalid); }
    }
    private static String mailbox(String value) {
        AlertValidation.require(value != null && value.length() <= 254 && value.indexOf('@') > 0 && value.indexOf('@') == value.lastIndexOf('@'), "mailbox", "invalid-address");
        String local = value.substring(0, value.indexOf('@')); String domain = value.substring(value.indexOf('@') + 1).toLowerCase(Locale.ROOT);
        AlertValidation.require(local.length() <= 64 && local.matches("[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+(\\.[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+)*"), "mailbox", "invalid-local-part");
        String[] labels = domain.split("\\.", -1);
        AlertValidation.require(labels.length >= 2 && java.util.Arrays.stream(labels).allMatch(label -> label.matches("[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")), "mailbox", "invalid-domain");
        return local + "@" + domain;
    }
}
