package io.tapstate.core.alerting;

import java.nio.charset.StandardCharsets;
import java.util.*;

/** Trusted static descriptions for existing form controls and bounded HTTPS JSON adapters. No runtime code loading. */
public final class AlertChannelCatalog {
    public enum TransportFamily { EMAIL, HTTPS_JSON }
    public record Field(String key,String label,int maximumBytes,boolean required,String defaultValue) {
        public Field {
            AlertValidation.require(key!=null&&key.matches("[a-z][a-z0-9-]{0,63}"),"parameterKey","invalid");
            label=AlertValidation.text(label,"parameterLabel",128);
            AlertValidation.require(maximumBytes>0&&maximumBytes<=256,"parameterBytes","outside-budget");
            AlertValidation.require(defaultValue==null||defaultValue.getBytes(StandardCharsets.UTF_8).length<=maximumBytes,"parameterDefault","outside-budget");
        }
    }
    public record Descriptor(AlertChannelPolicy.Capability capability,String label,TransportFamily transportFamily,int transportVersion,List<Field> fields) {
        public Descriptor {
            Objects.requireNonNull(capability);Objects.requireNonNull(transportFamily);
            AlertValidation.require(capability.family()==AlertChannelPolicy.Family.OP_EXTERNAL&&transportVersion==1,"channelDescriptor","unsupported");
            label=AlertValidation.text(label,"channelLabel",128);fields=List.copyOf(fields);
            AlertValidation.require(fields.size()<=16&&fields.stream().map(Field::key).distinct().count()==fields.size(),"parameterFields","invalid-or-too-many");
        }
        public Map<String,String> parameters(Map<String,String> input) {
            AlertValidation.require(input!=null&&fields.stream().map(Field::key).collect(java.util.stream.Collectors.toSet()).containsAll(input.keySet()),"parameters","unregistered-field");
            var values=new TreeMap<String,String>();
            for(var field:fields){String value=input.containsKey(field.key())?input.get(field.key()):field.defaultValue();
                AlertValidation.require(value!=null||!field.required(),"parameters."+field.key(),"required");
                if(value!=null){AlertValidation.require(value.getBytes(StandardCharsets.UTF_8).length<=field.maximumBytes()
                        &&value.codePoints().noneMatch(Character::isISOControl)&&(!field.required()||!value.isBlank()),"parameters."+field.key(),"invalid-text");values.put(field.key(),value);}
            }
            return Collections.unmodifiableMap(values);
        }
    }
    private final List<Descriptor> descriptors;
    public AlertChannelCatalog(Collection<Descriptor> descriptors){
        this.descriptors=List.copyOf(descriptors);
        AlertValidation.require(this.descriptors.size()<=32&&this.descriptors.stream().map(d->d.capability().typeId()).distinct().count()==this.descriptors.size(),"channels","duplicate-or-too-many");
    }
    public static AlertChannelCatalog defaults(){return new AlertChannelCatalog(List.of(
            new Descriptor(new AlertChannelPolicy.Capability("email",1,AlertChannelPolicy.Family.OP_EXTERNAL),"Email",TransportFamily.EMAIL,1,List.of()),
            new Descriptor(new AlertChannelPolicy.Capability("webhook",1,AlertChannelPolicy.Family.OP_EXTERNAL),"Webhook",TransportFamily.HTTPS_JSON,1,List.of())));}
    public List<Descriptor> descriptors(){return descriptors;}
    public Descriptor require(String id){return descriptors.stream().filter(d->d.capability().typeId().equals(id)).findFirst()
            .orElseThrow(()->new io.tapstate.core.common.TapstateException(AlertError.CHANNEL_UNAVAILABLE,Map.of("channel",id,"phase","registration"),null));}
    public AlertChannelCatalog with(Collection<Descriptor> extensions){var values=new ArrayList<>(descriptors);values.addAll(extensions);return new AlertChannelCatalog(values);}
}
