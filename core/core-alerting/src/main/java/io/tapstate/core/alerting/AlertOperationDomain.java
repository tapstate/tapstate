package io.tapstate.core.alerting;

/** A bounded, host-derived logical access identity. Source creation identity stays unchanged. */
public final class AlertOperationDomain {
    public static final int MAX_RETAINED=64;
    private AlertOperationDomain() { }
    public static String qualifier(String ruleIncarnation,String domain) {
        AlertValidation.text(ruleIncarnation,"ruleIncarnation",128);
        AlertValidation.require(domain!=null && domain.matches("[0-9a-f]{64}"),"operationDomain","invalid-token");
        return ruleIncarnation+":op2:"+domain;
    }
    public static boolean belongs(String ruleIncarnation,AlarmKey key) {
        if(ruleIncarnation.equals(key.qualifier()))return true;
        String prefix=ruleIncarnation+":op2:";
        return supported(key) && key.qualifier().startsWith(prefix)
                && key.qualifier().substring(prefix.length()).matches("[0-9a-f]{64}");
    }
    public static String token(String ruleIncarnation,AlarmKey key) {
        if(ruleIncarnation.equals(key.qualifier()))return null;
        AlertValidation.require(belongs(ruleIncarnation,key),"operationDomain","foreign-rule");
        return key.qualifier().substring((ruleIncarnation+":op2:").length());
    }
    public static boolean supported(AlarmKey key) {
        return key.typeId().equals("source.connection-failed") && key.resource().kind().equals("source")
                || key.typeId().equals("cluster.storage-unavailable") && key.resource().kind().equals("cluster-storage")
                && !key.resource().id().equals("cache");
    }
}
