package io.tapstate.adapters.pdk;

/** Local, explicitly enabled call receipts for an owned benchmark process. */
public interface PdkWriteReturnProbeMBean {
    long getPid();
    long getJvmStartTimeMillis();
    long getNanoTime();
    String getClockMetadata();
    String[] getColdMetadata();
    String getWindow();
    String getState();
    long getCompletedCalls();
    long getFailedCalls();
    long getReportedRecords();
    long getOpenCalls();
    long getRetainedBytes();
    String getCostStages();
    String getWorkBudget();
    boolean armWorkBudgetFinalReceipt(String absoluteBodyPath, String nonce);
    boolean start(String window);
    boolean stop();
    byte[] read(long completionCursor);
}
