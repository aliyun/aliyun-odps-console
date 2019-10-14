package com.aliyun.odps.ship.common;

import org.apache.commons.lang.time.StopWatch;

public class DshipStopWatch extends StopWatch {
    private boolean printIOElapsedTime = false;
    private String name;

    public DshipStopWatch(String name, boolean printIOElapsedTime) {
        this.name = name;
        this.printIOElapsedTime = printIOElapsedTime;
        this.start();
        this.suspend();
    }

    public String getFormattedSummary() {
        if (printIOElapsedTime) {
            long elapsedTime = this.getTime();
            return String.format(", %s: %s", name, Util.toReadableMilliseconds(elapsedTime));
        } else {
            return "";
        }
    }
}
