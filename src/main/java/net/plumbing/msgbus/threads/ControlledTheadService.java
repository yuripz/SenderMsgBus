package net.plumbing.msgbus.threads;

import org.springframework.context.annotation.Bean;
import org.slf4j.Logger;

public class ControlledTheadService implements Runnable {
private Integer theadNum;
private  Logger MessegeSend_Log;

    ControlledTheadService(Integer theadNum, Logger MessegeSend_Log ) {
        this.MessegeSend_Log = MessegeSend_Log;
        this.theadNum = theadNum;
    }

    private Integer theadRunCount = 1;
    private final Integer  theadRunTotalCount = 10;

    @Bean

    public void run() {
        if ( theadNum != null ) {
                MessegeSend_Log.info("ControlledTheadService[" + theadNum + "]: is runing [" + theadRunCount + "] times");
                try {
                    Thread.sleep(theadNum * 100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                MessegeSend_Log.info("ControlledTheadService[" + theadNum + "]: is finished[ " + theadRunCount + "] times");
                theadRunCount += 1;
        }
    }
}
