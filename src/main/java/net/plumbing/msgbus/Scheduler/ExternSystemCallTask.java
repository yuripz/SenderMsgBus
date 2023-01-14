package net.plumbing.msgbus.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.springframework.context.annotation.Bean;
//import org.springframework.scheduling.annotation.EnableScheduling;
//import org.springframework.stereotype.Component;

//@Component
//@EnableScheduling
public class ExternSystemCallTask implements Runnable {
    private Integer WaitTimeBetweenScan=1000;
    private Integer theadNum=null;
    private String MessageType_4_Scheduler;

    public void setWaitTimeBetweenScan(Integer waitTimeBetweenScan) {
        this.WaitTimeBetweenScan = waitTimeBetweenScan;
    }
    public void setMessageType_4_Scheduled( String messageType_4_Scheduler) { this.MessageType_4_Scheduler = messageType_4_Scheduler; }
    public static final Logger externSystemCallTas_Log = LoggerFactory.getLogger(ExternSystemCallTask.class);
    public void setTheadNum( int TheadNum) {
        this.theadNum = TheadNum;
    }

    private int theadRunCount = 0;
    private  int  theadRunTotalCount = 1;

//    @Bean( name = "ExternSystemCallTaskRun")
    public void run() {
        externSystemCallTas_Log.info("ExternSystemCallTask[" + theadNum + "]: is started");
       if ( theadNum == null ) return;
        for ( theadRunCount = 0 ; true; theadRunCount += 1 )
        try {
            externSystemCallTas_Log.info("ExternSystemCallTask[" + theadNum + "]: is runing [" + theadRunCount + "] times");
            Thread.sleep( WaitTimeBetweenScan);

        } catch (InterruptedException e) {
            externSystemCallTas_Log.error("ExternSystemCallTask[" + theadNum + "]: is interrapted: " + e.getMessage());
            e.printStackTrace();
        }
    }

}
