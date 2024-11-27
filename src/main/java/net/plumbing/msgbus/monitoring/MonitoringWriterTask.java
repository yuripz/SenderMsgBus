package net.plumbing.msgbus.monitoring;


import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
// import SenderApplication;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.time.Instant;

import net.plumbing.msgbus.model.MonitoringQueueVO;


@Component
// @Scope("prototype")


public class MonitoringWriterTask implements Runnable
{
     // public MonitoringWriterTask() { super(); }

    public void setContext( ApplicationContext Context ) {
        // this.context = Context;
        // this.SenderExecutor = (ThreadPoolTaskExecutor) context.getBean("messageSender");
        // this.schedulerService= new ControlledTheadService(theadNum, monitorWriter_Log );
    }

    //private ThreadPoolTaskExecutor SenderExecutor;
    // private ControlledTheadService schedulerService;

    //private ApplicationContext context;
    private   String name;
    public static final Logger monitorWriter_Log = LoggerFactory.getLogger(MonitoringWriterTask.class);



    private String MonitoringDbURL;
    private String MonitoringDbLogin;
    private String MonitoringDbPasswd;
    private String MonitoringStoreTableName;
    private String MonitoringdataSourceClassName;
    private Integer theadNum=null;
    private Integer WaitTimeBetweenScan;
    private Integer NumMessageInScan;



    public void setWaitTimeBetweenScan(Integer waitTimeBetweenScan) {
        this.WaitTimeBetweenScan = waitTimeBetweenScan;
    }
    public void setNumMessageInScan(Integer numMessageInScan) { this.NumMessageInScan = numMessageInScan +1;    } // потому что rownum < numMessageInScan +1

    public void setMonitoringDbParam(String dataSourceClassName, String db_URL, String db_userid , String db_password, String dataStoreTableName) {
        this.MonitoringdataSourceClassName = dataSourceClassName;
        this.MonitoringDbLogin = db_userid;
        this.MonitoringDbPasswd = db_password;
        this.MonitoringDbURL = db_URL;
        this.MonitoringStoreTableName = dataStoreTableName;
    }


    public void setTheadNum( int TheadNum) {
        this.theadNum = TheadNum;
    }

    private int theadRunCount = 0;
    private  int  theadRunTotalCount = 1;

 @Bean( name = "МonitoringWriterTaskRun")
   // @Scheduled(initialDelay = 100, fixedRate = 1000)
    public void run() {
        //if (( theadNum != null ) && ((theadNum == 17) || (theadNum == 18) || (theadNum == 19) || (theadNum == 20)) )
        if (( theadNum != null ) ) // && (theadNum == 0))
        {
            Integer count = 0;
            AppendDataAccess monitorDataAccess = new AppendDataAccess();
            // Установаливем " соединение" , что бы сбрасывать  очередь в БД
            monitorWriter_Log.info(
                    "make_Monitoring_Connection( " + this.MonitoringdataSourceClassName + ", " +
                            this.MonitoringDbURL + ", " + this.MonitoringDbLogin + ", " + this.MonitoringDbPasswd + ", " + this.MonitoringStoreTableName
            );
            Connection MonitorDB_Connection = monitorDataAccess.make_Monitoring_Connection(this.MonitoringdataSourceClassName,
                    this.MonitoringDbURL, this.MonitoringDbLogin, this.MonitoringDbPasswd, this.MonitoringStoreTableName,
                    monitorWriter_Log
            );


            monitorWriter_Log.info("MonitorWriterTask[" + theadNum + "]: is runing [" + theadRunCount + "] times");
            MonitoringQueueVO monitoringQueueVO = new MonitoringQueueVO();
            if (monitorDataAccess == null)
            {
                monitorWriter_Log.warn("MonitorWriterTask[" + theadNum + "]: cann't make DB connection");
                return;
            }

            Date moment = new Date(1451665447567L); // Задаем количество миллисекунд Unix-time с того-самого-момента
            long startTimestamp = Instant.ofEpochSecond(0L).until(Instant.now(), ChronoUnit.SECONDS);
            theadRunTotalCount =1;
            for ( theadRunCount = 0 ; theadRunCount < theadRunTotalCount; theadRunCount += 1 ) {
                long secondsFromEpoch = Instant.ofEpochSecond(0L).until(Instant.now(), ChronoUnit.SECONDS);
                theadRunTotalCount +=1;


                // this.SenderExecutor.execute(schedulerService);
                try {
                    monitorWriter_Log.warn("MonitorWriterTask[" + theadNum + "]: before read4queue ConcurrentQueue SizeOf: " + ConcurrentQueue.SizeOf());
                    do {
                        monitoringQueueVO = ConcurrentQueue.read4queue();
                        // monitorWriter_Log.warn("MonitorWriterTask[" + theadNum + "]: ConcurrentQueue SizeOf after read4queue: " + ConcurrentQueue.SizeOf());
                        if ( monitoringQueueVO !=null )
                        try {
                            // monitorWriter_Log.info( "MonitorWriterTask[" + theadNum + "] after read4queue: " + monitoringQueueVO.toString() );
                            monitorDataAccess.stmtInsertData.setLong(1, monitoringQueueVO.Queue_Id);
                            monitorDataAccess.stmtInsertData.setString(2, monitoringQueueVO.Queue_Direction);
                            monitorDataAccess.stmtInsertData.setTimestamp(3,  monitoringQueueVO.Queue_Date );
                            monitorDataAccess.stmtInsertData.setInt( 4,  monitoringQueueVO.Msg_Status );
                            monitorDataAccess.stmtInsertData.setTimestamp(5,  monitoringQueueVO.Msg_Date );
                            monitorDataAccess.stmtInsertData.setInt( 6,  monitoringQueueVO.Operation_Id );
                            //monitorDataAccess.stmtInsertData.setBigDecimal(7, monitoringQueueVO.OutQueue_Id );
                            monitorDataAccess.stmtInsertData.setLong(7, 0L );
                            monitorDataAccess.stmtInsertData.setString(8, monitoringQueueVO.Msg_Type );
                            monitorDataAccess.stmtInsertData.setString(9, monitoringQueueVO.Msg_Reason );
                            monitorDataAccess.stmtInsertData.setInt( 10,  monitoringQueueVO.MsgDirection_Id );
                            monitorDataAccess.stmtInsertData.setInt( 11,  monitoringQueueVO.Msg_InfoStreamId );
                            monitorDataAccess.stmtInsertData.setString(12, monitoringQueueVO.Msg_Type_own );
                            monitorDataAccess.stmtInsertData.setString(13, monitoringQueueVO.Msg_Result );
                            monitorDataAccess.stmtInsertData.setString(14, monitoringQueueVO.SubSys_Cod );
                            monitorDataAccess.stmtInsertData.setInt( 15,  monitoringQueueVO.Retry_Count );
                            monitorDataAccess.stmtInsertData.setString(16, monitoringQueueVO.Prev_Queue_Direction);
                            monitorDataAccess.stmtInsertData.setTimestamp(17,  monitoringQueueVO.Prev_Msg_Date );
                            monitorDataAccess.stmtInsertData.setTimestamp(18,  monitoringQueueVO.Queue_Create_Date );
                            monitorDataAccess.stmtInsertData.setLong(19, monitoringQueueVO.Perform_Object_Id );
                            monitorDataAccess.stmtInsertData.setTimestamp(20,  monitoringQueueVO.Req_Dt );
                            monitorDataAccess.stmtInsertData.setString(21,  monitoringQueueVO.Request );
                            monitorDataAccess.stmtInsertData.setTimestamp(22,  monitoringQueueVO.Req_Dt );
                            monitorDataAccess.stmtInsertData.setString(23,  monitoringQueueVO.Response );
                            monitorDataAccess.stmtInsertData.executeUpdate();
                        } catch (SQLException e) {
                            monitorWriter_Log.error(e.getMessage());
                            System.err.println("MonitorWriterTask[" + theadNum + "]: monitorDataAccess.stmtInsertData.executeUpdate() fault что то пошло совсем не так...");
                            e.printStackTrace();
                            monitorWriter_Log.error("MonitorWriterTask[" + theadNum + "]: monitorDataAccess.stmtInsertData.executeUpdate() fault что то пошло совсем не так...");
                            return;
                        }
                    }
                    while ( monitoringQueueVO !=null );
                    try { if (MonitorDB_Connection != null ) MonitorDB_Connection.commit();
                    } catch (SQLException e) {
                        monitorWriter_Log.error(e.getMessage());
                        System.err.println(" \"MonitorDB_Connection.commit()\": что то пошло совсем не так...");
                        e.printStackTrace( );
                        monitorWriter_Log.error(" \"MonitorDB_Connection.commit()\": что то пошло совсем не так...");
                        return;
                    }
                    monitorWriter_Log.info( "MonitorWriterTask[" + theadNum + "]: Ждём'c в " + theadRunCount + " раз " +  WaitTimeBetweenScan + " миллисек., уже " + (secondsFromEpoch - startTimestamp ) + " сек., начиная с =" + startTimestamp + " текущее время =" + secondsFromEpoch
                            // +"secondsFromEpoch - startTimestamp=" + (secondsFromEpoch - startTimestamp) +  " Long.valueOf(60L * TotalTimeTasks)=" + Long.valueOf(60L * TotalTimeTasks)
                    );
                    Thread.sleep( WaitTimeBetweenScan);

                } catch (InterruptedException e) {
                    monitorWriter_Log.error("MonitorWriterTask[" + theadNum + "]: is interrapted: " + e.getMessage());
                    e.printStackTrace();
                }

             //   monitorWriter_Log.info("MonitorWriterTask[" + theadNum + "]: is finished[ " + theadRunCount + "] times");

            }
            monitorWriter_Log.info("MonitorWriterTask[" + theadNum + "]: is finished[ " + theadRunCount + "] times");
            // this.SenderExecutor.shutdown();
            try {
                if (MonitorDB_Connection != null ) MonitorDB_Connection.close();
            } catch (SQLException e) {
                monitorWriter_Log.error(e.getMessage());
                e.printStackTrace();
            }
        }
    }


}
