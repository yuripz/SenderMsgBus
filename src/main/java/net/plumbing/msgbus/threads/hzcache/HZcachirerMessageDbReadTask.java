package net.plumbing.msgbus.threads.hzcache;
import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.map.IMap;
import net.plumbing.msgbus.common.ApplicationProperties;
import net.plumbing.msgbus.threads.TheadDataAccess;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class HZcachirerMessageDbReadTask implements Runnable {

    private String HrmsSchema;
    private String HrmsPoint;
    private String hrmsDbLogin;
    private String hrmsDbPasswd;
    private Integer theadNum;
    private Long TotalTimeTasks;
    private Integer WaitTimeBetweenScan;
    private Integer NumMessageInScan;
    private Integer ApiRestWaitTime;
    private Integer FirstInfoStreamId;
    private Integer CuberNumId;
    private Integer totalCachiererTaskNum;
    private IMap<Long, HzQueueVO> hzMsgSenderMap;

    public String getCurrentTaskStatus() {return CurrentTaskStatus; }

    public void setWaitTimeBetweenScan(Integer waitTimeBetweenScan) {
        this.WaitTimeBetweenScan = waitTimeBetweenScan;
    }
    public void setNumMessageInScan(Integer numMessageInScan) { this.NumMessageInScan = numMessageInScan +1;    } // потому что rownum < numMessageInScan +1

    public void setHrmsPoint(String hrmspoint) {
        this.HrmsPoint = hrmspoint;
    }
    public void setHrmsSchema( String HrmsSchema) { this.HrmsSchema = HrmsSchema; }
    public void setHrmsDbPasswd(String hrmsDbPasswd) {
        this.hrmsDbPasswd = hrmsDbPasswd;
    }
    public void setHrmsDbLogin(String hrmsDbLogin) {
        this.hrmsDbLogin = hrmsDbLogin;
    }
    public void setTotalCachiererTaskNum( int TotalCachiererTaskNum ) { this.totalCachiererTaskNum= TotalCachiererTaskNum; }

    public void setTotalTimeTasks(Long totalTimeTasks) {
        this.TotalTimeTasks = totalTimeTasks;
    }
    public void setTheadNum( int TheadNum) {
        this.theadNum = TheadNum;
    }
    public void setCuberNumId( int cuberNumId) { this.CuberNumId=cuberNumId;}
    public void setFirstInfoStreamId( int FirstInfoStreamId) { this.FirstInfoStreamId=FirstInfoStreamId;}
    public void setHZmsgSenderMap( IMap<Long, HzQueueVO> hzMsgSenderMap ) { this.hzMsgSenderMap= hzMsgSenderMap; }
    private long theadRunCount = 0L;
    private  long  theadRunTotalCount = 1L;
    private String CurrentTaskStatus;
    private Logger CachirerMessageDbRead_Log;
    public void setCachirerMessageDbRead_Loger(Logger cachirerMessageDbRead_Log) {this.CachirerMessageDbRead_Log = cachirerMessageDbRead_Log;}


    public void run() {
        //if (( theadNum != null ) && ((theadNum == 17) || (theadNum == 18) || (theadNum == 19) || (theadNum == 20)) )
        if ((theadNum == null)) // && (theadNum == 0))
        {
            CurrentTaskStatus = "theadNum == null, return";
            return;
        }
        CurrentTaskStatus = "theadNum ==" + theadNum + " running";

        TheadDataAccess theadDataAccess = new TheadDataAccess();
        theadDataAccess.setDbSchema( HrmsSchema );
        // Установаливем "техническое соединение", что бы зачитывать очередь, на нём же будут отрабатываться и забросы к "локальному" экземпляру БД
        Connection Hermes_Connection = theadDataAccess.make_Hermes_Connection_Only(  HrmsPoint, hrmsDbLogin, hrmsDbPasswd,
                ApplicationProperties.InternalDbPgSetupConnection,  theadNum,
                CachirerMessageDbRead_Log
        );
        if ( Hermes_Connection == null) {
            CachirerMessageDbRead_Log.error("HZcachirerMessageDbReadTask: НЕ удалось Установить соединение , что бы зачитывать очередь, либо подготовить запросы к БД ");
            return;
        }

        CachirerMessageDbRead_Log.info("HZcachirerMessageDbReadTask[{}]: is runing ", theadNum);
        String rdbmsVendor = theadDataAccess.getRdbmsVendor();
        String selectMessageSQL;
        PreparedStatement stmtMsgQueue;

        if (rdbmsVendor.equals("oracle") ) // для PostGree используем псевдостолбец CTID с типом ::tid
        { selectMessageSQL = """
                            select * from ( select q.ROWID,
                                    q.queue_Id,
                                    q.queue_Direction,
                                    q.queue_date,
                                    q.msg_Status,
                                    q.Msg_Date,
                                    q.Operation_id,
                                    to_Char(q.outqueue_id, '9999999999999999') as outQueue_Id,
                                    q.msg_Type,
                                    q.msg_Reason,
                                    q.msgDirection_Id,
                                    q.msg_InfoStreamId,
                                    q.msg_Type_own,
                                    q.msg_Result,
                                    q.subSys_Cod,
                                    COALESCE(q.retry_count, 0) as Retry_Count,
                                    q.Prev_Queue_Direction,
                                    q.Prev_Msg_Date,
                                    COALESCE(q.queue_create_date, q.queue_date) as Queue_Create_Date,
                                    q.Perform_Object_Id, Current_TimeStamp as Curr_Server_Time, Priority_Level
                                    from\040
                            """
                + HrmsSchema +
                """
                .MESSAGE_QUEUE Q where 1=1 and mod(Q.queue_Id,?) =? and q.msg_InfoStreamId > ?\040
                  and q.queue_Id > ? and Q.queue_Direction in( 'OUT','SEND')\040
                  and Q.Msg_Date < Current_TimeStamp order by q.queue_id asc) QUEUE
                """;

            // selectMessageSQL = PreSelectMessageSQL + " and Q.Msg_Date < Current_TimeStamp order by Q.Priority_Level asc , Q.queue_id asc ) QUEUE where rownum < " + NumMessageInScan;
        }
        else {
            selectMessageSQL = """
                            select * from ( select CTID::varchar as ROWID,
                                    q.queue_Id,
                                    q.queue_Direction,
                                    COALESCE(q.queue_date, now() AT TIME ZONE 'Europe/Moscow' - Interval '1' Minute ) as Queue_Date,
                                    q.msg_Status,
                                    q.Msg_Date,
                                    q.Operation_id,
                                    to_Char(q.outqueue_id, '9999999999999999') as outQueue_Id,
                                    q.msg_Type,
                                    q.msg_Reason,
                                    q.msgDirection_Id,
                                    q.msg_InfoStreamId,
                                    q.msg_Type_own,
                                    q.msg_Result,
                                    q.subSys_Cod,
                                    COALESCE(q.retry_count, 0) as Retry_Count,
                                    q.Prev_Queue_Direction,
                                    q.Prev_Msg_Date,
                                    COALESCE(q.queue_create_date, COALESCE(q.queue_date, now() AT TIME ZONE 'Europe/Moscow' - Interval '1' Minute  )) as Queue_Create_Date,
                                    q.Perform_Object_Id, now() AT TIME ZONE 'Europe/Moscow' as Curr_Server_Time, Priority_Level
                                    from\040
                            """+ HrmsSchema + """
                             .MESSAGE_QUEUE Q where 1=1 and mod(Q.queue_Id,?) =? and q.msg_InfoStreamId > ?\040
                               and q.queue_Id > ? and Q.queue_Direction in( 'OUT','SEND')\040
                               and Q.Msg_Date < now() AT TIME ZONE 'Europe/Moscow' order q.queue_id asc ) QUEUE
                             """
                    ;

                   }
        CachirerMessageDbRead_Log.info("HZcachirerMessageDbReadTask[{}]: Main_MESSAGE_QueueSelect:{{}} `mod(Q.queue_Id,{} ) ={}` and q.msg_InfoStreamId >= {} and q.queue_Id > ?  `)",
                theadNum, selectMessageSQL, this.totalCachiererTaskNum, (this.theadNum-1), (this.FirstInfoStreamId-1));
        // Готовим набор SQL и задаём постоянные фильтры потока
        try {
            stmtMsgQueue = theadDataAccess.Hermes_Connection.prepareStatement( selectMessageSQL);
            stmtMsgQueue.setInt(1, (this.totalCachiererTaskNum) );
            stmtMsgQueue.setInt(2, (this.theadNum-1) );
            stmtMsgQueue.setInt(3, (this.FirstInfoStreamId-1) );
        }catch (Exception e) {
            e.printStackTrace();
            CachirerMessageDbRead_Log.error(e.getMessage());
            return ;
        }

    long startTimestamp = Instant.ofEpochSecond(0L).until(Instant.now(), ChronoUnit.SECONDS);

   for ( theadRunCount = 0L; theadRunCount < theadRunTotalCount; theadRunCount += 1L ) {
            // бесконечный цикл
         long secondsFromEpoch = Instant.ofEpochSecond(0L).until(Instant.now(), ChronoUnit.SECONDS);
                theadRunTotalCount += 1L;
    try {
        int num_Message4Perform = 0;
        // stmtMsgQueue = theadDataAccess.Hermes_Connection.prepareStatement( selectMessageSQL);
        // and mod(Q.msg_InfoStreamId,totalCachiererTaskNum) = theadNum and q.msg_InfoStreamId >= FirstInfoStreamId

        // 1. Вычисляем максимальное значение ключа (queueId) из Hazelcast IMap, используя встроенный агрегатор

        Long maxIdInMap;
        if ((maxIdInMap = hzMsgSenderMap.aggregate(Aggregators.longMax("__key"))) == null) // Это самый эффективный способ
            maxIdInMap = 0L;
        stmtMsgQueue.setLong(4, (maxIdInMap));


        CurrentTaskStatus = "maxIdInMap: running for `mod(Q.msg_InfoStreamId," + totalCachiererTaskNum + " ) =" + theadNum +
                            "` and q.msg_InfoStreamId >= " + (this.FirstInfoStreamId) +
                            " and q.queue_Id > " + maxIdInMap + " `)";
        CachirerMessageDbRead_Log.info("HZcachirerMessageDbReadTask[{}]: {}", theadNum, CurrentTaskStatus);

        ResultSet rs = stmtMsgQueue.executeQuery();
        while (rs.next()) {
            num_Message4Perform += 1;
            HzQueueVO hzQueueVO = new HzQueueVO();
            hzQueueVO.HzQueueVO(
                    rs.getLong("Queue_Id"),
                    rs.getTimestamp("Msg_Date"),
                    rs.getString("Queue_Direction"),
                    rs.getInt("Priority_Level"),
                    rs.getInt("Msg_InfoStreamId"),
                    rs.getString("ROWID")
            );
            //if ((theadRunCount % 10L) == 1L)
            CachirerMessageDbRead_Log.info("HZcachirerMessageDbReadTask[{}]: rs.next() ==> messageQueueVO.Queue_Id:{} [Msg_InfoStreamId={}] [ {}] SubSys_Cod={}Curr_Server_Time=`{}`,  ROWID={}",
                    theadNum,  rs.getLong("Queue_Id"), rs.getInt("Msg_InfoStreamId"), rs.getString("Msg_Type"), rs.getString("SubSys_Cod"), rs.getTimestamp("Curr_Server_Time").toString(), rs.getString("ROWID"));

            hzMsgSenderMap.put(Long.valueOf(hzQueueVO.Queue_Id), hzQueueVO);
            // вместо java.sql.Timestamp.valueOf(LocalDateTime.now(ZoneId.of( "Europe/Moscow"))) локального компьютера берём время от сервера БД
            // messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
            hzQueueVO.setMsg_Date(rs.getTimestamp("Curr_Server_Time"));
        }
        CachirerMessageDbRead_Log.info("HZcachirerMessageDbReadTask[{}]: Ждём'c: `{}` < `{}` в {} раз {}сек., уже {}сек., начиная с ={} текущее время ={}",
                theadNum, num_Message4Perform, NumMessageInScan, theadRunCount, WaitTimeBetweenScan, secondsFromEpoch - startTimestamp, startTimestamp, secondsFromEpoch);
        // +"secondsFromEpoch - startTimestamp=" + (secondsFromEpoch - startTimestamp) +  " Long.valueOf(60L * TotalTimeTasks)=" + Long.valueOf(60L * TotalTimeTasks)
        CachirerMessageDbRead_Log.info("HZcachirerMessageDbReadTask[{}]:: Фоновое инкрементальное наполнение `hzMsgSenderMap` выполнено. Записей в карте: {}", theadNum, hzMsgSenderMap.size());

        Thread.sleep(WaitTimeBetweenScan * 1000);

    } catch (Exception e) {
        e.printStackTrace();
        CachirerMessageDbRead_Log.error("HZcachirerMessageDbReadTask[{}]: catch Exception: {}", theadNum, e.getMessage());
        return;
    }
}
        CachirerMessageDbRead_Log.info("HZcachirerMessageDbReadTask[{}]:: Фоновое инкрементальное наполнение `hzMsgSenderMap` завершено. Записей в карте: {}", theadNum, hzMsgSenderMap.size());


        /// /////////////////////////
        try {
            Hermes_Connection.close();
            CachirerMessageDbRead_Log.warn("finish: HZcachirerMessageDbReadTask Connection.close() ");
        }
       catch (java.sql.SQLException e) {
           CachirerMessageDbRead_Log.error("SQLException: HZcachirerMessageDbReadTask Connection.close() :{}",e.getMessage());
       }
    }
}
