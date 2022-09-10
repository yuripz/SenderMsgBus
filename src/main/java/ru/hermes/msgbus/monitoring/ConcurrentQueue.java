package ru.hermes.msgbus.monitoring;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import ru.hermes.msgbus.model.MessageQueueVO;
import ru.hermes.msgbus.model.MonitoringQueueVO;

import javax.validation.constraints.NotNull;

public class ConcurrentQueue {
    public static LinkedBlockingDeque<MonitoringQueueVO> MonitoringQueue;

    public static void Init( int copasity ) {
        MonitoringQueue = new  LinkedBlockingDeque<MonitoringQueueVO>(copasity);
        MonitoringQueue.clear();

    }
    private static boolean add2queue( MonitoringQueueVO monitoringQueueVO) {

        return MonitoringQueue.offer(monitoringQueueVO); // MonitoringQueue.offerLast(monitoringQueueVO);
    }

    public static MonitoringQueueVO read4queue() {

        return MonitoringQueue.poll();  // .pollFirst();
    }

    public static int  SizeOf() {

        return MonitoringQueue.size();
    }

    public static boolean addMessageQueueVO2queue( @NotNull MessageQueueVO messageQueueVO, String pRequest, String pResponse, MonitoringQueueVO pmonitoringQueueVO, Logger MessegeSend_Log)
    {
        //return false;

        MonitoringQueueVO monitoringQueueVO= new MonitoringQueueVO();

        boolean is_added2queue;
    monitoringQueueVO.Queue_Id = messageQueueVO.getQueue_Id();
    monitoringQueueVO.Msg_Date = messageQueueVO.getMsg_Date(); // java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) );
    monitoringQueueVO.OutQueue_Id = messageQueueVO.getOutQueue_Id();
    monitoringQueueVO.Msg_InfoStreamId = messageQueueVO.getMsg_InfoStreamId() ; //theadNum + this.FirstInfoStreamId;
    monitoringQueueVO.Queue_Direction = messageQueueVO.getQueue_Direction();
    monitoringQueueVO.Queue_Date= messageQueueVO.getQueue_Date(); //java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ); //localDate.format( DTformatter ); //dateFormat.format( localDate);
    monitoringQueueVO.Msg_Status = messageQueueVO.getMsg_Status();
    monitoringQueueVO.MsgDirection_Id =messageQueueVO.getMsgDirection_Id();
    monitoringQueueVO.Operation_Id= messageQueueVO.getOperation_Id();
    monitoringQueueVO.Msg_Type=messageQueueVO.getMsg_Type();
    monitoringQueueVO.Msg_Reason = messageQueueVO.getMsg_Reason();
    monitoringQueueVO.Msg_Type_own = messageQueueVO.getMsg_Type_own();
    monitoringQueueVO.Msg_Result = messageQueueVO.getMsg_Result();
    monitoringQueueVO.SubSys_Cod= messageQueueVO.getSubSys_Cod();
    monitoringQueueVO.Prev_Queue_Direction = messageQueueVO.getPrev_Queue_Direction();
    monitoringQueueVO.Retry_Count = messageQueueVO.getRetry_Count();
    monitoringQueueVO.Prev_Msg_Date = messageQueueVO.getPrev_Msg_Date(); // java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) );
    monitoringQueueVO.Perform_Object_Id = messageQueueVO.getPerform_Object_Id();
    monitoringQueueVO.Queue_Create_Date= messageQueueVO.getQueue_Create_Date(); // Timestamp.from(Instant.now() ); // java.time.OffsetDateTime.now(ZoneOffset.of("Europe/Moscow"))); // java.sql.Timestamp( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) );
    monitoringQueueVO.Req_Dt = java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) );
    monitoringQueueVO.Request =  pRequest;
    monitoringQueueVO.Resp_Dt = java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ); // java.time.OffsetDateTime.now( ZoneOffset.of("Europe/Moscow" ));
    monitoringQueueVO.Response =  pResponse ;

        //MessegeSend_Log.warn( "Queue_Id[" + monitoringQueueVO.Queue_Id + "]: try ConcurrentQueue.add2queue()->" + monitoringQueueVO.toString() );
        is_added2queue =  ConcurrentQueue.add2queue(monitoringQueueVO);
        // MessegeSend_Log.warn("Queue_Id[" + monitoringQueueVO.Queue_Id + "]: after ConcurrentQueue.add2queue()->" + is_added2queue + ";  MonitoringQueueSize= "+ MonitoringQueue.size() + "; Msg_Type=" + monitoringQueueVO.Msg_Type );

        return is_added2queue;


    }
}
