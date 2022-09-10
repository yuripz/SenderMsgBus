package net.plumbing.msgbus.model;

public class MonitoringQueueVO {
    public long    Queue_Id;          // собственный идентификатор сообщения
    public java.sql.Timestamp    Queue_Date;           //  время создания  сообщения
    // public long OutQueue_Id;
    // public java.math.BigDecimal OutQueue_Id;
    public java.sql.Timestamp    Msg_Date;             //   время установки последнего  статуса
    public int     Msg_Status=0;         //  статус сообщения
    public int     MsgDirection_Id;     // Идентификатор sysId: для входящего - источник, для исходящего - получатель
    public int     Msg_InfoStreamId;  //  поток обработки сообщения
    public int     Operation_Id;         // Номер операции в сообщении ( ссылка )
    public String  Queue_Direction;      // Этап обработки
    public String  Msg_Type;
    public String  Msg_Reason;
    public String  Msg_Type_own;
    public String  Msg_Result;
    public String  SubSys_Cod;
    public String  Prev_Queue_Direction;
    public int     Retry_Count;
    public java.sql.Timestamp    Prev_Msg_Date;
    public long Perform_Object_Id;
    public java.sql.Timestamp    Queue_Create_Date;
    public java.sql.Timestamp    Req_Dt;
    public String Request;
    public java.sql.Timestamp Resp_Dt;
    public String Response;

     /* !! не должен использоваться
    @Override
    public String toString(){
        return "MonitoringQueueVO= { Queue_Id: "+Queue_Id+ ", Queue_Direction: "+Queue_Direction+", Prev_Queue_Direction:" +Prev_Queue_Direction + ", Queue_Date:" +Queue_Date + ", OutQueue_Id: "+OutQueue_Id+", Msg_Date:" +Msg_Date + "\n" +
                // ", Msg_Status: "+Msg_Status+", MsgDirection_Id:" +MsgDirection_Id + ", Msg_InfoStreamId: "+Msg_InfoStreamId+", Operation_Id:" +Operation_Id +
                // ", Retry_Count: "+Retry_Count+", Prev_Msg_Date:" +Prev_Msg_Date + ", Perform_Object_Id: "+Perform_Object_Id+", Queue_Create_Date:" +Queue_Create_Date +
                ", Req_Dt: "+Req_Dt+", Resp_Dt:" +Resp_Dt + " }"
                ;
    }
    */
}
