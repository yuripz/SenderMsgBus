package ru.hermes.msgbus.model;

public class MessageQueueVO {
    protected long    Queue_Id;          // собственный идентификатор сообщения
    protected java.sql.Timestamp    Queue_Date;  //  время создания  сообщения
    protected long    OutQueue_Id;
    protected java.sql.Timestamp    Msg_Date;             //   время установки последнего  статуса
    protected int     Msg_Status=0;         //  статус сообщения
    protected int     MsgDirection_Id;     // Идентификатор sysId: для входящего - источник, для исходящего - получатель
    protected int     Msg_InfoStreamId;  //  поток обработки сообщения
    protected int     Operation_Id;         // Номер операции в сообщении ( ссылка )
    protected String  Queue_Direction;      // Этап обработки
    protected String  Msg_Type;
    protected String  Msg_Reason;
    protected String  Msg_Type_own;
    protected String  Msg_Result;
    protected String  SubSys_Cod;
    protected String  Prev_Queue_Direction;
    protected int     Retry_Count;
    protected java.sql.Timestamp    Prev_Msg_Date;
    protected java.sql.Timestamp    Queue_Create_Date;
    protected long    Perform_Object_Id;

    public void setMessageQueue(
            long    Queue_Id,
            java.sql.Timestamp    Queue_Date,
            long    OutQueue_Id,
            java.sql.Timestamp    Msg_Date,
            int     Msg_Status,
            int     MsgDirection_Id,
            int     Msg_InfoStreamId,
            int     Operation_Id,
            String  Queue_Direction,
            String  Msg_Type,
            String  Msg_Reason,
            String  Msg_Type_own,
            String  Msg_Result,
            String  SubSys_Cod,
            String  Prev_Queue_Direction,
            int     Retry_Count,
            java.sql.Timestamp    Prev_Msg_Date,
            java.sql.Timestamp    Queue_Create_Date, // Дата создания ( неизменяемая )
            long    Perform_Object_Id
    )
    {
        this.Queue_Id    =        Queue_Id;               // собственный идентификатор сообщения
        this.Queue_Date  =        Queue_Date;               //  время создания  сообщения
        this.OutQueue_Id  =       OutQueue_Id;
        this.Msg_Date  =          Msg_Date;                 //   время установки последнего  статуса
        this.Msg_Status    =      Msg_Status;               //  статус сообщения
        this.MsgDirection_Id  =   MsgDirection_Id;         // Идентификатор sysId: для входящего - источник; для исходящего - получатель
        this.Msg_InfoStreamId  =  Msg_InfoStreamId;      //  поток обработки сообщения
        this.Operation_Id  =      Operation_Id;             // Номер операции в сообщении ( ссылка )
        this.Queue_Direction  =   Queue_Direction;          // Этап обработки
        this.Msg_Type  =          Msg_Type;
        this.Msg_Reason  =        Msg_Reason;
        this.Msg_Type_own  =      Msg_Type_own;
        this.Msg_Result  =        Msg_Result;
        this.SubSys_Cod  =        SubSys_Cod;
        this.Prev_Queue_Direction=Prev_Queue_Direction;
        this.Retry_Count  =       Retry_Count;
        this.Prev_Msg_Date  =     Prev_Msg_Date;
        this.Queue_Create_Date = Queue_Create_Date;
        this.Perform_Object_Id = Perform_Object_Id;
    }
    public  void  setRetry_Count( int Retry_Count ) { this.Retry_Count = Retry_Count; }
    public  void  setMsg_Status( int Msg_Status ) { this.Msg_Status = Msg_Status; }
    public  void  setQueue_Direction( String Queue_Direction ) { this.Queue_Direction = Queue_Direction; }
    public  void  setPrev_Queue_Direction( String Prev_Queue_Direction ) { this.Prev_Queue_Direction = Prev_Queue_Direction; }
    public  void  setMsg_Reason( String Msg_Reason ) { this.Msg_Reason = Msg_Reason; }
    public  void  setMsg_Result( String Msg_Result ) { this.Msg_Result = Msg_Result; }
    public  void  setMsg_Date( java.sql.Timestamp Msg_Date ) { this.Msg_Date = Msg_Date; }
    public  void  setPrev_Msg_Date( java.sql.Timestamp Prev_Msg_Date ) { this.Prev_Msg_Date = Prev_Msg_Date; }
    public  void setMsg_InfoStreamId(int Msg_InfoStreamId) { this.Msg_InfoStreamId = Msg_InfoStreamId; }

    public  int  getRetry_Count() { return this.Retry_Count;  }
    public  long  getQueue_Id() { return this.Queue_Id; }
    public  long  getOutQueue_Id() { return this.OutQueue_Id; }
    public  int  getMsgDirection_Id() { return this.MsgDirection_Id; }
    public String getSubSys_Cod() { return this.SubSys_Cod; }
    public  int  getOperation_Id() { return this. Operation_Id; }
    public String getMsg_Type() { return this.Msg_Type; }
    public String getQueue_Direction() { return this.Queue_Direction; }
    public  int  getMsg_InfoStreamId()  { return this.Msg_InfoStreamId; }
    public  int  getMsg_Status() { return this.Msg_Status; }
    public String getMsg_Reason() { return this.Msg_Reason; }
    public String getMsg_Type_own() { return this.Msg_Type_own; }
    public String getMsg_Result() { return this.Msg_Result; }
    public String getPrev_Queue_Direction()  { return this.Prev_Queue_Direction; }
    public  long  getPerform_Object_Id()  { return this.Perform_Object_Id; }
    public  java.sql.Timestamp  getMsg_Date() { return ( this.Msg_Date);} // Дата последнего изменения статуса
    public  java.sql.Timestamp  getPrev_Msg_Date() { return ( this.Prev_Msg_Date);} // Дата предыдыдущего изменения статуса
    public  java.sql.Timestamp  getQueue_Create_Date() { return ( this.Queue_Create_Date);} // Дата создания ( неизменяемая )
    public  java.sql.Timestamp  getQueue_Date() { return this.Queue_Date ;} // Дата постановки в очередь ( неизменяемая )
}
