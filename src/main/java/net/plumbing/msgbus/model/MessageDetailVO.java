package net.plumbing.msgbus.model;

import net.plumbing.msgbus.common.XMLchars;

public class MessageDetailVO {

    // protected long    Queue_Id;  // ссылка на Идентификатор сообщения
    public String Tag_Id; // имя поля в собщениии        NVARCHAR2(128) not null,
    public String Tag_Value; // значение поля в собщениии     NVARCHAR2(2000),
    public int Tag_Num; //      NUMBER,
    public int Tag_Par_Num; //   NUMBER,

    public void setMessageQueue(
            String Tag_Id,
            String Tag_Value,
            int Tag_Num,
            int Tag_Par_Num ) {
        this.Tag_Id = Tag_Id;
        this.Tag_Num = Tag_Num;
        this.Tag_Par_Num = Tag_Par_Num;
        this.Tag_Value = Tag_Value;
    }
    public String getTag_Value() {
        if ( this.Tag_Value != null ) return this.Tag_Value;
        else return XMLchars.Space;
    }

    /*
    public  int MessageRowNum=0;
    public  HashMap<Integer, MessageTemplateVO > AllMessageTemplate = new HashMap<Integer, MessageTemplateVO >();

    protected String  Queue_Direction; // Этап обработки
    protected long    Queue_Date;    //  время создания  сообщения
    protected int     Msg_Status=0;   //  статус сообщения
    protected long    Msg_Date;      //   время установки последнего  статуса
    protected int     Operation_Id;      // Номер операции в сообщении ( ссылка )
    protected long    OutQueue_Id;
    protected String  Msg_Type;
    protected String  Msg_Reason;
    protected int     MsgDirection_Id;     // Идентификатор sysId: для входящего - источник, для исходящего - получатель
    protected int     Msg_InfoStreamId=0; //  поток обработки сообщения
    protected String  Msg_Type_own;
    protected String  Msg_Result;
    protected String  SubSys_Cod;
    protected int     Retry_Count;
    protected String  Prev_Queue_Direction;
    protected long    Prev_Msg_Date;
    


    protected String  MsgDirection_Cod=null;  // Внутренний код внешней подсистемы
    protected int     operId=0;     //  Оператор
    protected String  direct=null;   // направление передачи
    protected long    queueDate;    //  время создания  сообщения
    protected long    msgDate;      //   время установки последнего  статуса
*/
}
