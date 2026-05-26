package net.plumbing.msgbus.threads.hzcache;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import net.plumbing.msgbus.model.MessageQueueVO;

import javax.validation.constraints.NotNull;


public class HzQueueVO implements Serializable, IdentifiedDataSerializable,  Comparable<HzQueueVO> {

    protected long    Queue_Id;          // собственный идентификатор сообщения
    protected java.sql.Timestamp Msg_Date;  //  время , указанное для обработки сообщения
    protected String  Queue_Direction;
    protected int     Msg_InfoStreamId;
    protected int Priority_Level;
    protected String  rowId;

    public HzQueueVO() { // Default constructor
        // super();
    }
    public void HzQueueVO ( // constructor,  using  4 HZ
                              long    Queue_Id,
                              java.sql.Timestamp    Msg_Date, String Queue_Direction, int     Msg_InfoStreamId,
                              int Priority_Level, String rowId
    )
    {
        this.Queue_Id    =        Queue_Id;               // собственный идентификатор сообщения
        this.Msg_Date  =        Msg_Date;               //  время создания  сообщения
        this.Queue_Direction  =   Queue_Direction;          // Этап обработки
        this.Msg_InfoStreamId=    Msg_InfoStreamId;
        this.Priority_Level=     Priority_Level;
        this.rowId=rowId;
    }
    public String toSring () {
        return ( "{\"MessageQueue\":{" +
                "\"rowId\"=\""+ (rowId) + "\"," +
                "\"Queue_Id\"=" + (Queue_Id) + ","+
                "\"Msg_Date\"=\"" +  (Msg_Date.toString()) + "\"," + // Используем writeObject/readObject для Timestamp
                "\"Queue_Direction\"=\"" +  Queue_Direction + "\"," +
                "\"Msg_InfoStreamId\"=\"" +  Msg_InfoStreamId + ","+
                "\"Priority_Level\"=" +(Priority_Level) + "} }"  //' != null ? Priority_Level : -1)
        );
    }
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HzQueueVO that = (HzQueueVO) o;
        return Queue_Id == that.Queue_Id &&
                Objects.equals(rowId, that.rowId) &&
                Objects.equals(Msg_Date, that.Msg_Date) &&
                Objects.equals(Queue_Direction, that.Queue_Direction) &&
                Priority_Level ==  that.Priority_Level;
    }
    @Override
    public int hashCode() {
        return Objects.hash(rowId, Queue_Id,  Msg_Date,  Queue_Direction, Msg_InfoStreamId, Priority_Level);
    }
    // IdentifiedDataSerializable Implementation
    public static final int FACTORY_ID = 1; // Уникальный FACTORY_ID для этой фабрики
    public static final int MESSAGE_QUEUE_ENTRY_CLASS_ID = 11; // Уникальный CLASS_ID

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }
    @Override
    public int getClassId() {
        return MESSAGE_QUEUE_ENTRY_CLASS_ID;
    }
    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(Queue_Id);
        out.writeLong(Msg_Date != null ? Msg_Date.getTime() : -1L);
        out.writeString(Queue_Direction != null ? Queue_Direction : "");
        out.writeInt(Msg_InfoStreamId);
        out.writeInt(Priority_Level);
        out.writeString(rowId != null ? rowId : "");
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        Queue_Id = in.readLong();
        long qdTime = in.readLong();
        Msg_Date = qdTime >= 0 ? new java.sql.Timestamp(qdTime) : null;
        Queue_Direction = in.readString();
        Msg_InfoStreamId = in.readInt();
        Priority_Level = in.readInt();
        rowId = in.readString();
    }
    @Override
    public int compareTo(@NotNull HzQueueVO o) {
        if (this == o) return 0;
        if (o == null || getClass() != o.getClass()) return -1;

        HzQueueVO that = (HzQueueVO) o;
        if (Queue_Id < that.Queue_Id) return -1;
        boolean returns = (Queue_Id == that.Queue_Id &&
                            Priority_Level ==  that.Priority_Level);
        if (returns) { return 0; }
        else return 1;

    }

    public String getRowId() { return rowId; }
    public void setRowId(String rowId) { this.rowId = rowId; }
    public Integer getPriorityLevel() { return Priority_Level; }
    public void setPriorityLevel(Integer priorityLevel) { this.Priority_Level = priorityLevel; }
    public  void setMsg_InfoStreamId(int Msg_InfoStreamId) { this.Msg_InfoStreamId = Msg_InfoStreamId; }
    public  int  getMsg_InfoStreamId()  { return this.Msg_InfoStreamId; }
    public  void  setMsg_Date( java.sql.Timestamp Msg_Date ) { this.Msg_Date = Msg_Date; }
    public  java.sql.Timestamp  getMsg_Date() { return ( this.Msg_Date);} // Дата последнего изменения статуса
}
