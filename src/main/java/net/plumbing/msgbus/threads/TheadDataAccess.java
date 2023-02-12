package net.plumbing.msgbus.threads;

//import oracle.jdbc.internal.PreparedStatement;
import java.sql.*;

//import oracle.jdbc.internal.OracleTypes;
//import oracle.jdbc.internal.OracleRowId;
//import oracle.sql.NUMBER;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;
import java.math.BigDecimal;

import net.plumbing.msgbus.common.XMLchars;
import net.plumbing.msgbus.model.MessageQueueVO;
// import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static net.plumbing.msgbus.common.XMLchars.DirectOUT;

public class TheadDataAccess {
    private final int maxReasonLen = 1996;
    public Connection Hermes_Connection;
    public PreparedStatement stmtMsgQueueDet = null;
    private PreparedStatement stmtMsgQueueConfirmationTag = null;
    private PreparedStatement stmtMsgQueueBody = null;
    private PreparedStatement stmtMsgLastBodyTag = null;
    private PreparedStatement stmtMsgQueueConfirmation = null;
    private String dbSchema = "orm";
    private String rdbmsVendor;

    public void setDbSchema(String DbSchema) {
        this.dbSchema = DbSchema;
    }
    public String getRdbmsVendor() {
        return rdbmsVendor;
    }

    private String selectMESSAGE_QUEUE ;
    private PreparedStatement stmtSelectMESSAGE_QUEUE;

    public PreparedStatement stmt_New_Queue_Insert;
    public String INSERT_Message_Queue;

    private PreparedStatement stmtUPDATE_MessageQueue_Queue_Date4Send;
    // HE-5481  q.Queue_Date = sysdate -> надо отображать дату первой попытки отправки
    private String UPDATE_MessageQueue_Queue_Date4Send ;

    private PreparedStatement stmtUPDATE_MessageQueue_Out2Send;
    // HE-5481  q.Queue_Date = sysdate -> надо отображать дату первой попытки отправки
    private  String UPDATE_MessageQueue_Out2Send ;
    private PreparedStatement stmtUPDATE_MessageQueue_Out2ErrorOUT;
    private String UPDATE_MessageQueue_Out2ErrorOUT ;

    private PreparedStatement stmtUPDATE_MessageQueue_Send2ErrorOUT;
    private  String UPDATE_MessageQueue_Send2ErrorOUT;

    private  String UPDATE_MessageQueue_SetMsg_Reason ;
    private PreparedStatement stmtUPDATE_MessageQueue_SetMsg_Reason;

    private  String UPDATE_MessageQueue_SetMsg_Result ;
    private PreparedStatement stmtUPDATE_MessageQueue_SetMsg_Result;

    private PreparedStatement stmtUPDATE_MessageQueue_Send2AttOUT;
    private  String UPDATE_MessageQueue_Send2AttOUT;

    private PreparedStatement stmt_UPDATE_MessageQueue_Send2finishedOUT;
    private  String UPDATE_MessageQueue_Send2finishedOUT ;

    private PreparedStatement stmt_UPDATE_MessageQueue_DirectionAsIS;
    private  String UPDATE_MessageQueue_DirectionAsIS ;

    private String UPDATE_MessageQueue_OUT2Ok;
    private PreparedStatement stmt_UPDATE_MessageQueue_OUT2Ok ;

    private PreparedStatement stmt_UPDATE_MessageQueue_after_FaultResponse;
    private PreparedStatement stmt_UPDATE_after_FaultGet;

    private  String UPDATE_QUEUE_InfoStreamId;
    private PreparedStatement stmt_UPDATE_QUEUE_InfoStreamId;
    private  String UPDATE_QUEUElog_Response;
    private PreparedStatement stmt_UPDATE_QUEUElog;

    private  String INSERT_QUEUElog_Request;
    // TODO 4_Postgre
    //private PreparedStatement stmt_INSERT_QUEUElog;
    // TODO Для Oracle используется call insert into returning ROWID into
    public CallableStatement stmt_INSERT_QUEUElog;

    public PreparedStatement stmt_DELETE_Message_Details;
    public  String DELETE_Message_Details;

    public PreparedStatement stmt_DELETE_Message_Confirmation;
    public  String DELETE_Message_Confirmation ;
    public PreparedStatement stmt_DELETE_Message_ConfirmationH;
    public  String DELETE_Message_ConfirmationH;

    public  String selectMessageStatement;
    public PreparedStatement stmt_New_Queue_Prepare;

    // public PreparedStatement stmt_Query_Message_Confirmation;
    // public String SELECT_Message_Confirmation = "select from " + dbSchema + ".MESSAGE_QueueDET D where D.queue_id =? and d.Tag_num >= ?";

    public PreparedStatement stmt_INSERT_Message_Details;
    public String INSERT_Message_Details ;

    public void close_Hermes_Connection() {
        try {
            Hermes_Connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Connection make_Hermes_Connection(String dst_point, String db_userid, String db_password, Logger dataAccess_log) {
        Connection Target_Connection = null;
        String connectionUrl;

        if (dst_point == null) {
            connectionUrl = "jdbc:oracle:thin:@//10.242.36.8:1521/hermes12"; // Test-Capsul !!!
            //connectionUrl = "jdbc:oracle:thin:@//10.32.245.4:1521/hermes"; // Бой !!!
        } else {
            //connectionUrl = "jdbc:oracle:thin:@"+dst_point;
            connectionUrl = dst_point;
        }
        // попробуй ARTX_PROJ / rIYmcN38St5P
        // hermes / uthvtc
        //String db_userid = "HERMES";
        //String db_password = "uthvtc";
        String ClassforName;
        if (connectionUrl.indexOf("oracle") > 0) {
            ClassforName = "oracle.jdbc.driver.OracleDriver";
            rdbmsVendor = "oracle";
        } else {
            ClassforName = "org.postgresql.Driver";
            rdbmsVendor = "postgresql";
        }

        dataAccess_log.info("Try(thead) Hermes getConnection: " + connectionUrl + " as " + db_userid + " , Class.forName:" + ClassforName + " RDBMS (" + rdbmsVendor + ")");
        try {
            // Establish the connection.
            // Class.forName("oracle.jdbc.driver.OracleDriver");
            Class.forName(ClassforName);
            Target_Connection = DriverManager.getConnection(connectionUrl, db_userid, db_password);
            Target_Connection.setAutoCommit(false);
            if (!rdbmsVendor.equals("oracle")) {
                PreparedStatement stmt_SetTimeZone = Target_Connection.prepareStatement("set SESSION time zone 3");//.nativeSQL( "set SESSION time zone 3" );
                stmt_SetTimeZone.execute();
                stmt_SetTimeZone.close();
            }
            // Handle any errors that may have occurred.
        } catch (Exception e) {
            dataAccess_log.error(e.getMessage());
            e.printStackTrace();
            return ((Connection) null);
        }

        Hermes_Connection = Target_Connection;

        dataAccess_log.info("Hermes(thead) getConnection: " + connectionUrl + " as " + db_userid + " done");

        if (make_SelectNew_Queue(  dataAccess_log) == null ) {
            dataAccess_log.error( "make_SelectNew_Queue() fault");
            return null;
        }
        if ( make_UPDATE_MessageQueue_OUT2Ok( dataAccess_log) == null ) {
            dataAccess_log.error( "make_UPDATE_MessageQueue_OUT2Ok() fault");
            return null;
        }

        if (make_insert_Message_Queue( dataAccess_log) == null ) {
            dataAccess_log.error( "make_insert_Message_Queue() fault");
            return null;
        }

        if (make_SelectMESSAGE_QUEUE(dataAccess_log) == null) {
            dataAccess_log.error("make_SelectMESSAGE_QUEUE() fault");
            return null;
        }
        if (make_Message_Query(dataAccess_log) == null) {
            dataAccess_log.error("make_Message_Query() fault");
            return null;
        }
        if (make_Message_LastBodyTag_Query(dataAccess_log) == null) {
            dataAccess_log.error("make_Message_LastBodyTag_Query() fault");
            return null;
        }
        if (make_Message_ConfirmationTag_Query(dataAccess_log) == null) {
            dataAccess_log.error("make_Message_ConfirmationTag_Query() fault");
            return null;
        }
        if (make_MessageBody_Query(dataAccess_log) == null) {
            dataAccess_log.error("make_MessageBody_Query() fault");
            return null;
        }

        if (make_MessageConfirmation_Query(dataAccess_log) == null) {
            dataAccess_log.error("make_MessageConfirmation_Query() fault");
            return null;
        }

        if (make_Message_Update_Out2Send(dataAccess_log) == null) {
            dataAccess_log.error("make_Message_Update_Out2Send() fault");
            return null;
        }
        if (make_Message_Update_Out2ErrorOUT(dataAccess_log) == null) {
            dataAccess_log.error("make_Message_Update_Out2ErrorOUT() fault");
            return null;
        }

        if (make_delete_Message_Details(dataAccess_log) == null) {
            dataAccess_log.error("make_delete_Message_Details() fault");
            return null;
        }

        if (make_insert_Message_Details(dataAccess_log) == null) {
            dataAccess_log.error("make_insert_Message_Details() fault");
            return null;
        }

        if (make_Message_Update_Send2ErrorOUT(dataAccess_log) == null) {
            dataAccess_log.error("make_Message_Update_Send2ErrorOUT() fault");
            return null;
        }
        if (make_DELETE_Message_Confirmation(dataAccess_log) == null) {
            dataAccess_log.error("make_DELETE_Message_Confirmation() fault");
            return null;
        }

        if (make_UPDATE_MessageQueue_Send2finishedOUT(dataAccess_log) == null) {
            dataAccess_log.error("make_UPDATE_MessageQueue_Send2finishedOUT() fault");
            return null;
        }

        if (make_UPDATE_MessageQueue_Send2AttOUT(dataAccess_log) == null) {
            dataAccess_log.error("make_UPDATE_MessageQueue_Send2finishedOUT() fault");
            return null;
        }
        if (make_UPDATE_MessageQueue_DirectionAsIS(dataAccess_log) == null) {
            dataAccess_log.error("make_UPDATE_MessageQueue_DirectionAsIS() fault");
            return null;
        }

        if (make_UPDATE_QUEUElog(dataAccess_log) == null) {
            dataAccess_log.error("make_UPDATE_QUEUElog() fault");
            return null;
        }
        if (make_UPDATE_QUEUE_InfoStreamId(dataAccess_log) == null) {
            dataAccess_log.error("make_UPDATE_QUEUElog() fault");
            return null;
        }

        if (make_INSERT_QUEUElog(dataAccess_log) == null) {
            dataAccess_log.error("make_UPDATE_QUEUElog() fault");
            return null;
        }

        if (make_UPDATE_MessageQueue_SetMsg_Reason(dataAccess_log) == null) {
            dataAccess_log.error("make_UPDATE_MessageQueue_SetMsg_Reason() fault");
            return null;
        }
        if (make_UPDATE_MessageQueue_SetMsg_Result(dataAccess_log) == null) {
            dataAccess_log.error("make_UPDATE_MessageQueue_SetMsg_Reason() fault");
            return null;
        }

        if (make_Message_Update_Queue_Queue_Date4Send(dataAccess_log) == null) {
            dataAccess_log.error("make_UPDATE_MessageQueue_SetMsg_Reason() fault");
            return null;
        }
        return Target_Connection;
    }

    //stmt_UPDATE_MessageQueue_OUT2Ok
    public PreparedStatement  make_UPDATE_MessageQueue_OUT2Ok( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {
            UPDATE_MessageQueue_OUT2Ok =
                    "update " + dbSchema + ".MESSAGE_QUEUE " +
                            "set Queue_Direction = 'OUT'" +
                            ", Queue_Date= current_timestamp" +
                            ", Msg_Status = 0" +
                            ", Msg_Date= current_timestamp" +
                            ", Operation_Id=?" +
                            ", Outqueue_Id=? " +
                            ", msg_type = ?" +
                            ", Msg_Reason = ?" +
                            ", MsgDirection_Id= ?" +
                            ", msg_type_own = ?" +
                            ", msg_result = null" +
                            ", subsys_cod = ?" +
                            ", msg_infostreamid = ?" +
                            ", Retry_Count=1 " + // 1030 = Ошибка преобразования из OUT в SEND
                            ", Prev_Queue_Direction='TMP'" +
                            ", Prev_Msg_Date=Msg_Date " +
                            "where 1=1 and Queue_Id = ?  " ;
            StmtMsg_Queue = this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_OUT2Ok );
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return (  null );
        }
        this.stmt_UPDATE_MessageQueue_OUT2Ok = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public  int doUPDATE_MessageQueue_OUT2Ok(Long Queue_Id, Integer Operation_Id, Integer Msg_InfoStreamId,
                                             Integer MsgDirection_Id , String SubSys_Cod,
                                             String Msg_Type, String Msg_Type_own,
                                             String Msg_Reason, String OutQueue_Id,
                                             Logger dataAccess_log ) {
        //dataAccess_log.info( "[" + Queue_Id + "] doUPDATE_MessageQueue_In: \"update ARTX_PROJ.MESSAGE_QUEUE Q " +
        //        "set q.Queue_Direction = 'IN', q.Msg_Reason = '"+ Msg_Reason+ "' " +
        //        ", q.Msg_Date= current_timestamp,  q.Msg_Status = 0, q.Retry_Count= 1 " +
        //        ", q.Prev_Queue_Direction='TMP', q.Prev_Msg_Date=q.Msg_Date " +
        // "where 1=1 and q.Queue_Id = "+ Queue_Id +"  ;" );
        try {
            stmt_UPDATE_MessageQueue_OUT2Ok.setInt( 1, Operation_Id );
            stmt_UPDATE_MessageQueue_OUT2Ok.setLong( 2, Long.parseLong(OutQueue_Id) );
            stmt_UPDATE_MessageQueue_OUT2Ok.setString( 3, Msg_Type );
            stmt_UPDATE_MessageQueue_OUT2Ok.setString( 4, Msg_Reason.length() > maxReasonLen ? Msg_Reason.substring(0, maxReasonLen) : Msg_Reason );
            stmt_UPDATE_MessageQueue_OUT2Ok.setInt( 5, MsgDirection_Id );
            stmt_UPDATE_MessageQueue_OUT2Ok.setString( 6, Msg_Type_own );
            stmt_UPDATE_MessageQueue_OUT2Ok.setString( 7, SubSys_Cod );
            stmt_UPDATE_MessageQueue_OUT2Ok.setInt( 8, Msg_InfoStreamId );
            stmt_UPDATE_MessageQueue_OUT2Ok.setLong( 9, Queue_Id );
            stmt_UPDATE_MessageQueue_OUT2Ok.executeUpdate();

            Hermes_Connection.commit();

        } catch (SQLException e) {
            dataAccess_log.error( "update " + dbSchema + ".MESSAGE_QUEUE for [" + Queue_Id+  "]:  doUPDATE_MessageQueue_In2Ok ) fault: " + e.getMessage() );
            System.err.println( "update " + dbSchema + ".MESSAGE_QUEUE for [" + Queue_Id+  "]: doUPDATE_MessageQueue_In2Ok )) fault: ");
            try {
                Hermes_Connection.rollback(); } catch (SQLException SQLe) {
                dataAccess_log.error( "[" + Queue_Id + "] rollback(" + UPDATE_MessageQueue_OUT2Ok + ") fault: " + SQLe.getMessage() );
                System.err.println( "[" + Queue_Id + "] rollback (" + UPDATE_MessageQueue_OUT2Ok + ") fault: " + SQLe.getMessage()  );
            }
            e.printStackTrace();
            return -1;
        }
        return 0;
    }


    public PreparedStatement make_SelectNew_Queue(  Logger dataAccess_log )
    {
        PreparedStatement StmtMsg_Queue;
        try {
            if ( rdbmsVendor.equals("oracle") )
                selectMessageStatement = "select " + dbSchema + ".MESSAGE_QUEUE_SEQ.NEXTVAL as queue_id," +
                        //"select nextval('" + dbSchema + ".MESSAGE_QUEUE_SEQ') as queue_id," +
                        " '" + DirectOUT +"' as queue_direction," +
                        " current_timestamp  Queue_Date," +
                        " 0 msg_status," +
                        " current_timestamp Msg_Date," +
                        " 0 operation_id," +
                        " '0' as outqueue_id," +
                        " 'Undefine' as msg_type," +
                        " NULL as  msg_reason," +
                        " 0 msgdirection_id," +
                        " 100001 msg_infostreamid," +
                        " NULL msg_type_own," +
                        " NULL msg_result," +
                        " NULL subsys_cod," +
                        " 0 as Retry_Count," +
                        " NULL prev_queue_direction," +
                        " current_timestamp Prev_Msg_Date, " +
                        " current_timestamp Queue_Create_Date " +
                        "from DUAL "  ;
            else selectMessageStatement = "select nextval('" + dbSchema + ".MESSAGE_QUEUE_SEQ') as queue_id," +
                    " '" + DirectOUT +"' as queue_direction," +
                    " clock_timestamp()  Queue_Date," +
                    " 0 msg_status," +
                    " clock_timestamp() Msg_Date," +
                    " 0 operation_id," +
                    " '0' as outqueue_id," +
                    " 'Undefine' as msg_type," +
                    " NULL as  msg_reason," +
                    " 0 msgdirection_id," +
                    " 100001 msg_infostreamid," +
                    " NULL msg_type_own," +
                    " NULL msg_result," +
                    " NULL subsys_cod," +
                    " 0 as Retry_Count," +
                    " NULL prev_queue_direction," +
                    " clock_timestamp() Prev_Msg_Date, " +
                    " clock_timestamp() Queue_Create_Date ";

            //dataAccess_log.info( "MESSAGE_QueueSelect4insert:" + selectMessageStatement );
            StmtMsg_Queue = this.Hermes_Connection.prepareStatement( selectMessageStatement);
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }

        this.stmt_New_Queue_Prepare = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public PreparedStatement  make_insert_Message_Queue( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        try {
            INSERT_Message_Queue= "INSERT into " + dbSchema + ".MESSAGE_Queue " +
                    "(QUEUE_ID, QUEUE_DIRECTION, QUEUE_DATE, MSG_STATUS, MSG_DATE, OPERATION_ID, OUTQUEUE_ID, MSG_TYPE) " +
                    "values (?, '"+ DirectOUT +"', current_timestamp, 0, current_timestamp, 0, 0,          'Undefine')";
            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement( INSERT_Message_Queue );
        } catch (SQLException e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_New_Queue_Insert = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public PreparedStatement make_UPDATE_QUEUElog(Logger dataAccess_log) {
        PreparedStatement StmtMsg_Queue;
        UPDATE_QUEUElog_Response = "update " + dbSchema + ".MESSAGE_QUEUElog set Resp_DT = current_timestamp, Response = ? where QUEUE_ID= ? and ROWID = ?";
        try {

            StmtMsg_Queue = (PreparedStatement) this.Hermes_Connection.prepareStatement(UPDATE_QUEUElog_Response);
        } catch (Exception e) {
            dataAccess_log.error(e.getMessage());
            e.printStackTrace();
            return null;
        }

        this.stmt_UPDATE_QUEUElog = StmtMsg_Queue;
        return StmtMsg_Queue;
    }


    public PreparedStatement make_UPDATE_QUEUE_InfoStreamId(Logger dataAccess_log) {
        PreparedStatement StmtMsg_Queue;
        if ( rdbmsVendor.equals("oracle") )
            UPDATE_QUEUE_InfoStreamId = "update " + dbSchema + ".MESSAGE_QUEUE q set msg_infostreamid = ? where q.ROWID = ?";
        else
            UPDATE_QUEUE_InfoStreamId = "update " + dbSchema + ".MESSAGE_QUEUE q set msg_infostreamid = ? where CTID= ?::tid";  // CTID='(921,21)'::tid  ;
        try {
            StmtMsg_Queue = this.Hermes_Connection.prepareStatement(UPDATE_QUEUE_InfoStreamId);
        } catch (Exception e) {
            dataAccess_log.error( "prepareStatement `" + UPDATE_QUEUE_InfoStreamId + "` fault: " + e.getMessage());
            System.err.println("prepareStatement `" + UPDATE_QUEUE_InfoStreamId + "` fault: ");
            e.printStackTrace();
            return null;
        }

        this.stmt_UPDATE_QUEUE_InfoStreamId = StmtMsg_Queue;
        return StmtMsg_Queue;
    }

    //public final String UPDATE_QUEUElog_Response="update " + dbSchema + ".MESSAGE_QUEUElog L set l.Resp_DT = current_timestamp, l.Response = ? where l.Queue_Id = ?";
    //public PreparedStatement stmt_UPDATE_QUEUElog;
    public int doUPDATE_QUEUElog( // TODO : for Ora  @NotNull java.sql.RowId
                                  @NotNull String ROWID_QUEUElog,
                                  @NotNull long Queue_Id, String sResponse,
                                  Logger dataAccess_log) {

        try {
            // TODO for Postgree !!!
            stmt_UPDATE_QUEUElog.setString(3, ROWID_QUEUElog);
            //stmt_UPDATE_QUEUElog.setRowId( 3,ROWID_QUEUElog);
            stmt_UPDATE_QUEUElog.setLong(2, Queue_Id);
            stmt_UPDATE_QUEUElog.setString(1, sResponse);
            stmt_UPDATE_QUEUElog.executeUpdate();
            dataAccess_log.info("[" + Queue_Id + "] commit: " + UPDATE_QUEUElog_Response + " = '" + sResponse + "' " +
                    "update " + dbSchema + ".MESSAGE_QUEUElog L set systimestamp where l.Queue_Id = " + Queue_Id + "  ;");
            Hermes_Connection.commit();

        } catch (Exception e) {
            dataAccess_log.error("doUPDATE_QUEUElog for [" + Queue_Id + "]: " + UPDATE_QUEUElog_Response + ") fault: " + e.getMessage());
            System.err.println("doUPDATE_QUEUElog for [" + Queue_Id + "]: " + UPDATE_QUEUElog_Response + ") fault: " + e.getMessage());
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public int doUPDATE_QUEUE_InfoStreamId_by_RowId(// TODO : for Ora  @NotNull java.sql.RowId
                                                    @NotNull String ROWID_QUEUE,
                                                    @NotNull long Queue_Id, int Msg_InfoStreamId,
                                           Logger dataAccess_log) {

        try {
            stmt_UPDATE_QUEUE_InfoStreamId.setString(2, ROWID_QUEUE);
            // stmt_UPDATE_QUEUE_InfoStreamId.setRowId(2, ROWID_QUEUE);
            stmt_UPDATE_QUEUE_InfoStreamId.setInt(1, Msg_InfoStreamId);

            int state_Update = stmt_UPDATE_QUEUE_InfoStreamId.executeUpdate();
            // Savepoint savepoint_InfoStreamId = Hermes_Connection.setSavepoint("UPDATE_QUEUE_InfoStreamId");
            dataAccess_log.info("[" + Queue_Id + "] commit:" + UPDATE_QUEUE_InfoStreamId + " ; Msg_InfoStreamId=" + Msg_InfoStreamId + " state_Update=" + state_Update);
            Hermes_Connection.commit();

        } catch (Exception e) {
            dataAccess_log.error("[" + Queue_Id + "]: " + UPDATE_QUEUE_InfoStreamId + ") fault: " + e.getMessage());
            System.err.println("[" + Queue_Id + "]: " + UPDATE_QUEUE_InfoStreamId + ") fault: " + e.getMessage());
            e.printStackTrace();
            return -1;
        }
        return 0;
    }


    private PreparedStatement // TODO Ora: CallableStatement
    make_INSERT_QUEUElog(Logger dataAccess_log) {
        if (!rdbmsVendor.equals("oracle")) {
            // TODO 4_Postgre
            PreparedStatement StmtMsg_Queue;
            INSERT_QUEUElog_Request="insert into " + dbSchema + ".MESSAGE_QUEUElog  ( Queue_Id, Req_dt, RowId, Request ) values( ?, current_timestamp, cast(nextval( '"+ dbSchema + ".message_queuelog_seq') as varchar), ?) ";
            try {  StmtMsg_Queue = this.Hermes_Connection.prepareCall(INSERT_QUEUElog_Request);
            } catch (Exception e) {
                dataAccess_log.error(e.getMessage());
                e.printStackTrace();
                return ((PreparedStatement) null);
            }
            this.stmt_INSERT_QUEUElog = (CallableStatement)StmtMsg_Queue;
            // TODO RowId Postgree RowId
            // this.stmt_INSERT_QUEUElog = StmtMsg_Queue;
            return StmtMsg_Queue;
        }
        else
        { CallableStatement StmtMsg_Queue;
            INSERT_QUEUElog_Request = "{call insert into " + dbSchema + ".MESSAGE_QUEUElog L ( Queue_Id, Req_dt, request ) values( ?, systimestamp, ?) returning ROWID into ? }";
            try {  StmtMsg_Queue = this.Hermes_Connection.prepareCall(INSERT_QUEUElog_Request);
            } catch (Exception e) {
                dataAccess_log.error(e.getMessage());
                e.printStackTrace();
                return ((CallableStatement) null);
            }
            this.stmt_INSERT_QUEUElog = (CallableStatement)StmtMsg_Queue;
            // TODO RowId Oracle native RowId
            return StmtMsg_Queue;
        }
    }


    //public final String INSERT_QUEUElog_Request="insert into " + dbSchema + ".MESSAGE_QUEUElog L ( Queue_Id, Req_dt, request ) values( ?, current_timestamp, ?)";
    // public PreparedStatement stmt_INSERT_QUEUElog;
    public String // TODO RowId Postgree RowId
            doINSERT_QUEUElog(long Queue_Id, String sRequest,
                                    Logger dataAccess_log ) {
        //dataAccess_log.info( "[" + Queue_Id + "] do {call insert into " + dbSchema + ".MESSAGE_QUEUElog L ( Queue_Id, Req_dt, request ) values(" + Queue_Id + ", current_timestamp, '"+sRequest+ "' ) returning ROWID into ? };" );
        int count ;
        String ROWID_QUEUElog=null;
        try {
            stmt_INSERT_QUEUElog.setLong( 1, Queue_Id );
            stmt_INSERT_QUEUElog.setString( 2, sRequest );
            // TODO for Oracle ROWID, в случае Postgree комментим !!!
            if ( rdbmsVendor.equals("oracle") )
            stmt_INSERT_QUEUElog.registerOutParameter( 3, Types.ROWID );

            count = stmt_INSERT_QUEUElog.executeUpdate();
            if (count>0)
            {
                // TODO for Oracle ROWID, в случае Postgree комментим !!!
                if ( rdbmsVendor.equals("oracle") )
                ROWID_QUEUElog  = stmt_INSERT_QUEUElog.getString(3); //rest is not null and not empty

                else { // TODO RowId Postgree
                    Statement stmt = Hermes_Connection.createStatement();

                    // get the postgresql serial field value with this query
                    ResultSet rs = stmt.executeQuery("select cast(currval('message_queuelog_seq')as varchar)");
                    if (rs.next()) {
                        ROWID_QUEUElog = rs.getString(1);
                    }
                    stmt.close();
                }
                dataAccess_log.info( "[" + Queue_Id + "] ROWID = stmt_INSERT_QUEUElog.executeUpdate() => " + ROWID_QUEUElog);
            }
            Hermes_Connection.commit();
            // dataAccess.do_Commit();

        } catch (Exception e) {
            dataAccess_log.error( "insert into " + dbSchema + ".MESSAGE_QUEUElog for [" + Queue_Id+  "]: " + INSERT_QUEUElog_Request + ") fault: " + e.getMessage() );
            try {
                Hermes_Connection.rollback(); } catch (SQLException SQLe) {
                dataAccess_log.error( "[" + Queue_Id + "] rollback(" + INSERT_QUEUElog_Request + ") fault: " + SQLe.getMessage() );
                System.err.println( "[" + Queue_Id + "] rollback (" + INSERT_QUEUElog_Request + ") fault: " + SQLe.getMessage()  );
            }
            e.printStackTrace();
            return ROWID_QUEUElog;
        }
        return ROWID_QUEUElog;
    }



    public PreparedStatement  make_insert_Message_Details( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        INSERT_Message_Details = "INSERT into " + dbSchema + ".MESSAGE_QueueDET (QUEUE_ID, TAG_ID, TAG_VALUE, TAG_NUM, TAG_PAR_NUM) " +
                "values (?, ?, ?, ?, ?)";
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement( INSERT_Message_Details );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_INSERT_Message_Details = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public PreparedStatement  make_UPDATE_MessageQueue_Send2finishedOUT( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        UPDATE_MessageQueue_Send2finishedOUT =
                "update " + dbSchema + ".MESSAGE_QUEUE Q " +
                        "set Queue_Direction = ?, Msg_Reason = ?" +
                        ", Msg_Date= current_timestamp,  Msg_Status = ?, Retry_Count= ? " +
                        ", Prev_Queue_Direction='SEND', Prev_Msg_Date=Msg_Date " +
                        "where 1=1 and q.Queue_Id = ?  ";
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement( UPDATE_MessageQueue_Send2finishedOUT );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_UPDATE_MessageQueue_Send2finishedOUT = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public  int doUPDATE_MessageQueue_Send2finishedOUT(@NotNull Long Queue_Id, String Queue_Direction,
                                                       String pMsg_Reason,
                                                       int Msg_Status, int Retry_Count,
                                                       Logger dataAccess_log ) {
        try {
            stmt_UPDATE_MessageQueue_Send2finishedOUT.setString( 1, Queue_Direction );
            stmt_UPDATE_MessageQueue_Send2finishedOUT.setString( 2, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmt_UPDATE_MessageQueue_Send2finishedOUT.setInt( 3, Msg_Status );
            stmt_UPDATE_MessageQueue_Send2finishedOUT.setInt( 4, Retry_Count );
            stmt_UPDATE_MessageQueue_Send2finishedOUT.setLong( 5, Queue_Id );
            stmt_UPDATE_MessageQueue_Send2finishedOUT.executeUpdate();

            Hermes_Connection.commit();
            dataAccess_log.info( "[" + Queue_Id + "] commit: doUPDATE_MessageQueue_Send2finishedOUT: \"update " + dbSchema + ".MESSAGE_QUEUE Q " +
                    "set Queue_Direction = '"+Queue_Direction+ "', Msg_Reason = '"+ pMsg_Reason+ "' " +
                    ", Msg_Date= current_timestamp,  Msg_Status = "+ Msg_Status + ", Retry_Count= ? " +
                    ", Prev_Queue_Direction='SEND', Prev_Msg_Date=Msg_Date " +
                    "where 1=1 and q.Queue_Id = "+ Queue_Id +" ; Retry_Count= ? " + Retry_Count);

        } catch (Exception e) {

            dataAccess_log.error( "update " + dbSchema + ".MESSAGE_QUEUE for [" + Queue_Id+  "]: " + UPDATE_MessageQueue_Send2finishedOUT + ") fault: " + e.getMessage() );
            System.err.println( "update " + dbSchema + ".MESSAGE_QUEUE for [" + Queue_Id+  "]: " + UPDATE_MessageQueue_Send2finishedOUT + ") fault: ");
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public PreparedStatement  make_UPDATE_MessageQueue_DirectionAsIS( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        UPDATE_MessageQueue_DirectionAsIS =
                "update " + dbSchema + ".MESSAGE_QUEUE Q " +
                        "set Msg_Date= (current_timestamp + ? * interval '1' second) , Msg_Reason = ?, Msg_Status = ?, Retry_Count= ?, Prev_Msg_Date=Msg_Date " +
                        "where 1=1 and q.Queue_Id = ?";
        try {
            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement( UPDATE_MessageQueue_DirectionAsIS );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_UPDATE_MessageQueue_DirectionAsIS = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public  int doUPDATE_MessageQueue_DirectionAsIS(@NotNull Long Queue_Id, int Retry_interval,
                                                       String pMsg_Reason,
                                                       int Msg_Status, int Retry_Count,
                                                       Logger dataAccess_log ) {
        try {
            BigDecimal queueId = new BigDecimal( Queue_Id.toString() );
            //dataAccess_log.info("[" + Queue_Id + "] try UPDATE_MessageQueue_DirectionAsIS : ["+ UPDATE_MessageQueue_DirectionAsIS + "]" );
            //dataAccess_log.info("[" + Queue_Id + "] BigDecimal queueId =" + queueId.toString() );
            stmt_UPDATE_MessageQueue_DirectionAsIS.setInt( 1,  Retry_interval );
            stmt_UPDATE_MessageQueue_DirectionAsIS.setString( 2, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmt_UPDATE_MessageQueue_DirectionAsIS.setInt( 3, Msg_Status );
            stmt_UPDATE_MessageQueue_DirectionAsIS.setInt( 4, Retry_Count );
            stmt_UPDATE_MessageQueue_DirectionAsIS.setBigDecimal(5, queueId );
                    // NUMBER.formattedTextToNumber( Queue_Id.toString())  ) ; //.setNUMBER( 5, NUMBER.formattedTextToNumber() );
            stmt_UPDATE_MessageQueue_DirectionAsIS.executeUpdate();

            Hermes_Connection.commit();
            dataAccess_log.info("[" + Queue_Id + "] commit:" + UPDATE_MessageQueue_DirectionAsIS + " = " + Queue_Id.toString() + " Retry_Count=" + Retry_Count );

        } catch (Exception e) {

            dataAccess_log.error( "update " + dbSchema + ".MESSAGE_QUEUE for [" + Queue_Id+  "]: " + UPDATE_MessageQueue_DirectionAsIS + ") fault: " + e.getMessage() );
            System.err.println( "update " + dbSchema + ".MESSAGE_QUEUE for [" + Queue_Id+  "]: " + UPDATE_MessageQueue_DirectionAsIS + ") fault: ");
            e.printStackTrace();
            return -1;
        }
        return 0;
    }


    public PreparedStatement  make_DELETE_Message_Confirmation( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        PreparedStatement StmtMsg_QueueH;
        DELETE_Message_Confirmation = "delete from " + dbSchema + ".MESSAGE_QUEUEDET d where d.queue_id = ?  and d.tag_par_num >=" +
                "(select min(d.tag_num) from " + dbSchema + ".MESSAGE_QUEUEDET d where d.queue_id = ? and d.tag_id='Confirmation')";
        DELETE_Message_ConfirmationH = "delete from " + dbSchema + ".MESSAGE_QUEUEDET d where d.queue_id = ?  and d.tag_id='Confirmation'";
        try {
            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement( DELETE_Message_Confirmation );
            StmtMsg_QueueH = (PreparedStatement)this.Hermes_Connection.prepareStatement( DELETE_Message_ConfirmationH );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_DELETE_Message_Confirmation = StmtMsg_Queue;
        this.stmt_DELETE_Message_ConfirmationH = StmtMsg_QueueH;
        return  StmtMsg_Queue ;
    }

    public  int doDELETE_Message_Confirmation(@NotNull long Queue_Id, Logger dataAccess_log ) {
        dataAccess_log.info( "[" + Queue_Id + "] doDELETE_Message_Confirmation!" );
        try {
                // сначала удаляем всЁ, что растет из Confirmation
            stmt_DELETE_Message_Confirmation.setLong( 1, Queue_Id );
            stmt_DELETE_Message_Confirmation.setLong( 2, Queue_Id );
            stmt_DELETE_Message_Confirmation.executeUpdate();
                 // а теперь и сам Confirmation tag
            stmt_DELETE_Message_ConfirmationH.setLong( 1, Queue_Id );
            stmt_DELETE_Message_ConfirmationH.executeUpdate();

            Hermes_Connection.commit();
            dataAccess_log.info( "DELETE Confirmation for [" + Queue_Id+  "]: commit " + DELETE_Message_Confirmation + ")" );

        } catch (Exception e) {
            dataAccess_log.error( "DELETE Confirmation for [" + Queue_Id+  "]: " + DELETE_Message_Confirmation + ") fault: " + e.getMessage() );
            System.err.println( "DELETE Confirmation for [" + Queue_Id+  "]: " + DELETE_Message_Confirmation + ") fault: ");
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public PreparedStatement  make_delete_Message_Details( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        DELETE_Message_Details = "delete from " + dbSchema + ".MESSAGE_QueueDET D where D.queue_id =?";
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement( DELETE_Message_Details );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmt_DELETE_Message_Details = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }



    public PreparedStatement  make_Message_Update_Queue_Queue_Date4Send( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        UPDATE_MessageQueue_Queue_Date4Send =
                "update " + dbSchema + ".MESSAGE_QUEUE Q " +
                        "set Queue_Date = current_timestamp, Queue_Direction = 'SEND'" +
                        ", Msg_Date= current_timestamp,  Msg_Status = 0, Retry_Count=1 " +
                        ", Prev_Queue_Direction='OUT', Prev_Msg_Date=current_timestamp " +
                        "where 1=1 and q.Queue_Id = ?  ";
        try {
            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_Queue_Date4Send );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_Queue_Date4Send + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtUPDATE_MessageQueue_Queue_Date4Send = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public PreparedStatement  make_Message_Update_Out2Send( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        UPDATE_MessageQueue_Out2Send =
                "update " + dbSchema + ".MESSAGE_QUEUE Q " +
                        "set Queue_Date = current_timestamp, Queue_Direction = 'SEND', Msg_Reason = ?" +
                        ", Msg_Date= current_timestamp,  Msg_Status = 0, Retry_Count=1 " +
                        ", Prev_Queue_Direction='OUT', Prev_Msg_Date=current_timestamp " +
                        "where 1=1 and Queue_Id = ?  ";
        try {
            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_Out2Send );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_Out2Send + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtUPDATE_MessageQueue_Out2Send = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public  int  doUPDATE_MessageQueue_Queue_Date4Send( MessageQueueVO  messageQueueVO,   Logger dataAccess_log ) {

        long Queue_Id = messageQueueVO.getQueue_Id();

        messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
        messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
        messageQueueVO.setPrev_Queue_Direction(messageQueueVO.getQueue_Direction());

        messageQueueVO.setQueue_Direction(XMLchars.DirectSEND);
        try {
            dataAccess_log.info("[" + Queue_Id + "] doUPDATE_MessageQueue_Queue_Date4Send()" );
            stmtUPDATE_MessageQueue_Queue_Date4Send.setLong( 1, Queue_Id );
            stmtUPDATE_MessageQueue_Queue_Date4Send.executeUpdate();

            Hermes_Connection.commit();
            dataAccess_log.info( "[" + Queue_Id + "] doUPDATE_MessageQueue_Queue_Date4Send() commit(" + UPDATE_MessageQueue_Queue_Date4Send + ")" );

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Queue_Date4Send + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Queue_Date4Send + ") fault: " );
            e.printStackTrace();
            return -1;
        }
        return 0;

    }

    public  int doUPDATE_MessageQueue_Out2Send(  MessageQueueVO  messageQueueVO,  String pMsg_Reason, Logger dataAccess_log ) {
        long Queue_Id = messageQueueVO.getQueue_Id();

        messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
        messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
        messageQueueVO.setPrev_Queue_Direction(messageQueueVO.getQueue_Direction());

        messageQueueVO.setQueue_Direction(XMLchars.DirectSEND);
        try {
            dataAccess_log.info( "[" + Queue_Id + "] doUPDATE_MessageQueue_Out2Send:" + pMsg_Reason );

            stmtUPDATE_MessageQueue_Out2Send.setString( 1, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmtUPDATE_MessageQueue_Out2Send.setLong( 2, Queue_Id );
            stmtUPDATE_MessageQueue_Out2Send.executeUpdate();

            dataAccess_log.info( "[" + Queue_Id + "] commit doUPDATE_MessageQueue_Out2Send:"  );
            Hermes_Connection.commit();

        } catch (Exception e) {
            messageQueueVO.setMsg_Reason("UPDATE(\" + UPDATE_MessageQueue_Out2Send + \") fault: \" + e.getMessage()");
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Out2Send + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Out2Send + ") fault: " );
                    e.printStackTrace();
            return -1;
        }
        return 0;
    }
    public PreparedStatement  make_Message_Update_Send2ErrorOUT( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        UPDATE_MessageQueue_Send2ErrorOUT =
                "update " + dbSchema + ".MESSAGE_QUEUE Q " +
                        "set Queue_Direction = 'ERROUT', Msg_Reason = ?" +
                        ", Msg_Date= current_timestamp,  Msg_Status = ?, Retry_Count= ? " +
                        ", Prev_Queue_Direction='SEND', Prev_Msg_Date=Msg_Date " +
                        "where 1=1 and q.Queue_Id = ?  ";
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_Send2ErrorOUT );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_Send2ErrorOUT + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtUPDATE_MessageQueue_Send2ErrorOUT = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public int doUPDATE_MessageQueue_Send2ErrorOUT( MessageQueueVO  messageQueueVO,  String pMsg_Reason, int pMsgStatus, int pMsgRetryCount,  Logger dataAccess_log) {
        // dataAccess_log.info( "doUPDATE_MessageQueue_Send2ErrorOUT:" + pMsg_Reason );
        long Queue_Id = messageQueueVO.getQueue_Id();

        messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
        messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
        messageQueueVO.setPrev_Queue_Direction(messageQueueVO.getQueue_Direction());
        messageQueueVO.setMsg_Status(pMsgStatus);
        messageQueueVO.setMsg_Reason(pMsg_Reason);
        messageQueueVO.setQueue_Direction(XMLchars.DirectERROUT);

        try {
            stmtUPDATE_MessageQueue_Send2ErrorOUT.setString( 1,  pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason  );
            stmtUPDATE_MessageQueue_Send2ErrorOUT.setInt( 2, pMsgStatus );
            stmtUPDATE_MessageQueue_Send2ErrorOUT.setInt( 3, pMsgRetryCount );
            stmtUPDATE_MessageQueue_Send2ErrorOUT.setLong( 4, Queue_Id );
            stmtUPDATE_MessageQueue_Send2ErrorOUT.executeUpdate();

            Hermes_Connection.commit();
            dataAccess_log.info( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Send2ErrorOUT + ") commit, Retry_Count=" + pMsgRetryCount);

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Send2ErrorOUT + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Send2ErrorOUT + ") fault: " );
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public PreparedStatement  make_UPDATE_MessageQueue_Send2AttOUT( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        UPDATE_MessageQueue_Send2AttOUT =
                "update " + dbSchema + ".MESSAGE_QUEUE Q " +
                        "set Queue_Direction = 'ATTOUT', Msg_Result = ?" +
                        ", Msg_Date= current_timestamp,  Msg_Status = ?, Retry_Count= ? " +
                        ", Prev_Queue_Direction='SEND', Prev_Msg_Date=Msg_Date " +
                        "where 1=1 and q.Queue_Id = ?  ";
        try {

            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_Send2AttOUT );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_Send2AttOUT + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtUPDATE_MessageQueue_Send2AttOUT = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }


    public int doUPDATE_MessageQueue_Send2AttOUT(MessageQueueVO  messageQueueVO, String pMsg_Reason, int pMsgStatus, int pMsgRetryCount,  Logger dataAccess_log) {
        // dataAccess_log.info( "doUPDATE_MessageQueue_Send2ErrorOUT:" + pMsg_Reason );
        long Queue_Id = messageQueueVO.getQueue_Id();
        messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
        messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
        messageQueueVO.setPrev_Queue_Direction(messageQueueVO.getQueue_Direction());
        messageQueueVO.setMsg_Status(pMsgStatus);
        messageQueueVO.setMsg_Reason(pMsg_Reason);
        messageQueueVO.setQueue_Direction(XMLchars.DirectATTNOUT);
        try {
            stmtUPDATE_MessageQueue_Send2AttOUT.setString( 1, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmtUPDATE_MessageQueue_Send2AttOUT.setInt( 2, pMsgStatus );
            stmtUPDATE_MessageQueue_Send2AttOUT.setInt( 3, pMsgRetryCount );
            stmtUPDATE_MessageQueue_Send2AttOUT.setLong( 4, Queue_Id );
            stmtUPDATE_MessageQueue_Send2AttOUT.executeUpdate();

            Hermes_Connection.commit();
            dataAccess_log.info( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Send2AttOUT + ") commit, Retry_Count=" + pMsgRetryCount );

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Send2AttOUT + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Send2AttOUT + ") fault: " );
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public PreparedStatement  make_UPDATE_MessageQueue_SetMsg_Result( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        UPDATE_MessageQueue_SetMsg_Result =
                "update " + dbSchema + ".MESSAGE_QUEUE Q " +
                        "set Queue_Direction = ?, Msg_Status = ?, Msg_Result = ? " +
                        "where 1=1 and q.Queue_Id = ? ";
        try {
            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_SetMsg_Result );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_SetMsg_Result + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtUPDATE_MessageQueue_SetMsg_Result = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public int doUPDATE_MessageQueue_SetMsg_Result(MessageQueueVO messageQueueVO, String pQueue_Direction, int pMsgStatus, String pMsg_Result, Logger dataAccess_log) {
        // dataAccess_log.info( "doUPDATE_MessageQueue_Send2ErrorOUT:" + pMsg_Reason );
        long Queue_Id = messageQueueVO.getQueue_Id();
        dataAccess_log.info("[" + Queue_Id + "] doUPDATE_MessageQueue_SetMsg_Result()" );

        messageQueueVO.setMsg_Result( pMsg_Result );

        try {
            stmtUPDATE_MessageQueue_SetMsg_Result.setString( 1, pQueue_Direction );
            stmtUPDATE_MessageQueue_SetMsg_Result.setInt( 2, pMsgStatus );
            stmtUPDATE_MessageQueue_SetMsg_Result.setString( 3, pMsg_Result.length() > maxReasonLen ? pMsg_Result.substring(0, maxReasonLen) : pMsg_Result );
            stmtUPDATE_MessageQueue_SetMsg_Result.setLong( 4, Queue_Id );
            stmtUPDATE_MessageQueue_SetMsg_Result.executeUpdate();

            Hermes_Connection.commit();
            dataAccess_log.info( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_SetMsg_Result + ") commit "+ " Msg_Result=" + pMsg_Result );

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_SetMsg_Result + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_SetMsg_Result + ") fault: " );
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public PreparedStatement  make_UPDATE_MessageQueue_SetMsg_Reason( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        UPDATE_MessageQueue_SetMsg_Reason =
                "update " + dbSchema + ".MESSAGE_QUEUE Q " +
                        "set Queue_Direction = 'RESOUT', Msg_Reason = ?" +
                        ", Msg_Date= current_timestamp,  Msg_Status = ?, Retry_Count= ? " +
                        ", Prev_Queue_Direction='SEND', Prev_Msg_Date=Msg_Date " +
                        "where 1=1 and q.Queue_Id = ? ";
        try {
            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_SetMsg_Reason );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_SetMsg_Reason + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtUPDATE_MessageQueue_SetMsg_Reason = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public int doUPDATE_MessageQueue_SetMsg_Reason(MessageQueueVO messageQueueVO, String pMsg_Reason, int pMsgStatus, int pMsgRetryCount,  Logger dataAccess_log) {
        // dataAccess_log.info( "doUPDATE_MessageQueue_Send2ErrorOUT:" + pMsg_Reason );
        long Queue_Id = messageQueueVO.getQueue_Id();
        dataAccess_log.info("[" + Queue_Id + "] doUPDATE_MessageQueue_SetMsg_Reason()" );

        messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
        messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
        messageQueueVO.setMsg_Status( pMsgStatus );
        messageQueueVO.setMsg_Reason( pMsg_Reason );

        try {
            stmtUPDATE_MessageQueue_SetMsg_Reason.setString( 1, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmtUPDATE_MessageQueue_SetMsg_Reason.setInt( 2, pMsgStatus );
            stmtUPDATE_MessageQueue_SetMsg_Reason.setInt( 3, pMsgRetryCount );
            stmtUPDATE_MessageQueue_SetMsg_Reason.setLong( 4, Queue_Id );
            stmtUPDATE_MessageQueue_SetMsg_Reason.executeUpdate();

            Hermes_Connection.commit();
            dataAccess_log.info( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_SetMsg_Reason + ") commit "+ " Retry_Count=" + pMsgRetryCount );

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_SetMsg_Reason + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_SetMsg_Reason + ") fault: " );
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public PreparedStatement  make_Message_Update_Out2ErrorOUT( Logger dataAccess_log ) {
        PreparedStatement StmtMsg_Queue;
        UPDATE_MessageQueue_Out2ErrorOUT =
                "update " + dbSchema + ".MESSAGE_QUEUE Q " +
                        "set Queue_Direction = 'ERROUT', Msg_Reason = ?" +
                        ", Msg_Date= current_timestamp, Msg_Status = 1030, Retry_Count=1 " + // 1030 = Ошибка преобразования из OUT в SEND
                        ", Prev_Queue_Direction='OUT', Prev_Msg_Date=Msg_Date " +
                        "where 1=1 and q.Queue_Id = ?";
        try {
            StmtMsg_Queue = (PreparedStatement)this.Hermes_Connection.prepareStatement(UPDATE_MessageQueue_Out2ErrorOUT );
        } catch (Exception e) {
            dataAccess_log.error( "UPDATE(" + UPDATE_MessageQueue_Out2ErrorOUT + ") fault: " + e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtUPDATE_MessageQueue_Out2ErrorOUT = StmtMsg_Queue;
        return  StmtMsg_Queue ;
    }

    public int doUPDATE_MessageQueue_Out2ErrorOUT( MessageQueueVO messageQueueVO , String pMsg_Reason, Logger dataAccess_log) {
        // dataAccess_log.info( "doUPDATE_MessageQueue_Out2ErrorOUT:" + pMsg_Reason );
        long Queue_Id= messageQueueVO.getQueue_Id();
        messageQueueVO.setMsg_Reason(pMsg_Reason);
        messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
        messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
        messageQueueVO.setPrev_Queue_Direction(messageQueueVO.getQueue_Direction());
        messageQueueVO.setQueue_Direction(XMLchars.DirectERROUT );
        try {
            stmtUPDATE_MessageQueue_Out2ErrorOUT.setString( 1, pMsg_Reason.length() > maxReasonLen ? pMsg_Reason.substring(0, maxReasonLen) : pMsg_Reason );
            stmtUPDATE_MessageQueue_Out2ErrorOUT.setLong( 2, Queue_Id );
            stmtUPDATE_MessageQueue_Out2ErrorOUT.executeUpdate();

            Hermes_Connection.commit();
            dataAccess_log.info( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Out2ErrorOUT + ") commit ");

        } catch (Exception e) {
            dataAccess_log.error( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Out2ErrorOUT + ") fault: " + e.getMessage() );
            System.err.println( "[" + Queue_Id + "] UPDATE(" + UPDATE_MessageQueue_Out2ErrorOUT + ") fault: " );
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public int  do_SelectMESSAGE_QUEUE( MessageQueueVO messageQueueVO, Logger dataAccess_log ) {
        long Queue_Id = messageQueueVO.getQueue_Id();
        messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
        messageQueueVO.setPrev_Msg_Date( messageQueueVO.getMsg_Date() );
        messageQueueVO.setPrev_Queue_Direction(messageQueueVO.getQueue_Direction());
        try {
            stmtSelectMESSAGE_QUEUE.setLong(1, Queue_Id);
            ResultSet rs = stmtSelectMESSAGE_QUEUE.executeQuery();
            while (rs.next()) {
                messageQueueVO.setQueue_Direction( rs.getString("Queue_Direction") );
                messageQueueVO.setMsg_Reason( rs.getString("Msg_Reason") );
                messageQueueVO.setMsg_Status( rs.getInt("Msg_Status") );
                messageQueueVO.setMsg_Result( rs.getString("Msg_Result") );

                // dataAccess_log.info( "messageQueueVO.Queue_Id:" + rs.getLong("Queue_Id") +
                //        " [ " + rs.getString("Msg_Type") + "] SubSys_Cod=" + rs.getString("SubSys_Cod"));
                messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );

            }
            rs.close();
        } catch (Exception e) {
            dataAccess_log.error(e.getMessage());
            e.printStackTrace();
            dataAccess_log.error(  "[" + Queue_Id + "] do_SelectMESSAGE_QUEUE: что то пошло совсем не так...");
            return -1;
        }
        return  0;
    }

    public PreparedStatement make_SelectMESSAGE_QUEUE( Logger dataAccess_log ) {
        PreparedStatement stmtSelectMESSAGE_QUEUE;
        selectMESSAGE_QUEUE =
                "select " +
                        " Q.queue_id," +
                        " Q.queue_direction," +
                        " Q.queue_date Queue_Date, " +
                        " Q.msg_status," +
                        " Q.msg_date Msg_Date," +
                        " Q.operation_id," +
                        " to_Char(Q.outqueue_id, '9999999999999999') as outqueue_id," +
                        " Q.msg_type," +
                        " Q.msg_reason," +
                        " Q.msgdirection_id," +
                        " Q.msg_infostreamid," +
                        " Q.msg_type_own," +
                        " Q.msg_result," +
                        " Q.subsys_cod," +
                        " Q.prev_queue_direction," +
                        " Q.prev_msg_date Prev_Msg_Date, " +
                        " Q.Perform_Object_Id " +
                        "from " + dbSchema + ".MESSAGE_QUEUE Q " +
                        "where 1=1 and q.Queue_Id = ?  ";
        try {
            stmtSelectMESSAGE_QUEUE = (PreparedStatement)this.Hermes_Connection.prepareStatement( selectMESSAGE_QUEUE );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return (  null );
        }
        this.stmtSelectMESSAGE_QUEUE = stmtSelectMESSAGE_QUEUE;
        return  stmtSelectMESSAGE_QUEUE ;
    }

    public PreparedStatement  make_Message_Query( Logger dataAccess_log ) {
        PreparedStatement StmtMsgQueueDet;
        try {

        StmtMsgQueueDet = (PreparedStatement)this.Hermes_Connection.prepareStatement(
        "select d.Tag_Id, d.Tag_Value, d.Tag_Num, d.Tag_Par_Num from " + dbSchema + ".Message_QueueDet D where (1=1) and d.QUEUE_ID = ? order by d.Tag_Par_Num, d.Tag_Num"
        );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( null );
        }
        this.
                stmtMsgQueueDet = StmtMsgQueueDet;
        return  StmtMsgQueueDet ;
    }

    public PreparedStatement  make_Message_LastBodyTag_Query( Logger dataAccess_log ) {
        PreparedStatement StmtMsgQueueDet;
        try {
            StmtMsgQueueDet = (PreparedStatement)this.Hermes_Connection.prepareStatement(
                    "select Tag_Num from (" +
                            " select Tag_Num from (" +
                                    " select Tag_Num from  " + dbSchema + ".message_queuedet  WHERE QUEUE_ID = ? and Tag_Par_Num = 0 and tag_Id ='Confirmation'" +
                                    " union all" +
                                    " select max(Tag_Num) + 1  as  Tag_Num from  " + dbSchema + ".message_queuedet  WHERE QUEUE_ID = ?" +
                            " ) order by Tag_Num" +
                    " ) where rownum =1"
                    );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtMsgLastBodyTag = StmtMsgQueueDet;
        return  StmtMsgQueueDet ;
    }

    public PreparedStatement  make_Message_ConfirmationTag_Query( Logger dataAccess_log ) {
        PreparedStatement StmtMsgQueueDet;
        try {
            StmtMsgQueueDet = (PreparedStatement)this.Hermes_Connection.prepareStatement(
                            "select Tag_Num from ( select Tag_Num from  " + dbSchema + ".message_queuedet  WHERE QUEUE_ID = ? and Tag_Par_Num = 0 and tag_Id ='Confirmation' order by Tag_Num ) where rownum=1"
            );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtMsgQueueConfirmationTag = StmtMsgQueueDet;
        return  StmtMsgQueueDet ;
    }

    public PreparedStatement  make_MessageBody_Query( Logger dataAccess_log ) {
        PreparedStatement StmtMsgQueueDet;
        try {

            StmtMsgQueueDet = (PreparedStatement)this.Hermes_Connection.prepareStatement(
                    "select d.Tag_Id, d.Tag_Value, d.Tag_Num, d.Tag_Par_Num" +
                            " from " + dbSchema + ".message_queuedet D" +
                            " where (1=1)" +
                            " and d.QUEUE_ID = ? and d.Tag_Id < ? " +
                            " order by   Tag_Par_Num, Tag_Num "
            );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtMsgQueueBody = StmtMsgQueueDet;
        return  StmtMsgQueueDet ;
    }

    public PreparedStatement  make_MessageConfirmation_Query( Logger dataAccess_log ) {
        PreparedStatement StmtMsgQueueDet;
        try {

            StmtMsgQueueDet = (PreparedStatement)this.Hermes_Connection.prepareStatement(
                    "select d.Tag_Id, d.Tag_Value, d.Tag_Num, d.Tag_Par_Num" +
                            " from " + dbSchema + ".message_queuedet D" +
                            " where (1=1)" +
                            " and d.QUEUE_ID = ? and d.Tag_Id >= ? " +
                            " order by   Tag_Par_Num, Tag_Num "
            );
        } catch (Exception e) {
            dataAccess_log.error( e.getMessage() );
            e.printStackTrace();
            return ( (PreparedStatement) null );
        }
        this.stmtMsgQueueConfirmation = StmtMsgQueueDet;
        return  StmtMsgQueueDet ;
    }


}
