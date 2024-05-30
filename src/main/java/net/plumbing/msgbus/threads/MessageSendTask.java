package net.plumbing.msgbus.threads;

//import com.sun.istack.internal.NotNull;
import net.plumbing.msgbus.common.DataAccess;
import net.plumbing.msgbus.model.MessageDetails;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import net.plumbing.msgbus.model.MessageTemplate;
import net.plumbing.msgbus.model.MessageType;
import net.plumbing.msgbus.model.MessageQueueVO;
//import net.plumbing.msgbus.monitoring.ConcurrentQueue;
import net.plumbing.msgbus.model.MonitoringQueueVO;
import net.plumbing.msgbus.threads.utils.MessageRepositoryHelper;
import net.plumbing.msgbus.common.json.JSONObject;

import javax.jms.*;
import java.sql.*;
import java.sql.Connection;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import static net.plumbing.msgbus.common.XMLchars.DirectOUT;

@Component
@Scope("prototype")
//@Configuration
//@Bean (name="MessageSendTask")
public class MessageSendTask  implements Runnable
{
    public MessageSendTask() {
        super();
    }

    public void setContext( ApplicationContext Context ) {
        this.context = Context;
        // this.SenderExecutor = (ThreadPoolTaskExecutor) context.getBean("messageSender");
        // this.schedulerService= new ControlledTheadService(theadNum, MessegeSend_Log );
    }

    //private ThreadPoolTaskExecutor SenderExecutor;
    // private ControlledTheadService schedulerService;

    private ApplicationContext context;
    public static final Logger MessegeSend_Log = LoggerFactory.getLogger(MessageSendTask.class);

    //private ThreadSafeClientConnManager externalConnectionManager;

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
    private javax.jms.Connection JMSQueueConnection;

    public String getHrmsPpoint() {
        return HrmsPoint;
    }

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

    public void setTotalTimeTasks(Long totalTimeTasks) {
        this.TotalTimeTasks = totalTimeTasks;
    }



    public void setTheadNum( int TheadNum) {
        this.theadNum = TheadNum;
    }

    public void setJMSQueueConnection (javax.jms.Connection JMSQueueConnection  ) { this.JMSQueueConnection = JMSQueueConnection;}
    public void setApiRestWaitTime( int ApiRestWaitTime) { this.ApiRestWaitTime = ApiRestWaitTime; }
    public void setFirstInfoStreamId( int FirstInfoStreamId) { this.FirstInfoStreamId=FirstInfoStreamId;}
    public void setCuberNumId( int cuberNumId) { this.CuberNumId=cuberNumId;}
    private int theadRunCount = 0;
    private  int  theadRunTotalCount = 1;
    // private SSLContext sslContext = null;
    //private HttpClientBuilder httpClientBuilder  = null;
    // private java.net.http.HttpClient ApiRestHttpClient = null;

    @Bean( name = "MessageSendTaskRun")
    // @Scheduled(initialDelay = 100, fixedRate = 1000)
    public void run() {
        //if (( theadNum != null ) && ((theadNum == 17) || (theadNum == 18) || (theadNum == 19) || (theadNum == 20)) )
        if (( theadNum == null ) ) // && (theadNum == 0))
            return;

        MessegeSend_Log.info("AllMessageTemplates[" + theadNum + "]: size=" + MessageTemplate.AllMessageTemplate.size()); //.get( theadNum+1 ).getTemplate_name() );
        if ( theadNum == -1 ) {
            for (int i = 0; i < MessageTemplate.AllMessageTemplate.size(); i++) {
                MessegeSend_Log.info("AllMessageTemplates[" + theadNum + "]:[" + i + "]=" + MessageTemplate.AllMessageTemplate.get(i).getTemplate_name());
            }

            MessegeSend_Log.info("AllMessageType[" + theadNum + "]: size=" + MessageType.AllMessageType.size()); //.get( theadNum+1 ).getTemplate_name() );
            for (int i = 0; i < MessageType.AllMessageType.size(); i++) {
                MessegeSend_Log.info("MessageType: " + MessageType.AllMessageType.get(i).getMsg_TypeDesc());
            }
        }
        //int ReadTimeoutInMillis = ApiRestWaitTime * 1000;
        // int ConnectTimeoutInMillis = 5 * 1000;
        Integer MessageType_Connect= null;
        Session JMSsession = null;
        Destination JMSdestination = null;
        MessageConsumer JMSconsumer = null;
        String MessageDirectionsCode = null;
        while ( MessageDirectionsCode == null)
            try {
                MessageDirectionsCode =  MessageRepositoryHelper.look4MessageDirectionsCode_4_Num_Thread( theadNum + this.FirstInfoStreamId  , MessegeSend_Log );

                if ( MessageDirectionsCode == null )
                {
                    // try { Hermes_Connection.close(); } catch (SQLException e) { MessegeSend_Log.error(e.getMessage()); e.printStackTrace(); }
                    MessegeSend_Log.warn("НЕ удалось Найти подходящйю систему для № потока "+ (theadNum + this.FirstInfoStreamId) + " она нужна для очереди сообщений ActiveMQ, ждём " + (WaitTimeBetweenScan) + " с.");
                    Thread.sleep( WaitTimeBetweenScan * 1000 * 10 );
                }


            } catch (InterruptedException e) {
                MessegeSend_Log.error("MessageSendTask[" + theadNum + "]: is interrapted: " + e.getMessage());
                e.printStackTrace();
            }


        try {
            //Creating a non transactional session to send/receive JMS message.
            JMSsession = JMSQueueConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            // Getting the queue 'JCG_QUEUE'
            JMSdestination = JMSsession.createQueue("Q." + MessageDirectionsCode + ".IN");
            MessegeSend_Log.info("MessageSendTask[" + theadNum + "]: JMSdestination = createQueue [" + "Q." + MessageDirectionsCode + ".IN" + "] done");
            // MessageConsumer is used for receiving (consuming) messages
            JMSconsumer = JMSsession.createConsumer(JMSdestination);
        }
        catch (JMSException e ) {
            MessegeSend_Log.error("НЕ удалось Установить соединение с брокером сообщений ActiveMQ");
            return;
        }

        TheadDataAccess theadDataAccess = new TheadDataAccess();
        theadDataAccess.setDbSchema( HrmsSchema );
        // Установаливем " соединение" , что бы зачитывать очередь
        Connection Hermes_Connection = theadDataAccess.make_Hermes_Connection(  HrmsPoint, hrmsDbLogin, hrmsDbPasswd, this.FirstInfoStreamId + theadNum,
                MessegeSend_Log
        );
        if ( Hermes_Connection == null) {
            MessegeSend_Log.error("НЕ удалось Установить соединение , что бы зачитывать очередь, либо подготовить запросы к БД ");
            return;
        }

/*
        sslContext = MessageHttpSend.getSSLContext();
        if ( sslContext == null ) {
            return;
        }
        PoolingHttpClientConnectionManager syncConnectionManager = new PoolingHttpClientConnectionManager();
        syncConnectionManager.setMaxTotal( 4);
        syncConnectionManager.setDefaultMaxPerRoute(2);

        externalConnectionManager = new ThreadSafeClientConnManager();
        externalConnectionManager.setMaxTotal(99);
        externalConnectionManager.setDefaultMaxPerRoute( 99);


        RequestConfig rc;
        rc = RequestConfig.custom()
                .setConnectionRequestTimeout(ConnectTimeoutInMillis)
                .setConnectTimeout(ConnectTimeoutInMillis)
                .setSocketTimeout( ReadTimeoutInMillis)
                .build();

        httpClientBuilder = HttpClientBuilder.create()
                .disableDefaultUserAgent()
                .disableRedirectHandling()
                .disableAutomaticRetries()
                .setUserAgent("Mozilla/5.0")
                .setSSLContext(sslContext)
                .disableAuthCaching()
                .disableConnectionState()
                .disableCookieManagement()
                // .useSystemProperties() // HE-5663  https://stackoverflow.com/questions/5165126/without-changing-code-how-to-force-httpclient-to-use-proxy-by-environment-varia
                .setConnectionManager(syncConnectionManager)
                .setSSLHostnameVerifier(new NoopHostnameVerifier())
                .setConnectionTimeToLive( ApiRestWaitTime + 5, TimeUnit.SECONDS)
                .evictIdleConnections((long) (ApiRestWaitTime + 5)*2, TimeUnit.SECONDS);
        httpClientBuilder.setDefaultRequestConfig(rc);

        ApiRestHttpClient = httpClientBuilder.build();
        if ( ApiRestHttpClient == null) {
            return;
        }
        */

        //SimpleHttpClient..setConnectTimeout(ConnectTimeoutInMillis);
        //HttpParams paramZ =  SimpleHttpClient.getParams();
        //BasicHttpParams params = new BasicHttpParams(); // =  SimpleHttpClient.getParams();
        //HttpConnectionParams.setConnectionTimeout( params, ConnectTimeoutInMillis);
        //HttpConnectionParams.setSoTimeout(params, ReadTimeoutInMillis);

            /*
            ThreadPoolTaskExecutor SenderExecutor = (ThreadPoolTaskExecutor) context.getBean("messageSender");
            ControlledTheadService schedulerService= new ControlledTheadService(theadNum, MessegeSend_Log );
*/
        MessegeSend_Log.info("MessageSendTask[" + theadNum + "]: is runing ");

        PreparedStatement stmtMsgQueue;
        PreparedStatement stmtHelperMsgQueue=null;
        PreparedStatement stmtQueueLock;
        PreparedStatement stmtQueueLock4JMSconsumer;
        PreparedStatement stmtGetMessage4QueueId;
        // PreparedStatement stmtUpdateMessage4RowId; - вместо этого TheadDataAccess.doUPDATE_QUEUE_InfoStreamId()
        String rdbmsVendor = theadDataAccess.getRdbmsVendor();
        String selectMessageSQL;
        String selectMessage4QueueIdSQL;
        if ( theadDataAccess.Hermes_Connection != null )
            // Готовим набор SQL
            try {

                if (rdbmsVendor.equals("oracle") )
                selectMessage4QueueIdSQL= """
                        select q.ROWID, Q.queue_id, Q.queue_direction, COALESCE(Q.queue_date, Current_TimeStamp - Interval '1' Minute) as Queue_Date,
                        Q.msg_status, Q.msg_date Msg_Date, Q.operation_id, to_Char(Q.outqueue_id, '999999999999999') as outqueue_id,
                        Q.msg_type, Q.msg_reason, Q.msgdirection_id, Q.msg_infostreamid,
                        Q.msg_type_own,Q.msg_result, Q.subsys_cod,
                        COALESCE(Q.retry_count, 0) as Retry_Count, Q.prev_queue_direction, Q.prev_msg_date Prev_Msg_Date,
                        COALESCE(Q.queue_create_date, COALESCE(Q.queue_date, Current_timeStamp - Interval '1' Minute )) as Queue_Create_Date,
                        Q.Perform_Object_Id
                        from
                        """ + " " + HrmsSchema + ".MESSAGE_QUEUE q where q.queue_id=?";
                        // """ + " " + HrmsSchema + ".MESSAGE_QUEUE q where q.ROWID=? ";
                else // для PostGree используем псевдостолбец CTID с типом ::tid
                    selectMessage4QueueIdSQL= """
                        select CTID::varchar as ROWID, Q.queue_id, Q.queue_direction, COALESCE(Q.queue_date, clock_timestamp() AT TIME ZONE 'Europe/Moscow' - Interval '1' Minute) as Queue_Date,
                        Q.msg_status, Q.msg_date Msg_Date, Q.operation_id, to_Char(Q.outqueue_id, '999999999999999') as outqueue_id,
                        Q.msg_type, Q.msg_reason, Q.msgdirection_id, Q.msg_infostreamid,
                        Q.msg_type_own,Q.msg_result, Q.subsys_cod,
                        COALESCE(Q.retry_count, 0) as Retry_Count, Q.prev_queue_direction, Q.prev_msg_date Prev_Msg_Date,
                        COALESCE(Q.queue_create_date, COALESCE(Q.queue_date, clock_timestamp() AT TIME ZONE 'Europe/Moscow' - Interval '1' Minute )) as Queue_Create_Date,
                        Q.Perform_Object_Id
                        from
                        """ + " " + HrmsSchema + ".MESSAGE_QUEUE q where q.queue_id=?";
                        // """ + " " + HrmsSchema + ".MESSAGE_QUEUE q where CTID=?::tid ";
                stmtGetMessage4QueueId = theadDataAccess.Hermes_Connection.prepareStatement( selectMessage4QueueIdSQL );
                //stmtUpdateMessage4RowId = TheadDataAccess.Hermes_Connection.prepareStatement( "update ARTX_PROJ.MESSAGE_QUEUE q set q.msg_infostreamid = ? where q.ROWID=?" );
                // String PreSelectMessageSQL;
                if (rdbmsVendor.equals("oracle") ) // для PostGree используем псевдостолбец CTID с типом ::tid
                { selectMessageSQL = """
                            select * from ( select q.ROWID,
                                    q.queue_Id,
                                    q.queue_Direction,
                                    COALESCE(q.queue_date, Current_TimeStamp - Interval '1' Minute ) as Queue_Date,
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
                                    COALESCE(q.queue_create_date, COALESCE(q.queue_date, Current_TimeStamp - Interval '1' Minute )) as Queue_Create_Date,
                                    q.Perform_Object_Id, Current_TimeStamp as Curr_Server_Time
                                    from\040
                                    """
                                    + HrmsSchema +
                                    """
                                    .MESSAGE_QUEUE Q where 1=1 and Q.msg_InfoStreamId in ( ?, ? )\040
                                      and Q.queue_Direction in( 'OUT','SEND')\040
                                      and Q.Msg_Date < Current_TimeStamp order by Q.Priority_Level asc, Q.queue_id asc) QUEUE where rownum <\040
                                    """
                                    + NumMessageInScan;
                    stmtQueueLock = theadDataAccess.Hermes_Connection.prepareStatement( "select Q.Queue_Id, Q.Queue_Direction, Q.Msg_InfostreamId  from " + HrmsSchema + ".MESSAGE_QUEUE Q where q.ROWID=? for update nowait" );
                    stmtQueueLock4JMSconsumer = theadDataAccess.Hermes_Connection.prepareStatement( "select Q.ROWID, q.Queue_Direction, q.Msg_InfostreamId from " + HrmsSchema + ".MESSAGE_QUEUE q where q.Queue_Id=? for update nowait" );

                    // selectMessageSQL = PreSelectMessageSQL + " and Q.Msg_Date < Current_TimeStamp order by Q.Priority_Level asc , Q.queue_id asc ) QUEUE where rownum < " + NumMessageInScan;
                   }
                else {  // TODO Pg -" and Q.Msg_Date < Current_TimeStamp AT TIME ZONE 'Europe/Moscow'" - почему то не срабвтывает условие, java как бы в <time zone 0> хотя выставлено set SESSION time zone 3;
                    // TODO Pg - " and Q.Msg_Date < clock_timestamp() AT TIME ZONE 'Europe/Moscow'" -- попробовать
                    selectMessageSQL = """
                            select * from ( select CTID::varchar as ROWID,
                                    q.queue_Id,
                                    q.queue_Direction,
                                    COALESCE(q.queue_date, clock_timestamp() AT TIME ZONE 'Europe/Moscow' - Interval '1' Minute ) as Queue_Date,
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
                                    COALESCE(q.queue_create_date, COALESCE(q.queue_date, clock_timestamp() AT TIME ZONE 'Europe/Moscow' - Interval '1' Minute  )) as Queue_Create_Date,
                                    q.Perform_Object_Id, clock_timestamp() AT TIME ZONE 'Europe/Moscow' as Curr_Server_Time
                                    from\040
                            """+ HrmsSchema + """
                             .MESSAGE_QUEUE Q where 1=1 and Q.msg_InfoStreamId in ( ?, ? ) \040
                               and Q.queue_Direction in( 'OUT','SEND')\040
                               and Q.Msg_Date < clock_timestamp() AT TIME ZONE 'Europe/Moscow' order by q.Priority_Level asc , q.queue_id asc ) QUEUE Limit\040
                             """
                            + NumMessageInScan;
                    // CTID::varchar as ROWID и where CTID=?::tid
                    stmtQueueLock = theadDataAccess.Hermes_Connection.prepareStatement( "select Q.Queue_Id, Q.Queue_Direction, Q.Msg_InfostreamId  from " + HrmsSchema + ".MESSAGE_QUEUE Q where CTID=?::tid for update SKIP LOCKED" ); //  nowait -> SKIP LOCKED
                    stmtQueueLock4JMSconsumer = theadDataAccess.Hermes_Connection.prepareStatement( "select CTID::varchar as ROWID, q.Queue_Direction, q.Msg_InfostreamId from " + HrmsSchema + ".MESSAGE_QUEUE q where q.Queue_Id=? for update SKIP LOCKED" ); //  nowait -> SKIP LOCKED
                }
                MessegeSend_Log.info( "Main_MESSAGE_QueueSelect:{" + selectMessageSQL  + "} Q.Msg_InfostreamId ="  + (this.FirstInfoStreamId + theadNum ) ) ;
                stmtMsgQueue = theadDataAccess.Hermes_Connection.prepareStatement( selectMessageSQL);

                // Получеем перечень потоков, которым надо помогать ДАННОМУ потоку
                String List_Lame_Threads =  MessageRepositoryHelper.look4List_Lame_Threads_4_Num_Thread( theadNum + this.FirstInfoStreamId  , MessegeSend_Log );
                if ( List_Lame_Threads != null)
                {   String Lame_selectMessageSQL;
                    String Lame_PreSelectMessageSQL =
                        "select * from ( select q.ROWID, " +
                                " Q.queue_id," +
                                " Q.queue_direction," +
                                " COALESCE(Q.queue_date, Current_timeStamp -  Interval '1' Minute ) as Queue_Date," +
                                " Q.msg_status," +
                                " Q.msg_date," +
                                " Q.operation_id," +
                                " to_Char(Q.outqueue_id, '9999999999999999') as outQueue_Id," +
                                " Q.msg_type," +
                                " Q.msg_reason," +
                                " Q.msgdirection_id," +
                                " Q.msg_InfostreamId," +
                                " Q.msg_type_own," +
                                " Q.msg_Result," +
                                " Q.subSys_Cod," +
                                " COALESCE(Q.retry_count, 0) as Retry_Count," +
                                " Q.Prev_Queue_Direction," +
                                " Q.Prev_Msg_Date, " +
                                " COALESCE(Q.queue_create_date, COALESCE(Q.queue_date, Current_timeStamp - Interval '1' Minute  )) as Queue_Create_Date, " +
                                " Q.Perform_Object_Id, Current_TimeStamp as Curr_Server_Time " +
                                "from " + HrmsSchema + ".MESSAGE_QUEUE Q" +
                                " Where 1=1" +
                                " and Q.msg_infostreamid in (" + List_Lame_Threads + ")" +
                                " and Q.queue_direction='OUT'"  // in( 'OUT')",'SEND','RESOUT','DELOUT')"
                                ;
                    if (rdbmsVendor.equals("oracle") )
                        Lame_selectMessageSQL = Lame_PreSelectMessageSQL +" and Q.msg_date < Current_TimeStamp order by Q.Priority_Level asc , Q.queue_id asc ) QUEUE where rownum =1 " ;
                    else // TODO Pg -" and Q.Msg_Date < Current_TimeStamp AT TIME ZONE 'Europe/Moscow'"
                        Lame_selectMessageSQL = Lame_PreSelectMessageSQL + " order by Q.Priority_Level asc , Q.queue_id asc ) QUEUE Limit 1"  ;

                    MessegeSend_Log.info( "Helper_MESSAGE_QueueSelect: " + Lame_selectMessageSQL );
                    stmtHelperMsgQueue = theadDataAccess.Hermes_Connection.prepareStatement( Lame_selectMessageSQL );
                }
                else
                    MessegeSend_Log.info( "NO-Helper, no Helper_MESSAGE_QueueSelect!" );
            } catch (Exception e) {
                e.printStackTrace();
                MessegeSend_Log.error(e.getMessage());
                return ;
            }
        else
        {
            return;
        }
        // инициализируемся
        MessegeSend_Log .info("Setup Connection for thead:" + (this.FirstInfoStreamId + theadNum ) + " CuberNumId:" + CuberNumId + " rdbmsVendor=`" + rdbmsVendor + "`") ;
        if ( !rdbmsVendor.equals("oracle") ) {
            MessegeSend_Log .info("Try setup Connection for thead: " + (this.FirstInfoStreamId + theadNum ) + " `set SESSION time zone 3`");
            try {
                String SQLCurrentTimeStringRead= "SELECT to_char(current_timestamp, 'YYYY-MM-DD-HH24:MI:SS') as currentTime";
                PreparedStatement stmtCurrentTimeStringRead = DataAccess.Hermes_Connection.prepareStatement(SQLCurrentTimeStringRead );
                String CurrentTime="00000-00000";
                    PreparedStatement stmt_SetTimeZone = theadDataAccess.Hermes_Connection.prepareStatement("set SESSION time zone 3");//.nativeSQL( "set SESSION time zone 3" );
                    stmt_SetTimeZone.execute();
                    stmt_SetTimeZone.close();
                ResultSet rs = stmtCurrentTimeStringRead.executeQuery();
                while (rs.next()) {
                    CurrentTime = rs.getString("currentTime");
                }
                rs.close();
                MessegeSend_Log.info( "RDBMS CurrentTime for thead:" + (this.FirstInfoStreamId + theadNum ) + " CuberNumId:" + CuberNumId + " LocalDate ="+ CurrentTime );
            } catch (Exception e) {
                MessegeSend_Log.error("RDBMS setup Connection: `set SESSION time zone 3` fault: " + e.toString());
                e.printStackTrace();
            }
        }
        PerformQueueMessages PerformQueueMessages = new PerformQueueMessages();
        //PerformQueueMessages.setExternalConnectionManager( externalConnectionManager );
        MessageDetails Message = new MessageDetails();
        Message.ApiRestWaitTime = ApiRestWaitTime;
        MessageQueueVO messageQueueVO = new MessageQueueVO();
        // MonitoringQueueVO monitoringQueueVO = new MonitoringQueueVO();
        //Date moment = new Date(1451665447567L); // Задаем количество миллисекунд Unix-time с того-самого-момента
        long startTimestamp = Instant.ofEpochSecond(0L).until(Instant.now(), ChronoUnit.SECONDS);
            /*LocalDate todayLocalDate ;
            java.text.DateFormat dateFormat = new SimpleDateFormat("YYYYMMDDHH24mmss");
            dateFormat.setTimeZone(TimeZone.getTimeZone("Europe/Moscow"));
            LocalDateTime localDate = ZonedDateTime.ofInstant ( Instant.now() , ZoneId.of( "Europe/Moscow" ) ).toLocalDateTime();
            java.time.format.DateTimeFormatter DTformatter = java.time.format.DateTimeFormatter.ofPattern("YYYYMMddHHmmss");
            MessegeSend_Log.warn("MessageSendTask[" + theadNum + "]: localDate.toString=" + localDate.toString() + " localDate.format( DTformatter )=" + localDate.format( DTformatter ));
*/

        MessegeSend_Log.info("MessageSendTask[" + theadNum + "]: main stream scanning on " + HrmsSchema + ".MESSAGE_QUEUE is starting, InfoStreamId=" +  (this.FirstInfoStreamId + theadNum ) );
        for ( theadRunCount = 0; theadRunCount < theadRunTotalCount; theadRunCount += 1 ) {
            long secondsFromEpoch = Instant.ofEpochSecond(0L).until(Instant.now(), ChronoUnit.SECONDS);
            if ( secondsFromEpoch - startTimestamp > Long.valueOf(60L * TotalTimeTasks) )
                break;
            else
                theadRunTotalCount += 1;
            try {
                int num_Message4Perform = 0; // Если в очереди для потока нет данных, он заснет
                int num_HelpedMessage4Perform = 0; // Если поток обработал что-либо , помогая, то спать на чтении из очереди уже не нужно
                try {
                    ResultSet rLock = null;
                    stmtMsgQueue.setInt(1, (theadNum + this.FirstInfoStreamId) );
                    stmtMsgQueue.setInt(2, (theadNum + this.FirstInfoStreamId + this.CuberNumId * 1000 ) );

                    ResultSet rs = stmtMsgQueue.executeQuery();
                    // MessegeSend_Log.info("MessageSendTask[" + theadNum + "]: do scanning on `" + selectMessageSQL + "` using msg_InfostreamId in (" + (theadNum + this.FirstInfoStreamId) + ", "  + (theadNum + this.FirstInfoStreamId + this.CuberNumId * 1000 ) + ")");
                    while (rs.next()) {
                        num_Message4Perform +=1;
                        messageQueueVO.setMessageQueue(
                                rs.getLong("Queue_Id"),
                                rs.getTimestamp("Queue_Date"),
                                rs.getString("OutQueue_Id"),  //
                                rs.getTimestamp("Msg_Date"),
                                rs.getInt("Msg_Status"),
                                rs.getInt("MsgDirection_Id"),
                                rs.getInt("Msg_InfoStreamId"), // TO_DO + this.CuberNumId * 1000
                                rs.getInt("Operation_Id"),
                                rs.getString("Queue_Direction"),
                                rs.getString("Msg_Type"),
                                rs.getString("Msg_Reason"),
                                rs.getString("Msg_Type_own"),
                                rs.getString("Msg_Result"),
                                rs.getString("SubSys_Cod"),
                                rs.getString("Prev_Queue_Direction"),
                                rs.getInt("Retry_Count"),
                                rs.getTimestamp("Prev_Msg_Date"),
                                rs.getTimestamp("Queue_Create_Date"),
                                rs.getLong("Perform_Object_Id")
                        );

                        MessegeSend_Log.info( "messageQueueVO.Queue_Id:" + rs.getLong("Queue_Id") + " [Msg_InfoStreamId=" + rs.getInt("Msg_InfoStreamId") + "]" +
                                " [ " + rs.getString("Msg_Type") + "] SubSys_Cod=" + rs.getString("SubSys_Cod") +
                                "Curr_Server_Time=`" + (rs.getTimestamp("Curr_Server_Time").toString() ) +
                                "`,  ROWID=" + rs.getString("ROWID"));
                        // вместо java.sql.Timestamp.valueOf(LocalDateTime.now(ZoneId.of( "Europe/Moscow"))) локального компьютера берём время от сервера БД
                        // messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
                        messageQueueVO.setMsg_Date( rs.getTimestamp("Curr_Server_Time") );
                        // пробуем захватить запись
                        boolean isNoLock = true;
                        try {
                            // TODO Oracle stmtQueueLock.setRowId(1, rs.getRowId("ROWID") );
                            stmtQueueLock.setString(1, rs.getString("ROWID") );
                            int LockedMsg_InfoStreamId=0;
                            long LockedQueue_Id=0L;
                            String LockedQueue_Direction;
                            rLock = stmtQueueLock.executeQuery();
                            while (rLock.next()) {
                                LockedMsg_InfoStreamId = rLock.getInt("Msg_InfoStreamId");
                                LockedQueue_Id = rLock.getLong("Queue_Id");
                                LockedQueue_Direction = rLock.getString("Queue_Direction");
                                MessegeSend_Log.info( "Main Thread: stmtQueueLock.Queue_Id:" + LockedQueue_Id +
                                        " [Msg_InfoStreamId=" + LockedMsg_InfoStreamId + "]" +
                                        " [LockedQueue_Direction=" + LockedQueue_Direction + "]" +
                                        " do record locked" );
                            }
                            if ( LockedMsg_InfoStreamId != messageQueueVO.getMsg_InfoStreamId() ) // может быть 103 , а может быть и 2000 + 103, перечитываем как есть
                            { // пока читали, кто то уже забрал на себя
                                MessegeSend_Log.warn( "Main Thread: stmtQueueLock.Queue_Id:" + messageQueueVO.getQueue_Id() + " record can't be locked, " + LockedMsg_InfoStreamId + "!=" + messageQueueVO.getMsg_InfoStreamId()  );
                                isNoLock = false;
                            }
                            else { // запись захвачена, Ok
                                if (CuberNumId > 0) {// надо менять InfoStreamId что бы забрать под свой экземпляр Кубера + this.CuberNumId * 1000
                                    theadDataAccess.doUPDATE_QUEUE_InfoStreamId_by_RowId(rs.getString("ROWID"), LockedQueue_Id,
                                            (theadNum + this.FirstInfoStreamId + this.CuberNumId * 1000), MessegeSend_Log);
                                    messageQueueVO.setMsg_InfoStreamId( (theadNum + this.FirstInfoStreamId + this.CuberNumId * 1000) );
                                }
                                // Иначе Менять InfoStreamId не надо, это "свойй поток" нет параллельно работающих Sender'z
                                // TheadDataAccess.doUPDATE_QUEUE_InfoStreamId( rs.getRowId("ROWID") , LockedQueue_Id ,
                                //                                            (theadNum + this.FirstInfoStreamId), MessegeSend_Log );
                            }
                        }
                        catch (SQLException e) {
                            // Запись захвачена другим потоком
                            MessegeSend_Log.warn( "Main Thread: stmtQueueLock.Queue_Id:" + messageQueueVO.getQueue_Id() + " record can't be locked, " +e.getSQLState() + " :" + e.getMessage() );
                            isNoLock = false;
                            try {
                                theadDataAccess.Hermes_Connection.rollback();
                            }
                            catch (SQLException rollback_e) {
                                MessegeSend_Log.info( "Main Thread: stmtQueueLock4JMSconsumer.Queue_Id:" + messageQueueVO.getQueue_Id() + " Hermes_Connection.rollback() fault, " +e.getSQLState() + " :" + e.getMessage() );
                            }
                        }

                        if ( isNoLock )
                        { // запись захвачена
                            // -- это графана, не нужно -- ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()),  monitoringQueueVO, MessegeSend_Log);
                            // Очистили Message от всего, что там было
                            Message.ReInitMessageDetails( ApiRestWaitTime );
                            try {
                                MessegeSend_Log.warn( "Main Thread: (530:)stmtQueueLock.Queue_Id:" + messageQueueVO.getQueue_Id() + " record  locked, Msg_InfoStreamId=" + messageQueueVO.getMsg_InfoStreamId()  );
                                PerformQueueMessages.performMessage(Message, messageQueueVO, theadDataAccess, MessegeSend_Log);
                            } catch (Exception e) {
                                System.err.println("performMessage Exception Queue_Id:[" + messageQueueVO.getQueue_Id() + "] " + e.getMessage());
                                e.printStackTrace();
                                MessegeSend_Log.error("performMessage Exception Queue_Id:[" + messageQueueVO.getQueue_Id() + "] " + e.getMessage());
                                MessegeSend_Log.error("что то пошло совсем не так...");
                            }
                        }
                        if ( rLock != null)
                        { rLock.close(); theadDataAccess.Hermes_Connection.commit(); }

                    } // Цикл по выборке по своему потоку
                    rs.close();
                } catch (Exception e) {
                    MessegeSend_Log.error(e.getMessage());
                    e.printStackTrace();
                    MessegeSend_Log.error( "что то пошло совсем не так... см.:" + e.toString() );
                    return;
                }
                if ( num_Message4Perform <  NumMessageInScan ) // если в курсор был НЕ полон
                {
                    MessegeSend_Log.info("Ждём'c (курсор был НЕ полон): `" + num_Message4Perform + "` < `" + NumMessageInScan + "` в " + theadRunCount + " раз " + WaitTimeBetweenScan + "сек., уже " + (secondsFromEpoch - startTimestamp) + "сек., начиная с =" + startTimestamp + " текущее время =" + secondsFromEpoch);
                    // +"secondsFromEpoch - startTimestamp=" + (secondsFromEpoch - startTimestamp) +  " Long.valueOf(60L * TotalTimeTasks)=" + Long.valueOf(60L * TotalTimeTasks)
                    // Период ожидания JMS зависит от того, был ли конкурентный досту для "помощи" .
                    // Если помощник не смог взять блокировку - значит, помощников свободных много но работа для них есть, можно из рчереди читать не долго, 1/3 от обычного
                    int WaitTime4JmsQueue = WaitTimeBetweenScan * 1000 ;
                    // Если это поток-helper,  то проверяем, нужна ли помощь
                    if ( stmtHelperMsgQueue != null)
                    { num_HelpedMessage4Perform = 0;
                        // начинаем помогать
                        try {
                            ResultSet rLock = null;
                            ResultSet rs = stmtHelperMsgQueue.executeQuery();
                            // TODO : for Ora  @NotNull java.sql.RowId
                            String LockedROWID_QUEUE;
                            while (rs.next()) {
                                messageQueueVO.setMessageQueue(
                                        rs.getLong("Queue_Id"),
                                        rs.getTimestamp("Queue_Date"),
                                        rs.getString("OutQueue_Id"),
                                        rs.getTimestamp("Msg_Date"),
                                        rs.getInt("Msg_Status"),
                                        rs.getInt("MsgDirection_Id"),
                                        rs.getInt("Msg_InfoStreamId"),
                                        rs.getInt("Operation_Id"),
                                        rs.getString("Queue_Direction"),
                                        rs.getString("Msg_Type"),
                                        rs.getString("Msg_Reason"),
                                        rs.getString("Msg_Type_own"),
                                        rs.getString("Msg_Result"),
                                        rs.getString("SubSys_Cod"),
                                        rs.getString("Prev_Queue_Direction"),
                                        rs.getInt("Retry_Count"),
                                        rs.getTimestamp("Prev_Msg_Date"),
                                        rs.getTimestamp("Queue_Create_Date"),
                                        rs.getLong("Perform_Object_Id")
                                );
                                // TODO : for Ora java.sql.RowId = rs.getRowId("ROWID");
                                LockedROWID_QUEUE = rs.getString("ROWID");
                                MessegeSend_Log.info( "Helper: messageQueueVO.Queue_Id:" + rs.getLong("Queue_Id") + " [Msg_InfoStreamId=" + rs.getInt("Msg_InfoStreamId") + "]" +
                                        " [ " + rs.getString("Msg_Type") + "] SubSys_Cod=" + rs.getString("SubSys_Cod") + ",  ROWID=" + LockedROWID_QUEUE);
                                messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
                                // пробуем захватить запись
                                boolean isNoLock = true;
                                Long LockedQueue_Id=0L;
                                int LockedMsg_InfoStreamId=0;
                                String LockedQueue_Direction="NONE";
                                try {
                                    // TODO : for Ora java.sql.RowId => .setRowId(1, LockedROWID_QUEUE);
                                    stmtQueueLock.setString(1, LockedROWID_QUEUE);
                                    rLock = stmtQueueLock.executeQuery();
                                    while (rLock.next()) {
                                        LockedMsg_InfoStreamId = rLock.getInt("Msg_InfoStreamId");
                                        LockedQueue_Id = rLock.getLong("Queue_Id");
                                        LockedQueue_Direction = rLock.getString("Queue_Direction");
                                        MessegeSend_Log.info( "Helper: stmtQueueLock.Queue_Id:" + LockedQueue_Id +
                                                " [Msg_InfoStreamId=" + LockedMsg_InfoStreamId + "]" +
                                                " [Queue_Direction=" + LockedQueue_Direction + "]" +
                                                " record have locked" );

                                    }
                                    if (( LockedMsg_InfoStreamId != messageQueueVO.getMsg_InfoStreamId() ) || (! LockedQueue_Direction.equals(messageQueueVO.getQueue_Direction() ) ))
                                    { // пока читали, кто то уже забрал на себя
                                        MessegeSend_Log.warn( "Helper: stmtQueueLock.Queue_Id:" + messageQueueVO.getQueue_Id() + " record can't be locked, "
                                                + LockedMsg_InfoStreamId + "!=" + messageQueueVO.getMsg_InfoStreamId()  + " или "
                                                + LockedQueue_Direction + "!=" + messageQueueVO.getQueue_Direction());
                                        isNoLock = false;
                                    }
                                    else { // если запускается под управлением Cubernate надо лочить с учетом экземпляра CuberNumId
                                        if (theadDataAccess.doUPDATE_QUEUE_InfoStreamId_by_RowId( LockedROWID_QUEUE, LockedQueue_Id,
                                                (theadNum + this.FirstInfoStreamId + this.CuberNumId*1000), MessegeSend_Log)
                                                != 0
                                        ) // Не смогли установить свой №№ обработчика - значи, считакм, что блокировка не сработала.
                                            isNoLock = false;
                                        else
                                            messageQueueVO.setMsg_InfoStreamId( (theadNum + this.FirstInfoStreamId+ this.CuberNumId*1000) );
                                    }
                                }
                                catch (SQLException e) {
                                    // Запись захвачена другим потоком
                                    MessegeSend_Log.warn( "Helper: stmtQueueLock.Queue_Id:" + messageQueueVO.getQueue_Id() + " record can't be locked, " +e.getSQLState() + " :" + e.getMessage() );
                                    isNoLock = false;
                                }
                                // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()),  monitoringQueueVO, MessegeSend_Log);
                                if ( isNoLock )
                                { // запись
                                    // Очистили Message от всего, что там было
                                    Message.ReInitMessageDetails(ApiRestWaitTime);
                                    try {
                                        num_HelpedMessage4Perform +=1; // отмечаем, что конкретно помогаем
                                        MessegeSend_Log.warn( "Helped Thread: (628:)stmtQueueLock.Queue_Id:" + messageQueueVO.getQueue_Id() + " record  locked, Msg_InfoStreamId=" + messageQueueVO.getMsg_InfoStreamId()  );
                                        PerformQueueMessages.performMessage(Message, messageQueueVO, theadDataAccess, MessegeSend_Log);
                                    } catch (Exception e) {
                                        System.err.println("Helper: performMessage Exception Queue_Id:[" + messageQueueVO.getQueue_Id() + "] " + e.getMessage());
                                        e.printStackTrace();
                                        MessegeSend_Log.error("Helper: performMessage Exception Queue_Id:[" + messageQueueVO.getQueue_Id() + "] " + e.getMessage());
                                        MessegeSend_Log.error("Helper: что то с помощниками пошло совсем не так...");
                                    }
                                }
                                else {
                                    // Если помощник не смог взять блокировку - значит, помощников свободных много но работа для них есть, можно из рчереди читать не долго, 1/3 от обычного
                                    WaitTime4JmsQueue = (WaitTimeBetweenScan * 1000 ) /3;
                                }

                                if ( rLock != null)
                                { rLock.close(); theadDataAccess.Hermes_Connection.commit(); }
                            }
                            rs.close();
                        } catch (Exception e) {
                            MessegeSend_Log.error(e.getMessage());
                            e.printStackTrace();
                            MessegeSend_Log.error( "что то с помощниками пошло совсем не так...");
                            return;
                        }

                        // закончили помогать
                    }
                    if ( num_HelpedMessage4Perform == 0 ) // читаем из очереди, если не было работы Помощником !
                        try {
                            // JMSconsumer надо читать с блокировкой
                            if ( WaitTime4JmsQueue != (WaitTimeBetweenScan * 1000 ) )
                                WaitTime4JmsQueue = WaitTime4JmsQueue + (int )(Math.random() * 150 + 1) - 75;
                            if ( WaitTime4JmsQueue < 100 ) WaitTime4JmsQueue = 100;
                            TextMessage JMSTextMessage = (TextMessage) JMSconsumer.receive(WaitTime4JmsQueue*1); // (WaitTimeBetweenScan * 1000);

                            if ( JMSTextMessage != null ) {
                                String JMSMessageID = JMSTextMessage.getJMSMessageID();
                                MessegeSend_Log.info("Received message: (" + JMSMessageID + ") [" + JMSTextMessage.getText() + "]");
                                Destination tempDestResponce = JMSTextMessage.getJMSReplyTo();
                                if (tempDestResponce != null)
                                    MessegeSend_Log.info("tempDestResponce: (" + tempDestResponce.toString());

                                messageQueueVO.setQueue_Direction("NONE");
                                //  инициируем обработку с использованием JMS
                                PerfotmJMSMessage(  stmtQueueLock4JMSconsumer,
                                        stmtGetMessage4QueueId, selectMessage4QueueIdSQL,
                                        JMSTextMessage.getText(),
                                        Message, messageQueueVO, // monitoringQueueVO,
                                        PerformQueueMessages,
                                        theadDataAccess);
                                // Thread.sleep(WaitTimeBetweenScan * 1000);
                                if ( tempDestResponce != null ) {
                                    MessageProducer ReplyProducer = JMSsession.createProducer(tempDestResponce);
                                    TextMessage ReplyMessage = JMSsession.createTextMessage("{ \"Queue_Id\": \"" + messageQueueVO.getQueue_Id() + "\", \"Queue_Direction\": \"" + messageQueueVO.getQueue_Direction() + "\" , \"TheadNum\": \"" + theadNum + "\" }");
                                    MessegeSend_Log.info("Reply message: (" + ReplyMessage + ") [" + ReplyMessage.getText() + "]");
                                    ReplyMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
                                    ReplyMessage.setJMSDestination(tempDestResponce);
                                    ReplyProducer.send(ReplyMessage);
                                    ReplyProducer.close();
                                }

                            }else {
                                MessegeSend_Log.info("Received message: empty" );
                            }
                        } catch (JMSException e) {
                            MessegeSend_Log.error("JMSconsumer.receive: НЕ удалось зпроосить сообщений ActiveMQ (" + e.getMessage() + ")" );
                            Thread.sleep(WaitTimeBetweenScan * 1000);
                        }
                }
                else
                    MessegeSend_Log.info("НЕ ждём'c: в " + theadRunCount + " раз, а идем читать дальше " + WaitTimeBetweenScan + "сек., уже " + (secondsFromEpoch - startTimestamp) + "сек., начиная с =" + startTimestamp + " текущее время =" + secondsFromEpoch
                            // +"secondsFromEpoch - startTimestamp=" + (secondsFromEpoch - startTimestamp) +  " Long.valueOf(60L * TotalTimeTasks)=" + Long.valueOf(60L * TotalTimeTasks)
                    );
            } catch (Exception e) {
                MessegeSend_Log.error("MessageSendTask[" + theadNum + "]: is interrapted: " + e.getMessage());
                e.printStackTrace();
            }
            //   MessegeSend_Log.info("MessageSendTask[" + theadNum + "]: is finished[ " + theadRunCount + "] times");

        }
        MessegeSend_Log.info("MessageSendTask[" + theadNum + "]: is finished[ " + theadRunCount + "] times");
        // this.SenderExecutor.shutdown();
        try {
            Hermes_Connection.close();
        } catch (SQLException e) {
            MessegeSend_Log.error(e.getMessage());
            e.printStackTrace();
        }

    }

/*
private  boolean  LockMessage_Queue_ROW(RowId LockedROWID_QUEUE, MessageQueueVO messageQueueVO, @NotNull PreparedStatement stmtQueueLock, Logger MessegeSend_Log) {
    Long LockedQueue_Id = 0L;
    int LockedMsg_InfoStreamId = 0;
    String LockedQueue_Direction = null;
    try {
        stmtQueueLock.setRowId(1, LockedROWID_QUEUE);
        rLock = stmtQueueLock.executeQuery();
        while (rLock.next()) {
            LockedMsg_InfoStreamId = rs.getInt("Msg_InfoStreamId");
            LockedQueue_Id = rLock.getLong("Queue_Id");
            LockedQueue_Direction = rs.getString("Queue_Direction");
            MessegeSend_Log.info("Helper: stmtQueueLock.Queue_Id:" + LockedQueue_Id +
                    " [Msg_InfoStreamId=" + LockedMsg_InfoStreamId + "]" +
                    " [Queue_Direction=" + LockedQueue_Direction + "]" +
                    " record have locked");

        }
        if ((LockedMsg_InfoStreamId != messageQueueVO.getMsg_InfoStreamId()) || (!LockedQueue_Direction.equals(messageQueueVO.getQueue_Direction()))) { // пока читали, кто то уже забрал на себя
            MessegeSend_Log.warn("Helper: stmtQueueLock.Queue_Id:" + messageQueueVO.getQueue_Id() + " record can't be locked, "
                    + LockedMsg_InfoStreamId + "!=" + messageQueueVO.getMsg_InfoStreamId() + " или "
                    + LockedQueue_Direction + "!=" + messageQueueVO.getQueue_Direction());
            isNoLock = false;
        }
    }
}
*/

    private Long PerfotmJMSMessage ( PreparedStatement stmtQueueLock4JMSconsumer,
                                     PreparedStatement stmtGetMessage4QueueId, String selectMessage4QueueIdSQL,
                                     String MessageText,
                                     MessageDetails Message, MessageQueueVO messageQueueVO,
                                     // MonitoringQueueVO monitoringQueueVO,
                                     PerformQueueMessages PerformQueueMessages,
                                     TheadDataAccess theadDataAccess) {

        boolean isNoLock = true;
        ResultSet rLock = null;
        Long Queue_Id= null;
        Long performMessageResult = -7L;
        if ( MessageText.startsWith("{") ) { // Разбираем Json
            JSONObject RestResponseJSON = new JSONObject(MessageText);
            Object o_QUEUE_ID = RestResponseJSON.get("QUEUE_ID");
            String s_QUEUE_ID = null;
            if ( o_QUEUE_ID != null) {
                s_QUEUE_ID = o_QUEUE_ID.toString();
                Queue_Id = Long.parseUnsignedLong(s_QUEUE_ID);
            }
            else {
                return -2L;
            }

        }
        else {
            return -1L;
        }
/*
stmtGetMessage4QueueId = TheadDataAccess.Hermes_Connection.prepareStatement( selectMessage4QueueIdSQL );
         stmtUpdateMessage4RowId = TheadDataAccess.Hermes_Connection.prepareStatement( "update " + HrmsSchema + ".MESSAGE_QUEUE q set q.msg_infostreamid = ? where q.ROWID=?" );
 */
        // пробуем захватить запись
        MessegeSend_Log.info( "PerfotmJMSMessage: пробуем захватить (`"+ selectMessage4QueueIdSQL+ "`) запись Queue_Id="+ Queue_Id );

        String Queue_Direction=null;

        // TODO : for Ora java.sql.RowId QueueRowId = null;
        // RowId QueueRowId = null;
        String QueueRowId = null;

        try {
            stmtQueueLock4JMSconsumer.setLong(1, Queue_Id );
            rLock = stmtQueueLock4JMSconsumer.executeQuery();
            while (rLock.next()) {
                Queue_Direction = rLock.getString("Queue_Direction");
                if ( theadDataAccess.getRdbmsVendor().equals("oracle") ) {
                    QueueRowId = rLock.getString("rowid"); // TODO : for Ora  rLock.getRowId
                    MessegeSend_Log.info( "PerfotmJMSMessage: stmtQueueLock4JMSconsumer.Queue_Id="+ Queue_Id+ " :" + QueueRowId +
                            " record locked. msg_InfostreamId=" + rLock.getInt("msg_InfostreamId") + " Queue_Direction=[" + Queue_Direction + "]");
                }
                else {
                    QueueRowId = rLock.getString("rowid");
                    MessegeSend_Log.info( "PerfotmJMSMessage: stmtQueueLock4JMSconsumer.Queue_Id="+ Queue_Id+ " :" + QueueRowId +
                            " record locked. msg_InfostreamId=" + rLock.getInt("msg_InfostreamId") + " Queue_Direction=[" + Queue_Direction + "]");
                }
            }
        }
        catch (SQLException e) {
            // Запись захвачена другим потоком
            MessegeSend_Log.info( "PerfotmJMSMessage: stmtQueueLock4JMSconsumer.Queue_Id:" + Queue_Id + " record can't be locked, " +e.getSQLState() + " :" + e.getMessage() );
            try {
                theadDataAccess.Hermes_Connection.rollback();
            }
            catch (SQLException rollback_e) {
                MessegeSend_Log.info( "PerfotmJMSMessage: stmtQueueLock4JMSconsumer.Queue_Id:" + Queue_Id + " Hermes_Connection.rollback() fault, " +e.getSQLState() + " :" + e.getMessage() );
            }

            isNoLock = false;
        }
        if ( QueueRowId != null )
            MessegeSend_Log.info( "[" + Queue_Id + "] PerfotmJMSMessage: select for update вернул rowid ="+ QueueRowId );
        else
            MessegeSend_Log.warn( "[" + Queue_Id + "] PerfotmJMSMessage: select for update вернул rowid = NULL");
        // У "своих" из ОЧЕРЕДИ может увести сообщение только свой и он отпустит его рлмле SEND
        // надо проверять при блокировке, а не захватил ли сообщение какой нибудь ещё "Помощник". Такой конфтгурации пока нет, но... java.lang.NullPointerException: Cannot invoke "String.equals(Object)" because "Queue_Direction" is null
        if ( ( isNoLock ) && // запись захвачена и
             ( Queue_Direction != null )) // вдруг мог быть пустой курсор - читать было нечего
            // fix java.lang.NullPointerException: Cannot invoke "String.equals(Object)" because "Queue_Direction" is null
        {  //надо убедиться: что Queue_Direction = "OUT"
            if ( Queue_Direction.equals(DirectOUT) )
            {
                // Захватываем ( "update " + HrmsSchema + ".MESSAGE_QUEUE q set q.msg_infostreamid = ? where q.ROWID=?" ); с учётом Кубера
                int IsUpdated;
                IsUpdated =  theadDataAccess.doUPDATE_QUEUE_InfoStreamId_by_RowId(QueueRowId, Queue_Id, (theadNum + this.FirstInfoStreamId + this.CuberNumId * 1000 ), MessegeSend_Log);
                if ( IsUpdated != 0) {
                    try {
                        rLock.close();
                        theadDataAccess.Hermes_Connection.rollback();
                    } catch (SQLException ee) {
                        MessegeSend_Log.error(ee.getMessage());
                        System.err.println("sqlException Queue_Id:[" + Queue_Id + "] rollback()");
                        ee.printStackTrace();
                        MessegeSend_Log.error("PerfotmJMSMessage: Ошибка при закрытии SQL-ResultSet select for Update ...");
                        return -2L;
                    }
                    return -2L;
                }
                /* -- Заменили на theadDataAccess.doUPDATE_QUEUE_InfoStreamId(QueueRowId, Queue_Id, (theadNum + this.FirstInfoStreamId), MessegeSend_Log);
                 try { stmtUpdateMessage4RowId.setInt(1, theadNum + this.FirstInfoStreamId );
                      stmtUpdateMessage4RowId.setRowId(2, QueueRowId);
                      stmtUpdateMessage4RowId.executeUpdate();
                } catch (SQLException e) { MessegeSend_Log.error(e.getMessage()); System.err.println("sqlException Queue_Id:[" + Queue_Id + "]"); e.printStackTrace();
                    MessegeSend_Log.error( "PerfotmJMSMessage: Ошибка при \"update " + HrmsSchema + ".MESSAGE_QUEUE q set q.msg_infostreamid = ? where q.ROWID=?\" ...");
                    try { rLock.close(); theadDataAccess.Hermes_Connection.rollback();
                    } catch (SQLException ee) { MessegeSend_Log.error(ee.getMessage()); System.err.println("sqlException Queue_Id:[" + Queue_Id + "]"); ee.printStackTrace();
                        MessegeSend_Log.error( "PerfotmJMSMessage: Ошибка при закрытии SQL-ResultSet select for Update ...");
                        return -2L;
                    }
                    return -2L;
                }
                */
                // Очистили Message от всего, что там было
                Message.ReInitMessageDetails(ApiRestWaitTime);
                try {
                    stmtGetMessage4QueueId.setLong(1, Queue_Id ); //setString(1, QueueRowId ); // TODO : for Ora .setRowId( QueueRowId );
                    ResultSet rs = stmtGetMessage4QueueId.executeQuery();
                    boolean isRecordFoud = false;
                    while (rs.next()) {
                        isRecordFoud = true;
                        messageQueueVO.setMessageQueue(
                                rs.getLong("Queue_Id"),
                                rs.getTimestamp("Queue_Date"),
                                rs.getString("OutQueue_Id"), // get OutQueue_Id as to_Char(Q.outQueue_id)
                                rs.getTimestamp("Msg_Date"),
                                rs.getInt("Msg_Status"),
                                rs.getInt("MsgDirection_Id"),
                                rs.getInt("Msg_InfoStreamId"),
                                rs.getInt("Operation_Id"),
                                rs.getString("Queue_Direction"),
                                rs.getString("Msg_Type"),
                                rs.getString("Msg_Reason"),
                                rs.getString("Msg_Type_own"),
                                rs.getString("Msg_Result"),
                                rs.getString("SubSys_Cod"),
                                rs.getString("Prev_Queue_Direction"),
                                rs.getInt("Retry_Count"),
                                rs.getTimestamp("Prev_Msg_Date"),
                                rs.getTimestamp("Queue_Create_Date"),
                                rs.getLong("Perform_Object_Id")
                        );

                        MessegeSend_Log.info("PerfotmJMSMessage: messageQueueVO.Queue_Id = " + rs.getLong("Queue_Id") +
                                " [ " + rs.getString("Msg_Type") + "] SubSys_Cod=" + rs.getString("SubSys_Cod") + ",  ROWID=" + rs.getString("ROWID"));
                        messageQueueVO.setMsg_Date(java.sql.Timestamp.valueOf(LocalDateTime.now(ZoneId.of("Europe/Moscow"))));
                    }
                    if ( !isRecordFoud )
                        MessegeSend_Log.error( "PerfotmJMSMessage: не нашли запись в `"+ selectMessage4QueueIdSQL +"` =>`" + QueueRowId + "`");
                } catch (SQLException e) { MessegeSend_Log.error(e.getMessage()); System.err.println("sqlException Queue_Id:[" + Queue_Id + "]"); e.printStackTrace();
                    MessegeSend_Log.error( "PerfotmJMSMessage: Ошибка выборки `"+ selectMessage4QueueIdSQL + "` ...");
                    try { rLock.close(); theadDataAccess.Hermes_Connection.rollback();
                    } catch (SQLException ee) { MessegeSend_Log.error(ee.getMessage()); System.err.println("sqlException Queue_Id:[" + Queue_Id + "]"); ee.printStackTrace();
                        MessegeSend_Log.error( "PerfotmJMSMessage: Ошибка при закрытии SQL-ResultSet select for Update ...");
                        return -2L;
                    }
                    return -2L;
                }
                // ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()),  monitoringQueueVO, MessegeSend_Log);

                try {
                    MessegeSend_Log.warn( "PerfotmJMSMessage Thread: (894:)stmtQueueLock.Queue_Id:" + messageQueueVO.getQueue_Id() + " record  locked, Msg_InfoStreamId=" + messageQueueVO.getMsg_InfoStreamId()  );
                    performMessageResult = PerformQueueMessages.performMessage(Message, messageQueueVO, theadDataAccess, MessegeSend_Log);
                } catch (Exception e) {
                    System.err.println("PerfotmJMSMessage Exception Queue_Id:[" + messageQueueVO.getQueue_Id() + "] " + e.getMessage());
                    e.printStackTrace();
                    MessegeSend_Log.error("PerfotmJMSMessage Exception Queue_Id:[" + messageQueueVO.getQueue_Id() + "] " + e.getMessage());
                    MessegeSend_Log.error("что то пошло совсем не так...");
                }
            }
            else {
                // Запись уже обрабатывется, надо разблокировать
                MessegeSend_Log.warn( "Запись уже обрабатывется, надо разблокировать Queue_Id="+ Queue_Id );
                try {
                     rLock.close();
                        try {
                            theadDataAccess.Hermes_Connection.rollback();
                        }
                        catch (SQLException rollback_e) {
                            MessegeSend_Log.info( "PerfotmJMSMessage: stmtQueueLock4JMSconsumer.Queue_Id:" + messageQueueVO.getQueue_Id() + " Hermes_Connection.rollback() fault, " +rollback_e.getSQLState() + " :" + rollback_e.getMessage() );
                        }
                } catch (SQLException e) { MessegeSend_Log.error(e.getMessage()); System.err.println("sqlException Queue_Id:[" + Queue_Id + "]"); e.printStackTrace();
                    MessegeSend_Log.error( "Ошибка при закрытии SQL-ResultSet select for Update ...");
                    return -2L;
                }
            }

        }
        try {
            if ( rLock != null)
            { rLock.close(); theadDataAccess.Hermes_Connection.commit(); }
        } catch (SQLException e) { MessegeSend_Log.error(e.getMessage()); System.err.println("sqlException Queue_Id:[" + Queue_Id + "]"); e.printStackTrace();
            MessegeSend_Log.error( "Ошибка при закрытии SQL-ResultSet select for Update ...");
            return -2L;
        }
        return performMessageResult;
    }

}
