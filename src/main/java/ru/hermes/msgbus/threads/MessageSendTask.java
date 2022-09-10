package ru.hermes.msgbus.threads;

//import com.sun.istack.internal.NotNull;
//import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
//import org.apache.http.params.CoreConnectionPNames;
//import org.apache.http.params.HttpConnectionParams;
//import org.apache.http.params.BasicHttpParams;
//import org.apache.http.params.HttpParams;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import ru.hermes.msgbus.model.MessageDetails;
import ru.hermes.msgbus.model.MessageTemplate;
import ru.hermes.msgbus.model.MessageType;
import ru.hermes.msgbus.model.MessageQueueVO;
import ru.hermes.msgbus.monitoring.ConcurrentQueue;
import ru.hermes.msgbus.threads.utils.MessageHttpSend;
import ru.hermes.msgbus.model.MonitoringQueueVO;
import ru.hermes.msgbus.threads.utils.MessageRepositoryHelper;

import ru.hermes.msgbus.common.json.JSONObject;

import javax.jms.*;
import javax.net.ssl.SSLContext;
import java.sql.*;
import java.sql.Connection;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static ru.hermes.msgbus.common.XMLchars.DirectOUT;


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
    private   String name;
    public static final Logger MessegeSend_Log = LoggerFactory.getLogger(ru.hermes.msgbus.threads.MessageSendTask.class);

    private ThreadSafeClientConnManager externalConnectionManager;

    private String HrmsPoint;
    private String hrmsDbLogin;
    private String hrmsDbPasswd;
    private Integer theadNum;
    private Long TotalTimeTasks;
    private Integer WaitTimeBetweenScan;
    private Integer NumMessageInScan;
    private Integer ApiRestWaitTime;
    private Integer FirstInfoStreamId;
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

    public void setTotalTimeTasks(Long totalTimeTasks) {
        this.TotalTimeTasks = totalTimeTasks;
    }

    public void setHrmsDbPasswd(String hrmsDbPasswd) {
        this.hrmsDbPasswd = hrmsDbPasswd;
    }

    public void setHrmsDbLogin(String hrmsDbLogin) {
        this.hrmsDbLogin = hrmsDbLogin;
    }

    public void setTheadNum( int TheadNum) {
        this.theadNum = TheadNum;
    }

    public void setJMSQueueConnection (javax.jms.Connection JMSQueueConnection  ) { this.JMSQueueConnection = JMSQueueConnection;}
    public void setApiRestWaitTime( int ApiRestWaitTime) { this.ApiRestWaitTime = ApiRestWaitTime; }
    public void setFirstInfoStreamId( int FirstInfoStreamId) { this.FirstInfoStreamId=FirstInfoStreamId;}
    private int theadRunCount = 0;
    private  int  theadRunTotalCount = 1;
    private SSLContext sslContext = null;
    private HttpClientBuilder httpClientBuilder  = null;
    private CloseableHttpClient ApiRestHttpClient = null;

    @Bean( name = "MessageSendTaskRun")
    // @Scheduled(initialDelay = 100, fixedRate = 1000)
    public void run() {
        //if (( theadNum != null ) && ((theadNum == 17) || (theadNum == 18) || (theadNum == 19) || (theadNum == 20)) )
        if (( theadNum == null ) ) // && (theadNum == 0))
            return;
        Integer count = 0;


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
        int ReadTimeoutInMillis = ApiRestWaitTime * 1000;
        int ConnectTimeoutInMillis = 5 * 1000;
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

        TheadDataAccess TheadDataAccess = new TheadDataAccess();
        // Установаливем " соединение" , что бы зачитывать очередь
        Connection Hermes_Connection = TheadDataAccess.make_Hermes_Connection(HrmsPoint, hrmsDbLogin, hrmsDbPasswd,
                MessegeSend_Log
        );
        if ( Hermes_Connection == null) {
            MessegeSend_Log.error("НЕ удалось Установить соединение , что бы зачитывать очередь, либо подготовить запросы к БД ");
            return;
        }
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
        PreparedStatement stmtGetMessage4RowId;
        // PreparedStatement stmtUpdateMessage4RowId; - вместо этого TheadDataAccess.doUPDATE_QUEUE_InfoStreamId()

        if ( TheadDataAccess.Hermes_Connection != null )
            // Готовим набор SQL
            try {
                String selectMessage4RowIdSQL= "select q.ROWID, Q.queue_id, Q.queue_direction, NVL(Q.queue_date, sysdate - 1 / (24 * 60)) Queue_Date," +
                        " Q.msg_status, Q.msg_date Msg_Date, Q.operation_id, Q.outqueue_id, Q.msg_type, Q.msg_reason, Q.msgdirection_id, Q.msg_infostreamid," +
                        " Q.msg_type_own,Q.msg_result, Q.subsys_cod, NVL(Q.retry_count, 0) as Retry_Count, Q.prev_queue_direction, Q.prev_msg_date Prev_Msg_Date," +
                        " NVL(Q.queue_create_date, NVL(Q.queue_date, sysdate - 1 / (24 * 60))) Queue_Create_Date, Q.Perform_Object_Id" +
                        " from ARTX_PROJ.MESSAGE_QUEUE q where q.ROWID=?";
                stmtGetMessage4RowId = TheadDataAccess.Hermes_Connection.prepareStatement( selectMessage4RowIdSQL );
                //stmtUpdateMessage4RowId = TheadDataAccess.Hermes_Connection.prepareStatement( "update ARTX_PROJ.MESSAGE_QUEUE q set q.msg_infostreamid = ? where q.ROWID=?" );
                String selectMessageSQL =
                        "select * from ( select q.ROWID, " +
                                " Q.queue_id," +
                                " Q.queue_direction," +
                                " NVL(Q.queue_date, sysdate-1/(24*60)) Queue_Date, "+
                                " Q.msg_status," +
                                " Q.msg_date Msg_Date," +
                                " Q.operation_id," +
                                " Q.outqueue_id," +
                                " Q.msg_type," +
                                " Q.msg_reason," +
                                " Q.msgdirection_id," +
                                " Q.msg_infostreamid," +
                                " Q.msg_type_own," +
                                " Q.msg_result," +
                                " Q.subsys_cod," +
                                " NVL(Q.retry_count, 0) as Retry_Count," +
                                " Q.prev_queue_direction," +
                                " Q.prev_msg_date Prev_Msg_Date, " +
                                " NVL(Q.queue_create_date, NVL(Q.queue_date, sysdate-1/(24*60)))  Queue_Create_Date, " +
                                " Q.Perform_Object_Id " +
                                "from ARTX_PROJ.MESSAGE_QUEUE Q" +
                                " Where 1=1" +
                                " and Q.msg_infostreamid = ? "  +
                                " and Q.queue_direction in( 'OUT','SEND')" +  // ",'RESOUT','DELOUT')"
                                " and Q.msg_date < sysdate " +
                                " order by Q.Priority_Level asc , Q.queue_id asc ) where rownum < " + NumMessageInScan ;
                MessegeSend_Log.info( "Main_MESSAGE_QueueSelect:{" + selectMessageSQL  + "} Q.Msg_InfostreamId ="  + (this.FirstInfoStreamId + theadNum ) ) ;
                stmtMsgQueue = TheadDataAccess.Hermes_Connection.prepareStatement( selectMessageSQL);

                stmtQueueLock = TheadDataAccess.Hermes_Connection.prepareStatement( "select Q.Queue_Id, Q.Queue_Direction, Q.Msg_InfostreamId  from ARTX_PROJ.MESSAGE_QUEUE Q where q.ROWID=? for update nowait" );
                stmtQueueLock4JMSconsumer = TheadDataAccess.Hermes_Connection.prepareStatement( "select Q.ROWID, q.Queue_Direction, q.Msg_InfostreamId from ARTX_PROJ.MESSAGE_QUEUE q where q.Queue_Id=? for update nowait" );

                // Получеем перечень потоков, которым надо помогать ДАННОМУ потоку
                String List_Lame_Threads =  MessageRepositoryHelper.look4List_Lame_Threads_4_Num_Thread( theadNum + this.FirstInfoStreamId  , MessegeSend_Log );
                if ( List_Lame_Threads != null)
                {   selectMessageSQL =
                        "select * from ( select q.ROWID, " +
                                " Q.queue_id," +
                                " Q.queue_direction," +
                                " NVL(Q.queue_date, sysdate-1/(24*60)) Queue_Date, "+
                                " Q.msg_status," +
                                " Q.msg_date Msg_Date," +
                                " Q.operation_id," +
                                " Q.outqueue_id," +
                                " Q.msg_type," +
                                " Q.msg_reason," +
                                " Q.msgdirection_id," +
                                " Q.msg_infostreamid," +
                                " Q.msg_type_own," +
                                " Q.msg_result," +
                                " Q.subsys_cod," +
                                " NVL(Q.retry_count, 0) as Retry_Count," +
                                " Q.prev_queue_direction," +
                                " Q.prev_msg_date Prev_Msg_Date, " +
                                " NVL(Q.queue_create_date, NVL(Q.queue_date, sysdate-1/(24*60)))  Queue_Create_Date, " +
                                " Q.Perform_Object_Id " +
                                "from ARTX_PROJ.MESSAGE_QUEUE Q" +
                                " Where 1=1" +
                                " and Q.msg_infostreamid in (" + List_Lame_Threads + ")" +
                                " and Q.queue_direction='OUT'" +  // in( 'OUT')",'SEND','RESOUT','DELOUT')"
                                " and Q.msg_date < sysdate " +
                                " order by Q.Priority_Level asc , Q.queue_id asc ) where rownum =1 " ;
                    MessegeSend_Log.info( "Helper_MESSAGE_QueueSelect: " + selectMessageSQL );
                    stmtHelperMsgQueue = TheadDataAccess.Hermes_Connection.prepareStatement( selectMessageSQL);
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
        // инициализируем
        PerformQueueMessages PerformQueueMessages = new PerformQueueMessages();
        PerformQueueMessages.setExternalConnectionManager( externalConnectionManager );
        MessageDetails Message = new MessageDetails();
        MessageQueueVO messageQueueVO = new MessageQueueVO();
        MonitoringQueueVO monitoringQueueVO = new MonitoringQueueVO();
        //Date moment = new Date(1451665447567L); // Задаем количество миллисекунд Unix-time с того-самого-момента
        long startTimestamp = Instant.ofEpochSecond(0L).until(Instant.now(), ChronoUnit.SECONDS);
            /*LocalDate todayLocalDate ;
            java.text.DateFormat dateFormat = new SimpleDateFormat("YYYYMMDDHH24mmss");
            dateFormat.setTimeZone(TimeZone.getTimeZone("Europe/Moscow"));
            LocalDateTime localDate = ZonedDateTime.ofInstant ( Instant.now() , ZoneId.of( "Europe/Moscow" ) ).toLocalDateTime();
            java.time.format.DateTimeFormatter DTformatter = java.time.format.DateTimeFormatter.ofPattern("YYYYMMddHHmmss");
            MessegeSend_Log.warn("MessageSendTask[" + theadNum + "]: localDate.toString=" + localDate.toString() + " localDate.format( DTformatter )=" + localDate.format( DTformatter ));
*/

        MessegeSend_Log.info("MessageSendTask[" + theadNum + "]: scaning on ARTX_PROJ.MESSAGE_QUEUE is starting ");
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
                    ResultSet rs = stmtMsgQueue.executeQuery();
                    while (rs.next()) {
                        num_Message4Perform +=1;
                        messageQueueVO.setMessageQueue(
                                rs.getLong("Queue_Id"),
                                rs.getTimestamp("Queue_Date"),
                                rs.getLong("OutQueue_Id"),
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

                        MessegeSend_Log.info( "messageQueueVO.Queue_Id:" + rs.getLong("Queue_Id") + " [Msg_InfoStreamId=" + rs.getInt("Msg_InfoStreamId") + "]" +
                                " [ " + rs.getString("Msg_Type") + "] SubSys_Cod=" + rs.getString("SubSys_Cod") + ",  ROWID=" + rs.getRowId("ROWID"));
                        messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
                        // пробуем захватить запись
                        boolean isNoLock = true;
                        try {
                            stmtQueueLock.setRowId(1, rs.getRowId("ROWID") );
                            int LockedMsg_InfoStreamId=0;
                            Long LockedQueue_Id=0L;
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
                            if ( LockedMsg_InfoStreamId != messageQueueVO.getMsg_InfoStreamId() )
                            { // пока читали, кто то уже забрал на себя
                                MessegeSend_Log.warn( "Main Thread: stmtQueueLock.Queue_Id:" + messageQueueVO.getQueue_Id() + " record can't be locked, " + LockedMsg_InfoStreamId + "!=" + messageQueueVO.getMsg_InfoStreamId()  );
                                isNoLock = false;
                            }
                            // Менять InfoStreamId не надо, это "свойй поток"
                            // TheadDataAccess.doUPDATE_QUEUE_InfoStreamId( rs.getRowId("ROWID") , LockedQueue_Id ,
                            //                                            (theadNum + this.FirstInfoStreamId), MessegeSend_Log );
                        }
                        catch (SQLException e) {
                            // Запись захвачена другим потоком
                            MessegeSend_Log.warn( "Main Thread: stmtQueueLock.Queue_Id:" + messageQueueVO.getQueue_Id() + " record can't be locked, " +e.getSQLState() + " :" + e.getMessage() );
                            isNoLock = false;
                        }

                        if ( isNoLock )
                        { // запись
                            ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()),  monitoringQueueVO, MessegeSend_Log);
                            // Очистили Message от всего, что там было
                            Message.ReInitMessageDetails(sslContext, httpClientBuilder, null, ApiRestHttpClient);
                            try {
                                PerformQueueMessages.performMessage(Message, messageQueueVO, TheadDataAccess, MessegeSend_Log);
                            } catch (Exception e) {
                                System.err.println("performMessage Exception Queue_Id:[" + messageQueueVO.getQueue_Id() + "] " + e.getMessage());
                                e.printStackTrace();
                                MessegeSend_Log.error("performMessage Exception Queue_Id:[" + messageQueueVO.getQueue_Id() + "] " + e.getMessage());
                                MessegeSend_Log.error("что то пошло совсем не так...");
                            }
                        }
                        if ( rLock != null)
                        { rLock.close(); TheadDataAccess.Hermes_Connection.commit(); }
                    } // Цикл по выборке по своему потоку
                    rs.close();
                } catch (Exception e) {
                    MessegeSend_Log.error(e.getMessage());
                    e.printStackTrace();
                    MessegeSend_Log.error( "что то пошло совсем не так...");
                    return;
                }
                if ( num_Message4Perform <  NumMessageInScan ) // если в курсор был НЕ полон
                {
                    MessegeSend_Log.info("Ждём'c; в " + theadRunCount + " раз " + WaitTimeBetweenScan + "сек., уже " + (secondsFromEpoch - startTimestamp) + "сек., начиная с =" + startTimestamp + " текущее время =" + secondsFromEpoch);
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
                            RowId LockedROWID_QUEUE;
                            while (rs.next()) {
                                messageQueueVO.setMessageQueue(
                                        rs.getLong("Queue_Id"),
                                        rs.getTimestamp("Queue_Date"),
                                        rs.getLong("OutQueue_Id"),
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
                                LockedROWID_QUEUE = rs.getRowId("ROWID");
                                MessegeSend_Log.info( "Helper: messageQueueVO.Queue_Id:" + rs.getLong("Queue_Id") + " [Msg_InfoStreamId=" + rs.getInt("Msg_InfoStreamId") + "]" +
                                        " [ " + rs.getString("Msg_Type") + "] SubSys_Cod=" + rs.getString("SubSys_Cod") + ",  ROWID=" + LockedROWID_QUEUE);
                                messageQueueVO.setMsg_Date( java.sql.Timestamp.valueOf( LocalDateTime.now( ZoneId.of( "Europe/Moscow" ) ) ) );
                                // пробуем захватить запись
                                boolean isNoLock = true;
                                Long LockedQueue_Id=0L;
                                int LockedMsg_InfoStreamId=0;
                                String LockedQueue_Direction="NONE";
                                try {
                                    stmtQueueLock.setRowId(1, LockedROWID_QUEUE);
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
                                    else {
                                        if (TheadDataAccess.doUPDATE_QUEUE_InfoStreamId( LockedROWID_QUEUE, LockedQueue_Id,
                                                (theadNum + this.FirstInfoStreamId), MessegeSend_Log)
                                                != 0
                                        ) // Не смогли установить свой №№ обработчика - значи, считакм, что блокировка не сработала.
                                            isNoLock = false;
                                        else
                                            messageQueueVO.setMsg_InfoStreamId( (theadNum + this.FirstInfoStreamId) );
                                    }
                                }
                                catch (SQLException e) {
                                    // Запись захвачена другим потоком
                                    MessegeSend_Log.warn( "Helper: stmtQueueLock.Queue_Id:" + messageQueueVO.getQueue_Id() + " record can't be locked, " +e.getSQLState() + " :" + e.getMessage() );
                                    isNoLock = false;
                                }
                                ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()),  monitoringQueueVO, MessegeSend_Log);
                                if ( isNoLock )
                                { // запись
                                    // Очистили Message от всего, что там было
                                    Message.ReInitMessageDetails(sslContext, httpClientBuilder, null, ApiRestHttpClient);
                                    try {
                                        num_HelpedMessage4Perform +=1; // отмечаем, что конкретно помогаем
                                        PerformQueueMessages.performMessage(Message, messageQueueVO, TheadDataAccess, MessegeSend_Log);
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
                                { rLock.close(); TheadDataAccess.Hermes_Connection.commit(); }
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
                                        stmtGetMessage4RowId,
                                        JMSTextMessage.getText(),
                                        Message, messageQueueVO,
                                        monitoringQueueVO,
                                        PerformQueueMessages,
                                        TheadDataAccess);
                                // Thread.sleep(WaitTimeBetweenScan * 1000);
                                //////////////////////////////////////////////////////
                                if ( tempDestResponce != null ) {
                                    MessageProducer ReplyProducer = JMSsession.createProducer(tempDestResponce);
                                    TextMessage ReplyMessage = JMSsession.createTextMessage("{ \"Queue_Id\": \"" + messageQueueVO.getQueue_Id() + "\", \"Queue_Direction\": \"" + messageQueueVO.getQueue_Direction() + "\" , \"TheadNum\": \"" + theadNum + "\" }");
                                    MessegeSend_Log.info("Reply message: (" + ReplyMessage + ") [" + ReplyMessage.getText() + "]");
                                    ReplyMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
                                    ReplyMessage.setJMSDestination(tempDestResponce);
                                    ReplyProducer.send(ReplyMessage);
                                    ReplyProducer.close();
                                }
                                /////////////////////////////////////////////////////
                            }else {
                                MessegeSend_Log.info("Received message: empty" );
                            }
                        } catch (JMSException e) {
                            MessegeSend_Log.error("JMSconsumer.receive: НЕ удалось зпроосить сообщений ActiveMQ");

                        }
                }
                else
                    MessegeSend_Log.info("НЕ ждём'c; в " + theadRunCount + " раз, а идем читать дальше " + WaitTimeBetweenScan + "сек., уже " + (secondsFromEpoch - startTimestamp) + "сек., начиная с =" + startTimestamp + " текущее время =" + secondsFromEpoch
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
                                     PreparedStatement stmtGetMessage4RowId,
                                     // PreparedStatement stmtUpdateMessage4RowId,
                                     String MessageText,
                                     MessageDetails Message, MessageQueueVO messageQueueVO,
                                     MonitoringQueueVO monitoringQueueVO,
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
stmtGetMessage4RowId = TheadDataAccess.Hermes_Connection.prepareStatement( selectMessage4RowIdSQL );
         stmtUpdateMessage4RowId = TheadDataAccess.Hermes_Connection.prepareStatement( "update ARTX_PROJ.MESSAGE_QUEUE q set q.msg_infostreamid = ? where q.ROWID=?" );
 */
        // пробуем захватить запись
        MessegeSend_Log.info( "PerfotmJMSMessage: пробуем захватить запись Queue_Id="+ Queue_Id );

        String Queue_Direction=null;
        RowId QueueRowId = null;
        try {
            stmtQueueLock4JMSconsumer.setLong(1, Queue_Id );
            rLock = stmtQueueLock4JMSconsumer.executeQuery();
            while (rLock.next()) {
                Queue_Direction = rLock.getString("Queue_Direction");
                QueueRowId = rLock.getRowId("ROWID");
                // q.Queue_Direction, q.msg_InfostreamId
                MessegeSend_Log.info( "PerfotmJMSMessage: stmtQueueLock4JMSconsumer.Queue_Id="+ Queue_Id+ " :" + QueueRowId +
                        " record locked. msg_InfostreamId=" + rLock.getInt("msg_InfostreamId") + " Queue_Direction=[" + Queue_Direction + "]");
            }
        }
        catch (SQLException e) {
            // Запись захвачена другим потоком
            MessegeSend_Log.info( "Main Thread: stmtQueueLock.Queue_Id:" + Queue_Id + " record can't be locked, " +e.getSQLState() + " :" + e.getMessage() );

            isNoLock = false;
        }
        // У "своих" из ОЧЕРЕДИ может увести сообщение только свой и он отпустит его рлмле SEND
        // TODO : надо проверять при блокировке, а не захватил ли сообщение какой нибудь "Помощник". Такой конфтгурации пока нет, но...
        if ( isNoLock )
        { // запись захвачена, надо убедиться: что Queue_Direction = "OUT"
            if ( Queue_Direction.equals(DirectOUT) )
            {
                // Захватываем ( "update ARTX_PROJ.MESSAGE_QUEUE q set q.msg_infostreamid = ? where q.ROWID=?" );
                int IsUpdated = theadDataAccess.doUPDATE_QUEUE_InfoStreamId(QueueRowId, Queue_Id, (theadNum + this.FirstInfoStreamId), MessegeSend_Log);
                if ( IsUpdated != 0) {
                    try {
                        rLock.close();
                        theadDataAccess.Hermes_Connection.rollback();
                    } catch (SQLException ee) {
                        MessegeSend_Log.error(ee.getMessage());
                        System.err.println("sqlException Queue_Id:[" + Queue_Id + "]");
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
                    MessegeSend_Log.error( "PerfotmJMSMessage: Ошибка при \"update ARTX_PROJ.MESSAGE_QUEUE q set q.msg_infostreamid = ? where q.ROWID=?\" ...");
                    try { rLock.close(); theadDataAccess.Hermes_Connection.rollback();
                    } catch (SQLException ee) { MessegeSend_Log.error(ee.getMessage()); System.err.println("sqlException Queue_Id:[" + Queue_Id + "]"); ee.printStackTrace();
                        MessegeSend_Log.error( "PerfotmJMSMessage: Ошибка при закрытии SQL-ResultSet select for Update ...");
                        return -2L;
                    }
                    return -2L;
                }
                */
                // Очистили Message от всего, что там было
                Message.ReInitMessageDetails(sslContext, httpClientBuilder, null, ApiRestHttpClient);
                try {
                    stmtGetMessage4RowId.setRowId(1, QueueRowId );
                    ResultSet rs = stmtGetMessage4RowId.executeQuery();
                    while (rs.next()) {
                        messageQueueVO.setMessageQueue(
                                rs.getLong("Queue_Id"),
                                rs.getTimestamp("Queue_Date"),
                                rs.getLong("OutQueue_Id"),
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
                                " [ " + rs.getString("Msg_Type") + "] SubSys_Cod=" + rs.getString("SubSys_Cod") + ",  ROWID=" + rs.getRowId("ROWID"));
                        messageQueueVO.setMsg_Date(java.sql.Timestamp.valueOf(LocalDateTime.now(ZoneId.of("Europe/Moscow"))));
                    }
                } catch (SQLException e) { MessegeSend_Log.error(e.getMessage()); System.err.println("sqlException Queue_Id:[" + Queue_Id + "]"); e.printStackTrace();
                    MessegeSend_Log.error( "PerfotmJMSMessage: Ошибка при \"select q.ROWID, Q.queue_id, Q.queue_direction ... where q.ROWID=?\" ...");
                    try { rLock.close(); theadDataAccess.Hermes_Connection.rollback();
                    } catch (SQLException ee) { MessegeSend_Log.error(ee.getMessage()); System.err.println("sqlException Queue_Id:[" + Queue_Id + "]"); ee.printStackTrace();
                        MessegeSend_Log.error( "PerfotmJMSMessage: Ошибка при закрытии SQL-ResultSet select for Update ...");
                        return -2L;
                    }
                    return -2L;
                }
                ConcurrentQueue.addMessageQueueVO2queue(  messageQueueVO, messageQueueVO.getMsg_Type(), String.valueOf(messageQueueVO.getQueue_Id()),  monitoringQueueVO, MessegeSend_Log);

                try {
                    performMessageResult = PerformQueueMessages.performMessage(Message, messageQueueVO, theadDataAccess, MessegeSend_Log);
                } catch (Exception e) {
                    System.err.println("performMessage Exception Queue_Id:[" + messageQueueVO.getQueue_Id() + "] " + e.getMessage());
                    e.printStackTrace();
                    MessegeSend_Log.error("performMessage Exception Queue_Id:[" + messageQueueVO.getQueue_Id() + "] " + e.getMessage());
                    MessegeSend_Log.error("что то пошло совсем не так...");
                }
            }
            else {
                // Запись уже обрабатывется, надо разблокировать
                MessegeSend_Log.warn( "Запись уже обрабатывется, надо разблокировать Queue_Id="+ Queue_Id );
                try {
                    if ( rLock != null)
                    { rLock.close(); theadDataAccess.Hermes_Connection.rollback(); }
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
