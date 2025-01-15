package net.plumbing.msgbus;

import net.plumbing.msgbus.scheduler.ExternSystemCallTask;
import net.plumbing.msgbus.common.ApplicationProperties;
import net.plumbing.msgbus.common.DataAccess;
import net.plumbing.msgbus.common.ExtSystemDataAccess;
import net.plumbing.msgbus.config.*;
import net.plumbing.msgbus.init.InitMessageRepository;
import net.plumbing.msgbus.telegramm.NotifyByChannel;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import net.plumbing.msgbus.threads.utils.MessageRepositoryHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;

import org.springframework.boot.CommandLineRunner;
import net.plumbing.msgbus.model.MessageDirections;
//import net.plumbing.msgbus.monitoring.MonitoringWriterTask;
//import net.plumbing.msgbus.monitoring.ConcurrentQueue;
//import MonitoringConfig;
import net.plumbing.msgbus.mq.ActiveMQService;
import net.plumbing.msgbus.threads.MessageSendTask;

import java.net.InetAddress;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.activemq.broker.BrokerService;

import javax.jms.Connection;

@EnableScheduling
@SpringBootApplication (scanBasePackages = "net.plumbing.msgbus.*")

public class SenderApplication implements CommandLineRunner {

	public static final Logger AppThead_log = LoggerFactory.getLogger(SenderApplication.class);
	// static ThreadPoolTaskExecutor monitorWriterPool; // -- не используется
	static ThreadPoolTaskExecutor externSystemCallPool;

	@Autowired
	public ConnectionProperties connectionProperties;
	@Autowired
	public DBLoggingProperties dbLoggingProperties;
	@Autowired
	public TaskPollProperties taskPollProperties ;
	@Autowired
	public TelegramProperties telegramProperties;

	public static String propJDBC;
	public static String propExtJDBC;
	public static String firstInfoStreamId;
	public static final String ApplicationName="*Sender_BUS* v.5.01.15p";
	public static void main(String[] args) {
		SpringApplication.run(SenderApplication.class, args);
	}

	public void run(String... strings) throws Exception {
		int i;
		ApplicationContext context = new AnnotationConfigApplicationContext(Sender_AppConfig.class);

		AppThead_log.info("Hellow for SenderApplication ");
		NotifyByChannel.Telegram_setChatBotUrl( telegramProperties.getchatBotUrl() , AppThead_log );
		AppThead_log.info( "Telegram_sendMessage " + telegramProperties.getchatBotUrl() + " :" + "Starting " + ApplicationName + " on " + InetAddress.getLocalHost().getHostName()+ " (ip " +InetAddress.getLocalHost().getHostAddress() + " ) ");
		propJDBC = connectionProperties.gethrmsPoint();
		if ( propJDBC == null)  propJDBC = "jdbc UNKNOWN ! ";
		else {
			if ( propJDBC.indexOf("//") < 1  ) ;
			else {
				propJDBC = propJDBC.substring(propJDBC.indexOf("//") + 2);
				if ( propJDBC.indexOf("/") < 1  ) propJDBC = "INVALID db in jdbc ! `" + propJDBC + "`";
				else
				 propJDBC = propJDBC.substring(0, propJDBC.indexOf("/"));
			}
		}
		propExtJDBC = connectionProperties.getextsysPoint();
		if ( propExtJDBC == null)  propExtJDBC = "Ext jdbc UNKNOWN ! ";
		else {
			if ( propExtJDBC.indexOf("//") < 1  ) ;
			else {
				propExtJDBC = propExtJDBC.substring(propExtJDBC.indexOf("//") + 2);
				if ( propExtJDBC.indexOf("/") < 1  ) propExtJDBC = "INVALID db in jdbc ! `" + propExtJDBC + "`";
				else
					propExtJDBC = propExtJDBC.substring(0, propExtJDBC.indexOf("/"));
			}
		}
		firstInfoStreamId = connectionProperties.getfirstInfoStreamId(); // для импорта из ShutdownHook
		String sendedMessage_2_Telegram = "Starting " + ApplicationName + " -" + connectionProperties.getfirstInfoStreamId() + " on " + InetAddress.getLocalHost().getHostName()+
				" (ip `" +InetAddress.getLocalHost().getHostAddress() + "`, db `" + propJDBC+ "` as `"+ connectionProperties.gethrmsDbLogin() + "`, extDb `"
				+ propExtJDBC+ "` as `"+ connectionProperties.getextsysDbLogin()  + "` )";
		AppThead_log.warn( "testSring:[" + sendedMessage_2_Telegram + "] 4 sent"  );
		NotifyByChannel.Telegram_sendMessage( sendedMessage_2_Telegram, AppThead_log );
		String propConnectMsgBus = connectionProperties.getconnectMsgBus();
		if ( propConnectMsgBus == null) propConnectMsgBus = "tcp://0.0.0.0:61016";

		ActiveMQService activeMQService= new ActiveMQService();
		BrokerService MQbroker= activeMQService.ActiveMQbroker( propConnectMsgBus );
		// MQbroker.start(); // Сделаем это после ВСЕ Очередей!

		AppThead_log.info("ActiveMQbroker " + MQbroker.getBrokerName() + " started" );
		Properties props = System.getProperties();
		props.setProperty("com.sun.net.ssl.checkRevocation","false");

		ThreadPoolTaskExecutor taskExecutor = (ThreadPoolTaskExecutor) context.getBean("taskExecutor");

		/* --- monitorWriter для Графаны больше не используется , комментарим
		this.monitorWriter();

		AppThead_log.warn( dbLoggingProperties.toString() );
		// MonitoringWriterTask.setMonitoringDbParam()
		if ( dbLoggingProperties.gettotalNumTasks() != null) {
			int TotalWriterTask = Integer.parseInt(dbLoggingProperties.gettotalNumTasks());
			Integer WaitTimeBetweenWrite = Integer.parseInt(dbLoggingProperties.getwaitTimeScan());

			ConcurrentQueue.Init(Integer.parseInt(dbLoggingProperties.getqueueCopasity()));
			this.monitorWriterPool.setThreadGroupName("monitorWriter");

			if (TotalWriterTask > 0 )  // dbLoggingProperties.gettotalNumTasks() == 0 значит, выключен
			{

				MonitoringWriterTask[] monitorWriterTask = new MonitoringWriterTask[TotalWriterTask];
				for (i = 0; i < TotalWriterTask; i++) {
					monitorWriterTask[i] = new MonitoringWriterTask();// (MessageSendTask) context.getBean("MessageSendTask");
					//monitorWriterTask[ i ].setContext(  MntrContext );
					monitorWriterTask[i].setWaitTimeBetweenScan(WaitTimeBetweenWrite);
					monitorWriterTask[i].setMonitoringDbParam(dbLoggingProperties.getdataSourceClassName(), dbLoggingProperties.getjdbcUrl(),
							dbLoggingProperties.getmntrDbLogin(), dbLoggingProperties.getmntrDbPasswd(),
							dbLoggingProperties.getdataStoreTableName());
					monitorWriterTask[i].setTheadNum(i);

					this.monitorWriterPool.execute(monitorWriterTask[i]);
				}
			}
		}
*/
		AppThead_log.info( connectionProperties.toString() );
		//taskPollProperties.settotalNumTasks("10");
		//AppThead_log.info( taskPollProperties.toString() );

		//String propLongRetryCount = connectionProperties.getlongRetryCount();
		//if (propLongRetryCount == null) propLongRetryCount = "12";
		//String propShortRetryCount = connectionProperties.getshortRetryCount();
		//if (propShortRetryCount == null) propShortRetryCount = "3";

		//String propLongRetryInterval = connectionProperties.getlongRetryInterval();
		//if (propLongRetryInterval == null) propLongRetryInterval = "600";
		//String propShortRetryInterval = connectionProperties.getshortRetryInterval();
		//if (propShortRetryInterval == null) propShortRetryInterval = "30";

		int ShortRetryCount = Integer.parseInt( connectionProperties.getshortRetryCount() );
		int LongRetryCount = Integer.parseInt( connectionProperties.getlongRetryCount()  );
		int ShortRetryInterval = Integer.parseInt( connectionProperties.getshortRetryInterval() );
		int LongRetryInterval = Integer.parseInt( connectionProperties.getlongRetryInterval() );
		int WaitTimeBetweenScan = Integer.parseInt( connectionProperties.getwaitTimeScan() );
		int NumMessageInScan = Integer.parseInt( connectionProperties.getnumMessageInScan() );
		int ApiRestWaitTime = Integer.parseInt( connectionProperties.getapiRestWaitTime() );
		int CuberNumId = Integer.parseInt( connectionProperties.getcuberNumId() );

		int FirstInfoStreamId = 101;
		if ( connectionProperties.getfirstInfoStreamId() != null)
			FirstInfoStreamId = Integer.parseInt( connectionProperties.getfirstInfoStreamId() );
		String psqlFunctionRun = connectionProperties.getpsqlFunctionRun();
		String HrmsSchema =  connectionProperties.gethrmsDbSchema() ;
		ApplicationProperties.ExtSysSchema = connectionProperties.getextsysDbSchema();
		ApplicationProperties.ExtSysDbLogin = connectionProperties.getextsysDbLogin();
		ApplicationProperties.ExtSysPoint = connectionProperties.getextsysPoint();
		ApplicationProperties.CuberNumId = connectionProperties.getcuberNumId();

		// Установаливем "техническое соединение" , что бы считать конфигурацию из БД в public static HashMap'Z
		java.sql.Connection Target_Connection = DataAccess.make_DataBase_Connection( HrmsSchema, connectionProperties.gethrmsPoint(),
				connectionProperties.gethrmsDbLogin(),
				connectionProperties.gethrmsDbPasswd(),
				AppThead_log
		);

		if ( Target_Connection == null)
		{
			taskExecutor.shutdown();
			// this.monitorWriterPool.shutdown(); // -- monitorWriter для Графаны больше не используется , комментарим
			MQbroker.stop();
			NotifyByChannel.Telegram_sendMessage( "Do stopping " + ApplicationName + " -" + connectionProperties.getfirstInfoStreamId() + " *нет связи с БД*  ip:" + InetAddress.getLocalHost().getHostAddress()+
													", db `" + connectionProperties.gethrmsPoint() + "` as `"+ connectionProperties.gethrmsDbLogin() + "`), *stopping*", AppThead_log );
			System.exit(-22);
		}

		try {
			ApplicationProperties.extSystemDataSource = ExtSystemDataAccess.HiDataSource (connectionProperties.getextsysPoint(),
					connectionProperties.getextsysDbLogin(),
					connectionProperties.getextsysDbPasswd()
			);
			ApplicationProperties.extSystemDataSourcePoolMetadata = ExtSystemDataAccess.DataSourcePoolMetadata;
		} catch (Exception e) {
			AppThead_log.error("НЕ удалось подключится к базе данных внешней системы:" + e.getMessage());
			NotifyByChannel.Telegram_sendMessage( "Do stopping " + ApplicationName + " -" + connectionProperties.getfirstInfoStreamId() + " *extDB problem* `" +  e.getMessage() +  "` ip:" + InetAddress.getLocalHost().getHostAddress()+
												", db `" + connectionProperties.getextsysPoint() + "` as `"+ connectionProperties.getextsysDbLogin() + "`), *stopping*", AppThead_log );
			System.exit(-20);
		}

		AppThead_log.info("extSystem DataSource = " + ApplicationProperties.extSystemDataSource );
		if ( ApplicationProperties.extSystemDataSource != null )
		{
			AppThead_log.info("extSystem DataSource = " + ApplicationProperties.extSystemDataSource
					+ " JdbcUrl:" + ApplicationProperties.extSystemDataSource.getJdbcUrl()
					+ " isRunning:" + ApplicationProperties.extSystemDataSource.isRunning()
					+ " 4 dbSchema:" + ApplicationProperties.ExtSysSchema);
		} else {
			AppThead_log.error("НЕ удалось подключится к базе данных внешней системы: останавливаем" );
			NotifyByChannel.Telegram_sendMessage( "Do stopping " + ApplicationName + " -" + connectionProperties.getfirstInfoStreamId() + " *extDB problem*  ip:" + InetAddress.getLocalHost().getHostAddress()+
					", db `" + connectionProperties.getextsysPoint() + "` as `"+ connectionProperties.getextsysDbLogin() + "`), *stopping*", AppThead_log );
			System.exit(-20);
		}

		// Зачитываем MessageDirection

		int num_of_Systems = InitMessageRepository.SelectMsgDirections(ShortRetryCount, ShortRetryInterval, LongRetryCount, LongRetryInterval,
				AppThead_log );
		if ( num_of_Systems < 1 )
		{
			taskExecutor.shutdown();
			// this.monitorWriterPool.shutdown(); // -- monitorWriter для Графаны больше не используется , комментарим
			MQbroker.stop();
			NotifyByChannel.Telegram_sendMessage( "Shutdown " + ApplicationName + " -" + connectionProperties.getfirstInfoStreamId() + " on "
					+ InetAddress.getLocalHost().getHostName()+ " (ip `" +InetAddress.getLocalHost().getHostAddress() + "`, db `" + propJDBC
					+ "` as `"+ connectionProperties.gethrmsDbLogin() + "`), *нет доступа к репозиторию* , не получен список систем для взаимодействия", AppThead_log );
					//+ InetAddress.getLocalHost().getHostAddress() + " , *нет связи с БД*", AppThead_log );
			System.exit(-22);
		}
		//AppThead_log.info("keysAllMessageDirections: " + MessageDirections.AllMessageDirections.get(1).getMsgDirection_Desc() );
		// Устанвливаем очереди для каждой из СисТЕМ!
		for (i=0; i< MessageDirections.AllMessageDirections.size(); i++)
		{
			if (( MessageDirections.AllMessageDirections.get(i).getType_Connect() == 3 ) ||
					( MessageDirections.AllMessageDirections.get(i).getType_Connect() == 4 ) )
				// non-Persistent очереди для каждой из Soap-XmlHttp систем.
				activeMQService.NewQueue("Q."+ MessageDirections.AllMessageDirections.get(i).getMsgDirection_Cod() + ".IN");

		}
		// Получаем JMSQueueConnection для последующей передачи в
		Connection JMSQueueConnection= activeMQService.StartMQbroker();

		int TotalNumTasks;
		TotalNumTasks= Integer.parseInt( connectionProperties.gettotalNumTasks() );
		Long TotalTimeTasks = Long.parseLong( connectionProperties.gettotalTimeTasks());
		Long intervalReInit = Long.parseLong( connectionProperties.getintervalReInit());


		InitMessageRepository.SelectMsgTypes( AppThead_log );
		//AppThead_log.info("keysAllMessageDirections: " + MessageType.AllMessageType.get(1).getMsg_TypeDesc() );

		InitMessageRepository.SelectMsgTemplates( AppThead_log );
		//AppThead_log.info("keysAllMessageTemplates: " + MessageTemplate.AllMessageTemplate.get(12).getTemplate_name() );

		// Запуск пула потоков под scheduler

		int TotalScheduledTask = MessageRepositoryHelper.countMessageType_4_Scheduled("-CRON_!_DAEMON-");
		if (TotalScheduledTask > 0 )  // количество типов сообщений, запускаемых по расписанию == 0 значит, выключен
		{
		externSystemCallTask_Init( TotalScheduledTask );
		Integer WaitTimeBetweenWrite; /// Integer.parseInt(dbLoggingProperties.getwaitTimeScan());
		String MessageType_4_Scheduled;
		externSystemCallPool.setThreadGroupName("externSystemCal");

			ExternSystemCallTask[] externSystemCallTask = new ExternSystemCallTask[TotalScheduledTask];
			for (i = 0; i < TotalScheduledTask; i++) {

				MessageType_4_Scheduled = MessageRepositoryHelper.getMessageType_4_Scheduled( i,"-CRON_!_DAEMON-");
				if ( MessageType_4_Scheduled != null ) {
					externSystemCallTask[i] = new ExternSystemCallTask();
					externSystemCallTask[i].setMessageType_4_Scheduled( MessageType_4_Scheduled);
					//monitorWriterTask[ i ].setContext(  MntrContext );
					WaitTimeBetweenWrite = MessageRepositoryHelper.getMax_Retry_Time_by_MesssageType( MessageType_4_Scheduled, AppThead_log );
					if ( WaitTimeBetweenWrite != null )
					    externSystemCallTask[i].setWaitTimeBetweenScan(WaitTimeBetweenWrite);
					else
						externSystemCallTask[i].setWaitTimeBetweenScan(120);
					externSystemCallTask[ i ].setHrmsSchema( connectionProperties.gethrmsDbSchema());
					externSystemCallTask[ i ].setHrmsPoint( connectionProperties.gethrmsPoint());
					externSystemCallTask[ i ].setHrmsDbLogin( connectionProperties.gethrmsDbLogin());
					externSystemCallTask[ i ].setHrmsDbPasswd( connectionProperties.gethrmsDbPasswd());

					externSystemCallTask[i].setTheadNum(i);

					externSystemCallPool.execute(externSystemCallTask[i]);
				}
			}
		}
		else {
			AppThead_log.info("countMessageType_4_Scheduled(`-CRON_DAEMON-`) == 0,  Количество типов сообщений, запускаемых по расписанию == 0 значит, выключен");
			externSystemCallPool = null;
		}

		// int totalTasks = Integer.parseInt( "1" ); // TotalNumTasks; //Integer.parseInt( "50" ); //
		Long CurrentTime;
		CurrentTime = DataAccess.getCurrentTime( AppThead_log );
		DataAccess.InitDate.setTime( CurrentTime );
		AppThead_log.info(" New InitDate=" +  DataAccess.dateFormat.format( DataAccess.InitDate ) );


		MessageSendTask[] messageSendTask = new MessageSendTask[ TotalNumTasks ];
		for (i=0; i< TotalNumTasks; i++) {
			messageSendTask[ i ] = new MessageSendTask( );// (MessageSendTask) context.getBean("MessageSendTask");
			messageSendTask[ i ].setContext(  context );
			messageSendTask[ i ].setHrmsSchema( connectionProperties.gethrmsDbSchema());
			messageSendTask[ i ].setHrmsPoint( connectionProperties.gethrmsPoint());
			messageSendTask[ i ].setHrmsDbLogin( connectionProperties.gethrmsDbLogin());
			messageSendTask[ i ].setHrmsDbPasswd( connectionProperties.gethrmsDbPasswd());
			messageSendTask[ i ].setTotalTimeTasks( TotalTimeTasks );
			messageSendTask[ i ].setWaitTimeBetweenScan( WaitTimeBetweenScan );
			messageSendTask[ i ].setNumMessageInScan( NumMessageInScan );
			messageSendTask[ i ].setApiRestWaitTime( ApiRestWaitTime );
			messageSendTask[ i ].setFirstInfoStreamId( FirstInfoStreamId );
			messageSendTask[ i ].setCuberNumId( CuberNumId );
			messageSendTask[ i ].setTheadNum(i);
			messageSendTask[ i ].setJMSQueueConnection( JMSQueueConnection );
			taskExecutor.execute(messageSendTask[ i ]);
		}
		int count = 0;


		String CurrentTimeString;
		long timeToReInit;

		for (;;) {
			count = taskExecutor.getActiveCount();
			AppThead_log.info("Active Threads : " + count);
			try {

				// Thread.sleep(25000);
				Thread.sleep(20000);
				CurrentTime = DataAccess.getCurrentTime(AppThead_log);
				if ( CurrentTime != null )
				{
					Runtime r = Runtime.getRuntime();
					long freeMemory = r.maxMemory() - r.totalMemory() + r.freeMemory();
					AppThead_log.info(" `free memory`( heapSize=" + r.totalMemory() + ", heapFreeSize="+ r.freeMemory() + ") of a Java process before GC is : " + freeMemory );
					Runtime.getRuntime().gc();
					Thread.sleep(1000);
					freeMemory = r.maxMemory() - r.totalMemory() + r.freeMemory();
					AppThead_log.info(" `free memory`( heapSize=" + r.totalMemory() + ", heapFreeSize="+ r.freeMemory() + ") of a Java process after GC is : " + freeMemory );

					if ( count != TotalNumTasks )
						NotifyByChannel.Telegram_sendMessage( "*Количество потоков=*" + count +" !=" + TotalNumTasks +" у " + ApplicationName + " -" + connectionProperties.getfirstInfoStreamId() + " on "
								+ InetAddress.getLocalHost().getHostName()+ " (ip `" +InetAddress.getLocalHost().getHostAddress() + "`, db `" + propJDBC+ "`)", AppThead_log );
								//InetAddress.getLocalHost().getHostAddress(), AppThead_log );

					CurrentTimeString = DataAccess.getCurrentTimeString(AppThead_log);

					timeToReInit = (CurrentTime - DataAccess.InitDate.getTime()) / 1000;
					if ( timeToReInit > intervalReInit ) {
						AppThead_log.info("CurrentTimeString=" + CurrentTimeString + " (CurrentTime - DataAccess.InitDate.getTime())/1000: " + timeToReInit);
						// Перечитывать перечень систем нужно для обновления параментров, динамически используемых при обращении к веншним системам
						InitMessageRepository.ReReadMsgDirections( intervalReInit, AppThead_log);
						InitMessageRepository.ReReadMsgTypes(intervalReInit, AppThead_log);
						InitMessageRepository.ReReadMsgTemplates(intervalReInit, AppThead_log);
						DataAccess.InitDate.setTime(CurrentTime);
						AppThead_log.info(" New InitDate=" + DataAccess.dateFormat.format(DataAccess.InitDate));

						// если в
						try {
							if ((psqlFunctionRun != null) && (!psqlFunctionRun.equalsIgnoreCase("NONE")))
								DataAccess.doPSQL_Function_Run(psqlFunctionRun, AppThead_log);
						} catch ( SQLException e) {
							AppThead_log.error("проблема при вызове" + psqlFunctionRun + ": " + e.getMessage());
							System.err.println("AppThead_log: проблема при вызове" + psqlFunctionRun + ": " + e.getMessage());
							e.printStackTrace();
							break;
						}
					}
				}
				else
					break;
			} catch (InterruptedException | SQLException e) {
				AppThead_log.error("надо taskExecutor.shutdown! " + e.getMessage());
				e.printStackTrace();
				count = 0; // надо taskExecutor.shutdown();
				break;
			}
			if (count == 0) {
				/// taskExecutor.shutdown();
				break;
			}
		}
		taskExecutor.shutdown();
		// monitorWriterPool.shutdown(); -- monitorWriter для Графаны больше не используется , комментарим
		 NotifyByChannel.Telegram_sendMessage( "Shutdown " + ApplicationName + " -" + connectionProperties.getfirstInfoStreamId() + " on "
				 + InetAddress.getLocalHost().getHostName()+ " (ip `" +InetAddress.getLocalHost().getHostAddress() + "`, db `" + propJDBC
				 + "` as `"+ connectionProperties.gethrmsDbLogin() + "` extDb `"
				 + propExtJDBC+ "` as `"+ connectionProperties.getextsysDbLogin()  + "` ) , *exit!*", AppThead_log );
		System.exit(-22);
		//return;
	}

	/* -- monitorWriter для Графаны больше не используется , комментарим
	private void monitorWriter() {
		monitorWriterPool = new ThreadPoolTaskExecutor();
		monitorWriterPool.initialize();
//        pool.setCorePoolSize(taskPollProperties.getcorePoolSize());
//        pool.setMaxPoolSize(taskPollProperties.getmaxPoolSize());
		monitorWriterPool.setCorePoolSize(23);
		monitorWriterPool.setMaxPoolSize(24);
		monitorWriterPool.setWaitForTasksToCompleteOnShutdown(true);
		monitorWriterPool.setThreadNamePrefix("Monitor-");
		AppThead_log.info("ThreadPoolTaskExecutor for monitorWriter prepared: CorePoolSize(203), MaxPoolSize(204); ");
	}
	*/
 private  void  externSystemCallTask_Init ( int numberMessageType_4_Scheduled ) {
	 externSystemCallPool = new ThreadPoolTaskExecutor();
	 externSystemCallPool.initialize();
	 externSystemCallPool.setCorePoolSize(numberMessageType_4_Scheduled);
	 externSystemCallPool.setMaxPoolSize(numberMessageType_4_Scheduled + 1 );
	 externSystemCallPool.setWaitForTasksToCompleteOnShutdown(true);
	 externSystemCallPool.setThreadNamePrefix("CronD-");
	 AppThead_log.info("ThreadPoolTaskExecutor for externSystemCall prepared: CorePoolSize("+ numberMessageType_4_Scheduled + "), MaxPoolSize(" + (numberMessageType_4_Scheduled+1) + "); ");
 }

}
