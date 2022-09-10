package ru.hermes.msgbus;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;

import org.springframework.boot.CommandLineRunner;
import ru.hermes.msgbus.config.*;
import ru.hermes.msgbus.model.MessageDirections;
import ru.hermes.msgbus.monitoring.МonitoringWriterTask;
import ru.hermes.msgbus.monitoring.ConcurrentQueue;
//import ru.hermes.msgbus.monitoring.MonitoringConfig;
import ru.hermes.msgbus.mq.ActiveMQService;
//import ru.hermes.msgbus.telegramm.NotifyByChannel;
//import ru.hermes.msgbus.telegramm.ShutdownHook;
import ru.hermes.msgbus.threads.MessageSendTask;
import ru.hermes.msgbus.init.InitMessageRepository;

import ru.hermes.msgbus.common.DataAccess;

import java.net.InetAddress;

import java.sql.SQLException;
import java.util.Properties;
import org.apache.activemq.broker.BrokerService;

import javax.jms.Connection;

@EnableScheduling
@SpringBootApplication (scanBasePackages = "ru.hermes.msgbus.*")


public class SenderApplication implements CommandLineRunner {

	public static final Logger AppThead_log = LoggerFactory.getLogger(ru.hermes.msgbus.SenderApplication.class);
	static ThreadPoolTaskExecutor monitorWriterPool;

	@Autowired
	public ConnectionProperties connectionProperties;
	@Autowired
	public DBLoggingProperties dbLoggingProperties;
	@Autowired
	public  TaskPollProperties taskPollProperties ;
	@Autowired
	public TelegramProperties telegramProperties;


	public static void main(String[] args) {
		SpringApplication.run(SenderApplication.class, args);
	}

	public void run(String... strings) throws Exception {
		int i;
		boolean isNotifyOk;
		ApplicationContext context = new AnnotationConfigApplicationContext(Sender_AppConfig.class);

		AppThead_log.info("Hellow for SenderApplication ");
		ru.hermes.msgbus.telegramm.NotifyByChannel.Telegram_setChatBotUrl( telegramProperties.getchatBotUrl() , AppThead_log );
		 ru.hermes.msgbus.telegramm.NotifyByChannel.Telegram_sendMessage( "*Starting* Sender Application on " + InetAddress.getLocalHost().getHostAddress(), AppThead_log );
		String propConnectMsgBus = connectionProperties.getconnectMsgBus();
		if ( propConnectMsgBus == null) propConnectMsgBus = "tcp://localhost:61216";

		ActiveMQService activeMQService= new ActiveMQService();
		BrokerService MQbroker= activeMQService.ActiveMQbroker( propConnectMsgBus );
		// MQbroker.start(); // Сделаем это после ВСЕ Очередей!

		AppThead_log.info("ActiveMQbroker " + MQbroker.getBrokerName() + " started" );
		Properties props = System.getProperties();
		props.setProperty("com.sun.net.ssl.checkRevocation","false");

		ThreadPoolTaskExecutor taskExecutor = (ThreadPoolTaskExecutor) context.getBean("taskExecutor");
		this.monitorWriter();

		AppThead_log.warn( dbLoggingProperties.toString() );
		// МonitoringWriterTask.setMonitoringDbParam()
		if ( dbLoggingProperties.gettotalNumTasks() != null) {
			int TotalWriterTask = Integer.parseInt(dbLoggingProperties.gettotalNumTasks());
			Integer WaitTimeBetweenWrite = Integer.parseInt(dbLoggingProperties.getwaitTimeScan());

			ConcurrentQueue.Init(Integer.parseInt(dbLoggingProperties.getqueueCopasity()));
			this.monitorWriterPool.setThreadGroupName("monitorWriter");

			if (TotalWriterTask > 0 )  // dbLoggingProperties.gettotalNumTasks() == 0 значит, выключен
			{

				МonitoringWriterTask[] monitorWriterTask = new МonitoringWriterTask[TotalWriterTask];
				for (i = 0; i < TotalWriterTask; i++) {
					monitorWriterTask[i] = new МonitoringWriterTask();// (MessageSendTask) context.getBean("MessageSendTask");
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

		AppThead_log.info( connectionProperties.toString() );
		//taskPollProperties.settotalNumTasks("10");
		//AppThead_log.info( taskPollProperties.toString() );

		String propLongRetryCount = connectionProperties.getlongRetryCount();
		if (propLongRetryCount == null) propLongRetryCount = "12";
		String propShortRetryCount = connectionProperties.getshortRetryCount();
		if (propShortRetryCount == null) propShortRetryCount = "3";

		String propLongRetryInterval = connectionProperties.getlongRetryInterval();
		if (propLongRetryInterval == null) propLongRetryInterval = "600";
		String propShortRetryInterval = connectionProperties.getshortRetryInterval();
		if (propShortRetryInterval == null) propShortRetryInterval = "30";

		int ShortRetryCount = Integer.parseInt( connectionProperties.getshortRetryCount() );
		int LongRetryCount = Integer.parseInt( connectionProperties.getlongRetryCount()  );
		int ShortRetryInterval = Integer.parseInt( connectionProperties.getshortRetryInterval() );
		int LongRetryInterval = Integer.parseInt( connectionProperties.getlongRetryInterval() );
		int WaitTimeBetweenScan = Integer.parseInt( connectionProperties.getwaitTimeScan() );
		int NumMessageInScan = Integer.parseInt( connectionProperties.getnumMessageInScan() );
		int ApiRestWaitTime = Integer.parseInt( connectionProperties.getapiRestWaitTime() );
		int FirstInfoStreamId = 101;
		if ( connectionProperties.getfirstInfoStreamId() != null)
			FirstInfoStreamId = Integer.parseInt( connectionProperties.getfirstInfoStreamId() );
		String psqlFunctionRun = connectionProperties.getpsqlFunctionRun();


		// Установаливем "техническое соединение" , что бы считать конфигурацию из БД в public static HashMap'Z
		java.sql.Connection Target_Connection = DataAccess.make_Hermes_Connection(  connectionProperties.gethrmsPoint(),
				connectionProperties.gethrmsDbLogin(),
				connectionProperties.gethrmsDbPasswd(),
				AppThead_log
		);

		if ( Target_Connection == null)
		{
			taskExecutor.shutdown();
			this.monitorWriterPool.shutdown();
			MQbroker.stop();
			ru.hermes.msgbus.telegramm.NotifyByChannel.Telegram_sendMessage( "*Shutdown* Sender Application on " + InetAddress.getLocalHost().getHostAddress() + " , *нет связи с БД*", AppThead_log );
			System.exit(-22);
		}
		// Зачитываем MessageDirection


		InitMessageRepository.SelectMsgDirections(ShortRetryCount, ShortRetryInterval, LongRetryCount, LongRetryInterval,
				AppThead_log );
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

		// int totalTasks = Integer.parseInt( "1" ); // TotalNumTasks; //Integer.parseInt( "50" ); //
		Long CurrentTime;
		CurrentTime = DataAccess.getCurrentTime( AppThead_log );
		DataAccess.InitDate.setTime( CurrentTime );
		AppThead_log.info(" New InitDate=" +  DataAccess.dateFormat.format( DataAccess.InitDate ) );



		MessageSendTask[] messageSendTask = new MessageSendTask[ TotalNumTasks ];
		for (i=0; i< TotalNumTasks; i++) {
			messageSendTask[ i ] = new MessageSendTask( );// (MessageSendTask) context.getBean("MessageSendTask");
			messageSendTask[ i ].setContext(  context );
			messageSendTask[ i ].setHrmsPoint(  connectionProperties.gethrmsPoint() );
			messageSendTask[ i ].setHrmsDbLogin( connectionProperties.gethrmsDbLogin());
			messageSendTask[ i ].setHrmsDbPasswd( connectionProperties.gethrmsDbPasswd());
			messageSendTask[ i ].setTotalTimeTasks( TotalTimeTasks );
			messageSendTask[ i ].setWaitTimeBetweenScan( WaitTimeBetweenScan );
			messageSendTask[ i ].setNumMessageInScan( NumMessageInScan );
			messageSendTask[ i ].setApiRestWaitTime( ApiRestWaitTime );
			messageSendTask[ i ].setFirstInfoStreamId( FirstInfoStreamId );
			messageSendTask[ i ].setTheadNum(i);
			messageSendTask[ i ].setJMSQueueConnection( JMSQueueConnection );
			taskExecutor.execute(messageSendTask[ i ]);
		}
		Integer count = 0;


		String CurrentTimeString;
		Long timeToReInit;

		for (;;) {
			count = taskExecutor.getActiveCount();
			AppThead_log.info("Active Threads : " + count);
			try {

				// Thread.sleep(25000);
				Thread.sleep(10000);
				CurrentTime = DataAccess.getCurrentTime(AppThead_log);
				if ( CurrentTime != null )
				{
					Runtime r = Runtime.getRuntime();
					long freeMemory = r.maxMemory() - r.totalMemory() + r.freeMemory();
					AppThead_log.info(" \"free memory\" of a Java process before GC is : " + freeMemory );
					Runtime.getRuntime().gc();
					freeMemory = r.maxMemory() - r.totalMemory() + r.freeMemory();
					AppThead_log.info(" \"free memory\" of a Java process after GC is : " + freeMemory );

					if ( count != TotalNumTasks )
						ru.hermes.msgbus.telegramm.NotifyByChannel.Telegram_sendMessage( "*Количество потоков=*" + count +" !=" + TotalNumTasks +" у Sender Application on " + InetAddress.getLocalHost().getHostAddress(), AppThead_log );
					;

					CurrentTimeString = DataAccess.getCurrentTimeString(AppThead_log);

					timeToReInit = (CurrentTime - DataAccess.InitDate.getTime()) / 1000;
					if ( timeToReInit > intervalReInit ) {
						AppThead_log.info("CurrentTimeString=" + CurrentTimeString + " (CurrentTime - DataAccess.InitDate.getTime())/1000: " + timeToReInit.toString());
						InitMessageRepository.ReReadMsgDirections(CurrentTime, ShortRetryCount, ShortRetryInterval, LongRetryCount, LongRetryInterval, AppThead_log);
						InitMessageRepository.ReReadMsgTypes(AppThead_log);
						InitMessageRepository.ReReadMsgTemplates(AppThead_log);
						DataAccess.InitDate.setTime(CurrentTime);
						AppThead_log.info(" New InitDate=" + DataAccess.dateFormat.format(DataAccess.InitDate));

						// если в
						if ( (psqlFunctionRun != null) && (!psqlFunctionRun.equalsIgnoreCase("NONE")) )
							DataAccess.moveERROUT2RESOUT(psqlFunctionRun, AppThead_log);
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
		this.monitorWriterPool.shutdown();
		 ru.hermes.msgbus.telegramm.NotifyByChannel.Telegram_sendMessage( "*Shutdown* Sender Applicationon " + InetAddress.getLocalHost().getHostAddress() + " , *exit!*", AppThead_log );
		System.exit(-22);
		return;
	}

	private void monitorWriter() {
		this.monitorWriterPool = new ThreadPoolTaskExecutor();
		this.monitorWriterPool.initialize();
//        pool.setCorePoolSize(taskPollProperties.getcorePoolSize());
//        pool.setMaxPoolSize(taskPollProperties.getmaxPoolSize());
		this.monitorWriterPool.setCorePoolSize(203);
		this.monitorWriterPool.setMaxPoolSize(204);
		this.monitorWriterPool.setWaitForTasksToCompleteOnShutdown(true);
		this.monitorWriterPool.setThreadNamePrefix("Monitor-");
		AppThead_log.info("ThreadPoolTaskExecutor for monitorWriter prepared: CorePoolSize(203), MaxPoolSize(204); ");
	}
}
