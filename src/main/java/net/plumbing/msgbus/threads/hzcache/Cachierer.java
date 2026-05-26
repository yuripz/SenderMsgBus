package net.plumbing.msgbus.threads.hzcache;

import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import net.plumbing.msgbus.config.ConnectionProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public  class Cachierer {
    //public static String MESSAGE_QUEUE_MAP_NAME = "messageQueueMap";
    static ThreadPoolTaskExecutor dbReadCachiererPool;
    public static final Logger CachirerMessageDbRead_Log = LoggerFactory.getLogger(Cachierer.class);
    
    public static IMap init( int totalCachiererTaskNum , ConnectionProperties connectionProperties ) {
        dbReadCachiererPool = new ThreadPoolTaskExecutor();
        dbReadCachiererPool.initialize();
        dbReadCachiererPool.setCorePoolSize(totalCachiererTaskNum);
        dbReadCachiererPool.setMaxPoolSize(totalCachiererTaskNum + 1 );
        dbReadCachiererPool.setWaitForTasksToCompleteOnShutdown(true);
        dbReadCachiererPool.setThreadNamePrefix("CacheD-");
        CachirerMessageDbRead_Log.info("ThreadPoolTaskExecutor for dbReadCachierer prepared: CorePoolSize("+ totalCachiererTaskNum + "), MaxPoolSize(" + (totalCachiererTaskNum+1) + "); ");

        Config hzMsgSenderConfig= new Config();
        hzMsgSenderConfig.setClusterName( "msgSenderBus" );
        hzMsgSenderConfig.getJetConfig().setEnabled(false);


/*
        // Конфигурация для новой карты MESSAGE_QUEUE_MAP_NAME
        MapConfig messageQueueMapConfig = new MapConfig(MESSAGE_QUEUE_MAP_NAME);
        // Рекомендуется добавить индексы для полей, по которым будут фильтроваться данные
        messageQueueMapConfig.addIndexConfig(new IndexConfig(IndexType.SORTED, "Queue_Id")); // Добавляем индекс
        messageQueueMapConfig.addIndexConfig(new IndexConfig(IndexType.HASH, "Queue_Direction"));
        messageQueueMapConfig.addIndexConfig(new IndexConfig(IndexType.SORTED, "Msg_Date"));
        messageQueueMapConfig.addIndexConfig(new IndexConfig(IndexType.SORTED, "Msg_InfoStreamId"));
        hzMsgSenderConfig.getMapConfigs().put(MESSAGE_QUEUE_MAP_NAME, messageQueueMapConfig);
*/
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addDataSerializableFactory(HzQueueVODataSerializableFactory.FACTORY_ID, new HzQueueVODataSerializableFactory());
        hzMsgSenderConfig.setSerializationConfig(serializationConfig);
        HazelcastInstance hzInstance = Hazelcast.newHazelcastInstance( hzMsgSenderConfig );

        IMap<Long, HzQueueVO> hzMsgSenderMap = hzInstance.getMap("hzMsgSenderMap");
        // HASH для точных сравнений (equal, in)
        hzMsgSenderMap.addIndex(IndexType.HASH, "Msg_InfoStreamId");
        hzMsgSenderMap.addIndex(IndexType.HASH, "Queue_Direction");
        // SORTED для сравнений диапазонов (greaterThan, lessThan, between)
        hzMsgSenderMap.addIndex(IndexType.SORTED, "Queue_Id");
        hzMsgSenderMap.addIndex(IndexType.SORTED, "Msg_Date");

        CachirerMessageDbRead_Log.info("Индексы добавлены для полей 'Msg_InfoStreamId', 'Queue_Id'.");
        //hzMsgSenderMap.put("1","12345");
int i;
        int FirstInfoStreamId = 101;
        if ( connectionProperties.getfirstInfoStreamId() != null)
            FirstInfoStreamId = Integer.parseInt( connectionProperties.getfirstInfoStreamId() );

        HZcachirerMessageDbReadTask[] hzMessageDbReadTask = new HZcachirerMessageDbReadTask[ totalCachiererTaskNum ];
        for (i=0; i< totalCachiererTaskNum; i++) {
            hzMessageDbReadTask[ i ] = new HZcachirerMessageDbReadTask( );// (hzMessageDbReadTask) context.getBean("hzMessageDbReadTask");

            hzMessageDbReadTask[ i ].setHrmsSchema( connectionProperties.gethrmsDbSchema());
            hzMessageDbReadTask[ i ].setHrmsPoint( connectionProperties.gethrmsPoint());
            hzMessageDbReadTask[ i ].setHrmsDbLogin( connectionProperties.gethrmsDbLogin());
            hzMessageDbReadTask[ i ].setHrmsDbPasswd( connectionProperties.gethrmsDbPasswd());
            hzMessageDbReadTask[ i ].setWaitTimeBetweenScan( Integer.parseInt( connectionProperties.getwaitTimeScan() ) );
            hzMessageDbReadTask[ i ].setFirstInfoStreamId( FirstInfoStreamId );
            hzMessageDbReadTask[ i ].setTotalCachiererTaskNum( totalCachiererTaskNum );
            hzMessageDbReadTask[ i ].setHZmsgSenderMap( hzMsgSenderMap );

            hzMessageDbReadTask[ i ].setCuberNumId( Integer.parseInt( connectionProperties.getcuberNumId()) );
            hzMessageDbReadTask[ i ].setCachirerMessageDbRead_Loger( CachirerMessageDbRead_Log );
            hzMessageDbReadTask[ i ].setTheadNum(i+1);

            dbReadCachiererPool.execute(hzMessageDbReadTask[ i ]);
        }
        return hzMsgSenderMap;
    }


}
