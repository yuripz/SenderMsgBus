package net.plumbing.msgbus.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
//import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;


@Configuration
@ComponentScan(basePackages = "ru.hermes.msgbus.*")
public class Sender_AppConfig {

    private static final Logger AppConfig_log = LoggerFactory.getLogger(Sender_AppConfig.class);

    //    @Autowired
//    private  TaskPollProperties taskPollProperties ;

    @Bean(name = "taskExecutor")
    public ThreadPoolTaskExecutor taskExecutor() {

        ThreadPoolTaskExecutor pool = new ThreadPoolTaskExecutor();
//        pool.setCorePoolSize(taskPollProperties.getcorePoolSize());
//        pool.setMaxPoolSize(taskPollProperties.getmaxPoolSize());
        pool.setCorePoolSize(500);
        pool.setMaxPoolSize(503);
        pool.setWaitForTasksToCompleteOnShutdown(true);
        pool.setThreadNamePrefix("Sender-");
        AppConfig_log.info( "taskExecutor: getThreadNamePrefix:" + pool.getThreadNamePrefix() );

        AppConfig_log.info("ThreadPoolTaskExecutor for taskExecutor prepared: CorePoolSize(500), MaxPoolSize(503); ");
        return pool;
    }

}
