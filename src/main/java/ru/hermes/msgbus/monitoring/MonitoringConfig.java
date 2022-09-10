package ru.hermes.msgbus.monitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@ComponentScan(basePackages = "ru.hermes.msgbus.*")
public class MonitoringConfig {

    private static final Logger MntrConfig_log = LoggerFactory.getLogger(ru.hermes.msgbus.monitoring.MonitoringConfig.class);
/*

    @Bean(name = "monitorWriter")
    public ThreadPoolTaskExecutor monitorWriter() {
        ThreadPoolTaskExecutor pool = new ThreadPoolTaskExecutor();
//        pool.setCorePoolSize(taskPollProperties.getcorePoolSize());
//        pool.setMaxPoolSize(taskPollProperties.getmaxPoolSize());
        pool.setCorePoolSize(203);
        pool.setMaxPoolSize(204);
        pool.setWaitForTasksToCompleteOnShutdown(true);
        MntrConfig_log.info("ThreadPoolTaskExecutor for monitorWriter prepared: CorePoolSize(203), MaxPoolSize(204); ");
        return pool;
    }
    */
}
