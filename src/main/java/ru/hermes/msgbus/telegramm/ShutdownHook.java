package ru.hermes.msgbus.telegramm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.ComponentScan;
//import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.net.UnknownHostException;

//@Configuration
//@ComponentScan(basePackages = "ru.hermes.msgbus.*")
@Component

public class ShutdownHook {
    private static final Logger ShutdownHook_log = LoggerFactory.getLogger(ru.hermes.msgbus.telegramm.ShutdownHook.class);
    @PreDestroy
//    @Bean(name = "onExit")
    public void onExit() {
        ShutdownHook_log.info("###STOPing###");

        try {
            ru.hermes.msgbus.telegramm.NotifyByChannel.Telegram_sendMessage( "*Shutdown* Sender Applicationon " + InetAddress.getLocalHost().getHostAddress() + " , *exit!*", ShutdownHook_log );
            ShutdownHook_log.warn("Как бы типа => *Shutdown* Sender Applicationon " + InetAddress.getLocalHost().getHostAddress() + " , *exit!*" );
            // Thread.sleep(1 * 1000); InterruptedException |
        } catch ( UnknownHostException e) {
            ShutdownHook_log.error(" хрякнулось InetAddress.getLocalHost().getHostAddress()", e);;
        }
        ShutdownHook_log.info("###STOP FROM THE LIFECYCLE###");

    }
}
