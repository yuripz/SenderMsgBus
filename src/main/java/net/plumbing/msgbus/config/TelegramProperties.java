package net.plumbing.msgbus.config;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
@Component
@ConfigurationProperties(prefix ="telegramm" )
public class TelegramProperties {
    private String chatBotUrl;

    public String getchatBotUrl() {
        return chatBotUrl;
    }

    public void setchatBotUrl(String chatBotUrl) {
        this.chatBotUrl = chatBotUrl;
    }
}
