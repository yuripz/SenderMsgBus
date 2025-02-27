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

    private String httpProxyHost;
    public String gethttpProxyHost() {
        return httpProxyHost;
    }
    public void sethttpProxyHost(String httpProxyHost) {
        this.httpProxyHost = httpProxyHost;
    }

    private String httpProxyPort;
    public String gethttpProxyPort() {
        return httpProxyPort;
    }
    public void sethttpProxyPort(String httpProxyPort) {
        this.httpProxyPort = httpProxyPort;
    }
}
