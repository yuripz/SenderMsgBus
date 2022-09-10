package ru.hermes.msgbus.config;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;


@Validated
@SuppressWarnings({"unused", "WeakerAccess"})
@Component
@ConfigurationProperties(prefix = "security")

public class SecurityProperties {
    private String hermesLogin;
    private String hermesPassword;

    public String gethermesLogin() {
        return hermesLogin;
    }

    public void sethermesLogin(String hermesLogin) {
        this.hermesLogin = hermesLogin;
    }

    public String gethermesPassword() {
        return hermesPassword;
    }

    public void sethermesPassword(String hermesPassword) {
        this.hermesPassword = hermesPassword;
    }

    @Override
    public String toString() {
        return "SecurityProperties{" +
                "hermesPassword='" + hermesPassword + '\'' +
                ", hermesLogin='" + hermesLogin + '\'' +
                '}';
    }

}
