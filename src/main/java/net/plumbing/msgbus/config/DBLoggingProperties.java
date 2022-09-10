package net.plumbing.msgbus.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;


//@Validated
// @SuppressWarnings({"unused", "WeakerAccess"})
@Component
@ConfigurationProperties(prefix ="monitoring" )
public class DBLoggingProperties {
    private String mntrDbLogin;

    public String getmntrDbLogin() {
        return mntrDbLogin;
    }

    public void setmntrDbLogin(String mntrDbLogin) {
        this.mntrDbLogin = mntrDbLogin;
    }

    //    @Value("${mntrDbPasswd")
    private String mntrDbPasswd;

    public String getmntrDbPasswd() {
        return mntrDbPasswd;
    }

    public void setmntrDbPasswd(String mntrDbPasswd) {
        this.mntrDbPasswd = mntrDbPasswd;
    }

    //    @Value("${jdbcUrl")
    private String jdbcUrl;


    public String getjdbcUrl() {
        return jdbcUrl;
    }

    public void setjdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    // @Value("${totalNumTasks")
    private String totalNumTasks;
    public String gettotalNumTasks() {
        return totalNumTasks;
    }
    public void settotalNumTasks(String totalNumTasks) {
        this.totalNumTasks = totalNumTasks;
    }

    private String waitTimeScan;
    public String getwaitTimeScan() {
        return this.waitTimeScan;
    }
    public void setwaitTimeScan(String waitTimeScan) {
        this.waitTimeScan = waitTimeScan;
    }

    private String dataSourceClassName;
    public String getdataSourceClassName() {
        return this.dataSourceClassName;
    }
    public void setdataSourceClassName(String dataSourceClassName) {
        this.dataSourceClassName = dataSourceClassName;
    }

    private String queueCopasity;
    public String getqueueCopasity() {
        return queueCopasity;
    }
    public void setqueueCopasity(String queueCopasity) {
        this.queueCopasity = queueCopasity;
    }

    private String dataStoreTableName;
    public String getdataStoreTableName() {
        return this.dataStoreTableName;
    }
    public void setdataStoreTableName(String dataStoreTableName) {
        this.dataStoreTableName = dataStoreTableName;
    }

    @Override
    public String toString() {
        return "ConnectionProperties{" +
                "jdbcUrl='" + jdbcUrl + '\'' +
                ", mntrDbLogin='" + mntrDbLogin + '\'' +
                ", dataSourceClassName='" + dataSourceClassName + '\'' +
                '}'
                + "\n" +
                "queueCopasity=" + queueCopasity + "totalNumTasks=" + totalNumTasks +", waitTimeScan=" + waitTimeScan

                ;
    }

}
