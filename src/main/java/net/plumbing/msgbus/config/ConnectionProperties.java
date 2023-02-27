package net.plumbing.msgbus.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;


//@Validated
// @SuppressWarnings({"unused", "WeakerAccess"})
@Component
@ConfigurationProperties(prefix = "hermes")

public class ConnectionProperties {

    // @Value("${totalNumTasks")
    private String cuberNumId;
    public String getcuberNumId() {
        return cuberNumId;
    }
    public void setcuberNumId(String cuberNumId) {
        this.cuberNumId = cuberNumId;
    }

 // extSystem
    private String extsysDbSchema;
    public String getextsysDbSchema() {
        return extsysDbSchema;
    }
    public void setextsysDbSchema(String extsysDbSchema) {this.extsysDbSchema = extsysDbSchema;}
    //   @Value("${extsysDbLogin")
    private String extsysDbLogin;
    public String getextsysDbLogin() {
        return extsysDbLogin;
    }
    public void setextsysDbLogin(String extsysDbLogin) {
        this.extsysDbLogin = extsysDbLogin;
    }

    //    @Value("${extsysDbPasswd")
    private String extsysDbPasswd;
    public String getextsysDbPasswd() {
        return extsysDbPasswd;
    }
    public void setextsysDbPasswd(String extsysDbPasswd) {
        this.extsysDbPasswd = extsysDbPasswd;
    }

    //    @Value("${extsysPoint")
    private String extsysPoint;
    public String getextsysPoint() {
        return extsysPoint;
    }
    public void setextsysPoint(String extsysPoint) {
        this.extsysPoint = extsysPoint;
    }


    private String hrmsDbSchema;
    public String gethrmsDbSchema() {
        return hrmsDbSchema;
    }
    public void sethrmsDbSchema(String hrmsDbSchema) {this.hrmsDbSchema = hrmsDbSchema;}
 //   @Value("${hrmsDbLogin")
    private String hrmsDbLogin;
    public String gethrmsDbLogin() {
        return hrmsDbLogin;
    }
    public void sethrmsDbLogin(String hrmsDbLogin) {
        this.hrmsDbLogin = hrmsDbLogin;
    }

//    @Value("${hrmsDbPasswd")
    private String hrmsDbPasswd;
    public String gethrmsDbPasswd() {
        return hrmsDbPasswd;
    }
    public void sethrmsDbPasswd(String hrmsDbPasswd) {
        this.hrmsDbPasswd = hrmsDbPasswd;
    }

//    @Value("${hrmsPoint")
    private String hrmsPoint;
    public String gethrmsPoint() {
        return hrmsPoint;
    }
    public void sethrmsPoint(String hrmsPoint) {
        this.hrmsPoint = hrmsPoint;
    }

 //   @Value("$shortRetryCount")
    private String shortRetryCount;
    public String getshortRetryCount() {
        return shortRetryCount;
    }
    public void setshortRetryCount(String shortRetryCount) {
        this.shortRetryCount = shortRetryCount;
    }
    //    @Value("$shortRetryInterval")
    private String shortRetryInterval;
    public String getshortRetryInterval() {
        return shortRetryInterval;
    }
    public void setshortRetryInterval(String shortRetryInterval) {
        this.shortRetryInterval = shortRetryInterval;
    }

   // @Value("${longRetryCount")
    private String longRetryCount;
    public String getlongRetryCount() {
        return longRetryCount;
    }
    public void setlongRetryCount(String longRetryCount) {
        this.longRetryCount = longRetryCount;
    }

   // @Value("${longRetryInterval")
    private String longRetryInterval;
    public String getlongRetryInterval() {
        return longRetryInterval;
    }
    public void setlongRetryInterval(String longRetryInterval) {
        this.longRetryInterval = longRetryInterval;
    }

    // @Value("${totalNumTasks")
    private String totalNumTasks;
    public String gettotalNumTasks() {
        return totalNumTasks;
    }
    public void settotalNumTasks(String totalNumTasks) {
        this.totalNumTasks = totalNumTasks;
    }

    // @Value("${totalNumTasks")
    private String totalTimeTasks;
    public String gettotalTimeTasks() {
        return this.totalTimeTasks;
    }
    public void settotalTimeTasks(String totalTimeTasks) {
        this.totalTimeTasks = totalTimeTasks;
    }

    private String waitTimeScan;
    public String getwaitTimeScan() {
        return this.waitTimeScan;
    }
    public void setwaitTimeScan(String waitTimeScan) {
        this.waitTimeScan = waitTimeScan;
    }

    private String numMessageInScan;
    public String getnumMessageInScan() {
        return numMessageInScan;
    }
    public void setnumMessageInScan(String numMessageInScan) {
        this.numMessageInScan = numMessageInScan;
    }

    private String intervalReInit;
    public String getintervalReInit() {
        return intervalReInit;
    }
    public void setintervalReInit(String intervalReInit) {
        this.intervalReInit = intervalReInit;
    }

    private String firstInfoStreamId;
    public String getfirstInfoStreamId() {
        return firstInfoStreamId;
    }
    public void setfirstInfoStreamId(String firstInfoStreamId) {
        this.firstInfoStreamId = firstInfoStreamId;
    }

    private String apiRestWaitTime;
    public String getapiRestWaitTime() {
        return apiRestWaitTime;
    }
    public void setapiRestWaitTime(String apiRestWaitTime) {
        this.apiRestWaitTime = apiRestWaitTime;
    }

    private String psqlFunctionRun;
    public String getpsqlFunctionRun() {
        return psqlFunctionRun;
    }
    public void setpsqlFunctionRun(String psqlFunctionRun) {
        this.psqlFunctionRun = psqlFunctionRun;
    }
    // connectMsgBus
    private String connectMsgBus;
    public String getconnectMsgBus() {
        return connectMsgBus;
    }
    public void setconnectMsgBus(String connectMsgBus) {
        this.connectMsgBus = connectMsgBus;
    }

    @Override
    public String toString() {
        return "ConnectionProperties{" +
                "hrmsPoint='" + hrmsPoint + '\'' +
                ", hrmsPoint='" + hrmsPoint + '\'' +
                '}' + "\n" +
                "longRetryCount=" + longRetryCount +", longRetryInterval=" + longRetryInterval
                + "\n" +
                "shortRetryCount=" + shortRetryCount +", shortRetryInterval=" + shortRetryInterval
                + "\n" +
                "totalNumTasks=" + totalNumTasks +", totalTimeTasks=" + totalTimeTasks
                + "\n" +
                "numMessageInScan=" + numMessageInScan +", waitTimeScan=" + this.waitTimeScan
                + "\n" +
                "intervalReInit=" + intervalReInit + ", firstInfoStreamId=" +this.firstInfoStreamId + ", " + this.connectMsgBus
                ;
    }

}

