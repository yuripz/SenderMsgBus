package net.plumbing.msgbus.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;


@Validated
// @SuppressWarnings({"unused", "WeakerAccess"})
@Component
@ConfigurationProperties(prefix = "taskpool")
public class TaskPollProperties {
    //@Value("${corePoolSize")
    private String corePoolSize;

    public String getcorePoolSize() {
        return corePoolSize;
    }

    public void setcorePoolSize(String corePoolSize) {
        this.corePoolSize = corePoolSize;
    }

    //@Value("${maxPoolSize")
    private String maxPoolSize;

    public String getmaxPoolSize() {
        return maxPoolSize;
    }

    public void setmaxPoolSize(String maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

//   // @Value("${totalNumTasks")
//    private String totalNumTasks;
//
//    public String gettotalNumTasks() {
//        return totalNumTasks;
//    }
//
//    public void settotalNumTasks(String totalNumTasks) {
//        this.totalNumTasks = totalNumTasks;
//    }
//
//    // @Value("${totalNumTasks")
//    private String totalTimeTasks;
//
//    public String gettotalTimeTasks() {
//        return totalTimeTasks;
//    }
//
//    public void settotalTimeTasks(String totalNumTasks) {
//        this.totalTimeTasks = totalTimeTasks;
//    }

    @Override
    public String toString() {
        return "TaskPollProperties{" +
                "corePoolSize='" + corePoolSize + '\'' +
                ", maxPoolSize='" + maxPoolSize + '\'' +
                '}';
    }
}
