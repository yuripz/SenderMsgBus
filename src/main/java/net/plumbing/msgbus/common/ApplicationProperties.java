package net.plumbing.msgbus.common;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.jdbc.metadata.HikariDataSourcePoolMetadata;

public  class ApplicationProperties {
    // extSystemDataSource
    public static String ExtSysSchema;
    public static String ExtSysPoint;
    public static String ExtSysDbLogin;
    public static String ExtSysDbPasswd;
    public static String CuberNumId;
    public static String ExtSysPgSetupConnection;
    public static String InternalDbPgSetupConnection;

    public void setExtSysSchema(String extSysSchema) {
        this.ExtSysSchema = extSysSchema;
    }

    public void setExtSysDbPasswd(String extSysDbPasswd) {
        this.ExtSysDbPasswd = extSysDbPasswd;
    }

    public void setExtSysDbLogin(String extSysDbLogin) {
        this.ExtSysDbLogin = extSysDbLogin;
    }

    public static HikariDataSource dataSource; //= HiDataSource();
    public static HikariDataSource extSystemDataSource;
    public static HikariDataSourcePoolMetadata DataSourcePoolMetadata;
    public static HikariDataSourcePoolMetadata extSystemDataSourcePoolMetadata;
}
