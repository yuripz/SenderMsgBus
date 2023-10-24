package net.plumbing.msgbus.common;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.plumbing.msgbus.SenderApplication;
import org.springframework.boot.jdbc.metadata.HikariDataSourcePoolMetadata;
import org.springframework.context.annotation.Bean;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.TimeUnit;

public class ExtSystemDataAccess {
    public static HikariDataSourcePoolMetadata DataSourcePoolMetadata = null;
    @Bean (destroyMethod = "close")
    public static  HikariDataSource HiDataSource(String JdbcUrl, String Username, String Password ){
        HikariConfig hikariConfig = new HikariConfig();
        String connectionUrl ;
        if ( JdbcUrl==null) {
            connectionUrl = "jdbc:oracle:thin:@//10.242.36.8:1521/hermes12"; // Test-Capsul !!!
            //connectionUrl = "jdbc:oracle:thin:@//10.32.245.4:1521/hermes"; // Бой !!!
        }
        else {
            //connectionUrl = "jdbc:oracle:thin:@"+dst_point;
            //connectionUrl = "jdbc:postgresql:"+dst_point;
            connectionUrl = JdbcUrl;
        }
        String ClassforName;
        if ( connectionUrl.indexOf("oracle") > 0 )
            ClassforName = "oracle.jdbc.driver.OracleDriver";
        else ClassforName = "org.postgresql.Driver";

//        hikariConfig.setDriverClassName("oracle.jdbc.driver.OracleDriver");
//        hikariConfig.setJdbcUrl( "jdbc:oracle:thin:@"+ JdbcUrl); //("jdbc:oracle:thin:@//10.242.36.8:1521/hermes12");
        SenderApplication.AppThead_log.info( "ExtSystemDataAccess: Try make hikariConfig: JdbcUrl `" + connectionUrl + "` as " + Username + " ["+ Password + "] , Class.forName:" + ClassforName);
        hikariConfig.setDriverClassName(ClassforName);
        hikariConfig.setJdbcUrl(  connectionUrl ); //("jdbc:oracle:thin:@//10.242.36.8:1521/hermes12");

        hikariConfig.setUsername( Username ); //("ARTX_PROJ");
        hikariConfig.setPassword( Password ); // ("rIYmcN38St5P");

        hikariConfig.setLeakDetectionThreshold(TimeUnit.MINUTES.toMillis(5));
        hikariConfig.setConnectionTimeout(TimeUnit.SECONDS.toMillis(30));
        hikariConfig.setValidationTimeout(TimeUnit.MINUTES.toMillis(1));
        hikariConfig.setIdleTimeout(TimeUnit.MINUTES.toMillis(5));
        hikariConfig.setMaxLifetime(TimeUnit.MINUTES.toMillis(10));

        hikariConfig.setMaximumPoolSize(30);
        hikariConfig.setMinimumIdle(10);
        // hikariConfig.setConnectionTestQuery("SELECT 1 from dual");
        if ( connectionUrl.indexOf("oracle") > 0 )
            hikariConfig.setConnectionTestQuery("SELECT 1 from dual");
        else hikariConfig.setConnectionTestQuery("SELECT 1 ");
        hikariConfig.setPoolName("ExtSystemCP");

        hikariConfig.addDataSourceProperty("dataSource.cachePrepStmts", "true");
        hikariConfig.addDataSourceProperty("dataSource.prepStmtCacheSize", "500");
        hikariConfig.addDataSourceProperty("dataSource.prepStmtCacheSqlLimit", "4096");
        hikariConfig.addDataSourceProperty("dataSource.useServerPrepStmts", "true");
        hikariConfig.addDataSourceProperty("dataSource.autoCommit", "false");
        SenderApplication.AppThead_log.info( "ExtSystemDataAccess: try make DataSourcePool: " + connectionUrl + " as " + Username + " , Class.forName:" + ClassforName);
        HikariDataSource dataSource;
        try {
            dataSource = new HikariDataSource(hikariConfig);
            //HikariPool hikariPool = new HikariPool(hikariConfig);
            DataSourcePoolMetadata = new HikariDataSourcePoolMetadata(dataSource);
        }
        catch (Exception e)
        { SenderApplication.AppThead_log.error( "new HikariDataSource() fault" + e.getMessage());
            return null;
        }
        SenderApplication.AppThead_log.info( "DataSourcePool ( at start ): getMax: " + DataSourcePoolMetadata.getMax()
                + ", getIdle: " + DataSourcePoolMetadata.getIdle()
                + ", getActive: " + DataSourcePoolMetadata.getActive()
                + ", getMax: " + DataSourcePoolMetadata.getMax()
                + ", getMin: " + DataSourcePoolMetadata.getMin()
        );
        SenderApplication.AppThead_log.info(
                "ConnectionTestQuery: " + dataSource.getConnectionTestQuery()
                        + ", IdleTimeout: " + dataSource.getIdleTimeout()
                        + ", LeakDetectionThreshold: " + dataSource.getLeakDetectionThreshold()
        );

        Connection tryConn;
        try {

             tryConn = dataSource.getConnection();
        }
        catch (java.sql.SQLException e)
        { SenderApplication.AppThead_log.error( "dataSource.getConnection() fault" + e.getMessage());
          return null;
        }
        String connectionTestQuery = "SELECT 1 ";
        try {
            if ( connectionUrl.indexOf("oracle") > 0 )
                connectionTestQuery = "SELECT 1 from dual";

            PreparedStatement prepareStatement = tryConn.prepareStatement( connectionTestQuery );
            prepareStatement.executeQuery();
            prepareStatement.close();
            SenderApplication.AppThead_log.info( "DataSourcePool ( at prepareStatement ): getMax: " + DataSourcePoolMetadata.getMax()
                    + ", getIdle: " + DataSourcePoolMetadata.getIdle()
                    + ", getActive: " + DataSourcePoolMetadata.getActive()
                    + ", getMax: " + DataSourcePoolMetadata.getMax()
                    + ", getMin: " + DataSourcePoolMetadata.getMin()
            );
            tryConn.close();
            SenderApplication.AppThead_log.info( "getJdbcUrl: "+ hikariConfig.getJdbcUrl());
        }
        catch (java.sql.SQLException e)
        { SenderApplication.AppThead_log.error( "dataSource connectionTestQuery fault `" + connectionTestQuery + "` :" +  e.getMessage());
            try { tryConn.close();
                }
            catch (java.sql.SQLException closeE)
            { SenderApplication.AppThead_log.error( "dataSource connection close() fault " +  closeE.getMessage()); }
            return null;
        }

        return dataSource;
    }
    /* */

}
