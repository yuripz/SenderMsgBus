package net.plumbing.msgbus.threads;

import com.zaxxer.hikari.HikariDataSource;
//import net.plumbing.msgbus.common.ApplicationProperties;
import net.plumbing.msgbus.common.ApplicationProperties;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ExtSystemDataConnection {
    public  Connection  ExtSystem_Connection=null;

    public ExtSystemDataConnection(long Queue_Id, Logger dataAccess_log) {
        Connection Target_Connection;

        String rdbmsVendor;
        HikariDataSource dataSource = ApplicationProperties.extSystemDataSource;
        String connectionUrl = dataSource.getJdbcUrl();

        //this.dbSchema = HrmsSchema;
        if (connectionUrl.indexOf("oracle") > 0) {
            rdbmsVendor = "oracle";
        } else {
            rdbmsVendor = "postgresql";
        }
        dataAccess_log.info("[{}] Try(thead) ExtSystem getConnection: {} as {} rdbmsVendor={}", Queue_Id, connectionUrl, ApplicationProperties.ExtSysDbLogin, rdbmsVendor);


        try {
            Target_Connection = dataSource.getConnection();
            Target_Connection.setAutoCommit(false);
        } catch (SQLException e) {
            dataAccess_log.error("[{}] ExtSystem getConnection() fault: {}", Queue_Id, e.getMessage());
            System.err.println( "["+ Queue_Id + "] ExtSystem getConnection() Exception" );
            e.printStackTrace();
            return ;
        }
        // dataAccess_log.info( "Hermes(thead) getConnection: " + connectionUrl + " as " + db_userid + " done" );


        if ( connectionUrl.indexOf("postgresql") > 0 ) {
            dataAccess_log.info("[{}] try setSetupConnection for ExtSystem `{}`", Queue_Id, ApplicationProperties.ExtSysPgSetupConnection);
            try {
                PreparedStatement stmt_SetTimeZone = Target_Connection.prepareStatement(ApplicationProperties.ExtSysPgSetupConnection);//.nativeSQL( "set SESSION time zone 3; set enable_bitmapscan to off; set max_parallel_workers_per_gather = 0;" );
                stmt_SetTimeZone.execute();
                stmt_SetTimeZone.close();
            } catch (SQLException e) {

                dataAccess_log.error("[{}] ExtSystem `{}` fault: {}", Queue_Id, ApplicationProperties.ExtSysPgSetupConnection, e.getMessage());
                System.err.println( "["+ Queue_Id + "] ExtSystem `" + ApplicationProperties.ExtSysPgSetupConnection +"` Exception" );
                e.printStackTrace();
                try { Target_Connection.close(); //.close(); ??
                } catch (SQLException SQLe) {
                    dataAccess_log.error("[{}] `ExtSystem Connection.close()` fault: {}", Queue_Id, e.getMessage());
                }
                return ;
            }
        }
        ExtSystem_Connection= Target_Connection;
        return  ;
    }
}
