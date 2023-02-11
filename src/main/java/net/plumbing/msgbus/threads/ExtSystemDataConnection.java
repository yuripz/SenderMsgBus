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
        // попробуй ARTX_PROJ / rIYmcN38St5P  || hermes / uthvtc
        //String db_userid = "HERMES";
        //String db_password = "uthvtc";
        //this.dbSchema = HrmsSchema;
        if (connectionUrl.indexOf("oracle") > 0) {
            rdbmsVendor = "oracle";
        } else {
            rdbmsVendor = "postgresql";
        }
        dataAccess_log.info("[" + Queue_Id + "] Try(thead) ExtSystem getConnection: " + connectionUrl + " as " + ApplicationProperties.ExtSysDbLogin + " rdbmsVendor=" + rdbmsVendor);


        try {
            Target_Connection = dataSource.getConnection();
            Target_Connection.setAutoCommit(false);
        } catch (SQLException e) {
            dataAccess_log.error("[" + Queue_Id + "] ExtSystem getConnection() fault: " + e.getMessage());
            System.err.println( "["+ Queue_Id + "] ExtSystem getConnection() Exception" );
            e.printStackTrace();
            return ;
        }
        // dataAccess_log.info( "Hermes(thead) getConnection: " + connectionUrl + " as " + db_userid + " done" );


        if (!rdbmsVendor.equals("oracle")) {
            dataAccess_log.info("[" + Queue_Id + "] try ExtSystem `set SESSION time zone 3`");
            try {
                PreparedStatement stmt_SetTimeZone = Target_Connection.prepareStatement("set SESSION time zone 3");//.nativeSQL( "set SESSION time zone 3" );
                stmt_SetTimeZone.execute();
                stmt_SetTimeZone.close();
            } catch (SQLException e) {

                dataAccess_log.error("[" + Queue_Id + "] ExtSystem `set SESSION time zone 3` fault: " + e.getMessage());
                System.err.println( "["+ Queue_Id + "] ExtSystem `set SESSION time zone 3` Exception" );
                e.printStackTrace();
                try { Target_Connection.close(); //.close(); ??
                } catch (SQLException SQLe) {
                    dataAccess_log.error("[" + Queue_Id + "] `ExtSystem Connection.close()` fault: " + e.getMessage());
                }
                return ;
            }
        }
        ExtSystem_Connection= Target_Connection;
        return  ;
    }
}
