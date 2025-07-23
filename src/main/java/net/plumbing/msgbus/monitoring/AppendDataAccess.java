package net.plumbing.msgbus.monitoring;
// import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.springframework.beans.factory.annotation.Autowired;
//import DataAccess;


public class AppendDataAccess {
    public static Connection  Hermes_Connection;
    public  static Date InitDate;
    private static DateFormat dateFormat;

    //@Autowired
    /*
    static JdbcTemplate jdbcTemplate;
    static DriverManagerDataSource dataSource;
    */
    public  PreparedStatement stmtInsertData;

    public  Connection make_Monitoring_Connection(  String dataSourceClassName, String pConnectionUrl, String db_userid , String db_password,
                                                    String dataStoreTableName, Logger dataAccess_log) {
        Connection Target_Connection ;
        String connectionUrl ;
        if ( pConnectionUrl==null) {
            connectionUrl = "jdbc:oracle:thin:@//5.6.7.8:1521/hermesXX"; // Test-Capsul !!!
        }
        else {
            connectionUrl = pConnectionUrl;
        }

        dataAccess_log.info("Try monitoring getConnection by ({}) : {} as {}", dataSourceClassName, connectionUrl, db_userid);
        try {
            // Establish the connection.
            if ( dataSourceClassName != null )
                Class.forName( dataSourceClassName) ;
            else
                Class.forName("oracle.jdbc.driver.OracleDriver");

            Target_Connection = DriverManager.getConnection(connectionUrl, db_userid, db_password);
            // Handle any errors that may have occurred.
            Target_Connection.setAutoCommit(false);

            AppendDataAccess.Hermes_Connection = Target_Connection;
            dateFormat = new SimpleDateFormat("YYYY-MM-dd HH24:mi:ss");
            dataAccess_log.info("prepareStatement: [insert into {}" + SQLInsertData + "]", dataStoreTableName);
            stmtInsertData = AppendDataAccess.Hermes_Connection.prepareStatement("insert into " + dataStoreTableName + SQLInsertData );


        } catch (SQLException |ClassNotFoundException e) {
            e.printStackTrace();
            return null ;
        }

        dataAccess_log.info("monitoring getConnection: {} as {}  done", connectionUrl, db_userid);

        return Target_Connection;
    }


    private static final String SQLInsertData= " (" +
            " queue_id," +
            " queue_direction," +
            " queue_date," +
            " msg_status," +
            " msg_date," +
            " operation_id," +
            " outqueue_id," +
            " msg_type," +
            " msg_reason," +
            " msgdirection_id," +
            " msg_infostreamid," +
            " msg_type_own," +
            " msg_result," +
            " subsys_cod," +
            " retry_count," +
            " prev_queue_direction," +
            " prev_msg_date," +
            " queue_create_date," +
            " perform_object_id," +
            " req_dt," +
            " request," +
            " resp_dt," +
            " response) " +
            "values( ?," +
            " ?," +
            " ?," +
            " ?," +
            " ?," +
            " ?," +
            " ?," +
            " ?," +
            " ?," +
            " ?," +
            " ?," +
            " ?," +
            " ?," +
            " ?," +
            " ?," +
            " ?," +
            " ?," +
            " ?," +
            " ?," +
            " ?," +
            " ?," +
            " ?," +
            " ? )";

}
