package net.plumbing.msgbus.common;

// import com.sun.istack.internal.NotNull;
//import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import javax.validation.constraints.NotNull;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.Set;

public  class DataAccess {

    public static Connection  Hermes_Connection;
    public  static Timestamp InitDate; // Date InitDate;
    public static DateFormat dateFormat;

    //@Autowired

    private static  String SQLCurrentTimeStringRead;
    private static  String SQLCurrentTimeDateRead;
    private static PreparedStatement stmtCurrentTimeStringRead;
    private static PreparedStatement stmtCurrentTimeDateRead;
    public static  String HrmsSchema="orm";
    public static  String rdbmsVendor="oracle";

    public static  Connection make_DataBase_Connection(String DbSchema, String dst_point, String db_userid , String db_password, String InternalDbPgSetSetupConnection ,Logger dataAccess_log) {
        Connection Target_Connection = null;
        String connectionUrl ;
        if ( dst_point==null) {
            connectionUrl = "jdbc:oracle:thin:@//5.6.7.8:1521/hermesXX"; // Test-Capsul !!!
        }
        else {
            //connectionUrl = "jdbc:oracle:thin:@"+dst_point;
            //connectionUrl = "jdbc:postgresql:"+dst_point;
            connectionUrl = dst_point;
        }

        String ClassforName;
        if ( connectionUrl.indexOf("oracle") > 0 ) {
            ClassforName = "oracle.jdbc.driver.OracleDriver";
            XMLchars.MAX_TAG_VALUE_BYTE_SIZE = 3992;  //   for Oracle, it must be 3992
        }
        else {
            ClassforName = "org.postgresql.Driver";
            XMLchars.MAX_TAG_VALUE_BYTE_SIZE = 32778; // for PostGreSQL 32778;
        }
        HrmsSchema =  DbSchema;
        if ( connectionUrl.contains("oracle") ) rdbmsVendor="oracle";
        else rdbmsVendor="postgresql";
        HrmsSchema =  DbSchema;

        dataAccess_log.info("Try DataBase getConnection: [{}] as {} to RDBMS ({}) DbSchema:{}", connectionUrl, db_userid, rdbmsVendor, DbSchema);

        try {
            // Establish the connection.
            // Class.forName("oracle.jdbc.driver.OracleDriver");
            // Class.forName("org.postgresql.Driver");
            Class.forName(ClassforName);
            Target_Connection = DriverManager.getConnection(connectionUrl, db_userid, db_password);
            // Handle any errors that may have occurred.
            Target_Connection.setAutoCommit(false);
            if ( !rdbmsVendor.equals("oracle") ) {
                SQLCurrentTimeStringRead= "SELECT to_char( now() AT TIME ZONE 'Europe/Moscow', 'YYYYMMDDHH24MISS') as InitTime";
                SQLCurrentTimeDateRead= "SELECT now() AT TIME ZONE 'Europe/Moscow' as InitTime";
                dataAccess_log.info("Try setup Connection: `{}`", InternalDbPgSetSetupConnection ) ;
                PreparedStatement stmt_SetTimeZone = Target_Connection.prepareStatement(InternalDbPgSetSetupConnection);//.nativeSQL( "set SESSION time zone 3; set enable_bitmapscan to off; set max_parallel_workers_per_gather = 0;");
                stmt_SetTimeZone.execute();
                stmt_SetTimeZone.close();
                Target_Connection.commit();
            }
            else
            {
                /*DatabaseMetaData databaseMetaData  = Target_Connection.getMetaData();
                ResultSet rs= databaseMetaData.getClientInfoProperties();
                dataAccess_log.info( "RDBMS get main ClientInfoProperties:");
                while (rs.next()) {
                    //CurrentTime = rs.getString("NAME");
                    dataAccess_log.info( "RDBMS ClientInfoProperties: NAME=`"+ rs.getString("NAME") + "` DESCRIPTION [" + rs.getString("DESCRIPTION")  + "] DEFAULT_VALUE=" + rs.getString("DEFAULT_VALUE")  );
                }
                rs.close();
                */


                // используем DUAL
                   SQLCurrentTimeStringRead= "SELECT to_char(current_timestamp, 'YYYYMMDDHH24MISS') as InitTime FROM dual";
                   SQLCurrentTimeDateRead= "SELECT current_timestamp as InitTime FROM dual";
                   /*
                    CallableStatement stmtDBMS_SET_MODULE = Target_Connection.prepareCall("call DBMS_APPLICATION_INFO.SET_MODULE( 'Sender', 'mainSenderThread')"  );
                    stmtDBMS_SET_MODULE.execute();
                    stmtDBMS_SET_MODULE.close();

                    stmtDBMS_SET_MODULE = Target_Connection.prepareCall("call DBMS_APPLICATION_INFO.SET_CLIENT_INFO( 'thread N 00')"  );
                    stmtDBMS_SET_MODULE.execute();
                    stmtDBMS_SET_MODULE.close();

                DBMS_APPLICATION_INFO.SET_MODULE(
                        module_name => 'Sender',
                        action_name => 'main_Sender');

                DBMS_APPLICATION_INFO.SET_CLIENT_INFO (
                        client_info => 'thread N 00' );
                        */
               // Устанавлеваем Tracing Properties

                Target_Connection.setClientInfo("OCSID.CLIENTID", "Sender");
                Target_Connection.setClientInfo("OCSID.MODULE", "SenderMain");
                Target_Connection.setClientInfo("OCSID.ACTION", "ReRead config");
            }

            Properties propClientInfo = Target_Connection.getClientInfo();
            if ( propClientInfo != null ) {
                dataAccess_log.warn("RDBMS ClientInfoProperties: propClientInfo :{} size={}", propClientInfo, propClientInfo.size());
                Set<String> propKeysSet = propClientInfo.stringPropertyNames();
                String[]  propKeys =propKeysSet.toArray(new String[0]);
                for ( int i=0; i < propKeys.length; i++ ) {
                    String propKey = propKeys[i];
                    if ( propKey != null )
                        dataAccess_log.info("RDBMS ClientInfoProperties: NAME=`{}` VALUE={}", propKey, propClientInfo.getProperty(propKey));
                    else
                        dataAccess_log.info( "RDBMS ClientInfoProperties: key is null" );
                }
            }
            else
                dataAccess_log.warn( "RDBMS ClientInfoProperties: propClientInfo is null" );
            /*dataSource = new DriverManagerDataSource(connectionUrl);
            dataSource.setDriverClassName("oracle.jdbc.OracleDriver");
            dataSource.setPassword(db_password);
            dataSource.setUsername(db_userid);
            */
            DataAccess.Hermes_Connection = Target_Connection;
            dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
            stmtCurrentTimeStringRead = DataAccess.Hermes_Connection.prepareStatement(SQLCurrentTimeStringRead );
            stmtCurrentTimeDateRead = DataAccess.Hermes_Connection.prepareStatement(SQLCurrentTimeDateRead );
            // PreparedStatement stmtInitTimeDateRead = DataAccess.Hermes_Connection.prepareStatement(SQLCurrentTimeDateRead );
            ResultSet rs = null;
            rs = stmtCurrentTimeDateRead.executeQuery();
            while (rs.next()) {
                InitDate = rs.getTimestamp("InitTime");
                dataAccess_log.info("RDBMS make_DataBase_Connection CurrentTime: LocalDateTime ={} getTime={} mSec., {}", InitDate.toString(), InitDate.getTime(), dateFormat.format(InitDate));
            }
            rs.close();
            DataAccess.Hermes_Connection.commit();
            // stmtInitTimeDateRead.close();
        } catch (Exception e) {
            dataAccess_log.error("RDBMS getConnection fault: {}", e.toString());
            e.printStackTrace();
            return ( (Connection) null );
        }

        dataAccess_log.info("RDBMS make_DataBase_Connection: {} as {} at {} done", connectionUrl, db_userid, dateFormat.format(InitDate));
        /*
        jdbcTemplate = new JdbcTemplate( dataSource );
        InitDate = jdbcTemplate.queryForObject(
                "SELECT current_timestamp FROM dual", Date.class);
        */
      if ( InitDate != null)
          dataAccess_log.info("RDBMS make_DataBase_Connection: LocalDate ={} getTime={} mSec., {}", InitDate.toString(), InitDate.getTime(), dateFormat.format(InitDate));


        return Target_Connection;
    }


    public static String getCurrentTimeString(@NotNull Logger dataAccess_log ) {
        String CurrentTime="00000000000000";
        try {

            ResultSet rs = null;
            rs = stmtCurrentTimeStringRead.executeQuery();
            while (rs.next()) {
                CurrentTime = rs.getString("InitTime");
            }
            rs.close();
            Hermes_Connection.commit();
            dataAccess_log.info("RDBMS CurrentTime getCurrentTimeString(): LocalDate ={}", CurrentTime);
        } catch (Exception e) {
            dataAccess_log.error("getCurrentTimeString `{}` fault:{}", SQLCurrentTimeStringRead, sStackTrace.strInterruptedException(e));

        }
        return ( CurrentTime );

    }

    public static Long getCurrentTime(@NotNull Logger dataAccess_log )
    throws SQLException {
        Timestamp CurrentTime=null;
        LocalDateTime localDateTime;
        try {
            ResultSet rs = null;
            rs = stmtCurrentTimeDateRead.executeQuery();
            while (rs.next()) {
                CurrentTime = rs.getTimestamp("InitTime");
            }
            rs.close();
            Hermes_Connection.commit();
            if ( CurrentTime != null) {
                localDateTime = CurrentTime.toLocalDateTime();
                dataAccess_log.info("RDBMS CurrentTime(): LocalDate ={} getTime={} mSec.,  getLocalDateTime={} `{}`", CurrentTime.toLocalDateTime(), CurrentTime.getTime(), localDateTime.toString(), dateFormat.format(CurrentTime));

                return CurrentTime.getTime();
            }
            else return null;

        } catch (SQLException e) {
            dataAccess_log.error("getCurrentTimeDate fault: {}", sStackTrace.strInterruptedException(e));
            throw e;
            //return null;
        }


    }

    public static Integer doPSQL_Function_Run(@NotNull String pSQL_function, @NotNull Logger dataAccess_log ) throws java.sql.SQLException {
        CallableStatement callableStatement =null;
        int count;
        String sErrorDescription;

        // # HE-5467 Необходимо обеспечить обработку ПРИЁМА ответов на ИСХОДЯЩИЕ события для инициации вызова прикладного обработчика при любых ошибках и сбоях внешней системы при интеграции
        try {
             callableStatement = DataAccess.Hermes_Connection.prepareCall (pSQL_function);


        } catch (Exception e) {
            dataAccess_log.error("prepareCall({} fault: {}", pSQL_function, sStackTrace.strInterruptedException(e));
            DataAccess.Hermes_Connection.rollback();
            return -2;
        }
   try {
       dataAccess_log.info("doPSQL_Function_Run: try executeCall({}", pSQL_function);
        // register OUT parameter
        callableStatement.registerOutParameter(1, Types.VARCHAR);

        // Step 2.C: Executing CallableStatement
            try {
            callableStatement.execute();
                } catch (SQLException e) {
                dataAccess_log.error(", SQLException callableStatement.execute({} ):={}", pSQL_function, e.toString());
                    callableStatement.close();
                    DataAccess.Hermes_Connection.rollback();
                    return -3;
               }

        // get count and print in console
            sErrorDescription = callableStatement.getString(1);
            dataAccess_log.info( pSQL_function + " = " + sErrorDescription);
            SQLWarning warning = callableStatement.getWarnings();
            count = 0;
            while (warning != null) {
                count = count +1;
                // System.out.println(warning.getMessage());
                dataAccess_log.warn("doPSQL_Function_Run: [{} ] callableStatement.SQLWarning: {}", count, warning.getMessage());
                warning = warning.getNextWarning();
            }

            callableStatement.close();

        } catch (Exception e) {
       dataAccess_log.error("doPSQL_Function_Run: executeCall({}) fault: {}", pSQL_function, sStackTrace.strInterruptedException(e));
            try {
                callableStatement.close();
            }catch (SQLException eSQL) {
                dataAccess_log.error("doPSQL_Function_Run: callableStatement.close({}) fault: {}", pSQL_function, sStackTrace.strInterruptedException(e));
            }
            DataAccess.Hermes_Connection.rollback();
            return -4;
        }
        DataAccess.Hermes_Connection.commit();
        return 0;
    }
/*
    public static String getCurrentTimeString(@NotNull Logger dataAccess_log ) {
        if ( jdbcTemplate !=null ) {
            String CurrentTime = jdbcTemplate.queryForObject(
                    "SELECT to_char(current_timestamp, 'YYYYMMDDHHMISS' ) FROM dual", String.class);
            if ( CurrentTime != null)
                dataAccess_log.info( "Hermes CurrentTime: LocalDate ="+ CurrentTime );
            return CurrentTime;
        }
        else
            return null;
    }

    public static Long getCurrentTime(@NotNull Logger dataAccess_log ) {
        if ( jdbcTemplate !=null ) {
            Date CurrentTime = jdbcTemplate.queryForObject(
                    "SELECT current_timestamp FROM dual", Date.class);
            if ( CurrentTime != null)
                dataAccess_log.info( "Hermes CurrentTime: LocalDate ="+ CurrentTime.toLocalDate().toString() + " getTime=" + CurrentTime.getTime()  + " mSec., " + dateFormat.format( CurrentTime )  );
            return CurrentTime.getTime();
        }
        else
            return null;
    }
    */
}


