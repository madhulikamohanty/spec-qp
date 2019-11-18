package de.mpii.trinitreloaded.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * A class for managing database connections to Postgres.
 *
 *
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 */
public class DBConnection {

  //private static HikariConfig config = new HikariConfig(Config.servContext+Config.dbConfigFile);
  //private static HikariDataSource ds = new HikariDataSource(config);
  private static DBConfig dbc = new DBConfig(Config.servContext+Config.dbConfigFile);
  private static final String JDBC_DRIVER = "org.postgresql.Driver"; 

  public static Connection getConnection() throws SQLException {
    //return ds.getConnection();
    Connection conn=null;
    try{
      Class.forName(JDBC_DRIVER);
      String url = "jdbc:postgresql://"+dbc.getServerName()+"/"+dbc.getDbName();
      conn=DriverManager.getConnection(url, dbc.getUsername(),dbc.getPassword());
    }catch(Exception e){
      e.printStackTrace();
    }
    return conn;
  }
}
