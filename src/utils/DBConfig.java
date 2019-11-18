package de.mpii.trinitreloaded.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
/**
 * A class for database configuration parameters.
 * 
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 */
public class DBConfig {
  private String serverName;
  private String dbName;
  private String username;
  private String password;
  private String graphDBUsername;
  private String graphDBPassword;
  public DBConfig(String configFile){
    Properties props = new Properties();
    FileInputStream in;
    try {
      in = new FileInputStream(configFile);
      props.load(in);
      in.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    setServerName(props.getProperty("dataSource.serverName"));
    setDbName(props.getProperty("dataSource.databaseName"));
    setUsername(props.getProperty("dataSource.user"));
    setPassword(props.getProperty("dataSource.password"));
    setGraphDBUsername(props.getProperty("dataSource.graphusername"));
    setGraphDBPassword(props.getProperty("dataSource.graphpassword"));
  }
  
  public String getServerName() {
    return serverName;
  }
  
  public void setServerName(String serverName) {
    this.serverName = serverName;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getGraphDBUsername() {
    return graphDBUsername;
  }

  public void setGraphDBUsername(String graphDBUsername) {
    this.graphDBUsername = graphDBUsername;
  }

  public String getGraphDBPassword() {
    return graphDBPassword;
  }

  public void setGraphDBPassword(String graphDBPassword) {
    this.graphDBPassword = graphDBPassword;
  }
}
