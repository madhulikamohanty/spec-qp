package de.mpii.trinitreloaded.utils;

import virtuoso.jena.driver.VirtGraph;

/**
 * A class for managing database connections to Virtuoso.
 *
 *
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 */
public class GraphConnection {
  private static DBConfig dbc = new DBConfig(Config.servContext+Config.dbConfigFile);

  public static VirtGraph getConnection() {
    VirtGraph set = null;
    try {
      String url = "jdbc:virtuoso://"+dbc.getServerName()+":1111/";
      set = new VirtGraph (url, dbc.getGraphDBUsername(), dbc.getGraphDBPassword());
    }catch (Exception e) {
      e.printStackTrace();
    }
    return set;
  }
}
