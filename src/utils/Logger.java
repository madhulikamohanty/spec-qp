package de.mpii.trinitreloaded.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

import de.mpii.trinitreloaded.utils.Config.LoggingLevel;

/**
 * A class for debugging and logging purpose.
 * Set the value of {@code debugmode} in {@link Config} to <code>true</code>
 * to print the debug statements.
 * 
 * Usage:
 * Call the static methods, {@code print()} or {@code println()}, to print the debug statements.
 * Call the static methods, {@code startLog()} and {@code stopLog()}, to start and stop logging.
 * 
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 * 
 */
public class Logger {
  public static String logFile;
  public static boolean logging = false;
  static BufferedWriter bw;

  public static void startLog(){
    String db = "";
    if(Config.isRDFDB)
      db = "Virtuoso";
    else
      db = "PostgreSQL";
    logFile = Config.queryFile.replace(".txt", "") + "-db-" + db + "-histtype-" + 
        Config.histType + "-numbuckets-" + Config.numBuckets + "-result-k-" + Config.k + ".log";
    System.out.println("Starting to log------>");
    System.out.println("***************************************************");
    System.out.println("Check the " + logFile + " file for logs.");
    System.out.println("***************************************************");
    logging = true;

    try {
      // Rename if already exists.
      File f = new File(logFile);
      char fileNum = '1';
      String newLogFile = "";
      boolean changed = false;
      while(f.exists()){
        newLogFile = logFile + "(" + fileNum + ")";
        fileNum++;
        f = new File(newLogFile);
        changed = true;
      }

      if(changed)
        logFile = newLogFile;

      bw = new BufferedWriter(new FileWriter(logFile));
      println(Config.getVal(), LoggingLevel.EXPERIMENTS);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void stopLog(){
    try {
      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    logging = false;
    System.out.println("Log over!!");
  }

  public static void print(Object printString, LoggingLevel dlvl) {
    if (Config.debugmode == true && dlvl.getNumVal()<=Config.loggingLevel.getNumVal()) {
      System.out.println(printString.toString());
    }
    if(dlvl==LoggingLevel.EXPERIMENTS && logging){
      try {
        bw.write(printString.toString());
        bw.flush();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void println(Object printString, LoggingLevel dlvl) {
    if (Config.debugmode == true && dlvl.getNumVal()<=Config.loggingLevel.getNumVal()) {
      System.out.println(printString.toString() + "\n");
    }
    if(dlvl==LoggingLevel.EXPERIMENTS && logging){
      try {
        bw.write(printString.toString());
        bw.newLine();
        bw.flush();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
