package de.mpii.trinitreloaded.queryprocessing;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import de.mpii.trinitreloaded.datastructures.Answer;
import de.mpii.trinitreloaded.datastructures.HashMapBasedAnswer;
import de.mpii.trinitreloaded.datastructures.TriplePattern;
import de.mpii.trinitreloaded.utils.Config;
import de.mpii.trinitreloaded.utils.Config.LoggingLevel;
import de.mpii.trinitreloaded.utils.DBConnection;
import de.mpii.trinitreloaded.utils.Logger;

/**
 * A scan operator for individual {@link TriplePattern}.
 *
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 * @author Mohamed Yahya (myahya@mpi-inf.mpg.de)
 * 
 */
public class PopularityBasedScan implements Operator {
  final TriplePattern input;
  Connection conn;
  Statement stmt;
  ResultSet rs;
  String dbCmd;
  boolean isOpen;
  Answer current;
  double maxScore = Double.NaN;
  boolean toBeCounted;

  public PopularityBasedScan(TriplePattern input) {
    this.toBeCounted = true;
    this.input = input;
  }

  @Override
  public String toString() {
    return "PopularityBasedScan [input=" + input + "]";
  }

  public PopularityBasedScan(TriplePattern input, boolean toBeCounted) {
    this.toBeCounted = toBeCounted;
    this.input = input;
  }

  public boolean open() throws SQLException {
    if (this.isOpen) {
      return true;
    }
    dbCmd = getDBCommand();
    conn = DBConnection.getConnection();
    stmt = conn.createStatement();
    rs = stmt.executeQuery(dbCmd);
    this.isOpen = true;
    current = null;
    if (conn != null) {
      return true;
    } else {
      return false;
    }
  }

  private String getDBCommand() {
    List<String> whereClause = Lists.newArrayList();
    String cmd ="";
    String tblName = null;
    if(this.input.isObjectResource || !this.input.isObjectConst){ //Different scoring scheme for semantic and textual types' matches.
      tblName = Config.dataTableName;
      cmd +=
          "SELECT d.subject as subject, d.predicate as predicate , d.object as object, inlinks AS score FROM "+ tblName+ " d, "+ Config.scoreTableName;
    }
    else{
      tblName = Config.textualTypeDataTableName;
      cmd +=
          "SELECT d.subject as subject, d.predicate as predicate , "
              + "d.object as object, d.count AS score FROM "+ tblName+ " d";
    }
    cmd += " WHERE ";
    if (this.input.isSubjectConst) {
      whereClause.add("subject='" + QueryPlanner.format(this.input.subject) + "' ");
    }

    if (this.input.isPredicateConst) {
      whereClause.add("predicate='" + QueryPlanner.format(this.input.predicate) + "' ");
    } 

    if (this.input.isObjectConst) {
      whereClause.add("object='" + QueryPlanner.format(this.input.object) + "' ");
    } 
    if(this.input.isObjectResource || !this.input.isObjectConst)
      whereClause.add("subject = entity");

    cmd +=
        Joiner.on(" AND ").join(whereClause) + " ORDER BY score DESC";
    Logger.println("PopularityBasedScan cmd: " + cmd, LoggingLevel.VARIABLEVALUES);
    return cmd;
  }

  public double getScoreMultiplier() {
    if (Double.isNaN(maxScore)) {
      try {
        maxScore = rs.getDouble("score");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    return 1 / maxScore;
  }

  public boolean close() throws SQLException {
    if (!isOpen) {
      return true;
    }
    rs.close();
    stmt.close();
    conn.close();
    this.isOpen = false;
    return true;
  }

  public boolean hasNext() throws SQLException {
    if (current != null) {
      return true;
    }
    try {
      if (rs.next()) {
        HashMapBasedAnswer ans = new HashMapBasedAnswer(toBeCounted);
        if (!this.input.isSubjectConst) {
          String var = this.input.subject;
          String binding = rs.getString("subject");
          ans.setVariableBinding(var, binding);
        }
        if (!this.input.isObjectConst) {
          String var = this.input.object;
          String binding = rs.getString("object");
          ans.setVariableBinding(var, binding);
        }
        if (!this.input.isPredicateConst) {
          String var = this.input.predicate;
          String binding = rs.getString("predicate");
          ans.setVariableBinding(var, binding);
        }
        Double score = rs.getDouble("score") * getScoreMultiplier() * Config.scoreMultipler;
        ans.setScore(score);
        ans.setQuery(this.input.toString());
        current = ans;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    if (current == null) {
      return false;
    } else {
      return true;
    }
  }

  public Answer next() throws Exception {
    if (current != null) {
      Answer tuple = current;
      current = null;
      return tuple;
    } else {
      throw new NoSuchElementException("No more results.");
    }

  }

  public Double getMaxScore() {
    return this.maxScore;
  }

  public String getPartQuery() {
    return this.input.toString();
  }
}
