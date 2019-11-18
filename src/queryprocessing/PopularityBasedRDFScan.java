package de.mpii.trinitreloaded.queryprocessing;

import org.apache.jena.query.*;
import virtuoso.jena.driver.*;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.common.collect.Maps;
import de.mpii.trinitreloaded.datastructures.Answer;
import de.mpii.trinitreloaded.datastructures.HashMapBasedAnswer;
import de.mpii.trinitreloaded.datastructures.TriplePattern;
import de.mpii.trinitreloaded.utils.Config;
import de.mpii.trinitreloaded.utils.GraphConnection;
import de.mpii.trinitreloaded.utils.Logger;
import de.mpii.trinitreloaded.utils.Config.LoggingLevel;

/**
 * A scan operator for individual {@link TriplePattern} over RDF DB.
 *
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 * 
 */
public class PopularityBasedRDFScan implements Operator{
  final TriplePattern input;
  VirtGraph conn;
  Query stmt;
  ResultSet results;
  QuerySolution rs;
  String dbCmd;
  boolean isOpen;
  Answer current;
  double maxScore = Double.NaN;
  boolean toBeCounted;

  public PopularityBasedRDFScan(TriplePattern input) {
    this.toBeCounted = true;
    this.input = input;
  }

  @Override
  public String toString() {
    return "PopularityBasedRDFScan [input=" + input + "]";
  }

  public PopularityBasedRDFScan(TriplePattern input, boolean toBeCounted) {
    this.toBeCounted = toBeCounted;
    this.input = input;
  }

  public boolean open() {
    if (this.isOpen) {
      return true;
    }
    dbCmd = getDBCommand();
    conn = GraphConnection.getConnection();
    stmt = QueryFactory.create(dbCmd);
    VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create (stmt, conn);
    results = vqe.execSelect();
    this.isOpen = true;
    current = null;
    if (conn != null) {
      return true;
    } else {
      return false;
    }
  }

  private String getDBCommand() {
    Map<String,String> vars = Maps.newHashMap();
    String cmd ="";


    cmd += "SELECT";
    if (this.input.isSubjectConst) {
      vars.put("subject", format(this.input.subject));
    }
    else{
      vars.put("subject", "?s");
      cmd += " ?s";
    }

    if (this.input.isPredicateConst) {
      vars.put("predicate", format(this.input.predicate));
    } 
    else{
      vars.put("predicate", "?p");
      cmd += " ?p";
    }
    if(this.input.isObjectResource){ 
      cmd +=
          " ?score FROM NAMED "+  format(this.input.object) + " FROM NAMED "+ Config.rdfScoreTableName;
    }
    else{
      cmd +=
          " ?score FROM NAMED "+  format(this.input.object);
    }

    cmd += " WHERE {";

    if(this.input.isObjectResource){
      cmd +=
          " GRAPH "+  format(this.input.object) + " { "+ vars.get("subject") + " " + vars.get("predicate") + " ?c .} GRAPH " 
              + Config.rdfScoreTableName + " { "+ vars.get("subject") + " <http://xkg/hasCount> ?score .}";
    }
    else{
      cmd +=
          " GRAPH "+  format(this.input.object) + " { "+ vars.get("subject") + " " + vars.get("predicate") + " ?score .} ";
    }

    cmd+="} ORDER BY DESC(?score)";
    Logger.println("PopularityBasedRDFScan cmd: " + cmd, LoggingLevel.VARIABLEVALUES);
    return cmd;
  }

  private String format(String str) {
    str = str.replace(" ", "_");
    if (str.contains(":")) {
      return "<"+str+">";
    }
    else if (str.startsWith("<")){
      return str.replace("<", "<"+Config.graphURI);
    }
    return "<" + Config.graphURI + str +">";
  }

  public double getScoreMultiplier() {
    if (Double.isNaN(maxScore)) {
      try {
        maxScore = rs.getLiteral("score").getInt();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return 1 / maxScore;
  }

  public boolean close(){
    if (!isOpen) {
      return true;
    }
    conn.close();
    this.isOpen = false;
    return true;
  }

  public boolean hasNext() {
    if (current != null) {
      return true;
    }
    try {
      if (results.hasNext()) {
        rs = results.nextSolution();
        HashMapBasedAnswer ans = new HashMapBasedAnswer(toBeCounted);
        if (!this.input.isSubjectConst) {
          String var = this.input.subject;
          String binding = rs.get("s").toString();
          ans.setVariableBinding(var, binding);
        }
        if (!this.input.isObjectConst) {
          String var = this.input.object;
          String binding = rs.get("o").toString();
          ans.setVariableBinding(var, binding);
        }
        if (!this.input.isPredicateConst) {
          String var = this.input.predicate;
          String binding = rs.get("p").toString();
          ans.setVariableBinding(var, binding);
        }
        Double score = rs.getLiteral("score").getInt() * getScoreMultiplier() * Config.scoreMultipler;
        ans.setScore(score);
        /*if(ans.getScore()==0.0){
          return false;
        }*/
        ans.setQuery(this.input.toString());
        //Debug.println("CurrentAns:" + ans.toString(), Config.debugLevel.VARIABLEVALUES);
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
