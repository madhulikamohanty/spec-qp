package de.mpii.trinitreloaded.queryprocessing;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import de.mpii.trinitreloaded.datastructures.Answer;
import de.mpii.trinitreloaded.datastructures.LogicalQueryPlan;
import de.mpii.trinitreloaded.datastructures.MultiBucketHistogram;
import de.mpii.trinitreloaded.datastructures.TriplePatternInPlan;
import de.mpii.trinitreloaded.datastructures.ProbabilityDistribution;
import de.mpii.trinitreloaded.datastructures.Query;
import de.mpii.trinitreloaded.datastructures.ScoreProbabilityDistributionFunction;
import de.mpii.trinitreloaded.datastructures.TriplePattern;
import de.mpii.trinitreloaded.utils.Config;
import de.mpii.trinitreloaded.utils.Config.LoggingLevel;
import de.mpii.trinitreloaded.utils.Config.PlanType;
import de.mpii.trinitreloaded.utils.Convolution;
import de.mpii.trinitreloaded.utils.DBConnection;
import de.mpii.trinitreloaded.utils.Logger;
import de.mpii.trinitreloaded.utils.Timer;

/**
 * A {@link LogicalQueryPlan} generator for a given {@link Query}.
 *
 * Following construction, the entry point is <code>generateQueryPlan()</code>.
 *
 *
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 */

public class QueryPlanner {

  Query q;
  List<ProbabilityDistribution> triplePatternPDFs;
  ProbabilityDistribution allTriplePatternJoinPDF;
  public long duration;
  PlanType planType;

  public QueryPlanner(Query q) {
    this.q = q;
    this.triplePatternPDFs = new ArrayList<ProbabilityDistribution>();
    this.duration = 0;
  }

  /**
   * Generates a query plan for the query using the scheme indicated by the {@link PlanType}.
   *
   * <ul>
   * <li>NONSPECULATIVE:Non-Speculative</li>
   * <li>SPECULATIVEWITHDISJUNCTION:Speculative with disjunctive execution of highly selective
   * triple patterns</li>
   * <li>FULLYSPECULATIVE:Fully Speculative</li>
   * <li>ORIGINAL: Original query with no relaxations</li>
   * <li>SINGLESPECULATIVE: Single thread Speculative plan, identical to FULLYSPECULATIVE now. 
   * It was meant for use when SPeculative plan also had a multi-threaded version.</li>
   * </ul>
   */
  public LogicalQueryPlan generateQueryPlan(PlanType val) {
    this.planType = val;
    switch (val) {
    case NONSPECULATIVE:
      return generateNonSpeculativeQueryPlan();
    case SPECULATIVE_WITH_DISJUNCTION:
      throw new UnsupportedOperationException("SPECULATIVE_WITH_DISJUNCTION");
    case FULLYSPECULATIVE:
      return generateFullySpeculativeQueryPlan();
    case ORIGINAL:
      return generateOriginalQueryPlan();
    case SINGLESPECULATIVE:
      return generateFullySpeculativeQueryPlan();
    default:
      throw new UnsupportedOperationException("Invalid plan type");
    }
  }

  /**
   * Generates a {@link LogicalQueryPlan} for the original query without any relaxations.
   *
   * @return A {@link LogicalQueryPlan} for original query execution only.
   */
  private LogicalQueryPlan generateOriginalQueryPlan() {
    Set<TriplePatternInPlan> plan = Sets.newHashSet();
    Set<TriplePattern> newPartition = new HashSet<TriplePattern>();
    for (int i = 0; i < q.triplePatterns.size(); i++) {
      newPartition.add(q.triplePatterns.get(i));
    }
    plan.add(new TriplePatternInPlan(newPartition, false));
    LogicalQueryPlan queryplan = new LogicalQueryPlan(plan, Config.PlanType.ORIGINAL);
    return queryplan;
  }

  /**
   * Generates a {@link LogicalQueryPlan} for non speculative execution.
   *
   * @return A {@link LogicalQueryPlan} for non speculative execution.
   */
  private LogicalQueryPlan generateNonSpeculativeQueryPlan() {
    Set<TriplePatternInPlan> plan = Sets.newHashSet();
    for (int i = 0; i < q.triplePatterns.size(); i++) {
      Set<TriplePattern> tpSet = new HashSet<TriplePattern>();
      tpSet.add(q.triplePatterns.get(i));
      plan.add(new TriplePatternInPlan(tpSet, true));
    }
    LogicalQueryPlan queryplan = new LogicalQueryPlan(plan, Config.PlanType.NONSPECULATIVE);
    return queryplan;
  }

  /**
   * Generates a {@link LogicalQueryPlan} for fully speculative execution.
   *
   * @return A {@link LogicalQueryPlan} for fully speculative execution.
   */
  private LogicalQueryPlan generateFullySpeculativeQueryPlan() {

    // TODO Obtain all stats that should not be timed (ideally part of the stats we maintain)
    constructTriplePatternsPDF();

    // TODO Now do real work: create the plan
    // Timer timer = new Timer();
    // timer.start();
    // Debug.println(getJoinCardinality(this.q.triplePatterns.size()-1));
    constructAllTriplePatternsJoinPDF();
    Logger.println("Original Join PDF:"+this.allTriplePatternJoinPDF, LoggingLevel.EXPERIMENTS);
    Set<TriplePattern> mainPartition = Sets.newHashSet(q.triplePatterns);
    Set<TriplePatternInPlan> plan = Sets.newHashSet();
    for (int i = 0; i < q.triplePatterns.size(); i++) {
      boolean toBePartitioned = isRelaxationLikely(i);
      Logger.println("Partition required for " + q.triplePatterns.get(i) + "?" + toBePartitioned,
          LoggingLevel.EXPERIMENTS);
      /**
       * Partition this triple pattern if its relaxation is likely.
       */
      if (toBePartitioned == true) {
        Logger.println("Partitioning a triple pattern:" + q.triplePatterns.get(i).toString(),
            LoggingLevel.INTERMEDIATEINFO);
        /**
         * Create a separate partition for this triple pattern. Do this only if the main partition
         * has more than a single triple pattern.
         */
        if (mainPartition.size() > 0) {
          mainPartition.remove(q.triplePatterns.get(i));
          Set<TriplePattern> newPartition = new HashSet<TriplePattern>();
          newPartition.add(q.triplePatterns.get(i));
          plan.add(new TriplePatternInPlan(newPartition,true));
        }
      }
    }
    if(mainPartition.size()>0)
      plan.add(new TriplePatternInPlan(mainPartition,false));
    LogicalQueryPlan queryplan = new LogicalQueryPlan(plan, this.planType);
    // timer.stop();
    // timer.getDuration();
    return queryplan;
  }

  /**
   * Generates the {@link ProbabilityDistribution} of the original join result from the
   * {@link Query}.
   */
  private void constructAllTriplePatternsJoinPDF() {

    allTriplePatternJoinPDF = this.triplePatternPDFs.get(0);
    for (int j = 1; j < this.q.triplePatterns.size(); j++) {
      double joinCardinality = getJoinCardinality(j);
      double joinSelectivity =
          joinCardinality
          / (allTriplePatternJoinPDF.getNumResults() * this.triplePatternPDFs.get(j).getNumResults());
      Logger.println("Joining " + j + " with joinCardinality:" + joinCardinality
          + " and joinSelectivity:" + joinSelectivity, LoggingLevel.INTERMEDIATEINFO);
      if (joinCardinality == 0) {
        if(Config.histType==Config.HistogramType.POWERLAW)
          allTriplePatternJoinPDF = new ScoreProbabilityDistributionFunction(0.0,0.0,0.0,0,0);
        else
          allTriplePatternJoinPDF = new MultiBucketHistogram(new ArrayList<Double>(),new ArrayList<Double>(),0.0,0,0.0,0);
        return;
      }
      Timer t = new Timer();
      t.start();
      allTriplePatternJoinPDF =
          Convolution.convolute(allTriplePatternJoinPDF, this.triplePatternPDFs.get(j),
              joinSelectivity);
      t.stop();
      this.duration += t.getDuration();
    }
  }

  /**
   * Computes the join cardinality without relaxation.
   *
   * @param j
   *          The number of {@link TriplePattern} in the {@link Query} whose join cardinality is
   *          sought.
   * @return The join cardinality of joining first {@code j} {@link TriplePattern}.
   */

  private double getJoinCardinality(int j) {
    return getJoinCardinality(j, false, null, -1);
  }

  /**
   * Computes the join cardinality of a join.
   *
   * @param endIndex
   *          The number of {@link TriplePattern} in the {@link Query} whose join cardinality is
   *          sought.
   * @param relaxation
   *          Whether there is any relaxed {@link TriplePattern}.
   * @param relaxed
   *          If {@code relaxation} is {@code true}, then it holds the relaxed {@link TriplePattern}.
   * @param relaxedIndex
   *          The index of the {@link TriplePattern} which is relaxed.
   * @return The join cardinality of joining first {@code endIndex} {@link TriplePattern}.
   */
  private double getJoinCardinality(int endIndex, boolean relaxation, TriplePattern relaxed,
      int relaxedIndex) {

    String cmd = "SELECT count(*) as tcount FROM " ;
    
    char tbl_name = 'a';
    List<String> tblClause = Lists.newArrayList();
    if(!Config.isSyntheticData){
      for (int i = 0; i <= endIndex; i++) {
        if (i == relaxedIndex && relaxation && !relaxed.isObjectResource) {
          tblClause.add(Config.textualTypeDataTableName + " " + tbl_name);
        } else {
          tblClause.add(Config.dataTableName + " " + tbl_name);
        }
        tbl_name++;
      }
    }
    else{
      for (int i = 0; i <= endIndex; i++) {
        tblClause.add(Config.syntheticDataTableName + " " + tbl_name);
        tbl_name++;
      }
    }
    
    cmd += Joiner.on(" , ").join(tblClause);
    /* Needed for evaluating exact counts.*/
    if(!Config.isSyntheticData){
      cmd += ", " + Config.scoreTableName + " WHERE a.subject=entity AND "; // TODO: This is assuming that all the queries query for only one resource.
    }
    else
      cmd+=" WHERE ";
    
    HashMap<String, String> mapOfVariablesToRelations =
        getMapOfVariablesToRelations(this.q, relaxation, relaxed, relaxedIndex);
    List<String> whereClause = Lists.newArrayList();
    char tblname = 'a';
    TriplePattern tp = null;
    for (int i = 0; i <= endIndex; i++) {
      if (i == relaxedIndex && relaxation) {
        tp = relaxed;
      } else {
        tp = this.q.triplePatterns.get(i);
      }
      if (tp.isSubjectConst) {
        whereClause.add(tblname + ".subject='" + format(tp.subject) + "' ");
      } else {
        whereClause.add(tblname + ".subject=" + mapOfVariablesToRelations.get(tp.subject) + " ");
      }
      if (tp.isPredicateConst) {
        whereClause.add(tblname + ".predicate='" + format(tp.predicate) + "' ");
      } else {
        whereClause
        .add(tblname + ".predicate=" + mapOfVariablesToRelations.get(tp.predicate) + " ");
      }
      if (tp.isObjectConst) {
        whereClause.add(tblname + ".object='" + format(tp.object) + "' ");
      } else {
        whereClause.add(tblname + ".object=" + mapOfVariablesToRelations.get(tp.object) + " ");
      }
      tblname++;
    }
    cmd += Joiner.on(" AND ").join(whereClause);
    Long count = (long) 0;
    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      conn = DBConnection.getConnection();
      stmt = conn.createStatement();
      Logger.println("Querying for join cardinality:" + cmd, LoggingLevel.VARIABLEVALUES);
      rs = stmt.executeQuery(cmd);
      while (rs.next()) {
        count = rs.getLong("tcount");
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (stmt != null) {
          stmt.close();
        }
        if (conn != null) {
          conn.close();
        }
      } catch (Exception e) {
      }
    }
    return count;
  }

  /**
   * Constructs a map of the variables to the first occurrence in the query for later use in
   * construction of the database query.
   *
   * @return A map of the variables to the first occurrence in the query.
   */
  private HashMap<String, String> getMapOfVariablesToRelations(Query q2, boolean relaxation,
      TriplePattern relaxed, int relaxedIndex) {
    HashMap<String, String> map = new HashMap<String, String>();
    TriplePattern tp = null;
    char tbl_name = 'a';
    for (int i = 0; i < q2.triplePatterns.size(); i++) {
      if (i == relaxedIndex && relaxation) {
        tp = relaxed;
      } else {
        tp = q2.triplePatterns.get(i);
      }
      if (!tp.isSubjectConst && !map.containsKey(tp.subject)) {
        map.put(tp.subject, tbl_name + ".subject");
      }
      if (!tp.isPredicateConst && !map.containsKey(tp.predicate)) {
        map.put(tp.predicate, tbl_name + ".predicate");
      }
      if (!tp.isObjectConst && !map.containsKey(tp.object)) {
        map.put(tp.object, tbl_name + ".object");
      }
      tbl_name++;
    }
    return map;
  }

  /**
   * Returns <code>true</code> if relaxation is likely.
   *
   * A relaxation is likely for Incremental Weighting only when the original join does not have 'k' results. Otherwise, a relaxation is 
   * likely if the expected value of k'th score of the original join PDF is less
   * than the expected highest score of the relaxation join.
   */
  private boolean isRelaxationLikely(int i) {
    if(Config.isIncrementalWeighting){ 
      Relaxation topmostRelaxation = findTopmostRelaxation(i);
      if (topmostRelaxation == null) {
        return false;
      }
      TriplePattern tp = q.triplePatterns.get(i);
      TriplePattern tp_relaxed = null;
      switch(topmostRelaxation.field){
      case 0:
        tp_relaxed = new TriplePattern(topmostRelaxation.relaxation, tp.predicate, tp.object);
        break;

      case 1:
        tp_relaxed = new TriplePattern(tp.subject, topmostRelaxation.relaxation, tp.object);
        break;

      case 2:
        tp_relaxed = new TriplePattern(tp.subject, tp.predicate, topmostRelaxation.relaxation);
        break;
      }

      double joinCardinality = getJoinCardinality(this.q.triplePatterns.size()-1, true, tp_relaxed, i);
      if(joinCardinality>=Config.k)
        return false;
      else
        return true;
    }
    else{
      ProbabilityDistribution relaxationJoinPDF = constructRelaxationJoinPDF(i);
      if (relaxationJoinPDF == null) {
        return false;
      }
      Timer t = new Timer();
      t.start();
      double E_k_originalJoin = this.allTriplePatternJoinPDF.getPercentile(Config.k);
      double E_1_relaxationJoin = relaxationJoinPDF.getPercentile(1);

      // Round up the values upto 2 decimal places.
      E_k_originalJoin = Math.round(E_k_originalJoin*100.0)/100.0;
      E_1_relaxationJoin = Math.round(E_1_relaxationJoin*100.0)/100.0;

      Logger.println("E_k_originalJoin:" + E_k_originalJoin + " E_1_relaxationJoin:"
          + E_1_relaxationJoin, Config.LoggingLevel.EXPERIMENTS);
      t.stop();
      this.duration += t.getDuration();
      if (E_k_originalJoin < E_1_relaxationJoin) {
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Constructs the join {@link ProbabilityDistribution} with relaxation.
   *
   * @param i
   *          The index of the {@link TriplePattern} in the {@link Query} which is to be relaxed.
   * @return {@link ProbabilityDistribution} of the join with the {@code i} th
   *         {@link TriplePattern} relaxed.
   */
  private ProbabilityDistribution constructRelaxationJoinPDF(int i) {
    TriplePattern tp = q.triplePatterns.get(i);

    Timer t = new Timer();
    t.start();
    Relaxation topmostRelaxation = findTopmostRelaxation(i);
    t.stop();
    this.duration += t.getDuration();

    if (topmostRelaxation == null) {
      return null;
    }
    Logger.print("Topmost Relaxation:" + topmostRelaxation.relaxation, LoggingLevel.VARIABLEVALUES);
    TriplePattern tp_relaxed = null;
    switch(topmostRelaxation.field){
    case 0:
      tp_relaxed = new TriplePattern(topmostRelaxation.relaxation, tp.predicate, tp.object);
      break;

    case 1:
      tp_relaxed = new TriplePattern(tp.subject, topmostRelaxation.relaxation, tp.object);
      break;

    case 2:
      tp_relaxed = new TriplePattern(tp.subject, tp.predicate, topmostRelaxation.relaxation);
      break;
    }

    ProbabilityDistribution relaxationPDF =
        generatePDF(tp_relaxed, topmostRelaxation.weight);
    Logger.println("Relaxation PDF:"+relaxationPDF, LoggingLevel.EXPERIMENTS);
    if (relaxationPDF == null) {
      return null;
    }
    ProbabilityDistribution relaxationJoinPDF = this.triplePatternPDFs.get(0);

    for (int j = 1; j < this.q.triplePatterns.size(); j++) {
      if (j != i) {
        double joinCardinality = getJoinCardinality(j, true, tp_relaxed, i);
        double joinSelectivity =
            joinCardinality
            / (relaxationJoinPDF.getNumResults() * this.triplePatternPDFs.get(j).getNumResults());
        if (joinCardinality == 0) {
          return null;
        }

        Timer t_conv = new Timer();
        t_conv.start();
        relaxationJoinPDF =
            Convolution
            .convolute(relaxationJoinPDF, this.triplePatternPDFs.get(j), joinSelectivity);
        t_conv.stop();
        this.duration += t_conv.getDuration();

      } else {
        double joinCardinality = getJoinCardinality(j, true, tp_relaxed, i);
        double joinSelectivity =
            joinCardinality / (relaxationJoinPDF.getNumResults() * relaxationPDF.getNumResults());
        if (joinCardinality == 0) {
          return null;
        }

        Timer t_conv = new Timer();
        t_conv.start();
        relaxationJoinPDF =
            Convolution.convolute(relaxationJoinPDF, relaxationPDF, joinSelectivity);
        t_conv.stop();
        this.duration += t_conv.getDuration();

      }
    }
    Logger.println("Relaxation Join PDF:"+relaxationJoinPDF, LoggingLevel.EXPERIMENTS);
    return relaxationJoinPDF;
  }

  /**
   * Finds the topmost relaxation.
   *
   * @param i
   *          The index of the {@link TriplePattern} whose relaxation is sought.
   * @return The topmost {@link Relaxation} for this {@link TriplePattern}.
   */
  private Relaxation findTopmostRelaxation(int i) {

    TriplePattern tp = this.q.triplePatterns.get(i);
    Relaxation topmostRelaxation = null;
    String relaxationRelationalTable="";
    String seekField="", fetchField1="", fetchField2="";
    int field=-1;

    /**
     * Check if the object is to be relaxed.
     */
    if(tp.isObjectConst){
      if (tp.isObjectResource) { 
        seekField = "semantic_type"; 
        if(!Config.isSyntheticData)
          relaxationRelationalTable = Config.semanticTextualParaphrasesTblName;
        else
          relaxationRelationalTable = Config.syntheticParaphraseTblName;
        fetchField1 = "textual_type";
        fetchField2 = "prob_textual_type_given_semantic_type";
      } else if(!tp.isObjectResource){
        seekField = "textual_type";
        if(!Config.isSyntheticData)
          relaxationRelationalTable = Config.textualSemanticParaphrasesTblName;
        else
          relaxationRelationalTable = Config.syntheticParaphraseTblName;
        fetchField1 = "semantic_type";
        fetchField2 = "prob_semantic_type_given_textual_type";
      }
      String cmd =
          "SELECT " + fetchField1 + "," + fetchField2 + " FROM " + relaxationRelationalTable
          + " WHERE " + seekField + " = '" + tp.object + "' ORDER BY " + fetchField2
          + " DESC LIMIT 1";
      field = 2;
      topmostRelaxation = getRelaxationFromDB(cmd,field);
    }

    if(!Config.onlyObjectRelaxed){ // If relaxation is allowed for predicates and subjects, check for them.
      /**
       * Check if the predicate is to be relaxed.
       */
      if(tp.isPredicateConst && !Config.isSyntheticData){
        if (tp.isPredicateResource) { 
          seekField = "predicate";
          relaxationRelationalTable = Config.predicateRelationParaphraseTblName;
          // fetchField = "relation";
          fetchField1 = "relation";
          fetchField2 = "cp";
        } else if(!tp.isPredicateResource){
          seekField = "relation";
          relaxationRelationalTable = Config.relationRelationParaphraseTblName;
          fetchField1 = "paraphrase";
          fetchField2 = "cp";
        }
        String cmd =
            "SELECT " + fetchField1 + "," + fetchField2 + " FROM " + relaxationRelationalTable
            + " WHERE " + seekField + " = '" + tp.predicate + "' ORDER BY " + fetchField2
            + " DESC LIMIT 1";
        field = 1;
        Relaxation rP = getRelaxationFromDB(cmd,field);
        if(topmostRelaxation==null)
          topmostRelaxation = rP;
        if(rP!=null && topmostRelaxation.weight<rP.weight)
          topmostRelaxation = rP;
      }
      /**
       * Check if the subject is to be relaxed.
       */
      if(tp.isSubjectConst && !Config.isSyntheticData){
        if (tp.isSubjectResource) { 
          seekField = "semantic_type"; 
          relaxationRelationalTable = Config.semanticTextualParaphrasesTblName;
          fetchField1 = "textual_type";
          fetchField2 = "prob_textual_type_given_semantic_type";
        } else if(!tp.isSubjectResource){
          seekField = "textual_type";
          relaxationRelationalTable = Config.textualSemanticParaphrasesTblName;
          fetchField1 = "semantic_type";
          fetchField2 = "prob_semantic_type_given_textual_type";
        }
        String cmd =
            "SELECT " + fetchField1 + "," + fetchField2 + " FROM " + relaxationRelationalTable
            + " WHERE " + seekField + " = '" + tp.subject + "' ORDER BY " + fetchField2
            + " DESC LIMIT 1";
        field = 0;
        Relaxation rS = getRelaxationFromDB(cmd,field);
        if(topmostRelaxation==null)
          topmostRelaxation = rS;
        if(rS!=null && topmostRelaxation.weight<rS.weight)
          topmostRelaxation = rS;
      }
    }
    return topmostRelaxation;
  }

  private Relaxation getRelaxationFromDB(String cmd, int field) {
    Connection conn = null;
    Statement statement = null;
    ResultSet rs = null;
    try {
      conn = DBConnection.getConnection();
      Logger.print("Getting relaxation:" + cmd, LoggingLevel.VARIABLEVALUES);
      statement = conn.createStatement();
      rs = statement.executeQuery(cmd);
      while (rs.next()) {
        return new Relaxation(rs.getString(1), rs.getDouble(2), field);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
        if (statement != null) {
          statement.close();
        }
        if (conn != null) {
          conn.close();
        }
      } catch (Exception e) {
      }
    }
    return null;
  }

  /**
   * Constructs the {@link ProbabilityDistribution} of a weighted {@link TriplePattern}
   * .
   *
   * @param tp
   *          The {@link TriplePattern} whose {@link ProbabilityDistribution} is to be
   *          constructed.
   * @param weight
   *          The weight of the {@link TriplePattern}. It is 1.0 if the {@link TriplePattern} is
   *          unrelaxed.
   * @return The {@link ProbabilityDistribution} of {@code tp}.
   */
  private ProbabilityDistribution generatePDF(TriplePattern tp, double weight) {
    ProbabilityDistribution pdf = null;

    if(Config.histType==Config.HistogramType.POWERLAW)
      pdf = get2BucketPDF(tp, weight);
    else
      pdf = getMultiBucketPDF(tp, weight);

    return pdf;
  }

  /**
   * Function to generate a {@link ScoreProbabilityDistributionFunction} for input {@link TriplePattern}.
   * 
   * @param tp Input {@link TriplePattern}.
   * @param weight Weight, if the input {@code tp} is relaxed.
   * @return A {@link ScoreProbabilityDistributionFunction} for input {@link TriplePattern}.
   */
  private ProbabilityDistribution get2BucketPDF(TriplePattern tp, double weight) {
    ProbabilityDistribution pdf = null;
    long inflectionRank = 0;
    double totalCumulativeScore= 0;
    if(Config.adaptiveInflectionRank)
      totalCumulativeScore = getSumOfTotalScore(tp);
    else
      inflectionRank = Config.inflectionRank;

    Operator sc;

    if(Config.isSyntheticData){
      if(Config.isRDFDB)
        sc = new SyntheticRDFScan(tp, false);
      else
        sc = new SyntheticScan(tp, false);
    }
    else if(Config.isRDFDB)
    {
      sc = new PopularityBasedRDFScan(tp, false);
    }
    else
    {
      sc = new PopularityBasedScan(tp, false);
    }

    long rankR = 0;
    try {

      double scoreAtRankR = 0.0;
      double cumulativeScoreAtRankR = 0.0;
      double cumulativeScoreAtRankN = 0.0;
      double maxScore = 0.0;

      sc.open();
      int resultsProcessed = 0;
      while(sc.hasNext()){

        Answer a = sc.next();
        double count = a.getScore();
        if (resultsProcessed == 0) {
          maxScore = count; // It will always be 1.0 in our current setting where scores for a triple pattern are normalized by the maximum score.
        }
        if(Config.adaptiveInflectionRank){
          resultsProcessed++;
          if (cumulativeScoreAtRankR <= totalCumulativeScore*Config.fractionOfScoreInTheHead) {
            rankR = resultsProcessed;
            cumulativeScoreAtRankR += count;
            scoreAtRankR = count;
          }
        }
        else{
          if (resultsProcessed <= inflectionRank) {
            cumulativeScoreAtRankR += count;
            scoreAtRankR = count;
          }
        }
        cumulativeScoreAtRankN += count;
      }

      pdf = new ScoreProbabilityDistributionFunction(scoreAtRankR * weight, cumulativeScoreAtRankR * weight, cumulativeScoreAtRankN * weight, resultsProcessed, (maxScore) * weight);

      if(Config.adaptiveInflectionRank)
        Logger.println("RankR for:"+tp+" is:"+rankR, LoggingLevel.VARIABLEVALUES);

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (sc != null) {
          sc.close();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return pdf;
  }

  /**
   * Function to generate a {@link MultiBucketHistogram} for input {@link TriplePattern}.
   * 
   * @param tp Input {@link TriplePattern}.
   * @param weight Weight, if the input {@code tp} is relaxed.
   * @return A {@link MultiBucketHistogram} for input {@link TriplePattern}.
   */
  private ProbabilityDistribution getMultiBucketPDF(TriplePattern tp, double weight) {
    return new MultiBucketHistogram(tp, weight);
  }

  /**
   * Get total score from a {@link TriplePattern}.
   * 
   * @param tp
   *        The {@link TriplePattern} whose total score is required.
   * @return The sum of the scores from the matches of {@code tp}.
   */
  private double getSumOfTotalScore(TriplePattern tp) {
    Operator sc;
    if(Config.isSyntheticData){
      if(Config.isRDFDB)
        sc = new SyntheticRDFScan(tp, false);
      else
        sc = new SyntheticScan(tp, false);
    }
    else if(Config.isRDFDB)
    {
      sc = new PopularityBasedRDFScan(tp, false);
    }
    else
    {
      sc = new PopularityBasedScan(tp, false);
    }
    double totalScore = 0.0;
    try {
      sc.open();
      while(sc.hasNext()){
        Answer a = sc.next();
        totalScore+=a.getScore();
      }
      sc.close();
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return totalScore;
  }

  public static String format(String str) {
    str = str.replace("'", "''");
    if (!str.startsWith("<")) {
      return "<" + str + ">";
    } else {
      return str;
    }
  }

  /**
   * Constructs the {@link ProbabilityDistribution} of all the {@link TriplePattern} in
   * the query.
   */
  private void constructTriplePatternsPDF() {
    for (int i = 0; i < q.triplePatterns.size(); i++) {
      TriplePattern tp = q.triplePatterns.get(i);
      Logger.print("Constructing pdf for:" + tp.toString(), LoggingLevel.INTERMEDIATEINFO);
      ProbabilityDistribution pdf = generatePDF(tp, 1);
      this.triplePatternPDFs.add(pdf);
      Logger.println("TriplePatternPDF-" + (i + 1) + ":" + pdf, LoggingLevel.EXPERIMENTS);
    }
  }

  public long getDuration() {
    return this.duration;
  }

  /**
   * A class representing relaxation for a triple pattern.
   * 
   * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
   *
   */
  public class Relaxation {
    public final String relaxation;
    public final double weight;
    public final int field; /** 0=subject; 1=predicate; 2=object**/

    public Relaxation(String r, double w, int field) {
      this.relaxation = r;
      this.weight = w;
      this.field = field;
    }
  }

}
