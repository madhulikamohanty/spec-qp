package de.mpii.trinitreloaded.experiments;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import de.mpii.trinitreloaded.datastructures.Answer;
import de.mpii.trinitreloaded.datastructures.HashMapBasedAnswer;
import de.mpii.trinitreloaded.datastructures.LogicalQueryPlan;
import de.mpii.trinitreloaded.datastructures.Query;
import de.mpii.trinitreloaded.queryprocessing.Operator;
import de.mpii.trinitreloaded.queryprocessing.QueryParser;
import de.mpii.trinitreloaded.queryprocessing.QueryPlanner;
import de.mpii.trinitreloaded.queryprocessing.RJCodeGen;
import de.mpii.trinitreloaded.utils.Config;
import de.mpii.trinitreloaded.utils.Logger;
import de.mpii.trinitreloaded.utils.Timer;
import de.mpii.trinitreloaded.utils.Config.LoggingLevel;
import de.mpii.trinitreloaded.utils.Config.PlanType;
import de.mpii.trinitreloaded.queryprocessing.RankJoin;

/**
 * Runs experiments on real data from YAGO+Clueweb(XKG) triples and Twitter tweets with
 * {@link RJCodeGen}.
 * 
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 */
public class RankJoinExperiments extends Experiments {

  @Override
  public void oneTimeRun() {
    // Take input query.
    String query=takeInput();
    List<Answer> answers = processRJQuery(query, PlanType.FULLYSPECULATIVE, 0);
    List<Answer> groundTruthAnswers = processRJQuery(query, PlanType.NONSPECULATIVE, 0);
    Precision pr = new Precision(new SimpleSetMeasureGroundTruthProvider(new HashSet<Answer>(groundTruthAnswers)));
    Recall rcall = new Recall(new SimpleSetMeasureGroundTruthProvider(new HashSet<Answer>(groundTruthAnswers)));
    Logger.println("Precision:"+pr.score(new HashSet<Answer>(answers)), LoggingLevel.EXPERIMENTS);
    Logger.println("Recall:"+rcall.score(new HashSet<Answer>(answers)), LoggingLevel.EXPERIMENTS);

  }

  @Override
  public void executeExperiments() {
    for(int kVal: Config.kVals){
      Config.k=kVal;
      executeOneRJRun();
      //collateResults();
    }
  }

  private void executeOneRJRun() {

    Logger.startLog();
    // Read queries from file
    String query;
    BufferedReader br;
    try {
      br = new BufferedReader(new FileReader(Config.queryFile));
      while ((query = br.readLine()) != null) {
        query = br.readLine();
        Logger.println("Next Query:"+query, LoggingLevel.EXPERIMENTS);
        Precision pr = null;
        Recall rcall = null;

        // Run Non-Spec plan Config.numRuns times.
        Logger.println("Running Non-Spec " + Config.numRuns + " times.", LoggingLevel.EXPERIMENTS);
        for(int runCount=0;runCount<Config.numRuns;runCount++){
          List<Answer> groundTruthAnswers = processRJQuery(query, PlanType.NONSPECULATIVE, runCount);
          pr = new Precision(new SimpleSetMeasureGroundTruthProvider(new HashSet<Answer>(groundTruthAnswers)));
          rcall = new Recall(new SimpleSetMeasureGroundTruthProvider(new HashSet<Answer>(groundTruthAnswers)));
        }

        // Run Full-Spec plan Config.numRuns times.
        Logger.println("Running Full-Spec " + Config.numRuns + " times.", LoggingLevel.EXPERIMENTS);
        for(int runCount=0;runCount<Config.numRuns;runCount++){
          List<Answer> answers = processRJQuery(query, PlanType.FULLYSPECULATIVE, runCount);
          Logger.println("Precision-FullSpec:"+pr.score(new HashSet<Answer>(answers)), LoggingLevel.EXPERIMENTS);
          Logger.println("Recall-FullSpec:"+rcall.score(new HashSet<Answer>(answers)), LoggingLevel.EXPERIMENTS);
        }
        br.readLine();
      }
    } catch (FileNotFoundException e1) {
      e1.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    Logger.stopLog();
    collateResults();
  }

  protected static List<Answer> processRJQuery(String query, PlanType planType, int runCount) {
    // Make count=0 for no. of answer objects
    HashMapBasedAnswer.countOfAnswerObjects = 0;
    Logger.println("Plan Type:"+planType.toString(), LoggingLevel.EXPERIMENTS);
    // Parse the query.
    QueryParser qp = new QueryParser();
    Query q = qp.parse(query);

    // Generate a query plan
    QueryPlanner qplanner = new QueryPlanner(q);

    /** 
     * Choose appropriate {@link Config.PlanType}
     */
    LogicalQueryPlan qplan = qplanner.generateQueryPlan(planType); 
    if(runCount == 0)
      Logger.println("Query Plan:"+qplan.toString(),LoggingLevel.EXPERIMENTS);

    /**
     * Time the execution of the {@link LogicalQueryPlan}.
     */
    Timer t = new Timer();
    t.start();

    // Execute Query
    List<Answer> answersTopK = new ArrayList<Answer>();
    // Actual code for experiments.
    switch(planType){
    case ORIGINAL:
    case SINGLESPECULATIVE:
    case FULLYSPECULATIVE:
    case NONSPECULATIVE:
      RJCodeGen cg = new RJCodeGen(qplan);
      Operator result = cg.translate();
      try {
        result.open();
        for (int i=0;i<Config.k;i++){
          if(result.hasNext()){
            Answer a = result.next();
            a.setScore(Math.round(a.getScore()*100.0)/100.0);
            answersTopK.add(a);
          }
          else
            break;
        }
        result.close();
      } catch (SQLException e) {
        e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      }
      break;

    default:
      break;
    }
    t.stop();

    for(int i=0;i<answersTopK.size();i++){
      Answer a = answersTopK.get(i);
      Logger.println("Answer-"+(i+1), LoggingLevel.EXPERIMENTS);
      for(String var : a.getVariables()){
        try {
          Logger.println("Variable "+var+":"+a.getVariableBinding(var).replace(Config.graphURI, ""), LoggingLevel.EXPERIMENTS);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      Logger.println("Score:"+a.getScore(), LoggingLevel.EXPERIMENTS);
      Logger.println("Join Score:"+a.getJoinScore(), LoggingLevel.EXPERIMENTS);
      Logger.println("Comes from relaxation?"+a.comesFromRelaxation(), LoggingLevel.EXPERIMENTS);
      Logger.println("Real Query:"+a.getQuery(), LoggingLevel.EXPERIMENTS);
    }
    long totalRunTime = t.getDuration()+qplanner.getDuration();
    Logger.println("Query Planning time:" + qplanner.getDuration(),LoggingLevel.EXPERIMENTS);
    Logger.println("Query Execution time:" + t.getDuration(),LoggingLevel.EXPERIMENTS);
    Logger.println("Time Taken:"+totalRunTime, LoggingLevel.EXPERIMENTS);
    Logger.println("No. of Answer objects created:"+HashMapBasedAnswer.countOfAnswerObjects, LoggingLevel.EXPERIMENTS);

    return answersTopK;

  }
}
