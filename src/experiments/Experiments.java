package de.mpii.trinitreloaded.experiments;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;

import com.google.common.base.Joiner;

import de.mpii.trinitreloaded.datastructures.Answer;
import de.mpii.trinitreloaded.datastructures.HashMapBasedAnswer;
import de.mpii.trinitreloaded.datastructures.LogicalQueryPlan;
import de.mpii.trinitreloaded.datastructures.Query;
import de.mpii.trinitreloaded.queryprocessing.CodeGen;
import de.mpii.trinitreloaded.queryprocessing.Operator;
import de.mpii.trinitreloaded.queryprocessing.QueryParser;
import de.mpii.trinitreloaded.queryprocessing.QueryPlanner;
import de.mpii.trinitreloaded.utils.Config;
import de.mpii.trinitreloaded.utils.Logger;
import de.mpii.trinitreloaded.utils.Timer;
import de.mpii.trinitreloaded.utils.Config.LoggingLevel;
import de.mpii.trinitreloaded.utils.Config.PlanType;

public abstract class Experiments {
  /**
   * Run only one query with input from user.
   */
  public abstract void oneTimeRun();

  /**
   * Executes and collates the experiments using the specified {@link Config} parameters.
   */
  public abstract void executeExperiments();

  /**
   * Executes the experiments for one value of {@code Config.k}.
   */
  protected static void executeOneRun(){
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
          List<Answer> groundTruthAnswers = processQuery(query, PlanType.NONSPECULATIVE, runCount);
          pr = new Precision(new SimpleSetMeasureGroundTruthProvider(new HashSet<Answer>(groundTruthAnswers)));
          rcall = new Recall(new SimpleSetMeasureGroundTruthProvider(new HashSet<Answer>(groundTruthAnswers)));
        }

        // Run Full-Spec plan Config.numRuns times.
        Logger.println("Running Full-Spec " + Config.numRuns + " times.", LoggingLevel.EXPERIMENTS);
        for(int runCount=0;runCount<Config.numRuns;runCount++){
          List<Answer> answers = processQuery(query, PlanType.FULLYSPECULATIVE, runCount);
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

  /**
   * Process a query using the given plan.
   * 
   * @param query Query as a {@link String}.
   * @param planType Type of the plan from {@link PlanType}.
   * @param runCount The run number for multiple runs.
   * @return {@link List} of {@link Answer}s.
   * @deprecated Use {@link RankJoinExperiments#processRJQuery(String, PlanType, int)} instead.
   */
  @Deprecated
  protected static List<Answer> processQuery(String query, PlanType planType, int runCount) {
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
      CodeGen cg = new CodeGen(qplan);
      Operator result = cg.translate();
      try {
        result.open();
        for (int i=0;i<Config.k;i++){
          if(result.hasNext()){
            Answer a = result.next();
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
          Logger.println("Variable "+var+":"+a.getVariableBinding(var), LoggingLevel.EXPERIMENTS);
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
    Logger.println("Query Planning time:" + qplanner.getDuration(),LoggingLevel.INTERMEDIATEINFO);
    Logger.println("Query Execution time:" + t.getDuration(),LoggingLevel.INTERMEDIATEINFO);
    Logger.println("Time Taken:"+totalRunTime, LoggingLevel.EXPERIMENTS);
    Logger.println("No. of Answer objects created:"+HashMapBasedAnswer.countOfAnswerObjects, LoggingLevel.EXPERIMENTS);

    return answersTopK;
  }

  /**
   * Collate the results from the log file and generate a csv file.
   */
  public static void collateResults() {
    Config.resultFile = Logger.logFile.replace(".log", ".csv");
    BufferedReader br = null;
    BufferedWriter bw = null;
    try {
      br = new BufferedReader(new FileReader(Logger.logFile));
      bw = new BufferedWriter(new FileWriter(Config.resultFile));
      List<String> csvHeaders = Arrays.asList("Non-Spec Runtime(ms)", "Non-Spec Memory", "Orig Runtime(ms)", "Orig Memory", "Precision Orig",
          "Recall Orig", "FullSpec Runtime(ms)", "FullSpec Memory", "Precision FullSpec", "Recall FullSpec");

      String toWrite = "Query;Non-SpecPlan;";
      for(int cnt=0; cnt<Config.numRuns; cnt++)
        toWrite+=Joiner.on(cnt+"; ").join(csvHeaders.subList(0, 2)) + cnt + "; ";
      toWrite+="Avg Runtime; Avg Memory;";

      /**toWrite+="Orig Plan;";
      for(int cnt=0; cnt<Config.numRuns; cnt++)
        toWrite+=Joiner.on(cnt+"; ").join(csvHeaders.subList(2, 6)) + cnt + "; ";
      toWrite+="Avg Runtime; Avg Memory; Avg Precision; Avg Recall;";*/

      toWrite+="FullSpec Plan;";
      for(int cnt=0; cnt<Config.numRuns; cnt++)
        toWrite+=Joiner.on(cnt+"; ").join(csvHeaders.subList(6, 10)) + cnt + "; ";
      toWrite+="Avg Runtime; Avg Memory; Avg Precision; Avg Recall";
      bw.write(toWrite+"\n");

      String line;
      ArrayList<String> tagNames = new ArrayList<String>();
      tagNames.add("Next Query:");
      tagNames.add("Time Taken:");
      tagNames.add("No. of Answer objects created:");
      tagNames.add("Query Plan:");
      tagNames.add("Precision-Orig:");
      tagNames.add("Recall-Orig:");
      tagNames.add("Precision-FullSpec:");
      tagNames.add("Recall-FullSpec:");

      boolean firstQuery = true;
      boolean firstPlan = true;
      ArrayList<Integer> runtimes = new ArrayList<Integer>();
      ArrayList<Integer> memory = new ArrayList<Integer>();
      ArrayList<Double> precision = new ArrayList<Double>();
      ArrayList<Double> recall = new ArrayList<Double>();
      int avgRuntime=0, avgMemory=0;
      double avgPrec=0.0, avgRecall=0.0;
      while((line=br.readLine())!=null){
        boolean hasInfo = false;
        for(int i=0;i<tagNames.size();i++){
          if(line.contains(tagNames.get(i)))
            hasInfo = true;
        }
        if(hasInfo) {
          String[] vals = line.split(":",2);
          if((vals[0].contains("Next Query") && !firstQuery) || (vals[0].contains("Query Plan") && !firstPlan)){

            avgRuntime=0;
            avgMemory=0;
            avgPrec=0.0;
            avgRecall=0.0;
            for(int i=0; i<Config.numRuns; i++){
              avgRuntime+=runtimes.get(i);
              avgMemory+=memory.get(i);
              if(precision.size()>0){
                avgPrec+=precision.get(i);
                avgRecall+=recall.get(i);
              }
            }
            avgRuntime=avgRuntime/Config.numRuns;
            avgMemory=avgMemory/Config.numRuns;
            if(precision.size()>0){
              avgPrec=avgPrec/Config.numRuns;
              avgRecall=avgRecall/Config.numRuns;
            }
            if(precision.size()>0){
              bw.write(avgRuntime + "; "+ avgMemory + "; " + avgPrec + "; " + avgRecall + ";");
            }
            else{
              bw.write(avgRuntime + "; "+ avgMemory + ";");
            }
            if((vals[0].contains("Next Query"))){
              firstPlan = true;
              bw.write("\n");
            }
            runtimes.clear();
            memory.clear();
            precision.clear();
            recall.clear();
          }
          firstQuery = false;
          if(vals[0].contains("Time")){
            runtimes.add(Integer.parseInt(vals[1]));
            firstPlan = false;
          }
          if(vals[0].contains("Precision"))
            precision.add(Double.parseDouble(vals[1]));
          if(vals[0].contains("Recall"))
            recall.add(Double.parseDouble(vals[1]));
          if(vals[0].contains("Answer"))
            memory.add(Integer.parseInt(vals[1]));
          bw.write(vals[1].replace(";", ",")+";");
        }
      }
      avgRuntime=0;
      avgMemory=0;
      avgPrec=0.0;
      avgRecall=0.0;
      for(int i=0; i<Config.numRuns; i++){
        avgRuntime+=runtimes.get(i);
        avgMemory+=memory.get(i);
        if(precision.size()>0){
          avgPrec+=precision.get(i);
          avgRecall+=recall.get(i);
        }
      }
      avgRuntime=avgRuntime/Config.numRuns;
      avgMemory=avgMemory/Config.numRuns;
      if(precision.size()>0){
        avgPrec=avgPrec/Config.numRuns;
        avgRecall=avgRecall/Config.numRuns;
      }
      if(precision.size()>0){
        bw.write(avgRuntime + "; "+ avgMemory + "; " + avgPrec + "; " + avgRecall + ";");
      }
      else{
        bw.write(avgRuntime + "; "+ avgMemory + ";");
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }finally{
      try {
        br.close();
        bw.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Takes input from the user.
   * 
   * @return Input as a {@link String}.
   */
  protected
  static String takeInput() {
    Scanner in = new Scanner(System.in);
    String query = "";
    System.out.println("Enter a query:");
    query = in.nextLine();
    in.close();
    return query;
  }
}
