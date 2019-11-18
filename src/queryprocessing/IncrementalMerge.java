package de.mpii.trinitreloaded.queryprocessing;

import gnu.trove.list.TDoubleList;

import java.sql.SQLException;
import java.util.HashSet;
//import java.util.HashMap;
import java.util.List;
//import java.util.Map;

import de.mpii.trinitreloaded.datastructures.Answer;
import de.mpii.trinitreloaded.utils.Logger;
import de.mpii.trinitreloaded.utils.Config.LoggingLevel;
import gnu.trove.list.array.TDoubleArrayList;

import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;

import com.google.common.collect.Lists;

/**
 * An incremental merge operator.
 *
 * @author Mohamed Yahya (myahya@mpi-inf.mpg.de)
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 */
public class IncrementalMerge implements Operator {

  public static final double ORIGINAL_TERM_WEIGHT = 1.0;
  public final TDoubleList weights;
  public final TDoubleList initialScores;

  final List<WeightedOperator> ops; // for closing and printing

  final PriorityQueue<WeightedOperator> operators;
  final PriorityQueue<WeightedOperator> activeExp;

  final Operator original;

  WeightedOperator nextExp;
  double nextExpHigh;

  Answer tuple = null;
  Set<Answer> seenAnswers; // for keeping track of answers already seen. 
  // If an answer comes up again ( with a lower score), we should discard it.

  public IncrementalMerge(Operator original, List<Operator> inputs, TDoubleList weights, TDoubleList initialScores) {
    assert (initialScores.size() - 1 == weights.size());
    if (inputs.size() == 0) {
      throw new IllegalArgumentException("A merge operator needs something to merge!");
    }
    this.original = original;
    List<Operator> in = Lists.newArrayList(inputs);
    in.add(0, original);

    TDoubleList w = new TDoubleArrayList(weights);
    w.insert(0, ORIGINAL_TERM_WEIGHT);
    this.weights=w;

    this.ops = Lists.newArrayList();
    TDoubleList tis =new TDoubleArrayList(initialScores);
    tis.insert(0, 1);
    this.initialScores = tis;
    this.operators = createPQ(in, w, tis, ops);
    this.activeExp = new PriorityQueue<WeightedOperator>();
    this.seenAnswers = new HashSet<Answer>();
  }

  private static PriorityQueue<WeightedOperator> createPQ(
      List<Operator> inputs, TDoubleList weights, TDoubleList initialScores,
      List<WeightedOperator> ops) {
    assert (inputs.size() == weights.size());
    assert (inputs.size() == initialScores.size());
    PriorityQueue<WeightedOperator> operators = new PriorityQueue<WeightedOperator>();
    Logger.println("InputSize:"+inputs.size()+" weightsSize:"+weights.size()+" initialScoresSize:"+initialScores.size(), LoggingLevel.VARIABLEVALUES);
    for (int i = 0; i < inputs.size(); i++) {
      WeightedOperator weightedOp = new WeightedOperator(inputs.get(i), initialScores.get(i), weights.get(i));
      ops.add(weightedOp);
      operators.add(weightedOp);
    }
    return operators;
  }

  public boolean hasNext() throws SQLException {
    if (tuple != null) {
      return true;
    }

    if (activeExp.isEmpty()) {
      return false;
    }
    try{
      //Debug.println("IJ.hasNext() deciding.", DebugLevel.INTERMEDIATEINFO);
      WeightedOperator best = activeExp.poll();
      //System.out.println("best.operator.name: " + best.operator.toString());
      //Debug.println("best.operator.name: " + best.operator.toString(), DebugLevel.VARIABLEVALUES);
      //Debug.println("best.high: " + best.high, DebugLevel.VARIABLEVALUES);

      // operators should not contain best at this point.
      assert (!operators.contains(best));

      Logger.println("nextExpHigh: " + nextExpHigh, LoggingLevel.VARIABLEVALUES);

      if (best.high < nextExpHigh) {
        Logger.println("best.high < nextExpHigh", LoggingLevel.INTERMEDIATEINFO);
        operators.add(best);// Switched with line below. Was a bug
        best = nextExp; 
        nextExp = operators.poll(); // operators never empty.
        nextExpHigh = nextExp.high();
      }

      if (!best.isOpen) {
        //Debug.println("Opening best operator", DebugLevel.INTERMEDIATEINFO);
        best.open();
      }

      if (!best.hasNext()) {
        //Debug.println("Inc Merge best has no next.", DebugLevel.INTERMEDIATEINFO);
        assert (best.high == Double.NEGATIVE_INFINITY);
        best.close();
        return hasNext(); // recurse, activeExp (high) just lost best.
      }
      //Debug.println("Best has next.", DebugLevel.INTERMEDIATEINFO);
      tuple = best.next();
      // It should be a candidate again. Has to be added now, since next high is
      // adjusted after next() call above.
      activeExp.add(best);

      // Recurse if this answer has already been seen.
      if(seenAnswers.contains(tuple)){
        //System.out.println("Recursive call to hasNext()!!!!");
        //System.out.println("SeenAnswers:"+this.seenAnswers);
        //System.out.println("This answer:"+tuple);
        tuple = null;
        
        return hasNext();
      }
    }catch(Exception e){
      throw new RuntimeException(e);
    }

    return true;
  }

  public Answer next() throws SQLException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Answer result = tuple;
    tuple = null;
    seenAnswers.add(result);
    return result;
  }

  public boolean open() throws SQLException {
    activeExp.add(operators.poll());
    nextExp = operators.poll();
    nextExpHigh = nextExp.high;

    hasNext();

    return true;
  }

  public boolean close() throws SQLException {
    boolean retVal=true;
    for (WeightedOperator op : ops) {
      if(!op.close())
        retVal=false;
    }
    return retVal;
  }

  public String getPartQuery() {
    return null;
  }

}
