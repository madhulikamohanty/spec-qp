package de.mpii.trinitreloaded.queryprocessing;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.hash.TIntHashSet;
import gnu.trove.stack.array.TIntArrayStack;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.google.common.base.Preconditions;

import de.mpii.trinitreloaded.datastructures.Answer;
import de.mpii.trinitreloaded.utils.Logger;
import de.mpii.trinitreloaded.utils.Config.LoggingLevel;

/**
 * Generates the correct order of {@link RankJoin} for a list of {@link Operator}.
 * 
 * Following construction, the entry point is {@code translate()}.
 * 
 * @author Mohamed Yahya (myahya@mpi-inf.mpg.de)
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 */
public class RankJoinCodeGen{

  private final List<Operator> query;
  private final List<ArrayList<String>> variables;

  public RankJoinCodeGen( List<Operator> query, List<ArrayList<String>> variables) {
    Preconditions.checkArgument(query.size() > 1);
    this.variables = variables;
    this.query = query;
  }

  public WrappedRankJoin translate() {
    MultiWayJoinInfo joinInfo = createJoinInfo();
    JoinGraph joinGraph = new JoinGraph(joinInfo.joinConditions);
    return generateJoinGraph(joinInfo, joinGraph);
  }

  private WrappedRankJoin generateJoinGraph(MultiWayJoinInfo joinInfo, JoinGraph joinGraph) {
    int rel = 0;
    Operator currentRoot = null;

    TIntArrayStack stack = new TIntArrayStack();
    TIntHashSet seenRels = new TIntHashSet();

    HashSet<String> varsAvailable = new HashSet<String>();

    // DFS
    stack.push(0);
    while (stack.size() != 0) {
      int currentRel = stack.pop();
      if (!seenRels.contains(currentRel)) {
        // Visit
        if (currentRel == rel) {
          // Left-most relation
          currentRoot = query.get(currentRel);
        } else {
          // Right relations
          ArrayList<String> vars = joinInfo.getVariablesInRelation(currentRel);

          for (int i = 0; i < vars.size(); i++) {
            // Variable not seen before, so no join on it
            if (!varsAvailable.contains(vars.get(i))) {
              continue;
            }

            currentRoot = new RankJoin(currentRoot, query.get(currentRel), vars.get(i));
          }
        }
        seenRels.add(currentRel);
        // Record variables we can join on later
        varsAvailable.addAll(joinInfo.getVariablesInRelation(currentRel));
        TIntIterator neighbours = joinGraph.getNeighbours(currentRel).iterator();
        while (neighbours.hasNext()) {
          int n= neighbours.next();
          stack.push(n);
        }
      }
    }

    return new WrappedRankJoin((RankJoin) currentRoot);
  }

  private MultiWayJoinInfo createJoinInfo() {
    MultiWayJoinInfo multiWayJoinInfo = new MultiWayJoinInfo();

    HashMap<String,TIntList> varToRels = new HashMap<String,TIntList>();

    for (int rel = 0; rel < query.size(); rel++) {
      for (String var : this.variables.get(rel)) {
        if (!varToRels.containsKey(var)) {
          varToRels.put(var, new TIntArrayList());
        }

        varToRels.get(var).add(rel);
        multiWayJoinInfo.addVariableToRelation(rel, var);
      }
    }


    for (String var : varToRels.keySet()) {
      if (varToRels.get(var).size() > 1) { // no join for == 1
        for (int i = 0; i < varToRels.get(var).size() - 1; i++) {
          for (int j = i + 1; j < varToRels.get(var).size(); j++) {
            multiWayJoinInfo.addJoinCondition(new JoinCondition(varToRels.get(var).get(i),
                varToRels.get(var).get(j), var));
          }
        }
      } else {
        Logger.print("Variable {"+var+"} occurs in a single triple pattern", LoggingLevel.INTERMEDIATEINFO);
      }
    }

    return multiWayJoinInfo;
  }

  public class WrappedRankJoin implements Operator {

    public final RankJoin wrapped;

    public WrappedRankJoin(RankJoin wrapped){
      this.wrapped = wrapped;
    }

    public boolean hasNext() throws SQLException {
      return wrapped.hasNext();
    }

    public Answer next() throws SQLException {
      Answer a = wrapped.next();
      return a;
    }


    public boolean open() throws SQLException {
      return wrapped.open();
    }

    public boolean close() throws SQLException {
      wrapped.close();
      return true;
    }

    public double scoreOfNext() throws SQLException {
      return wrapped.scoreOfNext();
    }

    public String getPartQuery() {
      return wrapped.getPartQuery();
    }
  }

}
