package de.mpii.trinitreloaded.queryprocessing;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;

import de.mpii.trinitreloaded.datastructures.LogicalQueryPlan;
import de.mpii.trinitreloaded.datastructures.TriplePattern;
import de.mpii.trinitreloaded.datastructures.TriplePatternInPlan;
import de.mpii.trinitreloaded.utils.Config;

/**
 * Executes the {@link LogicalQueryPlan} generated by {@link QueryPlanner} using the correct order
 * of {@link Operator}.
 *
 * Following construction, the entry point is {@code translate()}.
 *
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 */
public class RJCodeGen extends CodeGen{


  public RJCodeGen(LogicalQueryPlan queryPlan) {
    super(queryPlan);
  }
  
  public Operator translate() {
    Set<TriplePatternInPlan> plan = queryPlan.getPlan();

    List<Operator> ops = Lists.newArrayList();
    List<ArrayList<String>> variables = new ArrayList<ArrayList<String>>();

    /**
     * Create {@link IncrementalMerge} for single {@link TriplePattern}. Create
     * {@link PopularityBasedScan} for triple patterns in conjunctive queries. Create {@link RankJoin} for all
     * the operators.
     */
    for (TriplePatternInPlan querySubset : plan) {
      switch (querySubset.getTPSet().size()) {
      case 1:
        ops.add(createIncrementalMerge(querySubset));
        ArrayList<String> vars = new ArrayList<String>();
        ArrayList<TriplePattern> tp = new ArrayList<TriplePattern>();
        tp.addAll(querySubset.getTPSet());
        for (String var : tp.get(0).variables()) {
          if (!vars.contains(var)) {
            vars.add(var);
          }
        }
        variables.add(vars);
        break;
      default:
        ops.addAll(createScans(querySubset.getTPSet()));

        ArrayList<TriplePattern> tp2 = new ArrayList<TriplePattern>();
        tp2.addAll(querySubset.getTPSet());
        for (int i = 0; i < tp2.size(); i++) {
          ArrayList<String> vars2 = new ArrayList<String>();
          for (String var : tp2.get(i).variables()) {
            if (!vars2.contains(var)) {
              vars2.add(var);
            }
          }
          variables.add(vars2);
        }

        break;
      }
    }

    if (ops.size() == 1) {
      return ops.get(0);
    } else {
      return createRankJoin(ops, variables);
    }
  }


  public ArrayList<Operator> createScans(Set<TriplePattern> querySubset) {
    ArrayList<TriplePattern> inputs = new ArrayList<TriplePattern>();
    inputs.addAll(querySubset);
    ArrayList<Operator> ops = new ArrayList<Operator>();
    for(TriplePattern tp:inputs){
      if(Config.isSyntheticData){
        if(Config.isRDFDB)
          ops.add(new SyntheticRDFScan(tp));
        else
          ops.add(new SyntheticScan(tp));
      }
      else if(Config.isRDFDB){
        ops.add(new PopularityBasedRDFScan(tp));
      } 
      else
      {
        ops.add(new PopularityBasedScan(tp));
      }
    }
    return ops;
  }

}