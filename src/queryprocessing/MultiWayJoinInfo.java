package de.mpii.trinitreloaded.queryprocessing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Information needed to perform the multiway join.
 * 
 * @author Mohamed Yahya (myahya@mpi-inf.mpg.de)
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 * 
 */
public class MultiWayJoinInfo {

  final List<JoinCondition> joinConditions;
  final HashMap<Integer,ArrayList<String>> relationsToVariables;

  public MultiWayJoinInfo() {
    joinConditions = new ArrayList<JoinCondition>();
    relationsToVariables = new HashMap<Integer,ArrayList<String>> ();
  }

  public void addVariableToRelation(int relation, String variable) {
    if (!relationsToVariables.containsKey((new Integer(relation)))) {
      relationsToVariables.put(relation, new ArrayList<String>());
    }

    relationsToVariables.get(relation).add(variable);
  }

  public ArrayList<String> getVariablesInRelation(int relation) {
    return relationsToVariables.get(relation);
  }

  public void addJoinCondition(JoinCondition jc) {
    this.joinConditions.add(jc);
  }

  public List<JoinCondition> joinConditions() {
    return joinConditions;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    for (JoinCondition jc : joinConditions) {
      sb.append(jc.toString());
      sb.append(", ");
    }
    sb.append("\n");
    sb.append("relationsToVariables: " + this.relationsToVariables);

    return sb.toString();
  }
}
