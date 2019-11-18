package de.mpii.trinitreloaded.queryprocessing;

import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.List;

/**
 * The join graph shows what relations are connected via joins.
 * 
 * @author Mohamed Yahya (myahya@mpi-inf.mpg.de)
 * 
 */
public class JoinGraph {

  private final TIntObjectHashMap<TIntHashSet> adjacencyLists;

  private static final TIntHashSet EMPTY_NEIGHBOUR_SET = new TIntHashSet();

  public JoinGraph(List<JoinCondition> joinConditions) {
    adjacencyLists = new TIntObjectHashMap<TIntHashSet>();
    for (JoinCondition jc : joinConditions) {
      if (!adjacencyLists.contains(jc.leftRel)) {
        adjacencyLists.put(jc.leftRel, new TIntHashSet());
      }
      if (!adjacencyLists.contains(jc.rightRel)) {
        adjacencyLists.put(jc.rightRel, new TIntHashSet());
      }

      adjacencyLists.get(jc.leftRel).add(jc.rightRel);
      adjacencyLists.get(jc.rightRel).add(jc.leftRel);
    }
  }

  TIntSet getNeighbours(int relationId) {
    TIntHashSet result = adjacencyLists.get(relationId);
    return result == null ? EMPTY_NEIGHBOUR_SET : result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    for (int rel : adjacencyLists.keys()) {
      sb.append(rel);
      sb.append(": {");
      for (int neighbour : getNeighbours(rel).toArray()) {
        sb.append(neighbour);
        sb.append(", ");
      }
      sb.append("}");
      sb.append("\n");
    }

    return sb.toString();
  }
}
