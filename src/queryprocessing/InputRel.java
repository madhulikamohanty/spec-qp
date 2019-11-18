package de.mpii.trinitreloaded.queryprocessing;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import de.mpii.trinitreloaded.datastructures.Answer;

/**
 * A relation serving as input to a {@link RankJoin} operator. This wraps an operator, and
 * provides a hash table used to allow joins with the data from the input
 * operator.
 * 
 * The user of this class should make sure not to hash bindings of variables
 * that do not entail a join.
 * 
 * @author Mohamed Yahya (myahya@mpi-inf.mpg.de)
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 * 
 */
class InputRel {
  Operator tuples;

  // The key here is a variable id.
  HashMap<String,Multimap<String, Answer>> hashTables;

  public InputRel(Operator tuples) {
    this.tuples = tuples;
    this.hashTables = new HashMap<String,Multimap<String, Answer>>();
  }

  public void addToHashTable(String joinVar, String string, Answer tuple) {
    if (!hashTables.containsKey(joinVar)) {
      hashTables.put(joinVar, ArrayListMultimap.<String, Answer> create());
    }

    hashTables.get(joinVar).put(string, tuple);
  }

  public Collection<Answer> get(String varId, String key) {
    if (!hashTables.containsKey(varId)) {
      return null;
    }
    if (!hashTables.get(varId).containsKey(key)) {
      return null;
    }
    return hashTables.get(varId).get(key);

  }

  public boolean isConsumed() throws SQLException {
    return !tuples.hasNext();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Set<String> keys = hashTables.keySet();

    for (String key : keys) {
      sb.append("Key:" + key);
      sb.append("\n");
      sb.append(hashTables.get(key) == null ? "null" : hashTables.get(key).toString());
    }
    sb.append("\n");

    return sb.toString();
  }
}