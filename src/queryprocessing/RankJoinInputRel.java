package de.mpii.trinitreloaded.queryprocessing;

/**
 * 
 * An input relation for the {@link RankJoin} operator.
 * 
 * This class maintains the highest and unseen upper bounds 
 * for the relation.
 *
 */
public class RankJoinInputRel extends InputRel {

  double top = 0;
  double bottom = 0;
  boolean firstTupleRead;

  public RankJoinInputRel(Operator tuples) {
    super(tuples);
    firstTupleRead = false;
  }

}
