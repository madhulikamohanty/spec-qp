package de.mpii.trinitreloaded.queryprocessing;

/**
 * A join condition.
 * 
 * @author Mohamed Yahya (myahya@mpi-inf.mpg.de)
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 * 
 */
public class JoinCondition {

  public final int leftRel;
  public final int rightRel;
  public final String joinVariable;

  public JoinCondition(int leftRel, int rightRel, String joinVariable) {
    this.leftRel = leftRel;
    this.rightRel = rightRel;
    this.joinVariable = joinVariable;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Join ").append(leftRel).append(" with ").append(rightRel).append(" on ")
        .append(joinVariable);
    return sb.toString();
  }

}
