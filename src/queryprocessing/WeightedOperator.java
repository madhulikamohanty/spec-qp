package de.mpii.trinitreloaded.queryprocessing;

import java.sql.SQLException;
import java.util.NoSuchElementException;

import de.mpii.trinitreloaded.datastructures.Answer;
import de.mpii.trinitreloaded.utils.Config.LoggingLevel;
import de.mpii.trinitreloaded.utils.Logger;

/**
 * An operator which multiplies scores of its input with a constant weight.
 *
 * This operator gives the consumer the ability to check the score of the next element to be read.
 * To support this, the operator needs to be initialized with the score of the first element it will
 * return. Doing so saves us from the need to open the underlying input until we know we will
 * actually consume something from there.
 *
 * Expected usage:
 * <ol>
 * <li />Is the value returned from {@code high()} is suitable, go to 2.
 * <li />if {@code hasNext()}, go to 3.
 * <li />call {@code next()}. The score of the returned answer should be the same as the one
 * obtained from {@code high()}.
 * </ol>
 *
 * @author Mohamed Yahya (myahya@mpi-inf.mpg.de)
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 *
 */
public class WeightedOperator implements Operator, Comparable<WeightedOperator> {

  final Operator operator;
  final double weight;

  double high;
  boolean isOpen;

  /** Contains the value returned by {@code next()} */
  Answer nexTuple;

  public WeightedOperator(Operator operator, double initialScore, double weight) {
    this.operator = operator;
    this.weight = weight;
    this.high = weight * initialScore;
    this.isOpen = false;
  }

  public boolean hasNext() {
    return nexTuple != null;
  }

  public Answer next() throws Exception {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    Answer ret = nexTuple;
    if (Double.compare(weight, 1.0) != 0) {
      ret.SetComesFromRelaxation(true);
    }

    readNextTuple(false);

    return ret;
  }

  public boolean open() throws SQLException {
    try {
      isOpen = true;
      operator.open();
      readNextTuple(true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return true;
  }

  private void readNextTuple(boolean opening) throws Exception {
    if (operator.hasNext()) {
      Logger.println("Reading operator's next value in weighted operator.",
          LoggingLevel.INTERMEDIATEINFO);
      nexTuple = operator.next();
      nexTuple.setScore(weight * nexTuple.getScore());
      assert (!opening || (nexTuple.getScore() == high));
      this.high = nexTuple.getScore();
    } else {
      Logger.println("Weighted Operator has no next.", LoggingLevel.INTERMEDIATEINFO);
      this.nexTuple = null;
      this.high = Double.NEGATIVE_INFINITY;
    }
  }

  public boolean close() throws SQLException {
    isOpen = false;
    return operator.close();
  }

  /**
   * Returns the score of the next answer that will be returned
   *
   * @return the score of the next answer that will be returned. {@code Double.NEGATIVE_INFINITY} is
   *         returned if a subsequent call hasNext() will return false.
   */
  public double high() {
    return high;
  }

  public int compareTo(WeightedOperator o) {
    return Double.compare(o.high(), this.high());
  }

  public boolean isOpen() {
    return isOpen;
  }

  public String getPartQuery() {
    return operator.getPartQuery();
  }
}
