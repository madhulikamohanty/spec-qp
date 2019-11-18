package de.mpii.trinitreloaded.datastructures;

/**
 * A probability distribution for scores.
 */
public interface ProbabilityDistribution {
  /**
   * Gives the expected score at a given rank.
   * @param rank Rank for which expected score is sought.
   * @return Expected score at the given rank.
   */
  public double getPercentile(long rank);

  /**
   * Getter for number of results.
   * @return Number of results.
   */
  public long getNumResults();

  /**
   * Getter for maximum score of the results.
   * @return Maximum score of results.
   */
  public double getMaxScore();

  /**
   * Print the distribution as a string.
   * @return String output.
   */
  public String toString();
}
