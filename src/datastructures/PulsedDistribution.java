package de.mpii.trinitreloaded.datastructures;

import java.util.ArrayList;

/**
 * The pulsed form of a continuous {@link ProbabilityDistribution}.
 * 
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 */
public interface PulsedDistribution {

  /**
   * Computes convolution of this distribution with another distribution.
   * @param pulsedPdf2 The second {@link PulsedDistribution}.
   * @return The convoluted {@link PulsedDistribution}.
   */
  public PulsedDistribution convolute(PulsedDistribution pulsedPdf2);
  
  /**
   * Computes the cumulative score at a given rank.
   * @param pRank
   *    The rank at which the expected cumulative score is sought.
   * @param numResults
   *    The total number of results.
   * @return A cumulative sum of the scores at rank {@code pRank}.
   */
  public double getCumulativeScore(long pRank, long numResults);
  
  /**
   * Computes the expected score having a given probability.
   *
   * @param rankRPercentilePoint
   *          The percentile point (probability value) at the rank whose expected score is sought.
   * @return An expected score having the probability {@code rankRPercentilePoint}.
   */
  public double getScoreAtRankR(double rankRPercentilePoint);
  
  /**
   * Getter for the discrete values sampled at various points of the distribution.
   * @return The list of discrete sampled values.
   */
  public ArrayList<Double> getSampledValues();
  
}
