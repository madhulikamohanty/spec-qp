package de.mpii.trinitreloaded.datastructures;

import java.util.ArrayList;

/**
 * A pulsed form of a {@link ScoreProbabilityDistributionFunction}. It comprises of a list of
 * {@code sampledValues} computed at intervals of {@code convolutionStepSize}.
 *
 * It also has the utility function {@code convolute()} to compute the convolution of two pulsed
 * form PDFs.
 *
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 */

public class PulsedScoreProbabilityDistribution implements PulsedDistribution{

  public final ArrayList<Double> sampledValues;
  public final double convolutionStepSize;

  public PulsedScoreProbabilityDistribution(ArrayList<Double> sampledValues,
      Double convolutionStepSize) {
    this.sampledValues = sampledValues;
    this.convolutionStepSize = convolutionStepSize;
  }

  /**
   * This function creates a pulsed form of a {@link ScoreProbabilityDistributionFunction}.
   *
   * @param pdf
   *          Original distribution in the form of {@link ScoreProbabilityDistributionFunction}.
   * @param convolutionStepSize
   *          The width of the step function.
   */
  public PulsedScoreProbabilityDistribution(ScoreProbabilityDistributionFunction pdf,
      Double convolutionStepSize) {
    sampledValues = new ArrayList<Double>();
    this.convolutionStepSize = convolutionStepSize;
    /**
     * {@code a} represents the lower half of the score distribution. {@code b} represents the upper
     * half of the score distribution.
     */
    double a, b;
    if (pdf.cumulativeScoreAtRankN != 0.0) { // To avoid divide by 0 and NaN.
      if (pdf.scoreAtRankR != 0.0) {
        a =
            (pdf.cumulativeScoreAtRankN - pdf.cumulativeScoreAtRankR)
                / (pdf.cumulativeScoreAtRankN * pdf.scoreAtRankR);
      } else {
        a = 0.0;
      }
      b = (pdf.cumulativeScoreAtRankR) / (pdf.cumulativeScoreAtRankN * (pdf.maxScore - pdf.scoreAtRankR));
    } else {
      a = 0.0;
      b = 0.0;
    }
    double step = 0.0;
    while (step <= pdf.maxScore) {
      /**
       * Assuming probability of a score 0 is 0 i.e., p(0) = 0.
       */
      if (step == 0.0) {
        sampledValues.add(a); // TODO Check what is better here, 0.0 or 'a';
      }
      if (step > 0.0 && step <= pdf.scoreAtRankR) {
        sampledValues.add(a);
      }
      if (step > pdf.scoreAtRankR) {
        sampledValues.add(b);
      }
      step += this.convolutionStepSize;
    }
  }

  /**
   * Convolutes the two input distributions.
   *
   * @param pulsedPdf1
   *          The first {@link ScoreProbabilityDistributionFunction}.
   * @param pulsedPdf2
   *          The second {@link ScoreProbabilityDistributionFunction}.
   * @return Returns a pulsed form of the convolution of the two distributions.
   */
  public PulsedScoreProbabilityDistribution convolute(PulsedDistribution pulsedPdf2) {
    ArrayList<Double> sampledValues1 = this.sampledValues;
    ArrayList<Double> sampledValues2 = pulsedPdf2.getSampledValues();
    ArrayList<Double> convPDFValues = new ArrayList<Double>();
    for (int k = 1; k <= (sampledValues1.size() + sampledValues2.size() - 1); k++) {
      Double convVal = 0.0;
      int j = Math.max(1, k - sampledValues2.size());
      for (; j <= Math.min(k, sampledValues1.size()); j++) {
        if ((j - 1) >= 0 && (j - 1) < sampledValues1.size() && (k - j) >= 0
            && (k - j) < sampledValues2.size()) {
          int index1 = (j - 1);
          int index2 = (k - j);
          convVal +=
              sampledValues1.get(index1) * sampledValues2.get(index2)
                  * this.convolutionStepSize;
        }
      }
      convPDFValues.add(convVal);
    }
    return new PulsedScoreProbabilityDistribution(convPDFValues, this.convolutionStepSize);
  }

  /**
   * Computes the cumulative score at a given rank.
   *
   * @param pRank
   *          The rank at which the expected cumulative score is sought.
   * @param numResults
   *          The total number of results.
   * @return A cumulative sum of the scores at rank {@code pRank}.
   */
  public double getCumulativeScore(long pRank, long numResults) {
    double cumulativeSum = 0.0;
    /**
     * Check to ensure that the rank sought is <= number of results.
     */
    if (pRank > numResults) {
      pRank = numResults;
    }
    for (long i = 1; i <= pRank; i++) {
      double percentilePoint = (new Double(numResults) - i + 1) / (new Double(numResults) + 1);
      cumulativeSum += this.getScoreAtRankR(percentilePoint);
    }
    return cumulativeSum;
  }

  /**
   * Computes the expected score having a given probability.
   *
   * @param rankRPercentilePoint
   *          The percentile point (probability value) at the rank whose expected score is sought.
   * @return An expected score having the probability {@code rankRPercentilePoint}.
   */
  public double getScoreAtRankR(double rankRPercentilePoint) {
    Double currentVal = 0.0;
    for (int i = 1; i < this.sampledValues.size(); i++) {
      currentVal += this.sampledValues.get(i - 1) * this.convolutionStepSize;
      if (currentVal > rankRPercentilePoint) {
        return (i - 1) * this.convolutionStepSize;
      }
    }
    if(currentVal.equals(0.0))
      return 0.0;
    return (this.sampledValues.size() - 1) * this.convolutionStepSize;
  }

  public ArrayList<Double> getSampledValues() {
    return this.sampledValues;
  }
  
  public String toString(){
    return this.sampledValues.toString();
  }

}
