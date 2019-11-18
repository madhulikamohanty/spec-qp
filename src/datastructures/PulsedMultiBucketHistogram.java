package de.mpii.trinitreloaded.datastructures;

import java.util.ArrayList;

import de.mpii.trinitreloaded.utils.Config;

/**
 * A pulsed form of a {@link MultiBucketHistogram}. It comprises of a list of
 * {@code sampledValues} computed at intervals of {@code convolutionStepSize}.
 *
 * It also has the utility function {@code convolute()} to compute the convolution of two pulsed
 * form PDFs.
 * TODO Most of the parts except the constructor is redundant with 
 * {@link PulsedScoreProbabilityDistribution}. Merge these.
 * 
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 */
public class PulsedMultiBucketHistogram implements PulsedDistribution {

  public final ArrayList<Double> sampledValues;
  public final double convolutionStepSize;

  public PulsedMultiBucketHistogram(ArrayList<Double> sampledValues,
      Double convolutionStepSize) {
    this.sampledValues = sampledValues;
    this.convolutionStepSize = convolutionStepSize;
  }

  /**
   * This function creates a pulsed form of a {@link MultiBucketHistogram}.
   *
   * @param pdf
   *          Original distribution in the form of {@link MultiBucketHistogram}.
   * @param convolutionStepSize
   *          The width of the step function.
   */
  public PulsedMultiBucketHistogram(MultiBucketHistogram pdf,
      Double convolutionStepSize) {
    sampledValues = new ArrayList<Double>();
    this.convolutionStepSize = convolutionStepSize;
    double step = 0.0;
    double currentLeftLimit, currentRightLimit, currentVal;
    if(pdf.cumulativeScoreAtRankN!=0.0 && pdf.scoreAtRanks.get(Config.numBuckets-2)!=0.0){
      currentVal = (pdf.cumulativeScoreAtRankN - pdf.cumulativeScoreAtRanks.get(Config.numBuckets-2))
          / (pdf.cumulativeScoreAtRankN * pdf.scoreAtRanks.get(Config.numBuckets-2));
    }
    else{
      currentVal = 0.0;
    }
    int scoreIndex = Config.numBuckets-2;
    currentLeftLimit = 0.0;
    currentRightLimit = pdf.scoreAtRanks.get(scoreIndex);
    while (step <= pdf.maxScore) {
      /**
       * Assuming probability of a score 0 is 0 i.e., p(0) = 0.
       */
      if (step == 0.0) {
        sampledValues.add(currentVal); // TODO Check what is better here, 0.0 or 'a';
      }
      else if (step > currentLeftLimit && step < currentRightLimit) {
        sampledValues.add(currentVal);
      }
      else{
        // Update current pdf value and limits.
        if(scoreIndex==0){ // If we have reached the last bucket, then use maxScore.
          if(pdf.cumulativeScoreAtRankN!=0.0){ // To avoid divide by 0 and NaN.
            currentVal = (pdf.cumulativeScoreAtRanks.get(scoreIndex)) / (pdf.cumulativeScoreAtRankN * (pdf.maxScore - pdf.scoreAtRanks.get(scoreIndex)));
          }
          else{
            currentVal = 0.0;
          }
          currentLeftLimit = currentRightLimit;
          currentRightLimit = pdf.maxScore;
        }
        else{
          scoreIndex--;
          if(pdf.cumulativeScoreAtRankN!=0.0){ // To avoid divide by 0 and NaN.
            currentVal = (pdf.cumulativeScoreAtRanks.get(scoreIndex+1) - pdf.cumulativeScoreAtRanks.get(scoreIndex))
                / (pdf.cumulativeScoreAtRankN * (pdf.scoreAtRanks.get(scoreIndex)-pdf.scoreAtRanks.get(scoreIndex+1)));;
          }
          else{
            currentVal = 0.0;
          }
          currentLeftLimit = currentRightLimit;
          currentRightLimit = pdf.scoreAtRanks.get(scoreIndex);
        }
        sampledValues.add(currentVal);
      }
      if(step == pdf.maxScore)
        sampledValues.add(currentVal);
      step += this.convolutionStepSize;
    }
  }

  /**
   * Convolutes this and the input distributions.
   *
   * @param pulsedPdf2
   *          The second {@link MultiBucketHistogram}.
   *          
   * @return Returns a pulsed form of the convolution of this and the input distributions.
   */
  public PulsedMultiBucketHistogram convolute(PulsedDistribution pulsedPdf2) {
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
    return new PulsedMultiBucketHistogram(convPDFValues, this.convolutionStepSize);
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
