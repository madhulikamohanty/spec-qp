package de.mpii.trinitreloaded.datastructures;

import de.mpii.trinitreloaded.utils.Config;
import de.mpii.trinitreloaded.utils.Logger;

/**
 * A probability distribution function of scores.
 *
 * Defines a PDF for scores of answers.
 *
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 */
public class ScoreProbabilityDistributionFunction implements ProbabilityDistribution{

  public final double scoreAtRankR;
  public final double cumulativeScoreAtRankR;
  public final double cumulativeScoreAtRankN;
  public final long numberOfResults;
  public final double maxScore;

  public ScoreProbabilityDistributionFunction(double scoreAtRankR, double cumulativeScoreAtRankR,
      double cumulativeScoreAtRankN, long numberOfResults, double maxScore) {
    this.scoreAtRankR = scoreAtRankR;
    this.cumulativeScoreAtRankR = cumulativeScoreAtRankR;
    this.cumulativeScoreAtRankN = cumulativeScoreAtRankN;
    this.numberOfResults = numberOfResults;
    this.maxScore = maxScore;
  }

  public double getPercentile(long rank) {
    if(rank>this.numberOfResults)
      return 0.0;
    double probabilityOfScoreAtGivenRank = new Double(this.numberOfResults - rank + 1) / new Double(this.numberOfResults + 1);
    double a,b,c;
    if(this.cumulativeScoreAtRankN!=0.0){ // To avoid divide by 0 and NaN.
      if(this.scoreAtRankR!=0.0)
        a = (this.cumulativeScoreAtRankN - this.cumulativeScoreAtRankR)
        / (this.cumulativeScoreAtRankN * this.scoreAtRankR);
      else
        a = 0.0;
      b = (this.cumulativeScoreAtRankR) / (this.cumulativeScoreAtRankN * (this.maxScore - this.scoreAtRankR));
    }
    else
    {
      a = 0.0;
      b = 0.0;
    }
    if(this.cumulativeScoreAtRankN!=0.0){
      c = (this.cumulativeScoreAtRankN - this.cumulativeScoreAtRankR)
          / (this.cumulativeScoreAtRankN) - b*this.scoreAtRankR;
    }
    else
      c = 0.0;
    Logger.println("a:"+a, Config.LoggingLevel.VARIABLEVALUES);
    Logger.println("b:"+b, Config.LoggingLevel.VARIABLEVALUES);
    Logger.println("c:"+c, Config.LoggingLevel.VARIABLEVALUES);
    Logger.println("a * this.scoreAtRankR:"+(a * this.scoreAtRankR), Config.LoggingLevel.VARIABLEVALUES);
    Logger.println("x_value:"+probabilityOfScoreAtGivenRank, Config.LoggingLevel.VARIABLEVALUES);
    if (a * this.scoreAtRankR >= probabilityOfScoreAtGivenRank) {
      if(a>0.0)
        return probabilityOfScoreAtGivenRank / a;
      else
        return 0.0;
    } else {
      if(b!=0.0)
        return (probabilityOfScoreAtGivenRank - c) / (b);
      else
        return 0.0;
    }
  }

  @Override
  public String toString() {
    return "ScoreProbabilityDistributionFunction [scoreAtRankR=" + scoreAtRankR + ", cumulativeScoreAtRankR="
        + cumulativeScoreAtRankR + ", cumulativeScoreAtRankN=" + cumulativeScoreAtRankN + ", numberOfResults="
        + numberOfResults + ", maxScore=" + maxScore + "]";
  }

  public long getNumResults() {
    return this.numberOfResults;
  }

  public double getMaxScore() {
    return this.maxScore;
  }

}
