package de.mpii.trinitreloaded.utils;

import de.mpii.trinitreloaded.datastructures.ThreeBucketHistogram;
import java.util.ArrayList;
import com.google.common.collect.Lists;
import de.mpii.trinitreloaded.datastructures.MultiBucketHistogram;
import de.mpii.trinitreloaded.datastructures.ProbabilityDistribution;
import de.mpii.trinitreloaded.datastructures.PulsedMultiBucketHistogram;
import de.mpii.trinitreloaded.datastructures.PulsedThreeBucketHistogram;
import de.mpii.trinitreloaded.datastructures.PulsedScoreProbabilityDistribution;
import de.mpii.trinitreloaded.datastructures.ScoreProbabilityDistributionFunction;

/**
 * A class to generate (approximate) convolution of two {@link ScoreProbabilityDistributionFunction}.
 * It is an approximation because we take two two-piecewise distributions as inputs, compute the convolution of the two (which is
 * a multiple-piecewise distribution) and then approximate this to a two-piecewise distribution again. It makes use of {@link PulsedScoreProbabilityDistribution}
 * to compute the convolution in pulsed forms.
 *
 * Usage:
 * Call the static method, {@code convolute()} and pass the two
 * {@link ScoreProbabilityDistributionFunction} objects whose convolution is desired.
 *
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 */
public class Convolution {
  public static final double CONVOLUTION_STEP_SIZE = Config.convolutionStepSize;//TODO compute step size on the fly.

  /**
   * Computes the convolution of two {@link ProbabilityDistribution}.
   * 
   * @param pdf1 First {@link ProbabilityDistribution}
   * @param pdf2 Second {@link ProbabilityDistribution}
   * @param joinSelectivity Join selectivity value of joining the two triple patterns.
   * @return Convoluted {@link ProbabilityDistribution}
   */
  public static ProbabilityDistribution convolute(ProbabilityDistribution pdf1,
      ProbabilityDistribution pdf2, double joinSelectivity) {
    if(pdf1.getClass()==ScoreProbabilityDistributionFunction.class)
      return convoluteSPD((ScoreProbabilityDistributionFunction)pdf1,(ScoreProbabilityDistributionFunction)pdf2,joinSelectivity);
    else
      return convoluteMultiBucketHistogram((MultiBucketHistogram)pdf1,(MultiBucketHistogram)pdf2,joinSelectivity);
  }


  /**
   * Computes convolution of two {@link ScoreProbabilityDistributionFunction}.
   * 
   * @param pdf1 First {@link ScoreProbabilityDistributionFunction}
   * @param pdf2 Second {@link ScoreProbabilityDistributionFunction}
   * @param joinSelectivity Join selectivity value of joining the two triple patterns.
   * @return Convoluted {@link ScoreProbabilityDistributionFunction}
   */
  public static ScoreProbabilityDistributionFunction convoluteSPD(ScoreProbabilityDistributionFunction pdf1,
      ScoreProbabilityDistributionFunction pdf2, double joinSelectivity) {
    ScoreProbabilityDistributionFunction convPDF = null;
    long numResults = (long) Math.floor((joinSelectivity*pdf1.getNumResults()*pdf2.getNumResults()));

    double rankRPercentilePoint = (new Double(numResults)-Config.inflectionRank+1)/(new Double(numResults)+1);
    if(rankRPercentilePoint<0) {
      rankRPercentilePoint=0;
    }

    /**
     * Create pulsed forms for each PDFs.
     */
    PulsedScoreProbabilityDistribution pulsedPdf1 = new PulsedScoreProbabilityDistribution(pdf1,Convolution.CONVOLUTION_STEP_SIZE);
    PulsedScoreProbabilityDistribution pulsedPdf2 = new PulsedScoreProbabilityDistribution(pdf2,Convolution.CONVOLUTION_STEP_SIZE);

    /**
     * Compute the convolution in the pulsed form.
     */
    PulsedScoreProbabilityDistribution convPulsedPDF = pulsedPdf1.convolute(pulsedPdf2);

    /**
     * Approximate the pulsed PDF into a 2-step function.
     */
    convPDF = new ScoreProbabilityDistributionFunction(convPulsedPDF.getScoreAtRankR(rankRPercentilePoint),
        convPulsedPDF.getCumulativeScore(Config.inflectionRank,numResults), convPulsedPDF.getCumulativeScore(numResults,numResults),
        numResults, pdf1.getMaxScore()+pdf2.getMaxScore());
    Logger.println("PulsedPDf1:"+pulsedPdf1.sampledValues, Config.LoggingLevel.VARIABLEVALUES);
    Logger.println("PulsedPDf2:"+pulsedPdf2.sampledValues, Config.LoggingLevel.VARIABLEVALUES);
    Logger.println("ConvPulsedPDF:"+convPulsedPDF.sampledValues, Config.LoggingLevel.VARIABLEVALUES);
    Logger.println("ConvPDF:"+convPDF, Config.LoggingLevel.VARIABLEVALUES);
    return convPDF;
  }

  /**
   * Computes convolution of two {@link MultiBucketHistogram}.
   * 
   * @param pdf1 First {@link MultiBucketHistogram}
   * @param pdf2 Second {@link MultiBucketHistogram}
   * @param joinSelectivity Join selectivity value of joining the two triple patterns.
   * @return Convoluted {@link MultiBucketHistogram}
   */
  private static MultiBucketHistogram convoluteMultiBucketHistogram(MultiBucketHistogram pdf1,
      MultiBucketHistogram pdf2, double joinSelectivity) {
    MultiBucketHistogram convPDF = null;
    int numResults = (int) Math.floor((joinSelectivity*pdf1.getNumResults()*pdf2.getNumResults()));


    /**
     * Create pulsed forms for each PDFs.
     */
    PulsedMultiBucketHistogram pulsedPdf1 = new PulsedMultiBucketHistogram((MultiBucketHistogram)pdf1,Convolution.CONVOLUTION_STEP_SIZE);
    PulsedMultiBucketHistogram pulsedPdf2 = new PulsedMultiBucketHistogram((MultiBucketHistogram)pdf2,Convolution.CONVOLUTION_STEP_SIZE);

    /**
     * Compute the convolution in the pulsed form.
     */
    PulsedMultiBucketHistogram convPulsedPDF = pulsedPdf1.convolute(pulsedPdf2);

    /**
     * Approximate the pulsed PDF into a multi-step function.
     */
    convPDF = getPDFFromPulse(convPulsedPDF, numResults, pdf1.getMaxScore()+pdf2.getMaxScore());

    Logger.println("PulsedPDf1:"+pulsedPdf1.sampledValues, Config.LoggingLevel.VARIABLEVALUES);
    Logger.println("PulsedPDf2:"+pulsedPdf2.sampledValues, Config.LoggingLevel.VARIABLEVALUES);
    Logger.println("ConvPulsedPDF:"+convPulsedPDF.sampledValues, Config.LoggingLevel.VARIABLEVALUES);
    Logger.println("ConvPDF:"+convPDF, Config.LoggingLevel.VARIABLEVALUES);
    return convPDF;  
  }


  private static MultiBucketHistogram getPDFFromPulse(PulsedMultiBucketHistogram convPulsedPDF, int numResults,
      double maxScore) {
    MultiBucketHistogram mbh = null;
    switch(Config.histType){
    case EQUIDEPTH:
    {
      ArrayList<Double> scoreAtRanksTemp = Lists.newArrayList();
      ArrayList<Double> cumulativeScoreAtRanksTemp = Lists.newArrayList();
      int bucketDepth = numResults/Config.numBuckets;
      if(numResults<Config.numBuckets)
        bucketDepth = 1;
      int bucketCount = 1;
      double cumulativeScore = 0.0;
      double count = 0.0;
      int currentNumItems = 0;
      for(int i=1;i<=numResults; i++) {
        double rankRPercentilePoint = (new Double(numResults)-i+1)/(new Double(numResults)+1);
        if(rankRPercentilePoint<0) {
          rankRPercentilePoint=0;
        }
        count = convPulsedPDF.getScoreAtRankR(rankRPercentilePoint);
        if(currentNumItems == bucketDepth && bucketCount < Config.numBuckets){ // We have crossed a bucket boundary, update values accordingly.
          scoreAtRanksTemp.add(count); // The bucket boundary for equi-depth histograms is determined by the number of items in each bucket.
          cumulativeScoreAtRanksTemp.add(cumulativeScore);
          currentNumItems = 0;
          bucketCount++;
        }
        cumulativeScore+=count;
        currentNumItems++;
      }

      //If the number of buckets haven't been reached, fill 0 in the remaining.
      while(bucketCount < Config.numBuckets){
        scoreAtRanksTemp.add(count);
        cumulativeScoreAtRanksTemp.add(cumulativeScore);
        count=0.0;
        bucketCount++;
      }

      // Initialize the class objects.
      mbh = new MultiBucketHistogram(scoreAtRanksTemp, cumulativeScoreAtRanksTemp, cumulativeScore, numResults, maxScore, Config.numBuckets);
      break;
    }

    case EQUIDEPTHSCORE:
    {
      ArrayList<Double> scoreAtRanksTemp = Lists.newArrayList();
      ArrayList<Double> cumulativeScoreAtRanksTemp = Lists.newArrayList();

      int bucketCount = 1;
      double totalCumulativeScore = 0.0, cumulativeScore = 0.0;
      double count = 0.0;

      // Get total cumulative score.
      for(int i=1;i<=numResults; i++) {
        double rankRPercentilePoint = (new Double(numResults)-i+1)/(new Double(numResults)+1);
        if(rankRPercentilePoint<0) {
          rankRPercentilePoint=0;
        }
        count = convPulsedPDF.getScoreAtRankR(rankRPercentilePoint);
        totalCumulativeScore+=count;
      }

      double bucketDepthScore = totalCumulativeScore/Config.numBuckets; // The cumulative score in each bucket.
      count = 0.0;

      for(int i=1;i<=numResults; i++) {
        double rankRPercentilePoint = (new Double(numResults)-i+1)/(new Double(numResults)+1);
        if(rankRPercentilePoint<0) {
          rankRPercentilePoint=0;
        }
        count = convPulsedPDF.getScoreAtRankR(rankRPercentilePoint);
        if(cumulativeScore >= (bucketCount*bucketDepthScore) && bucketCount < Config.numBuckets){ // We have crossed a bucket boundary, update values accordingly.
          scoreAtRanksTemp.add(count); // The bucket boundary for equi-depth-score histograms is determined by the cumulative score in each bucket.
          cumulativeScoreAtRanksTemp.add(cumulativeScore);
          bucketCount++;
        }
        cumulativeScore+=count;
      }

      //If the number of buckets haven't been reached, fill 0 in the remaining.
      while(bucketCount < Config.numBuckets){
        scoreAtRanksTemp.add(count);
        cumulativeScoreAtRanksTemp.add(cumulativeScore);
        count=0.0;
        bucketCount++;
      }

      // Initialize the class objects.
      mbh = new MultiBucketHistogram(scoreAtRanksTemp, cumulativeScoreAtRanksTemp, cumulativeScore, numResults, maxScore, Config.numBuckets);
      break;
    }

    case VOPTIMAL:
      throw new UnsupportedOperationException("Invalid plan type");

    case EQUIWIDTH:
    default:
    {
      ArrayList<Double> scoreAtRanksTemp = Lists.newArrayList();
      ArrayList<Double> cumulativeScoreAtRanksTemp = Lists.newArrayList();
      double bucketWidth = Math.round((maxScore/Config.numBuckets)*100.0)/100.0;
      double leftEnd, rightEnd;
      rightEnd = maxScore;
      leftEnd = rightEnd - bucketWidth;
      double cumulativeScore = 0.0;
      int bucketCount = 1;

      for(int i=1;i<=numResults; i++) {
        double rankRPercentilePoint = (new Double(numResults)-i+1)/(new Double(numResults)+1);
        if(rankRPercentilePoint<0) {
          rankRPercentilePoint=0;
        }
        double count = convPulsedPDF.getScoreAtRankR(rankRPercentilePoint);
        if(count < leftEnd){ // We have crossed a bucket boundary, update values accordingly.
          bucketCount++;
          scoreAtRanksTemp.add(leftEnd); // The bucket boundary for equi-width histograms is fixed by the maxScore and width of the bucket.
          cumulativeScoreAtRanksTemp.add(cumulativeScore);
          rightEnd = leftEnd;
          if(bucketCount == Config.numBuckets)
            leftEnd = 0.0;
          else
            leftEnd = rightEnd - bucketWidth;
        }
        cumulativeScore+=count;
      }

      //If the number of buckets haven't been reached, fill 0 in the remaining.
      while(bucketCount < Config.numBuckets){
        bucketCount++;
        scoreAtRanksTemp.add(leftEnd); // The bucket boundary for equi-width histograms is fixed by the maxScore and width of the bucket.
        cumulativeScoreAtRanksTemp.add(cumulativeScore);
        rightEnd = leftEnd;
        if(bucketCount == Config.numBuckets)
          leftEnd = 0.0;
        else
          leftEnd = rightEnd - bucketWidth;
      }

      mbh = new MultiBucketHistogram(scoreAtRanksTemp, cumulativeScoreAtRanksTemp, cumulativeScore, numResults, maxScore, Config.numBuckets);
    }
    }
    return mbh;
  }


  /**
   * Computes convolution of two {@link ThreeBucketHistogram}.
   * 
   * @param pdf1 First {@link ThreeBucketHistogram}
   * @param pdf2 Second {@link ThreeBucketHistogram}
   * @param joinSelectivity Join selectivity value of joining the two triple patterns.
   * @return Convoluted {@link ThreeBucketHistogram}
   */
  public static ThreeBucketHistogram convoluteThreeBucketHistogram(ThreeBucketHistogram pdf1,
      ThreeBucketHistogram pdf2, double joinSelectivity) {
    ThreeBucketHistogram convPDF = null;
    long numResults = (long) Math.floor((joinSelectivity*pdf1.getNumResults()*pdf2.getNumResults()));

    long inflectionR1 = numResults/3;
    long inflectionR2 = 2*numResults/3;

    double rankR1PercentilePoint = (new Double(numResults)-inflectionR1+1)/(new Double(numResults)+1);
    double rankR2PercentilePoint = (new Double(numResults)-inflectionR2+1)/(new Double(numResults)+1);
    if(rankR1PercentilePoint<0) {
      rankR1PercentilePoint=0;
    }
    if(rankR2PercentilePoint<0) {
      rankR2PercentilePoint=0;
    }

    /**
     * Create pulsed forms for each PDFs.
     */
    PulsedThreeBucketHistogram pulsedPdf1 = new PulsedThreeBucketHistogram((ThreeBucketHistogram)pdf1,Convolution.CONVOLUTION_STEP_SIZE);
    PulsedThreeBucketHistogram pulsedPdf2 = new PulsedThreeBucketHistogram((ThreeBucketHistogram)pdf2,Convolution.CONVOLUTION_STEP_SIZE);

    /**
     * Compute the convolution in the pulsed form.
     */
    PulsedThreeBucketHistogram convPulsedPDF = pulsedPdf1.convolute(pulsedPdf2);

    /**
     * Approximate the pulsed PDF into a 3-step function.
     */
    convPDF = new ThreeBucketHistogram(convPulsedPDF.getScoreAtRankR(rankR1PercentilePoint),
        convPulsedPDF.getCumulativeScore(inflectionR1,numResults), convPulsedPDF.getScoreAtRankR(rankR2PercentilePoint),
        convPulsedPDF.getCumulativeScore(inflectionR2,numResults), convPulsedPDF.getCumulativeScore(numResults,numResults),
        numResults, pdf1.getMaxScore()+pdf2.getMaxScore());

    Logger.println("PulsedPDf1:"+pulsedPdf1.sampledValues, Config.LoggingLevel.VARIABLEVALUES);
    Logger.println("PulsedPDf2:"+pulsedPdf2.sampledValues, Config.LoggingLevel.VARIABLEVALUES);
    Logger.println("ConvPulsedPDF:"+convPulsedPDF.sampledValues, Config.LoggingLevel.VARIABLEVALUES);
    Logger.println("ConvPDF:"+convPDF, Config.LoggingLevel.VARIABLEVALUES);
    return convPDF;  
  }

}
