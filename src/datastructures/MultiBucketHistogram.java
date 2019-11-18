package de.mpii.trinitreloaded.datastructures;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

import de.mpii.trinitreloaded.queryprocessing.Operator;
import de.mpii.trinitreloaded.queryprocessing.PopularityBasedScan;
import de.mpii.trinitreloaded.queryprocessing.SyntheticScan;
import de.mpii.trinitreloaded.utils.Config;
import de.mpii.trinitreloaded.utils.Logger;
//import de.mpii.trinitreloaded.utils.Config.LoggingLevel;

/**
 * A probability distribution function of scores for the answers to triple patterns.
 *
 * The distribution is modelled as an 'n' bucket histogram.
 * 
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 */
public class MultiBucketHistogram implements ProbabilityDistribution {

  public List<Double> scoreAtRanks; // Value of scores at each bucket boundary starting from the right end.
  public List<Double> cumulativeScoreAtRanks; // Stores cumulative scores at each bucket boundary starting from the right end.
  public double cumulativeScoreAtRankN; // Total cumulative score.
  public int numberOfResults; // TODO: int or long?
  public double maxScore;
  public int n; // Number of buckets in the histogram.


  public MultiBucketHistogram(ArrayList<Double> scoreAtRanks, ArrayList<Double> cumulativeScoreAtRanks, double cumulativeScoreAtRankN, int numberOfResults, double maxScore, int n) {
    super();
    this.scoreAtRanks = scoreAtRanks;
    this.cumulativeScoreAtRanks = cumulativeScoreAtRanks;
    this.cumulativeScoreAtRankN = cumulativeScoreAtRankN;
    this.numberOfResults = numberOfResults;
    this.maxScore = maxScore;
    this.n=n;
    assert this.numberOfResults>this.n : "The number of buckets in the histogram cannot be greater than the total number of triple pattern matches.";
  }

  public MultiBucketHistogram(TriplePattern tp, double weight) {

    int resultsCount = 0;
    double maxScore = 0.0;
    double totalCumulativeScore = 0.0;
    Operator sc, sc1;
    if(!Config.isSyntheticData){
      sc = new PopularityBasedScan(tp, false);
      sc1 = new PopularityBasedScan(tp, false);
    }
    else{
      sc = new SyntheticScan(tp, false);
      sc1 = new SyntheticScan(tp, false);
    }

    try {
      sc1.open();
      while (sc1.hasNext()) { 
        Answer a = sc1.next();
        double count = a.getScore();
        if (resultsCount == 0) {
          maxScore = count;
        }
        totalCumulativeScore+=count;
        resultsCount++;
      }
      sc1.close();
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
    // TODO: Check if this assertion is in fact true.
    assert resultsCount>Config.numBuckets : "The number of buckets in the histogram cannot be greater than the total number of triple pattern matches.";

    List<Double> scoreAtRanksTemp;
    List<Double> cumulativeScoreAtRanksTemp;

    switch(Config.histType){
    case EQUIDEPTH:
    {
      scoreAtRanksTemp = Lists.newArrayList();
      cumulativeScoreAtRanksTemp = Lists.newArrayList();
      int bucketDepth = resultsCount/Config.numBuckets;
      if(resultsCount<Config.numBuckets)
        bucketDepth = 1;
      int bucketCount = 1;
      double cumulativeScore = 0.0;
      double count = 0.0;
      int currentNumItems = 0;
      try {
        sc.open();
        while(sc.hasNext()){
          Answer a = sc.next();
          count = a.getScore();
          if(currentNumItems == bucketDepth && bucketCount < Config.numBuckets){ // We have crossed a bucket boundary, update values accordingly.
            scoreAtRanksTemp.add(count); // The bucket boundary for equi-depth histograms is determined by the number of items in each bucket.
            cumulativeScoreAtRanksTemp.add(cumulativeScore);
            currentNumItems = 0;
            bucketCount++;
          }
          cumulativeScore+=count;
          currentNumItems++;
        }
        sc.close();

        //If the number of buckets haven't been reached, fill 0 in the remaining.
        while(bucketCount < Config.numBuckets){
          scoreAtRanksTemp.add(count); 
          cumulativeScoreAtRanksTemp.add(cumulativeScore);
          count=0.0;
          bucketCount++;
        }

        // Initialize the class objects.
        this.scoreAtRanks = scoreAtRanksTemp;
        this.cumulativeScoreAtRanks = cumulativeScoreAtRanksTemp;
        this.cumulativeScoreAtRankN = cumulativeScore;
        this.numberOfResults = resultsCount;
        this.maxScore = maxScore;
        this.n = Config.numBuckets;

      } catch (SQLException e) {
        e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      }

      break;
    }
    case EQUIDEPTHSCORE:
    {
      scoreAtRanksTemp = Lists.newArrayList();
      cumulativeScoreAtRanksTemp = Lists.newArrayList();
      double bucketDepthScore = totalCumulativeScore/Config.numBuckets; // The cumulative score in each bucket.
      int bucketCount = 1;
      double cumulativeScore = 0.0;
      double count = 0.0;
      try {
        sc.open();
        while(sc.hasNext()){
          Answer a = sc.next();
          count = a.getScore();
          if(cumulativeScore >= (bucketCount*bucketDepthScore) && bucketCount < Config.numBuckets){ // We have crossed a bucket boundary, update values accordingly.
            scoreAtRanksTemp.add(count); // The bucket boundary for equi-depth-score histograms is determined by the cumulative score in each bucket.
            cumulativeScoreAtRanksTemp.add(cumulativeScore);
            bucketCount++;
          }
          cumulativeScore+=count;
        }
        sc.close();

        //If the number of buckets haven't been reached, fill 0 in the remaining.
        while(bucketCount < Config.numBuckets){
          scoreAtRanksTemp.add(count); 
          cumulativeScoreAtRanksTemp.add(cumulativeScore);
          count=0.0;
          bucketCount++;
        }

        // Initialize the class objects.
        this.scoreAtRanks = scoreAtRanksTemp;
        this.cumulativeScoreAtRanks = cumulativeScoreAtRanksTemp;
        this.cumulativeScoreAtRankN = cumulativeScore;
        this.numberOfResults = resultsCount;
        this.maxScore = maxScore;
        this.n = Config.numBuckets;

      } catch (SQLException e) {
        e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      }

      break;
    } 
    case VOPTIMAL:
      throw new UnsupportedOperationException("Invalid plan type");

    case EQUIWIDTH:
    default:
    {
      scoreAtRanksTemp = Lists.newArrayList();
      cumulativeScoreAtRanksTemp = Lists.newArrayList();
      double bucketWidth = Math.round((maxScore/Config.numBuckets)*100.0)/100.0;
      //Logger.println("Value of bucketWidth:"+ bucketWidth, Config.LoggingLevel.INTERMEDIATEINFO);
      //Logger.println("Value of maxScore:"+ maxScore, Config.LoggingLevel.INTERMEDIATEINFO);
      double leftEnd, rightEnd;
      rightEnd = maxScore;
      leftEnd = rightEnd - bucketWidth;
      //Logger.println("Value of leftEnd:"+ leftEnd, Config.LoggingLevel.INTERMEDIATEINFO);
      //Logger.println("Value of rightEnd:"+ rightEnd, Config.LoggingLevel.INTERMEDIATEINFO);
      double cumulativeScore = 0.0;
      int bucketCount = 1;
      double count = 0.0;
      try {
        sc.open();
        while(sc.hasNext()){
          Answer a = sc.next();
          count = a.getScore();
          if(count < leftEnd){ // We have crossed a bucket boundary, update values accordingly.
            /*Logger.println("MultiBucketHistogram: New bucket.", Config.LoggingLevel.INTERMEDIATEINFO);
            Logger.println("----------------------", Config.LoggingLevel.INTERMEDIATEINFO);
            Logger.println("Value of leftEnd:"+ leftEnd, Config.LoggingLevel.INTERMEDIATEINFO);
            Logger.println("Value of rightEnd:"+ rightEnd, Config.LoggingLevel.INTERMEDIATEINFO);
            Logger.println("----------------------", Config.LoggingLevel.INTERMEDIATEINFO);*/
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
        /*Logger.println("Value of count:"+ count, Config.LoggingLevel.INTERMEDIATEINFO);
        Logger.println("Value of bucketCount:"+ bucketCount, Config.LoggingLevel.INTERMEDIATEINFO);
        Logger.println("Value of leftEnd:"+ leftEnd, Config.LoggingLevel.INTERMEDIATEINFO);
        Logger.println("Value of rightEnd:"+ rightEnd, Config.LoggingLevel.INTERMEDIATEINFO);*/
        sc.close();

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

        // Initialize the class objects.
        this.scoreAtRanks = scoreAtRanksTemp;
        //Logger.println("MultiBucketHistogram -- Size of scoreAtRanks: "+ scoreAtRanks.size(), Config.LoggingLevel.INTERMEDIATEINFO);
        this.cumulativeScoreAtRanks = cumulativeScoreAtRanksTemp;
        //Logger.println("MultiBucketHistogram -- Size of cumulativeScoreAtRanks: "+ cumulativeScoreAtRanks.size(), Config.LoggingLevel.INTERMEDIATEINFO);
        this.cumulativeScoreAtRankN = cumulativeScore;
        this.numberOfResults = resultsCount;
        this.maxScore = maxScore;
        this.n = Config.numBuckets;

      } catch (SQLException e) {
        e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    }
  }

  /**
   * Get total score from a {@link TriplePattern}.
   * 
   * @param tp
   *        The {@link TriplePattern} whose total score is required.
   * @return The sum of the scores from the matches of {@code tp}.
   */
  @SuppressWarnings("unused")
  private double getSumOfTotalScore(TriplePattern tp) {
    Operator sc;
    if(!Config.isSyntheticData)
      sc = new PopularityBasedScan(tp, false);
    else
      sc = new SyntheticScan(tp, false);
    double totalScore = 0.0;
    try {
      sc.open();
      while(sc.hasNext()){
        Answer a = sc.next();
        totalScore+=a.getScore();
      }
      sc.close();
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return totalScore;
  }

  public double getPercentile(long rank) {
    if(rank>this.numberOfResults)
      return 0.0;
    double probabilityOfScoreAtGivenRank = new Double(this.numberOfResults - rank + 1) / new Double(this.numberOfResults + 1);

    // Computing constants first.
    double a,b0,c0;
    if(this.cumulativeScoreAtRankN!=0.0){ // To avoid divide by 0 and NaN.
      if(this.scoreAtRanks.get(this.n-2)!=0.0)
        a = (this.cumulativeScoreAtRankN - this.cumulativeScoreAtRanks.get(this.n-2))
        / (this.cumulativeScoreAtRankN * this.scoreAtRanks.get(this.n-2));
      else
        a = 0.0;

      b0 = (this.cumulativeScoreAtRanks.get(0))
          / (this.cumulativeScoreAtRankN * (this.maxScore-this.scoreAtRanks.get(0)));

      c0 = 1-b0*this.maxScore;
    }
    else
    {
      a = 0.0;
      b0 = 0.0;
      c0 = 0.0;
    }

    Logger.println("a:"+a, Config.LoggingLevel.VARIABLEVALUES);
    Logger.println("b0:"+b0, Config.LoggingLevel.VARIABLEVALUES);
    Logger.println("c0:"+c0, Config.LoggingLevel.VARIABLEVALUES);
    Logger.println("a * this.scoreAtRankR1:"+(a * this.scoreAtRanks.get(this.n-2)), Config.LoggingLevel.VARIABLEVALUES);
    Logger.println("x_value:"+probabilityOfScoreAtGivenRank, Config.LoggingLevel.VARIABLEVALUES);

    if (probabilityOfScoreAtGivenRank <= a * this.scoreAtRanks.get(this.n-2)) { // If it lies in the first bucket starting from 0.
      if(a>0.0)
        return probabilityOfScoreAtGivenRank / a;
      else
        return 0.0;
    } else if(probabilityOfScoreAtGivenRank>=(b0*this.scoreAtRanks.get(0) + c0)){ // If it lies in the last bucket.
      if(b0!=0.0)
        return (probabilityOfScoreAtGivenRank - c0) / (b0);
      else
        return 0.0;
    }
    else{ // Figure out which bucket it lies in and compute accordingly.
      return getValueForInternalBuckets(probabilityOfScoreAtGivenRank,b0,c0);
    }
  }

  private double getValueForInternalBuckets(double probabilityOfScoreAtGivenRank, double b0, double c0) {
    double cp,cq,bp,bq;
    cp = c0;
    bp = b0;
    for(int i=1;i<this.n-2;i++){
      if(this.cumulativeScoreAtRankN!=0.0)
        bq = (this.cumulativeScoreAtRanks.get(i+1) - this.cumulativeScoreAtRanks.get(i))
        / (this.cumulativeScoreAtRankN * (this.scoreAtRanks.get(i)-this.scoreAtRanks.get(i+1)));
      else
        bq = 0.0;
      cq = ((bp-bq)*this.scoreAtRanks.get(i+1))+cp;
      double start,end;
      start = bq*this.scoreAtRanks.get(i+1)+cq;
      end = bq*this.scoreAtRanks.get(i)+cq;
      if((probabilityOfScoreAtGivenRank>=start)&&(probabilityOfScoreAtGivenRank<=end))
        return (probabilityOfScoreAtGivenRank-cq)/bq;
      bp = bq;
      cp = cq;
    }
    return 0.0;
  }

  @Override
  public String toString() {
    return "MultiBucketHistogram [scoreAtRanks=" + scoreAtRanks + ", cumulativeScoreAtRanks=" + cumulativeScoreAtRanks
        + ", cumulativeScoreAtRankN=" + cumulativeScoreAtRankN + ", numberOfResults=" + numberOfResults + ", maxScore="
        + maxScore + ", n=" + n + "]";
  }

  public long getNumResults() {
    return this.numberOfResults;
  }

  public double getMaxScore() {
    return this.maxScore;
  }

}
