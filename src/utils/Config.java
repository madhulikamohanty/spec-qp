package de.mpii.trinitreloaded.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.Sets;

import de.mpii.trinitreloaded.queryprocessing.IncrementalMerge;
/**
 * Class having values for all the configuration parameters. 
 * The properties are read from {@value configFile} file.
 * 
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 */
public class Config {

  public static String configFile = "conf/config.properties";
  /** the value for 'k' in the top-k results sought. */
  public static int k; 
  /** The maximum number of relaxations to be considered by {@link IncrementalMerge}. */
  public static int numOfRelaxations;

  /** Set {@code true} if only 'object' is to be relaxed. Set {@code false} if relaxation is permitted in all parts of the {@link TriplePattern}. */
  public static boolean onlyObjectRelaxed;

  /** The number of runs to be executed while doing experiments. */
  public static int numRuns;

  /** The 'k' values to be considered for experiments. */
  public static List<Integer> kVals;

  /** Set to true for synthetic and twitter data. */
  public static boolean isSyntheticData;
  
  /** Set to true for using RDF database (Virtuoso). */
  public static boolean isRDFDB;

  public static boolean isIncrementalWeighting;

  public static boolean debugmode;
  public enum LoggingLevel {
    TESTRUN(0),
    /**
     * When only information regarding program flow should be printed.
     */
    EXPERIMENTS(1),
    /**
     * When any intermediate information about various computations should be printed.
     */
    INTERMEDIATEINFO(2),
    /**
     * When variable values at various points should be printed.
     */
    VARIABLEVALUES(3);

    private final int numVal;

    LoggingLevel(int numVal) {
      this.numVal = numVal;
    }

    public int getNumVal() {
      return numVal;
    }
  }
  public static LoggingLevel loggingLevel;
  public static String queryFile;
  public static String resultFile;
  public static String outputFileInclStopWords = "outputwStopWords.csv";
  public static String outputFileExclStopWords = "outputwoStopWords.csv";

  /** Set of stopwords */
  public static final Set<String> ENGLISH_STOP_WORDS_SET = Sets.newHashSet("0", "1", "2", "3", "4", "5", "6", "7", "8",
      "9", "000", "$", "about", "after", "all", "also", "an", "and", "another", "any", "are", "as", "at", "be",
      "because", "been", "before", "being", "between", "both", "but", "by", "came", "can", "come", "could", "did", "do",
      "does", "each", "else", "for", "from", "get", "got", "has", "had", "he", "have", "her", "here", "him", "himself",
      "his", "how", "if", "in", "into", "is", "it", "its", "just", "like", "make", "many", "me", "might", "more",
      "most", "much", "must", "my", "never", "now", "of", "on", "only", "or", "other", "our", "out", "over", "re",
      "said", "same", "see", "should", "since", "so", "some", "still", "such", "take", "than", "that", "the", "their",
      "them", "then", "there", "these", "they", "this", "those", "through", "to", "too", "under", "up", "use", "very",
      "want", "was", "way", "we", "well", "were", "what", "when", "where", "which", "while", "who", "will", "with",
      "would", "you", "your", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r",
      "s", "t", "u", "v", "w", "x", "y", "z", "''s");

  /** Database Info. */
  public static String servContext = "";
  public static String dbConfigFile = "conf/trinit-qp-db.properties";
  public static String dataTableName = "trinit.data";
  public static String textualTypeDataTableName = "trinit.textual_type_data";
  public static String syntheticDataTableName = "trinit.twitterdatanewunique";
  public static String semanticTextualParaphrasesTblName = "trinit.type_predicate_paraphrases";
  public static String textualSemanticParaphrasesTblName = "trinit.type_predicate_paraphrases";
  public static String predicateRelationParaphraseTblName = "trinit.predicate_relation_paraphrases";
  public static String relationRelationParaphraseTblName = "trinit.relation_relation_paraphrases";
  public static String syntheticParaphraseTblName = "trinit.twitterrelaxationsfromnewdataunique";
  public static String scoreTableName = "trinit.entityInlinksCount";
  public static String rdfScoreTableName = "<xkg>";
  public static String rdfSyntheticGraphName = "<twitter>";

  /** Convolution Step Size */
  public static double convolutionStepSize;

  /** Types of query plan generation schemes */
  public enum PlanType {
    /**
     * Classical top-k querying as implemented in original version of TriniT.
     */
    NONSPECULATIVE,
    /**
     * For highly selective triple patterns, evaluate them, with
     * their relaxations, disjunctively in one shot using the
     * underlying database. Non-selective triple patterns are
     * checked for likelihood of their relaxation being invoked,
     * but selective ones are not. This means that in a
     * situation involving selective triple patterns, a top-k
     * join operator will definitely be needed.
     */
    SPECULATIVE_WITH_DISJUNCTION,
    /**
     * Applies the scheme that considers
     * likelihood of relaxations being invoked. No
     * disjunctive execution of selective triple
     * patterns with their relaxations.
     */
    FULLYSPECULATIVE,
    /**
     * Applies the scheme that considers
     * likelihood of relaxations being invoked. No
     * disjunctive execution of selective triple
     * patterns with their relaxations. It is
     * executed by only a single thread instead of
     * multithreaded execution. Gives
     * approximate top-k.
     */
    SINGLESPECULATIVE,
    /**
     * Only the results of the original query.
     */
    ORIGINAL
  }

  /** Types of query plan generation schemes */
  public enum relaxationType {
    INCLUDINGSTOPWORDS, EXCLUDINGSTOPWORDS
  }

  /**
   * The number of buckets for the histogram distribution of the scores.
   */
  public static int numBuckets;

  /**
   * The types of multi-bucket histogram to be built if {@code numBuckets}>2.
   */
  public enum HistogramType {
    EQUIWIDTH(0),
    EQUIDEPTH(1),
    POWERLAW(2),
    EQUIDEPTHSCORE(3),
    VOPTIMAL(4);

    private final int numVal;

    HistogramType(int numVal) {
      this.numVal = numVal;
    }

    public int getNumVal() {
      return numVal;
    }
  }

  public static HistogramType histType;
  
  /**
   * Learn 'r' adaptively or take the fixed value.
   */
  public static boolean adaptiveInflectionRank;
  public static double fractionOfScoreInTheHead;

  /** The value of 'r' that captures the fat head in the PDF of scores. */
  //TODO Learn it individually for each triple pattern.
  //Ideally it should be a member of the probability distribution.
  public static long inflectionRank;

  /**
   * The constant which is multiplied to the weights to make probabilities in the range [0,1].
   */
  public static double scoreMultipler;
  public static String graphURI = "http://xkg/";
  public static String syntheticGraphURI = "http://twitter/";

  /**
   * Loads the properties from the properties file, {@value de.mpii.trinitreloaded.utils.Config#configFile}.
   */
  public static void loadProperties(){
    Properties props = new Properties();
    try {
      props.load(new FileInputStream(new File(configFile)));
      Config.k = Integer.parseInt(props.getProperty("k"));
      Config.queryFile = props.getProperty("queryFile");
      Config.resultFile = Config.queryFile.replace(".txt", "")+"-benchmarkResultswithk-"+k+".csv";
      Config.numOfRelaxations = Integer.parseInt(props.getProperty("numRelaxations"));
      Config.numRuns = Integer.parseInt(props.getProperty("numRuns"));
      String[] kValues = props.getProperty("kVals").split(";");
      Config.kVals = new ArrayList<Integer>();
      for(String kValue: kValues){
        kVals.add(Integer.parseInt(kValue));
      }
      if(props.getProperty("onlyObjectRelaxed").equals("true"))
        Config.onlyObjectRelaxed = true;
      else
        Config.onlyObjectRelaxed = false;
      if(props.getProperty("isSyntheticData").equals("true"))
        Config.isSyntheticData = true;
      else
        Config.isSyntheticData = false;
      if(props.getProperty("isRDFDB").equals("true"))
        Config.isRDFDB = true;
      else
        Config.isRDFDB = false;
      if(props.getProperty("isIncrementalWeighting").equals("true"))
        Config.isIncrementalWeighting = true;
      else
        Config.isIncrementalWeighting = false;
      if(props.getProperty("debugMode").equals("true"))
        Config.debugmode = true;
      else
        Config.debugmode = false;
      if(props.getProperty("adaptiveInflectionRank").equals("true"))
        Config.adaptiveInflectionRank = true;
      else
        Config.adaptiveInflectionRank = false;
      Config.numBuckets = Integer.parseInt(props.getProperty("numBuckets"));
      Config.convolutionStepSize = Double.parseDouble(props.getProperty("convolutionStepSize"));
      Config.fractionOfScoreInTheHead = Double.parseDouble(props.getProperty("fractionOfScoreInTheHead"));
      Config.inflectionRank = Integer.parseInt(props.getProperty("inflectionRank"));
      Config.scoreMultipler = Double.parseDouble(props.getProperty("scoreMultipler"));
      int logLevel = Integer.parseInt(props.getProperty("logLevel"));
      switch(logLevel){
      case 0:
        Config.loggingLevel = LoggingLevel.TESTRUN;
        break;
      case 1:
        Config.loggingLevel = LoggingLevel.EXPERIMENTS;
        break;
      case 2:
        Config.loggingLevel = LoggingLevel.INTERMEDIATEINFO;
        break;
      case 3:
        Config.loggingLevel = LoggingLevel.VARIABLEVALUES;
        break;
      default:
        Config.loggingLevel = LoggingLevel.EXPERIMENTS;
      }
      
      int histType = Integer.parseInt(props.getProperty("histType"));
      switch(histType){
      case 0:
        Config.histType = HistogramType.EQUIWIDTH;
        break;
      case 1:
        Config.histType = HistogramType.EQUIDEPTH;
        break;
      case 2:
        Config.histType = HistogramType.POWERLAW;
        break;
      case 3:
        Config.histType = HistogramType.EQUIDEPTHSCORE;
        break;
      case 4:
        Config.histType = HistogramType.VOPTIMAL;
        break;
     default:
        Config.histType = HistogramType.EQUIWIDTH;
      }

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Prints the set properties' values.
   */
  public static void printProperties() {
    System.out.println(Config.getVal());
  }

  /**
   * To get printable version of set properties.
   * 
   * @return String of properties' values.
   */
  public static String getVal() {
    String val = "*****Property Values*****\n";
    if(Config.isRDFDB)
      val+="DB=Virtuoso\n";
    else
      val+="DB=PostgreSQL\n";
    val+="k="+Config.k+"\n";
    val+="numOfRelaxations="+Config.numOfRelaxations+"\n";
    val+="onlyObjectRelaxed="+Config.onlyObjectRelaxed+"\n";
    val+="numRuns="+Config.numRuns+"\n";
    val+="kVals="+Config.kVals+"\n";
    val+="isSyntheticData="+Config.isSyntheticData+"\n";
    val+="debugMode="+Config.debugmode+"\n";
    val+="numBuckets="+Config.numBuckets+"\n";
    val+="histType="+Config.histType+"\n";
    val+="adaptiveInflectionRank="+Config.adaptiveInflectionRank+"\n";
    val+="convolutionStepSize="+Config.convolutionStepSize+"\n";
    val+="fractionOfScoreInTheHead="+Config.fractionOfScoreInTheHead+"\n";
    val+="inflectionRank="+Config.inflectionRank+"\n";
    val+="scoreMultipler="+Config.scoreMultipler+"\n";
    val+="logLevel="+Config.loggingLevel+"\n";
    
    val+="*****End of Property Values*****\n";
    
    return val;
  }
}
