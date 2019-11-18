package de.mpii.trinitreloaded.main;

import de.mpii.trinitreloaded.experiments.Experiments;
import de.mpii.trinitreloaded.experiments.RankJoinExperiments;
import de.mpii.trinitreloaded.utils.Config;
import de.mpii.trinitreloaded.utils.Logger;

/**
 * The class with the main function to start running the Query Engine.
 *
 *
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 */
@SuppressWarnings("unused")
public class Start {

  public static void main(String[] args) {
    /**
     * Loads the properties from properties file, {@code Config.configFile}.
     */
    Config.loadProperties();
    Config.printProperties();
    
    /**
     * Uncomment this to run experiments and collate results for XKG and Twitter
     * using {@link RankJoin}.
     * Experiments rjExpt = new RankJoinExperiments();
     * rjExpt.oneTimeRun();
     * rjExpt.executeExperiments();
     */
    Experiments rjExpt = new RankJoinExperiments();
    rjExpt.executeExperiments();
  }
}
