package de.mpii.trinitreloaded.datastructures;

import java.util.Set;
/**
 * Class to encapsulate the {@link TriplePattern} with the information
 * regarding its relaxation/non-relaxation.
 * 
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 */
public class TriplePatternInPlan{
  public final Set<TriplePattern> tps;
  public final boolean isRelaxed;

  public TriplePatternInPlan(Set<TriplePattern> tps, boolean isRelaxed){
    this.tps=tps;
    this.isRelaxed=isRelaxed;
  }
  
  public Set<TriplePattern> getTPSet(){
    return this.tps;
  }

  @Override
  public String toString() {
    return "[" + tps + ", isRelaxed=" + isRelaxed + "]";
  }
  
  
}