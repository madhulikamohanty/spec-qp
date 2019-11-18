package de.mpii.trinitreloaded.datastructures;

import java.util.Set;

import de.mpii.trinitreloaded.utils.Config.PlanType;

/**
 * A query execution plan. It is basically a partitioning of the
 * {@link TriplePattern} in the given {@link Query}.
 *
 *
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 */

public class LogicalQueryPlan {
  public final Set<TriplePatternInPlan> plan;
  public final PlanType planType;

  public LogicalQueryPlan(Set<TriplePatternInPlan> plan, PlanType planType) {
    this.plan = plan;
    this.planType = planType;
  }

  public Set<TriplePatternInPlan> getPlan() {       
    return this.plan;
  }       

  @Override    
  public String toString() {   
    return "LogicalQueryPlan [plan=" + plan + ", planType=" + planType + "]";    
  }

}
