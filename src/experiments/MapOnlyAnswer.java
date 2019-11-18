package de.mpii.trinitreloaded.experiments;

import java.util.HashMap;
import de.mpii.trinitreloaded.datastructures.Answer;

/**
 * An Answer with only the map of bindings and score for the computation of {@link Precision}
 * and {@link Recall} during experiments.
 *
 * @deprecated Use only scores' set to compute {@link Precision}
 * and {@link Recall}.
 * 
 * @author Madhulika Mohanty (madhulikam@cse.iitd.ac.in)
 *
 */
public class MapOnlyAnswer{
  public HashMap<String,String> bindings;
  public Double score;

  public MapOnlyAnswer(Answer ans) {
    super();

    this.bindings = new HashMap<String,String>();
    for(String var: ans.getVariables()){
      try {
        this.bindings.put(var, ans.getVariableBinding(var));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    this.score=ans.getScore();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    MapOnlyAnswer other = (MapOnlyAnswer) obj;
    /* Taking precision only on scores.
    if (bindings == null) {
      if (other.bindings != null)
        return false;
    } else if (!bindings.equals(other.bindings))
      return false;*/

    if(other.score==this.score)
      return true;
    else{
      return false;
    }
  }

  @Override
  public String toString() {
    return "MapOnlyAnswer [bindings=" + bindings + ", score=" + this.score + "]";
  }
}
