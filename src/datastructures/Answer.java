package de.mpii.trinitreloaded.datastructures;

import java.util.List;

/**
 * An answer is a mapping of variables to constants (tokens).
 */
public interface Answer {

  /**
   * Returns true if the answer contains a binding for the passed variable and false otherwise.
   */
  public boolean hasVariableBinding(String variable);

  /**
   * Returns the binding for the passed variable.
   *
   * TODO Change to a more appropriate exception.
   *
   * @throws Exception
   *           if the passed variable has no binding in this answer.
   */
  public String getVariableBinding(String variable) throws Exception;

  /**
   * Sets the binding for the passed variable.
   *
   * TODO Change to a more appropriate exception.
   *
   * @throws Exception
   *           if the passed variable is already bound in this answer.
   */
  public void setVariableBinding(String variable, String binding) throws Exception;

  /**
   * Returns the score of the answer.
   */
  public double getScore();

  /**
   * Sets the score of the answer.
   */
  public void setScore(double d);

  /**
   * Returns the list of the variables whose bindings are present in the answer.
   */
  public List<String> getVariables();

  /**
   * Returns the join score within this answer.
   *
   * Double.NaN means no such score is defined.
   */
  public double getJoinScore();

  /** Sets the join score within this answer */
  public void setJoinScore(double joinScore);

  /**
   * Sets the flag indicating whether a relaxation contributed to an answer.
   */
  public void SetComesFromRelaxation(boolean comesFromRelaxation);

  /**
   * Returns <code>true</code> if a relaxation contributed to obtaining this answer.
   */
  public boolean comesFromRelaxation();
  
  /**
   * Sets the value of the query whose answer this object holds.
   * @param query Query as a string.
   */
  public void setQuery(String query);
  
  /**
   * Returns the Query whose {@link Answer} this object stores.
   */
  public String getQuery();
  
  @Override
  public boolean equals(Object obj);

}
