package de.mpii.trinitreloaded.datastructures;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.common.collect.Maps;

public class HashMapBasedAnswer implements Answer, Comparable<HashMapBasedAnswer> {

  private final HashMap<String, String> map = Maps.newHashMap();
  private double answerScore;
  private double joinScore = Double.NaN;
  private boolean comesFromRelaxation = false;
  private String query;
  public static long countOfAnswerObjects = 0;

  public HashMapBasedAnswer() {
    super();
    countOfAnswerObjects++;
  }

  public HashMapBasedAnswer(boolean toBeCounted) {
    super();
    if(toBeCounted)
      countOfAnswerObjects++;
  }

  public boolean hasVariableBinding(String variable) {
    return map.containsKey(variable);
  }

  public String getVariableBinding(String variable) throws Exception {
    if (!hasVariableBinding(variable)) {
      throw new Exception("Binding for " + variable + " does not exist.");
    }

    return map.get(variable);
  }

  public void setVariableBinding(String variable, String binding) throws Exception {
    if (hasVariableBinding(variable)) {
      throw new Exception("Binding for " + variable + "already exists, value=" + getVariableBinding(variable));
    }

    map.put(variable, binding);
  }

  public double getScore() {
    return this.answerScore;
  }

  public void setScore(double score) {
    this.answerScore = score;//Math.round(score*100.0)/100.0;
  }

  public void putAll(Answer input) throws Exception {
    List<String> variables = input.getVariables();
    for(String var : variables){
      this.map.put(var, input.getVariableBinding(var));
    }
  }

  public List<String> getVariables() {
    List<String> variables = new ArrayList<String>();
    variables.addAll(this.map.keySet());
    return variables;
  }

  /**
   * Compare two {@link HashMapBasedAnswer} on the basis of scores for various {@link Operator}.
   */
  public int compareTo(HashMapBasedAnswer other) {
    return new Double(other.getScore()).compareTo(this.getScore());
  }
  @Override
  public String toString() {
    return "HashMapBasedAnswer [map=" + map + ", answerScore=" + answerScore + "]";
  }

  public double getJoinScore() {
    return this.joinScore;
  }

  public void setJoinScore(double joinScore) {
    this.joinScore = joinScore;
  }

  public boolean comesFromRelaxation() {
    return this.comesFromRelaxation;
  }

  public void SetComesFromRelaxation(boolean comesFromRelaxation) {
    this.comesFromRelaxation = comesFromRelaxation;
  }

  public void setQuery(String query){
    this.query = query;
  }

  public String getQuery(){
    return this.query;
  }

  /** This function is used to check already seen answers in {@link IncrementalMerge} */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((map == null) ? 0 : map.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    HashMapBasedAnswer other = (HashMapBasedAnswer) obj;
    if (map == null) {
      if (other.map != null)
        return false;
    } else if (!map.equals(other.map))
      return false;
    return true;
  }
  
  
  
}
