package de.mpii.trinitreloaded.experiments;

import java.util.ArrayList;
import java.util.Set;

import de.mpii.trinitreloaded.datastructures.Answer;

public class SimpleSetMeasureGroundTruthProvider implements SetMeasureGroundTruthProvider {

  public final ArrayList<Double> answers;

  public SimpleSetMeasureGroundTruthProvider(Set<Answer> answers) {
    this.answers = new ArrayList<Double>();
    for(Answer answer : answers)
      this.answers.add(new Double(answer.getScore()));
  }

  public ArrayList<Double> getGroundTruth() {
    return answers;
  }

}
