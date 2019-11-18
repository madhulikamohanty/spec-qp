package de.mpii.trinitreloaded.experiments;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import de.mpii.trinitreloaded.datastructures.Answer;

/**
 * Measures recall against a ground truth.
 *
 * @author Mohamed Yahya (myahya@mpi-inf.mpg.de)
 */
public class Recall extends SetEvalMeasure {

  public final SetMeasureGroundTruthProvider groundTruthProvider;

  public Recall(SetMeasureGroundTruthProvider groundTruthProvider) {
    this.groundTruthProvider = groundTruthProvider;
    if (groundTruthProvider.getGroundTruth().isEmpty()) {
      throw new IllegalArgumentException("Cannot have an empty ground truth for recall");
    }
  }

  @Override
  public double score(Set<Answer> answers) {
    if (answers.isEmpty()) {
      return 0.0;
    }

    ArrayList<Double> groundTruth = groundTruthProvider.getGroundTruth();

    double relCount = 0;

    Iterator<Answer> answerIt = answers.iterator();

    while (answerIt.hasNext()) {
      Answer answer = answerIt.next();
      //MapOnlyAnswer mapAnswer = new MapOnlyAnswer(answer);
      if (groundTruth.contains(answer.getScore())) {
        relCount++;
      }
    }

    double totalRel = groundTruth.size();

    if (relCount == 0) {
      return 0.0;
    }

    double recall = relCount / (totalRel);

    return recall;

  }
}
