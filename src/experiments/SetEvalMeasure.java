package de.mpii.trinitreloaded.experiments;

import java.util.Set;

import de.mpii.trinitreloaded.datastructures.Answer;

public abstract class SetEvalMeasure {

  public abstract double score(Set<Answer> answers);

}
