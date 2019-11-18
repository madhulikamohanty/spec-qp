package de.mpii.trinitreloaded.datastructures;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

/**
 * A triple pattern query with projection variables.
 */
public final class Query {

  public final Set<String> projectionVariables;
  public final List<TriplePattern> triplePatterns;

  public Query(Set<String> projectionVariables, List<TriplePattern> triplePatterns) {
    this.projectionVariables = projectionVariables;
    this.triplePatterns = triplePatterns;
    checkQueryValidity();
  }

  private void checkQueryValidity() {

    // Projections not empty
    if (projectionVariables == null || projectionVariables.isEmpty()) {
      throw new IllegalArgumentException("Projection variables must be supplied.");
    }

    // All projection variables in the query?

    if (!queryVariables().containsAll(projectionVariables)) {

      throw new IllegalArgumentException("Some projection variables are not in the query body. Body: "
          + queryVariables() + ", projection variables: " + projectionVariables);
    }

  }

  /**
   * Returns all variables in the query.
   */
  public Set<String> queryVariables() {
    Set<String> bodyVariables = Sets.newHashSet();
    for (TriplePattern t : triplePatterns) {
      bodyVariables.addAll(t.variables());
    }

    return bodyVariables;
  }
}
