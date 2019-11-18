package de.mpii.trinitreloaded.queryprocessing;

import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import de.mpii.trinitreloaded.datastructures.Query;
import de.mpii.trinitreloaded.datastructures.TriplePattern;

/**
 * Parse a user-supplied string into a {@link Query}.
 *
 * @author Mohamed Yahya (myahya@mpi-inf.mpg.de)
 */
public class QueryParser {

  public static final Pattern FIELD_PATTERN = Pattern.compile("^([A-Z]+):\\((.+?)\\)");
  public static final Pattern PARENTHASIZED_STRING_PATTERN = Pattern.compile("^\\((.+?)\\)$");

  public static final Pattern TRIPLE_PATTERN = Pattern.compile("[^\\s\"']+|'([^\"]*?)'");

  public static final Pattern VARIABLE_PATTERN = Pattern.compile("\\?.+");

  public static final String TRIPLE_PATTERN_SEPARATOR = ";";

  public QueryParser() {
  }

  /**
   * Returns a query from the user-supplied input.
   */
  public Query parse(String str) {

    String[] termsStr = str.split(TRIPLE_PATTERN_SEPARATOR);

    String projection = termsStr[0];
    Set<String> projectionVars = getProjectionVars(projection);

    List<TriplePattern> triplePatterns = Lists.newArrayList();

    // SELECT optional, if missing then all variables are projection variables.
    int i = projectionVars == null ? 0 : 1;

    for (; i < termsStr.length; i++) {
      String termStr = termsStr[i];
      triplePatterns.add(parseTriplePattern(termStr));
    }

    Query query = new Query(projectionVars, triplePatterns);

    return query;
  }

  /**
   * Returns the set of projection variables, or null if the passed clause
   * passes none.
   */
  public static Set<String> getProjectionVars(String projection) {
    Set<String> result = Sets.newLinkedHashSet();

    projection = projection.trim();
    String[] parts = projection.split("\\s+");

    if (!parts[0].equalsIgnoreCase("SELECT")) {
      return null;
    }

    for (int i = 1; i < parts.length; i++) {
      result.add(parts[i]);
    }

    return result;
  }

  protected static TriplePattern parseTriplePattern(String triplePatternStr) {
    triplePatternStr = triplePatternStr.trim();
    String fieldStr = extractField(triplePatternStr);
    String triplePatterValue = extractTermValue(triplePatternStr);

    String[] triple = getTripleParts(triplePatterValue);

    return new TriplePattern(fieldStr, triple[0], triple[1], triple[2]);
  }

  private static String[] getTripleParts(String termValue) {

    String[] result = new String[3];
    Matcher m = TRIPLE_PATTERN.matcher(termValue);

    int i = 0;

    while (m.find()) {
      if (i > 2) {
        throw new RuntimeException("Your term seems to have more than a triple.");
      }
      if (m.group(1) != null) {
        // Add double-quoted string without the quotes
        result[i] = m.group(1);
      } else {
        // Add unquoted word
        result[i] = m.group();
      }
      result[i] = result[i].replaceAll("APOS", "'");
      i++;
    }

    return result;
  }

  /**
   * Returns the triple pattern, with the field information stripped out, if it
   * exists.
   */
  private static String extractTermValue(String termStr) {
    String val = PARENTHASIZED_STRING_PATTERN.matcher(FIELD_PATTERN.matcher(termStr).replaceAll("$2")).replaceAll("$1")
        .trim();
    return val;
  }

  /** Returns null if no field given */
  private static String extractField(String termStr) {
    Matcher m = FIELD_PATTERN.matcher(termStr);
    if (m.matches()) {
      return m.group(1);
    }

    return null;
  }

}
