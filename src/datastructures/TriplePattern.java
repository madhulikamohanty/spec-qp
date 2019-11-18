package de.mpii.trinitreloaded.datastructures;

import java.util.Set;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import de.mpii.trinitreloaded.queryprocessing.QueryParser;

/**
 * A triple pattern.
 *
 * A triple pattern has an optional {@code field} field. This allows users to
 * specify which field this triple pattern needs to be matched against.
 *
 * 
 * @author Mohamed Yahya (myahya@mpi-inf.mpg.de)
 *
 */
public class TriplePattern {

  public final String field;
  public final String subject;
  public final String predicate;
  public final String object;

  public final boolean isSubjectConst;
  public final boolean isPredicateConst;
  public final boolean isObjectConst;
  public final boolean isSubjectResource;
  public final boolean isSubjectLiteral;
  public final boolean isPredicateResource;
  public final boolean isPredicateLiteral;
  public final boolean isObjectResource;
  public final boolean isObjectLiteral;

  public TriplePattern(String field, String subject, String predicate, String object) {
    this.field = field;

    this.subject = subject;
    this.predicate = predicate;
    this.object = object;

    this.isSubjectConst = !QueryParser.VARIABLE_PATTERN.matcher(subject).matches();
    this.isPredicateConst = !QueryParser.VARIABLE_PATTERN.matcher(predicate).matches();
    this.isObjectConst = !QueryParser.VARIABLE_PATTERN.matcher(object).matches();

    this.isSubjectResource = this.isSubjectConst && subject.startsWith("<");
    this.isSubjectLiteral = this.isSubjectConst && subject.startsWith("'");
    this.isPredicateResource = this.isPredicateConst && predicate.startsWith("<");
    this.isPredicateLiteral = this.isPredicateConst && predicate.startsWith("'");
    this.isObjectResource = this.isObjectConst && object.startsWith("<");
    this.isObjectLiteral = this.isObjectConst && object.startsWith("'");

  }

  public TriplePattern(String subject, String predicate, String object) {
    this(null, subject, predicate, object);
  }

  public Set<String> variables() {
    Set<String> result = Sets.newHashSet();

    if (!isSubjectConst) {
      result.add(subject);
    }
    if (!isPredicateConst) {
      result.add(predicate);
    }
    if (!isObjectConst) {
      result.add(object);
    }

    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }

    TriplePattern o = (TriplePattern) obj;
    return (this.field == null && o.field == null || this.field.equals(o.field)) && this.subject.equals(o.subject)
        && this.predicate.equals(o.predicate) && this.object.equals(o.object) && this.isSubjectConst == o.isSubjectConst
        && this.isPredicateConst == o.isPredicateConst && this.isObjectConst == o.isObjectConst;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(field).append(subject).append(predicate).append(object)
        .append(isSubjectConst).append(isPredicateConst).append(isObjectConst).toHashCode();
  }

  @Override
  public String toString() {
    return Joiner.on(" ").join(subject, predicate, object).toString();
  }
}
