package de.mpii.trinitreloaded.queryprocessing;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Random;

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Preconditions;

import de.mpii.trinitreloaded.datastructures.Answer;
import de.mpii.trinitreloaded.datastructures.HashMapBasedAnswer;
import de.mpii.trinitreloaded.utils.Config;
import de.mpii.trinitreloaded.utils.Config.LoggingLevel;
import de.mpii.trinitreloaded.utils.Logger;

/**
 * A Rank Join operator as per Ilyas et al., 2003.
 *
 */
public class RankJoin implements Operator {

  public final Operator left;
  public final Operator right;
  public final String joinVar;

  Random rand = new Random(2);

  private RankJoinInputRel leftRel;
  private RankJoinInputRel rightRel;
  private Answer current = null;
  private PriorityQueue<Answer> q;
  private double unseenUpperBound;
  boolean isOpen = false;

  public RankJoin(Operator left, Operator right, String joinVar) {

    this.left = left;
    this.right = right;
    this.joinVar = joinVar;

  }

  public boolean open() throws SQLException {
    if (isOpen) {
      return true;
    }
    left.open();
    right.open();

    leftRel = new RankJoinInputRel(left);
    rightRel = new RankJoinInputRel(right);
    q = new PriorityQueue<Answer>();
    unseenUpperBound = 0.0;

    this.isOpen = true;

    return true;
  }

  public boolean hasNext() throws SQLException {
    if (current != null) {
      return true;
    }

    // When there is not need to compute new join results since we have enough
    // already.
    if (!q.isEmpty()) {
      Answer tuple = q.peek();
      // Added the OR condition, which the paper does not have. It takes care of the situation when
      // both the relations have been exhausted.
      // Needed since threshold may not go down after last tuples have been
      // consumed from underlying relations.
      if (tuple.getScore() >= unseenUpperBound || (!left.hasNext() && !right.hasNext())) {
        q.poll();
        current = tuple;
        return true;
      }
    }

    // Let's compute a new join result, if possible.
    try {
      while (true) {
        RankJoinInputRel input = getInput();
        if (input != null) {

          RankJoinInputRel other = (input == leftRel) ? rightRel : leftRel;

          Answer scoredTuple = input.tuples.next();

          if (!input.firstTupleRead) {
            input.top = scoredTuple.getScore();
            input.firstTupleRead = true;
          }
          input.bottom = scoredTuple.getScore();
          input.addToHashTable(joinVar, scoredTuple.getVariableBinding(joinVar), scoredTuple);

          if (other.firstTupleRead) {
            // if other input has not read: (i) no join will be possible, and
            // (ii) there is no bound to talk about.

            double newUnseenUpperBound =
                Math.max(aggregateScore(leftRel.bottom, rightRel.top),
                    aggregateScore(leftRel.top, rightRel.bottom));
            if (newUnseenUpperBound < unseenUpperBound) {
              Logger.print("Bound down from {" + unseenUpperBound + "} to {" + newUnseenUpperBound
                  + "}", LoggingLevel.INTERMEDIATEINFO);
            }
            unseenUpperBound = newUnseenUpperBound;

            // For now, we have single matches.
            Collection<Answer> matches =
                other.get(joinVar, scoredTuple.getVariableBinding(joinVar));
            if (matches != null) {
              Collection<Answer> joinedTuples = constructJoin(scoredTuple, matches, input, other);
              for (Answer joinedTuple : joinedTuples) {
                q.add(joinedTuple);
              }
            }
          }
        }
        if (!q.isEmpty()) {
          Answer tuple = q.peek();
          if (tuple.getScore() >= unseenUpperBound || (!left.hasNext() && !right.hasNext())) {
            break;
          }
        } else if (!left.hasNext() && !right.hasNext()) {
          return false; // Empty queue -> nothing to return
        }

      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    current = q.poll();
    return true;
  }

  /**
   * Performs join, maintaining correct order.
   */
  private Collection<Answer> constructJoin(Answer inputTuple, Collection<Answer> matchedTuples,
      InputRel input, InputRel other) {
    Preconditions.checkArgument(input != other);
    List<Answer> joinResults = Lists.newArrayList();
    try {
      for (Answer matchedTuple : matchedTuples) {
        HashMapBasedAnswer tuple = new HashMapBasedAnswer();
        tuple.putAll(inputTuple);
        tuple.putAll(matchedTuple);
        tuple.setScore(aggregateScore(inputTuple.getScore(), matchedTuple.getScore()));

        if (!Double.isNaN(matchedTuple.getJoinScore())) {
          tuple.setJoinScore(matchedTuple.getJoinScore());
        } else if (!Double.isNaN(inputTuple.getJoinScore())) {
          tuple.setJoinScore(inputTuple.getJoinScore());
        }
        tuple.SetComesFromRelaxation(matchedTuple.comesFromRelaxation()
            || inputTuple.comesFromRelaxation());
        tuple.setQuery(inputTuple.getQuery()+","+matchedTuple.getQuery());
        joinResults.add(tuple);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    return joinResults;
  }

  private double aggregateScore(double score1, double score2) {
    return score1 + score2;
  }

  /**
   * Decides which of the two inputs to be read next.
   *
   * @throws SQLException
   */
  private RankJoinInputRel getInput() throws SQLException {
    RankJoinInputRel ret = null;
    if (left.hasNext() && right.hasNext()) {
      ret = (rand.nextInt() % 2 == 0) ? leftRel : rightRel;
      return ret;
    } else if (left.hasNext()) {
      ret = leftRel;
    } else if (right.hasNext()) {
      ret = rightRel;
    }

    return ret;
  }

  public Answer next() throws SQLException {
    if (hasNext()) {
      Answer toReturn = current;
      current = null;
      return toReturn;
    }
    throw new NoSuchElementException();
  }

  public boolean close() throws SQLException {
    if (!isOpen) {
      return true;
    }
    left.close();
    right.close();
    this.isOpen = false;
    return true;
  }

  public boolean areScoredTuplesMutable() {
    return false;
  }

  public double scoreOfNext() throws SQLException {
    if (!isOpen) {
      open();
    }
    if (!hasNext()) {
      Logger.println("hasNext gives back 0.0", Config.LoggingLevel.INTERMEDIATEINFO);
      return 0.0;
    }
    Logger
        .println("Highest from join: " + current.getScore(), Config.LoggingLevel.INTERMEDIATEINFO);
    return current.getScore();
  }

  public String getPartQuery() {
    return left.getPartQuery()+","+right.getPartQuery();
  }

}
