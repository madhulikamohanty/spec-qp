package de.mpii.trinitreloaded.queryprocessing;

import java.sql.SQLException;
import java.util.NoSuchElementException;

import de.mpii.trinitreloaded.datastructures.Answer;

/**
 * An database operator supporting the operations:
 * <ul>
 * <li/>open
 * <li />close
 * <li />hasNext
 * <li />next
 * </ul>
 *
 */
public interface Operator {

  /**
   * Performs initialization needed by the operator.
   *
   * @return <code>true</code> if initialization was successful,
   *         <code>false</code> otherwise
   * @throws SQLException 
   */
  public boolean open() throws SQLException;

  /**
   * Performs finalization needed by the operator.
   *
   * @return <code>true</code> if finalization was successful,
   *         <code>false</code> otherwise
   * @throws SQLException 
   */
  public boolean close() throws SQLException;

  /**
   * Returns <code>true</code> if the operator has more answers to offer.
   * @throws SQLException 
   */
  public boolean hasNext() throws SQLException;

  /**
   * Returns the next answer.
   * @throws SQLException 
   * @throws Exception 
   * @throws NoSuchElementException
   *           if the operator has no more elements to return. This will result
   *           from calling this method after <code>hasNext()</code> returns
   *           false
   */
  public Answer next() throws SQLException, Exception;
  
  /**
   * Returns the query whose answer is given by this operator.
   * @return String query.
   */
  public String getPartQuery();

}
