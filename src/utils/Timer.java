package de.mpii.trinitreloaded.utils;

import com.google.common.base.Preconditions;

/**
 * An execution timer.
 * 
 * @author Mohamed Yahya (myahya@mpi-inf.mpg.de)
 */
public class Timer {

  private static final long INVALID_TIME = 0;;

  long startTime;
  long endTime;

  public Timer() {
    reset();
  }

  public void start() {
    Preconditions.checkArgument(startTime == INVALID_TIME,
        "Make sure you reset the timer before reuse.");
    startTime = System.currentTimeMillis();
  }

  public void stop() {
    Preconditions.checkArgument(startTime != INVALID_TIME,
        "You stopped a timer that has not been started.");
    endTime = System.currentTimeMillis();
  }

  public void reset() {
    startTime = INVALID_TIME;
    endTime = INVALID_TIME;
  }

  public long getDuration() {
    Preconditions.checkArgument(startTime != INVALID_TIME && endTime != INVALID_TIME,
        "Make sure you have a valid duration to get.");
    return endTime - startTime;
  }

  public String toString() {
    return getDuration() + " ms";
  }

  public static Timer getTimer() {
    return new Timer();
  }
}
