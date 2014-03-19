/*
 * CS61C Spring 2014 Project2
 * Reminders:
 *
 * DO NOT SHARE CODE IN ANY WAY SHAPE OR FORM, NEITHER IN PUBLIC REPOS OR FOR DEBUGGING.
 *
 * This is one of the two files that you should be modifying and submitting for this project.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.Math;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SolveMoves {
  public static class Map extends Mapper<IntWritable, MovesWritable, IntWritable, ByteWritable> {
    /**
     * Configuration and setup that occurs before map gets called for the first time.
     *
     **/
    @Override
    public void setup(Context context) {
    }

    /**
     * The map function for the second mapreduce that you should be filling out.
     */
    @Override
    public void map(IntWritable key, MovesWritable val, Context context) throws IOException, InterruptedException {
      int[] moves = val.getMoves();
      for (int i = 0; i < moves.length; i++) {
        context.write(new IntWritable(moves[i]), new ByteWritable(val.getValue()));
      }
    }
  }

  public static class Reduce extends Reducer<IntWritable, ByteWritable, IntWritable, MovesWritable> {

    int boardWidth;
    int boardHeight;
    int connectWin;
    boolean OTurn;
    /**
     * Configuration and setup that occurs before map gets called for the first time.
     *
     **/
    @Override
    public void setup(Context context) {
      // load up the config vars specified in Proj2.java#main()
      boardWidth = context.getConfiguration().getInt("boardWidth", 0);
      boardHeight = context.getConfiguration().getInt("boardHeight", 0);
      connectWin = context.getConfiguration().getInt("connectWin", 0);
      OTurn = context.getConfiguration().getBoolean("OTurn", true);
    }

    /**
     * The reduce function for the second mapreduce that you should be filling out.
     */
    @Override
    public void reduce(IntWritable key, Iterable<ByteWritable> values, Context context) throws IOException, InterruptedException {
      // Find best value for this game board
      int bestStatus = 0;
      int bestMovesTillEnd = 0;
      boolean isValid = false;
      for (ByteWritable value: values) {
        int currentStatus = getWinStatus(value.get() & 3);
        int currentMovesTillEnd = (int) (value.get() >> 2);
        if (currentMovesTillEnd == 0) {
          isValid = true;
        }
        if (currentStatus == bestStatus) {
          if (currentStatus < 2) {
            if (currentMovesTillEnd > bestMovesTillEnd) {
              bestMovesTillEnd = currentMovesTillEnd;
            }
          } else {
            if (currentMovesTillEnd < bestMovesTillEnd) {
              bestMovesTillEnd = currentMovesTillEnd;
            }
          }
        } else if (currentStatus > bestStatus) {
          bestStatus = currentStatus;
          bestMovesTillEnd = currentMovesTillEnd;
        }
      }
      // If board is valid, generate parents and write to context
      if (isValid) {
        char player = 'O';
        if (OTurn) {
          player = 'X';
        }
        String currentState = Proj2Util.gameUnhasher(key.get(), boardWidth, boardHeight);
        ArrayList<IntWritable> allParents = new ArrayList<IntWritable>(1);
        for (int i = 0; i < boardWidth; i++) {
          for (int j = boardHeight - 1; j >= 0; j--) {
            if (currentState.charAt(i*boardHeight + j) == player) {
              char[] parentCharArray = currentState.toCharArray();
              parentCharArray[i*boardHeight + j] = ' ';
              allParents.add(new IntWritable(Proj2Util.gameHasher(new String(parentCharArray), boardWidth, boardHeight)));
              break;
            }
          }
        }
        int [] allParentsArray = new int[allParents.size()];
        for (int i = 0; i < allParents.size(); i++) {
          allParentsArray[i] = allParents.get(i).get();
        }
        MovesWritable move = new MovesWritable(getRealStatus(bestStatus), bestMovesTillEnd + 1, allParentsArray);
        context.write(key, move);
      }
    }

    /**
     * Map the real status of the game with the status of the game for the specific player where
     * win => 2, draw => 1 and loss => 0.
     * @param  realStatus the status of the game as specified in value of MovesWritable
     * @return the status of the game for the particular player
     */
    private int getWinStatus(int realStatus) {
      if (realStatus == 3) {
        return 1;
      } else if (OTurn) {
        if (realStatus == 1) {
          return 2;
        } else {
          return 0;
        }
      } else {
        if (realStatus == 2) {
          return 2;
        } else {
          return 0;
        }
      }
    }

    /**
     * Map the status of the game for the specific player back to the real status of the
     * game.
     * @param  winStatus the status of the game for the particular player
     * @return the status of the game as specified in value of MovesWritable
     */
    private int getRealStatus(int winStatus) {
      if (winStatus == 1) {
        return 3;
      } else if (OTurn) {
        if (winStatus == 2) {
          return 1;
        } else {
          return 2;
        }
      } else {
        if (winStatus == 0) {
          return 1;
        } else {
          return 2;
        }
      }
    }

  }
}
