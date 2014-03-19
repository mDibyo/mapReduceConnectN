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
      int bestMovesTillEnd = boardWidth*boardHeight + 1;
      boolean isValid = False;
      for (ByteWritable value: values) {
        int currentStatus = getStatus(value.get() & 3);
        int currentMovesTillEnd = value.get() >> 2;
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
        MovesWritable move = new MovesWritable(bestStatus, bestMovesTillEnd + 1, allParentsArray);
        context.write(key, bestMove);
      }
    }

    private int getStatus(ByteWritable realStatus) {
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

    /*
    @Override
    public void reduce(IntWritable key, Iterable<ByteWritable> values, Context context) throws IOException, InterruptedException {   
      HashMap<MovesWritable, Integer> dict = new HashMap<MovesWritable, Integer>();
      HashMap<MovesWritable, Integer> win = new HashMap<MovesWritable, Integer>();
      HashMap<MovesWritable, Integer> tie = new HashMap<MovesWritable, Integer>();
      HashMap<MovesWritable, Integer> loss = new HashMap<MovesWritable, Integer>();
      for (ByteWritable value: values) {
        MovesWritable move = new MovesWritable();
        move.setValue(value.get());
        if (!dict.containsKey(move)) {
          dict.put(move, 1);
        } else {
          dict.put(move, dict.get(move) + 1);
        }
      }
      // Minimax to find best move
      int winStatus = 2;
      char player = 'O';
      if (OTurn) {
        winStatus = 1;
        player = 'X';
      }
      for (MovesWritable move: dict.keySet()) {
        if (dict.get(move) == 1) {
          if (move.getMovesToEnd() != 0) {
            continue;
          }
        }
        int status = move.getStatus();
        if (status == winStatus) {
          win.put(move, move.getMovesToEnd());
        } else if (status == 3) {
          tie.put(move, move.getMovesToEnd());
        } else if (status != 0) {
          loss.put(move, move.getMovesToEnd());
        }
      }
      MovesWritable bestMove = new MovesWritable();
      if (!win.isEmpty()) {
        int bestNumberOfMoves = boardWidth * boardHeight + 1;
        for (MovesWritable move: win.keySet()) {
          if (move.getMovesToEnd() < bestNumberOfMoves) {
            bestMove = move;
            bestNumberOfMoves = bestMove.getMovesToEnd();
          }
        }
      } else if (!tie.isEmpty()) {
        int bestNumberOfMoves = 0;
        for (MovesWritable move: tie.keySet()) {
          if (move.getMovesToEnd() > bestNumberOfMoves) {
            bestMove = move;
            bestNumberOfMoves = bestMove.getMovesToEnd();
          }
        }
      } else if (!loss.isEmpty()) {
        int bestNumberOfMoves = 0;
        for (MovesWritable move: loss.keySet()) {
          if (move.getMovesToEnd() > bestNumberOfMoves) {
            bestMove = move;
            bestNumberOfMoves = bestMove.getMovesToEnd();
          }
        }
      }
      // Work on best move
      String currentState = Proj2Util.gameUnhasher(key.get(), boardWidth, boardHeight);
      bestMove.setMovesToEnd(bestMove.getMovesToEnd() + 1);
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
      bestMove.setMoves(allParentsArray);
      context.write(key, bestMove);
    } */

  }
}
