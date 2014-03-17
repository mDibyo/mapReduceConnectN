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
      // just one copy but that one copy is not a turn of 0;
      for (int move: val.getMoves()) {
        context.write(new IntWritable(move), new ByteWritable(val.getValue()));
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
      HashMap<MovesWritable, Integer> dict = new HashMap<MovesWritable, Integer>();
      HashMap<MovesWritable, Integer> win = new HashMap<MovesWritable, Integer>();
      HashMap<MovesWritable, Integer> tie = new HashMap<MovesWritable, Integer>();
      HashMap<MovesWritable, Integer> loss = new HashMap<MovesWritable, Integer>();
      MovesWritable bestMove;
      for (ByteWritable value: values) {
        MovesWritable move = new MovesWritable();
        move.setValue(value.get());
        if (!dict.containsKey(move)) {
          dict.put(move, 1);
        } else {
          dict.put(move, dict.get(move) + 1);
        }
      }
      for (MovesWritable move: dict.keySet()) {
        if (dict.get(move) == 1) {
          if (move.getMovesToEnd() != 0) {
            dict.remove(move);
          }
        }
      }
      int winStatus = 2;
      if (OTurn) {
        winStatus = 1;
      }
      for (MovesWritable move: dict.keySet()) {
        // move.setMovesToEnd(move.getMovesToEnd + 1);
        int status = move.getStatus();
        if (status == winStatus) {
          win.put(move, move.getMovesToEnd());
        } else if (status == 3) {
          tie.put(move, move.getMovesToEnd());
        } else {
          loss.put(move, move.getMovesToEnd());
        }
      }


    }
  }
}
