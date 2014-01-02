/* XXX Chapter 2. Data Flow
 *
 * http://my.safaribooksonline.com/book/software-engineering-and-development/9781449328917/hdfs-concepts/
 * id3668231#X2ludGVybmFsX0h0bWxWaWV3P3htbGlkPTk3ODE0NDkzMjg5MTclMkZpZDM1ODU5NTEmcXVlcnk9cGFydGl0aW9u
 */

/*
 * Hadoop Definitive Guide Chapter 6 -- Anatomy of a mapreduce run
 * 
 * Classic MapReduce (MapReduce 1)
 
 * A job run in classic MapReduce is illustrated in Figure 6-1. At the highest level, there are four independent entities:

 *	The client, which submits the MapReduce job.
 *	The jobtracker, which coordinates the job run. The jobtracker is a Java application whose main class is JobTracker.
 *	The tasktrackers, which run the tasks that the job has been split into. Tasktrackers are Java applications whose main class is TaskTracker.
 *	The distributed filesystem (normally HDFS, covered in Chapter 3), which is used for sharing job files between the other entities.


 * Job submission


 * The submit() method on Job creates an internal JobSummitter instance and calls
 * submitJobInternal() on it (step 1 in Figure 6-1). Having submitted the job, waitForCompletion()
 * polls the job’s progress once per second and reports the progress to the console if it has
 * changed since the last report. When the job completes successfully, the job counters are
 * displayed. Otherwise, the error that caused the job to fail is logged to the console.

 * The job submission process implemented by JobSummitter does the following:

 * 	Asks the jobtracker for a new job ID (by calling getNewJobId() on JobTracker) (step 2).

 *	Checks the output specification of the job. For example, if the output directory has not
 *	been specified or it already exists, the job is not submitted and an error is thrown to the
 *	MapReduce program.

 *	Computes the input splits for the job. If the splits cannot be computed (because the input
 *	paths don’t exist, for example), the job is not submitted and an error is thrown to the
 *	MapReduce program.

 * Copies the resources needed to run the job, including the job JAR file, the configuration file,
 * and the computed input splits, to the jobtracker’s filesystem in a directory named after the job
 * ID. The job JAR is copied with a high replication factor (controlled by the
 * mapred.submit.replication property, which defaults to 10) so that there are lots of copies across
 * the cluster for the tasktrackers to access when they run tasks for the job (step 3).

 * Tells the jobtracker that the job is ready for execution by calling submitJob() on JobTracker
 * (step 4).

 * Job initialization

 * When the JobTracker receives a call to its submitJob() method, it puts it into an internal queue
 * from where the job scheduler will pick it up and initialize it. Initialization involves creating
 * an object to represent the job being run, which encapsulates its tasks, and bookkeeping
 * information to keep track of the status and progress of its tasks (step 5).

 * To create the list of tasks to run, the job scheduler first retrieves the input splits computed
 * by the client from the shared filesystem (step 6). It then creates one map task for each
 * split. The number of reduce tasks to create is determined by the mapred.reduce.tasks property in
 * the Job, which is set by the setNumReduceTasks() method, and the scheduler simply creates this
 * number of reduce tasks to be run. Tasks are given IDs at this point.

 * In addition to the map and reduce tasks, two further tasks are created: a job setup task and a
 * job cleanup task. These are run by tasktrackers and are used to run code to set up the job before
 * any map tasks run, and to cleanup after all the reduce tasks are complete. The OutputCommitter
 * that is configured for the job determines the code to be run, and by default this is a
 * FileOutputCommitter. For the job setup task it will create the final output directory for the job
 * and the temporary working space for the task output, and for the job cleanup task it will delete
 * the temporary working space for the task output. The commit protocol is described in more detail
 * in Output Committers.

 * Task assignment

 * Tasktrackers run a simple loop that periodically sends heartbeat method calls to the
 * jobtracker. Heartbeats tell the jobtracker that a tasktracker is alive, but they also double as a
 * channel for messages. As a part of the heartbeat, a tasktracker will indicate whether it is ready
 * to run a new task, and if it is, the jobtracker will allocate it a task, which it communicates to
 * the tasktracker using the heartbeat return value (step 7).

 *  Tasktrackers have a fixed number of slots for map tasks and for reduce tasks, and these are set
 *  independently. For example, a tasktracker may be configured to run two map tasks and two reduce
 *  tasks simultaneously. (The precise number depends on the number of cores and the amount of
 *  memory on the tasktracker; see Memory.) In the context of a given job, the default scheduler
 *  fills empty map task slots before reduce task slots. So if the tasktracker has at least one
 *  empty map task slot, the jobtracker will select a map task; otherwise, it will select a reduce
 *  task.

 * To choose a reduce task, the jobtracker simply takes the next in its list of yet-to-be-run reduce
 * tasks, since there are no data locality considerations. For a map task, however, it takes into
 * account the tasktracker’s network location and picks a task whose input split is as close as
 * possible to the tasktracker. In the optimal case, the task is data-local

 * TaskRunner launches a new Java Virtual Machine (JVM, step 9) to run each task in (step 10), so
 * that any bugs in the user-defined map and reduce functions don’t affect the tasktracker (by
 * causing it to crash or hang, for example). However, it is possible to reuse the JVM between
 * tasks; see Task JVM Reuse.

 * The child process communicates with its parent through the umbilical interface. It informs the
 * parent of the task’s progress every few seconds until the task is complete.

 */

/* XXX partition, sort, combiner

 * Each map task has a circular memory buffer that it writes the output to. The buffer is 100 MB by
 * default, a size that can be tuned by changing the io.sort.mb property. When the contents of the
 * buffer reaches a certain threshold size (io.sort.spill.percent, which has the default 0.80, or
 * 80%), a background thread will start to spill the contents to disk. Map outputs will continue to
 * be written to the buffer while the spill takes place, but if the buffer fills up during this
 * time, the map will block until the spill is complete.

 * Spills are written in round-robin fashion to the directories specified by the mapred.local.dir
 * property, in a job-specific subdirectory.

 * Before it writes to disk, the thread first divides the data into partitions corresponding to the
 * reducers that they will ultimately be sent to. Within each partition, the background thread
 * performs an in-memory sort by key, and if there is a combiner function, it is run on the output
 * of the sort. Running the combiner function makes for a more compact map output, so there is less
 * data to write to local disk and to transfer to the reducer.

 * Each time the memory buffer reaches the spill threshold, a new spill file is created, so after
 * the map task has written its last output record, there could be several spill files. Before the
 * task is finished, the spill files are merged into a single partitioned and sorted output
 * file. The configuration property io.sort.factor controls the maximum number of streams to merge
 * at once; the default is 10.

 * If there are at least three spill files (set by the min.num.spills.for.combine property), the
 * combiner is run again before the output file is written. Recall that combiners may be run
 * repeatedly over the input without affecting the final result. If there are only one or two
 * spills, the potential reduction in map output size is not worth the overhead in invoking the
 * combiner, so it is not run again for this map output.

 * It is often a good idea to compress the map output as it is written to disk because doing so
 * makes it faster to write to disk, saves disk space, and reduces the amount of data to transfer to
 * the reducer. By default, the output is not compressed, but it is easy to enable this by setting
 * mapred.compress.map.output to true. The compression library to use is specified by
 * mapred.map.output.compression.codec; see Compression for more on compression formats.

 * The output file’s partitions are made available to the reducers over HTTP. The maximum number of
 * worker threads used to serve the file partitions is controlled by the tasktracker.http.threads
 * property; this setting is per tasktracker, not per map task slot. The default of 40 may need to
 * be increased for large clusters running large jobs. In MapReduce 2, this property is not
 * applicable because the maximum number of threads used is set automatically based on the number of
 * processors on the machine. (MapReduce 2 uses Netty, which by default allows up to twice as many
 * threads as there are processors.)
 
 * (below: Applicatoin Master is a YARN concept)
 * {{
 * YARN remedies the scalability shortcomings of “classic” MapReduce by splitting the
 * responsibilities of the jobtracker into separate entities. The jobtracker takes care of both job
 * scheduling (matching tasks with tasktrackers) and task progress monitoring (keeping track of
 * tasks, restarting failed or slow tasks, and doing task bookkeeping, such as maintaining counter
 * totals).

 * YARN separates these two roles into two independent daemons: a resource manager to manage the use
 * of resources across the cluster and an application master to manage the lifecycle of applications
 * running on the cluster.
 * }}

 * How do reducers know which machines to fetch map output from?

 * As map tasks complete successfully, they notify their parent tasktracker of the status update,
 * which in turn notifies the jobtracker. (In MapReduce 2, the tasks notify their application master
 * directly.) These notifications are transmitted over the heartbeat communication mechanism
 * described earlier. Therefore, for a given job, the jobtracker (or application master) knows the
 * mapping between map outputs and hosts. A thread in the reducer periodically asks the master for
 * map output hosts until it has retrieved them all.

 * Hosts do not delete map outputs from disk as soon as the first reducer has retrieved them, as the
 * reducer may subsequently fail. Instead, they wait until they are told to delete them by the
 * jobtracker (or application master), which is after the job has completed.

 * The map outputs are copied to the reduce task JVM’s memory if they are small enough (the buffer’s
 * size is controlled by mapred.job.shuffle.input.buffer.percent, which specifies the proportion of
 * the heap to use for this purpose); otherwise, they are copied to disk. When the in-memory buffer
 * reaches a threshold size (controlled by mapred.job.shuffle.merge.percent) or reaches a threshold
 * number of map outputs (mapred.inmem.merge.threshold), it is merged and spilled to disk. If a
 * combiner is specified, it will be run during the merge to reduce the amount of data written to
 * disk.

 * As the copies accumulate on disk, a background thread merges them into larger, sorted files. This
 * saves some time merging later on. Note that any map outputs that were compressed (by the map
 * task) have to be decompressed in memory in order to perform a merge on them.

 * When all the map outputs have been copied, the reduce task moves into the sort phase (which
 * should properly be called the merge phase, as the sorting was carried out on the map side), which
 * merges the map outputs, maintaining their sort ordering. This is done in rounds. For example, if
 * there were 50 map outputs and the merge factor was 10 (the default, controlled by the
 * io.sort.factor property, just like in the map’s merge), there would be five rounds. Each round
 * would merge 10 files into one, so at the end there would be five intermediate files.

 * Rather than have a final round that merges these five files into a single sorted file, the merge
 * saves a trip to disk by directly feeding the reduce function in what is the last phase: the
 * reduce phase. This final merge can come from a mixture of in-memory and on-disk segments.

 */


/* XXX A reduce side join is arguably one of the easiest implementations of a join in MapReduce, and
 * therefore is a very attractive choice. It can be used to execute any of the types of joins
 * described above with relative ease and there is no limitation on the size of your data
 * sets. Also, it can join as many data sets together at once as you need. All that said, a reduce
 * side join will likely require a large amount of network bandwidth because the bulk of the data is
 * sent to the reduce phase. This can take some time, but if you have resources available and aren’t
 * concerned about execution time, by all means use it! Unfortunately, if all of the data sets are
 * large, this type of join may be your only choice.

 * ORA mapreduce patterns gives the following types of joins:
 * 1) reduce side join -- normal join, can handle any size dataset, downside is data transfer overhead
 * 2) replicated join -- small dataset replicated thru hadoop distributed cache, large dataset needs to
 *                       occur on the left.
 * 3) composite join -- misnomer?  this seems very contrived, its essentially a merge join
 * 4) cartesian -- 
 */


//./ch02/src/main/java/MaxTemperature.java
// cc MaxTemperature Application to find the maximum temperature in the weather dataset
// vv MaxTemperature
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = new Job();        // XXX
    job.setJarByClass(MaxTemperature.class); // XXX
    job.setJobName("Max temperature");

    FileInputFormat.addInputPath(job, new Path(args[0])); // XXX
    FileOutputFormat.setOutputPath(job, new Path(args[1])); // XXX
    
    job.setMapperClass(MaxTemperatureMapper.class); 
    job.setReducerClass(MaxTemperatureReducer.class);

    job.setOutputKeyClass(Text.class); // XXX sets both Map and Reduce Key class.  For only map setMapOutputKeyClass()
    job.setOutputValueClass(IntWritable.class); // XXX and setMapOutputValueClass()
    
    System.exit(job.waitForCompletion(true) ? 0 : 1); // XXX
  }
}
// ^^ MaxTemperature

//=*=*=*=*
//./ch02/src/main/java/MaxTemperatureMapper.java
// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> { // XXX extends Mapper<I1, I2, O1, O2>

  private static final int MISSING = 9999;
  
  @Override
  public void map(LongWritable key, Text value, Context context) // XXX Context is defined inside Mapper
      throws IOException, InterruptedException {
    
    String line = value.toString(); // XXX
    String year = line.substring(15, 19);
    int airTemperature;
    if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
      airTemperature = Integer.parseInt(line.substring(88, 92));
    } else {
      airTemperature = Integer.parseInt(line.substring(87, 92));
    }
    String quality = line.substring(92, 93);
    if (airTemperature != MISSING && quality.matches("[01459]")) {
      context.write(new Text(year), new IntWritable(airTemperature));
    }
  }
}
// ^^ MaxTemperatureMapper

//=*=*=*=*
//./ch02/src/main/java/MaxTemperatureReducer.java
// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv MaxTemperatureReducer
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> { // XXX extends Reducer<I1, I2, O1, O2>
  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, // XXX Iterable
      Context context)
      throws IOException, InterruptedException {
    
    int maxValue = Integer.MIN_VALUE;
    for (IntWritable value : values) {
      maxValue = Math.max(maxValue, value.get());
    }
    context.write(key, new IntWritable(maxValue));
  }
}
// ^^ MaxTemperatureReducer

//=*=*=*=*
//./ch02/src/main/java/MaxTemperatureWithCombiner.java
// cc MaxTemperatureWithCombiner Application to find the maximum temperature, using a combiner function for efficiency
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// vv MaxTemperatureWithCombiner
public class MaxTemperatureWithCombiner {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithCombiner <input path> " +
          "<output path>");
      System.exit(-1);
    }
    
    Job job = new Job();
    job.setJarByClass(MaxTemperatureWithCombiner.class);
    job.setJobName("Max temperature");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(MaxTemperatureMapper.class);
    /*[*/job.setCombinerClass(MaxTemperatureReducer.class)/*]*/;
    job.setReducerClass(MaxTemperatureReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
// ^^ MaxTemperatureWithCombiner

//=*=*=*=*
//./ch02/src/main/java/OldMaxTemperature.java
// cc OldMaxTemperature Application to find the maximum temperature, using the old MapReduce API
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

// vv OldMaxTemperature
public class OldMaxTemperature {
  
  static class OldMaxTemperatureMapper /*[*/extends MapReduceBase/*]*/
    /*[*/implements Mapper/*]*/<LongWritable, Text, Text, IntWritable> {
  
    private static final int MISSING = 9999;
    
    @Override
    public void map(LongWritable key, Text value,
        /*[*/OutputCollector<Text, IntWritable> output, Reporter reporter/*]*/)
        throws IOException {
      
      String line = value.toString();
      String year = line.substring(15, 19);
      int airTemperature;
      if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
        airTemperature = Integer.parseInt(line.substring(88, 92));
      } else {
        airTemperature = Integer.parseInt(line.substring(87, 92));
      }
      String quality = line.substring(92, 93);
      if (airTemperature != MISSING && quality.matches("[01459]")) {
        /*[*/output.collect/*]*/(new Text(year), new IntWritable(airTemperature));
      }
    }
  }
  
  static class OldMaxTemperatureReducer /*[*/extends MapReduceBase/*]*/
    /*[*/implements Reducer/*]*/<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, /*[*/Iterator/*]*/<IntWritable> values,
        /*[*/OutputCollector<Text, IntWritable> output, Reporter reporter/*]*/)
        throws IOException {
      
      int maxValue = Integer.MIN_VALUE;
      while (/*[*/values.hasNext()/*]*/) {
        maxValue = Math.max(maxValue, /*[*/values.next().get()/*]*/);
      }
      /*[*/output.collect/*]*/(key, new IntWritable(maxValue));
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: OldMaxTemperature <input path> <output path>");
      System.exit(-1);
    }
    
    /*[*/JobConf conf = new JobConf(OldMaxTemperature.class);/*]*/
    /*[*/conf/*]*/.setJobName("Max temperature");

    FileInputFormat.addInputPath(/*[*/conf/*]*/, new Path(args[0]));
    FileOutputFormat.setOutputPath(/*[*/conf/*]*/, new Path(args[1]));
    
    /*[*/conf/*]*/.setMapperClass(OldMaxTemperatureMapper.class);
    /*[*/conf/*]*/.setReducerClass(OldMaxTemperatureReducer.class);

    /*[*/conf/*]*/.setOutputKeyClass(Text.class);
    /*[*/conf/*]*/.setOutputValueClass(IntWritable.class);

    /*[*/JobClient.runJob(conf);/*]*/
  }
}
// ^^ OldMaxTemperature
//=*=*=*=*
//./ch02/src/main/java/oldapi/MaxTemperature.java
package oldapi;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class MaxTemperature {

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }
    
    JobConf conf = new JobConf(MaxTemperature.class);
    conf.setJobName("Max temperature");

    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setMapperClass(MaxTemperatureMapper.class);
    conf.setReducerClass(MaxTemperatureReducer.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    JobClient.runJob(conf);
  }
}

//=*=*=*=*
//./ch02/src/main/java/oldapi/MaxTemperatureMapper.java
package oldapi;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MaxTemperatureMapper extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {

  private static final int MISSING = 9999;
  
  public void map(LongWritable key, Text value,
      OutputCollector<Text, IntWritable> output, Reporter reporter)
      throws IOException {
    
    String line = value.toString();
    String year = line.substring(15, 19);
    int airTemperature;
    if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
      airTemperature = Integer.parseInt(line.substring(88, 92));
    } else {
      airTemperature = Integer.parseInt(line.substring(87, 92));
    }
    String quality = line.substring(92, 93);
    if (airTemperature != MISSING && quality.matches("[01459]")) {
      output.collect(new Text(year), new IntWritable(airTemperature));
    }
  }
}

//=*=*=*=*
//./ch02/src/main/java/oldapi/MaxTemperatureReducer.java
package oldapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class MaxTemperatureReducer extends MapReduceBase
  implements Reducer<Text, IntWritable, Text, IntWritable> {

  public void reduce(Text key, Iterator<IntWritable> values,
      OutputCollector<Text, IntWritable> output, Reporter reporter)
      throws IOException {
    
    int maxValue = Integer.MIN_VALUE;
    while (values.hasNext()) {
      maxValue = Math.max(maxValue, values.next().get());
    }
    output.collect(key, new IntWritable(maxValue));
  }
}

//=*=*=*=*
//./ch02/src/main/java/oldapi/MaxTemperatureWithCombiner.java
package oldapi;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class MaxTemperatureWithCombiner {

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithCombiner <input path> " +
      		"<output path>");
      System.exit(-1);
    }
    
    JobConf conf = new JobConf(MaxTemperatureWithCombiner.class);
    conf.setJobName("Max temperature");

    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setMapperClass(MaxTemperatureMapper.class);
    /*[*/conf.setCombinerClass(MaxTemperatureReducer.class)/*]*/;
    conf.setReducerClass(MaxTemperatureReducer.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    JobClient.runJob(conf);
  }
}

//=*=*=*=*
//./ch03/src/main/java/DateRangePathFilter.java
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class DateRangePathFilter implements PathFilter {
  
  private final Pattern PATTERN = Pattern.compile("^.*/(\\d\\d\\d\\d/\\d\\d/\\d\\d).*$"); // XXX compile
  
  private final Date start, end;

  public DateRangePathFilter(Date start, Date end) {
    this.start = new Date(start.getTime()); // XXX defensive copy in constructor
    this.end = new Date(end.getTime());     // XXX defensive copy in constructor
  }
  
  public boolean accept(Path path) {
      Matcher matcher = PATTERN.matcher(path.toString()); // XXX PATTERN is compiled pattern .matcher(/* string to match */)
    if (matcher.matches()) {                              // XXX matcher.matches() boolean
      DateFormat format = new SimpleDateFormat("yyyy/MM/dd"); // XXX SimpleDateFormat
      try {
        return inInterval(format.parse(matcher.group(1))); // XXX DateFormat.parse returns Date
      } catch (ParseException e) {
        return false;
      }
    }
    return false;
  }

  private boolean inInterval(Date date) {
    return !date.before(start) && !date.after(end); // XXX Date.before(Date) Date.after(Date)
  }

}

//=*=*=*=*
//./ch03/src/main/java/FileCopyWithProgress.java
// cc FileCopyWithProgress Copies a local file to a Hadoop filesystem, and shows progress
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration; // XXX
import org.apache.hadoop.fs.FileSystem;      // XXX
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

// vv FileCopyWithProgress
public class FileCopyWithProgress {
  public static void main(String[] args) throws Exception {
    String localSrc = args[0];
    String dst = args[1];
    
    InputStream in = new BufferedInputStream(new FileInputStream(localSrc)); // XXX InputStream = BufferedInputStream(new FileInputStream())
    
    Configuration conf = new Configuration(); // XXX hadoop.conf.Configuration
    FileSystem fs = FileSystem.get(URI.create(dst), conf); // XXX hadoop.fs.FileSystem from URI.create + hadoop.conf.Configuration
    OutputStream out = fs.create(new Path(dst), new Progressable() { // XXX fs.create -> stream hadoop.util.Progressable
      public void progress() {
        System.out.print(".");
      }
    });
    
    IOUtils.copyBytes(in, out, 4096, true); // XXX hadoop.IOUtils.copyBytes - CLOSES in and out at the end
  }
}
// ^^ FileCopyWithProgress

//=*=*=*=*
//./ch03/src/main/java/FileSystemCat.java
// cc FileSystemCat Displays files from a Hadoop filesystem on standard output by using the FileSystem directly
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

// vv FileSystemCat
public class FileSystemCat {

  public static void main(String[] args) throws Exception {
    String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    InputStream in = null;
    try {
      in = fs.open(new Path(uri));
      IOUtils.copyBytes(in, System.out, 4096, false);
    } finally {
      IOUtils.closeStream(in);
    }
  }
}
// ^^ FileSystemCat

//=*=*=*=*
//./ch03/src/main/java/FileSystemDoubleCat.java
// cc FileSystemDoubleCat Displays files from a Hadoop filesystem on standard output twice, by using seek
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

// vv FileSystemDoubleCat
public class FileSystemDoubleCat {

  public static void main(String[] args) throws Exception {
    String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    FSDataInputStream in = null; // XXX FSDataInputStream is Seekable, pervious examples had InputStream (no seeking was done)
    try {
      in = fs.open(new Path(uri));
      IOUtils.copyBytes(in, System.out, 4096, false);
      in.seek(0); // go back to the start of the file XXX
      IOUtils.copyBytes(in, System.out, 4096, false);
    } finally {
      IOUtils.closeStream(in);
    }
  }
}
// ^^ FileSystemDoubleCat

//=*=*=*=*
//./ch03/src/main/java/ListStatus.java
// cc ListStatus Shows the file statuses for a collection of paths in a Hadoop filesystem 
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

// vv ListStatus
public class ListStatus {

  public static void main(String[] args) throws Exception {
    String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    
    Path[] paths = new Path[args.length];
    for (int i = 0; i < paths.length; i++) {
      paths[i] = new Path(args[i]);
    }
    
    FileStatus[] status = fs.listStatus(paths); // XXX Filesystem.listStatus(Path[]) -> FileStatus (see below)
    Path[] listedPaths = FileUtil.stat2Paths(status); // XXX FileStatus to Path[]
    for (Path p : listedPaths) {
      System.out.println(p);
    }
  }
}
// ^^ ListStatus

//=*=*=*=*
//./ch03/src/main/java/RegexExcludePathFilter.java
// cc RegexExcludePathFilter A PathFilter for excluding paths that match a regular expression
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

// vv RegexExcludePathFilter
public class RegexExcludePathFilter implements PathFilter { // XXX hadoop.fs.PathFilter
  
  private final String regex;

  public RegexExcludePathFilter(String regex) {
    this.regex = regex;
  }

  public boolean accept(Path path) { // XXX interface PathFilter
    return !path.toString().matches(regex);
  }
}
// ^^ RegexExcludePathFilter

//=*=*=*=*
//./ch03/src/main/java/RegexPathFilter.java
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegexPathFilter implements PathFilter {
  
  private final String regex;
  private final boolean include;

  public RegexPathFilter(String regex) {
    this(regex, true);
  }
  
  public RegexPathFilter(String regex, boolean include) {
    this.regex = regex;
    this.include = include;
  }

  public boolean accept(Path path) {
    return (path.toString().matches(regex)) ? include : !include;
  }

}

//=*=*=*=*
//./ch03/src/main/java/URLCat.java
// cc URLCat Displays files from a Hadoop filesystem on standard output using a URLStreamHandler
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

// vv URLCat
public class URLCat {

    /* XXX
     *
     * in = new URL("hdfs://host/path").openStream();  To make new URL understand hdfs://:

     * There’s a little bit more work required to make Java recognize Hadoop’s hdfs URL scheme. This
     * is achieved by calling the setURLStreamHandlerFactory method on URL with an instance of
     * FsUrlStreamHandlerFactory. This method can be called only once per JVM, so it is typically
     * executed in a static block. This limitation means that if some other part of your
     * program—perhaps a third-party component outside your control—sets a URLStreamHandlerFactory,
     * you won’t be able to use this approach for reading data from Hadoop. The next section
     * discusses an alternative.

     */
  static {
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
  }
  
  public static void main(String[] args) throws Exception {
    InputStream in = null;
    try {
      in = new URL(args[0]).openStream();
      IOUtils.copyBytes(in, System.out, 4096, false);
    } finally {
      IOUtils.closeStream(in);
    }
  }
}
// ^^ URLCat

//=*=*=*=*
//./ch03/src/test/java/CoherencyModelTest.java
// == CoherencyModelTest
// == CoherencyModelTest-NotVisibleAfterFlush
// == CoherencyModelTest-VisibleAfterFlushAndSync
// == CoherencyModelTest-LocalFileVisibleAfterFlush
// == CoherencyModelTest-VisibleAfterClose
import static org.hamcrest.CoreMatchers.is; // XXX
import static org.junit.Assert.assertThat;  // XXX 
// XXX junit gives assertEquals which is inferior for reasons given here: http://blogs.atlassian.com/2009/06/how_hamcrest_can_save_your_sou/

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.*;

public class CoherencyModelTest {
    /* XXX Hadoop has a set of testing classes, called MiniDFSCluster, MiniMRCluster, and
     * MiniYARNCluster, that provide a programmatic way of creating in-process clusters. Unlike the
     * local job runner, these allow testing against the full HDFS and MapReduce machinery. Bear in
     * mind, too, that tasktrackers in a mini-cluster launch separate JVMs to run tasks in, which
     * can make debugging more difficult.
     */
  private MiniDFSCluster cluster; // use an in-process HDFS cluster for testing
  private FileSystem fs;

  @Before
  public void setUp() throws IOException { // XXX set  up FileSystem in setUp
    Configuration conf = new Configuration();
    if (System.getProperty("test.build.data") == null) {
      System.setProperty("test.build.data", "/tmp");
    }
    cluster = new MiniDFSCluster(conf, 1, true, null); // XXX
    fs = cluster.getFileSystem(); // XXX
  }
  
  @After
  public void tearDown() throws IOException {
    fs.close();
    cluster.shutdown();
  }
  
  @Test
  public void fileExistsImmediatelyAfterCreation() throws IOException {
    // vv CoherencyModelTest
    Path p = new Path("p");
    fs.create(p);
    assertThat(fs.exists(p), is(true)); // XXX
    // ^^ CoherencyModelTest
    assertThat(fs.delete(p, true), is(true)); // XXX
  }
  
  @Test
  public void fileContentIsNotVisibleAfterFlush() throws IOException {
    // vv CoherencyModelTest-NotVisibleAfterFlush
    Path p = new Path("p");
    OutputStream out = fs.create(p);
    out.write("content".getBytes("UTF-8")); // XXX OutputStream.write(bytes[]) String.getBytes(encoding)
    /*[*/out.flush();/*]*/
    assertThat(fs.getFileStatus(p).getLen(), is(0L)); // XXX fs.getFileStatus(path).getLen()
    // ^^ CoherencyModelTest-NotVisibleAfterFlush
    out.close();
    assertThat(fs.delete(p, true), is(true));
  }
  
  @Test
  @Ignore("See https://issues.apache.org/jira/browse/HADOOP-4379")
  public void fileContentIsVisibleAfterFlushAndSync() throws IOException {
    // vv CoherencyModelTest-VisibleAfterFlushAndSync
    Path p = new Path("p");
    FSDataOutputStream out = fs.create(p);
    out.write("content".getBytes("UTF-8"));
    out.flush();
    // XXX org.apache.hadoop.fs.Syncable this is why you need FSDataOutputStream, OutputStream is not syncable
    /*[*/out.sync();/*]*/
    assertThat(fs.getFileStatus(p).getLen(), is(((long) "content".length())));
    // ^^ CoherencyModelTest-VisibleAfterFlushAndSync
    out.close();
    assertThat(fs.delete(p, true), is(true));
  }
  
  
  @Test
  public void localFileContentIsVisibleAfterFlushAndSync() throws IOException {
    File localFile = File.createTempFile("tmp", "");
    assertThat(localFile.exists(), is(true));
    // vv CoherencyModelTest-LocalFileVisibleAfterFlush
    FileOutputStream out = new FileOutputStream(localFile);
    out.write("content".getBytes("UTF-8"));
    out.flush(); // flush to operating system
    out.getFD().sync(); // sync to disk XXX getFD() returns object of class FileDescriptor (gives sync() and valid())
    assertThat(localFile.length(), is(((long) "content".length())));
    // ^^ CoherencyModelTest-LocalFileVisibleAfterFlush
    out.close();
    assertThat(localFile.delete(), is(true));
  }
  
  @Test
  public void fileContentIsVisibleAfterClose() throws IOException {
    // vv CoherencyModelTest-VisibleAfterClose
    Path p = new Path("p");     // hadoop.fs.Path
    OutputStream out = fs.create(p);
    out.write("content".getBytes("UTF-8"));
    /*[*/out.close();/*]*/
    assertThat(fs.getFileStatus(p).getLen(), is(((long) "content".length()))); // FileStatus also gives path, owner, replication, 
    // ^^ CoherencyModelTest-VisibleAfterClose
    assertThat(fs.delete(p, true), is(true));
  }

}

//=*=*=*=*
//./ch03/src/test/java/FileSystemDeleteTest.java
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;        // XXX
import org.junit.Test;          // XXX

public class FileSystemDeleteTest {

  private FileSystem fs;
  
  @Before
  public void setUp() throws Exception {
    fs = FileSystem.get(new Configuration());
    writeFile(fs, new Path("dir/file"));
  }
  
  private void writeFile(FileSystem fileSys, Path name) throws IOException {
    FSDataOutputStream stm = fileSys.create(name); // XXX FileSystem.create(Path)
    stm.close();
  }
  
  @Test
  public void deleteFile() throws Exception {
      /* XXX assertThat(call, is(true/false) */
    assertThat(fs.delete(new Path("dir/file"), false), is(true));
    assertThat(fs.exists(new Path("dir/file")), is(false));
    assertThat(fs.exists(new Path("dir")), is(true));
    assertThat(fs.delete(new Path("dir"), false), is(true));
    assertThat(fs.exists(new Path("dir")), is(false));
  }

  @Test
  public void deleteNonEmptyDirectoryNonRecursivelyFails() throws Exception {
    try {
      fs.delete(new Path("dir"), false); // XXX non-recursive
      fail("Shouldn't delete non-empty directory"); // XXX fail("if not needed")
    } catch (IOException e) {
      // expected
    }
  }
  
  @Test
  public void deleteDirectory() throws Exception {
    assertThat(fs.delete(new Path("dir"), true), is(true));
    assertThat(fs.exists(new Path("dir")), is(false));
  }
  
}

//=*=*=*=*
//./ch03/src/test/java/FileSystemGlobTest.java
// == FileSystemGlobTest
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.*;

public class FileSystemGlobTest {
  
  private static final String BASE_PATH = "/tmp/" +
    FileSystemGlobTest.class.getSimpleName(); // XXX classname - package name ie java.lang.String -> String
  
  private FileSystem fs;
  
  @Before
  public void setUp() throws Exception {
    fs = FileSystem.get(new Configuration());
    fs.mkdirs(new Path(BASE_PATH, "2007/12/30")); // XXX mkdirs
    fs.mkdirs(new Path(BASE_PATH, "2007/12/31"));
    fs.mkdirs(new Path(BASE_PATH, "2008/01/01"));
    fs.mkdirs(new Path(BASE_PATH, "2008/01/02"));
  }
  
  @After
  public void tearDown() throws Exception {
    fs.delete(new Path(BASE_PATH), true); // XXX recursive
  }
  
  @Test
  public void glob() throws Exception {
    assertThat(glob("/*"), is(paths("/2007", "/2008"))); // XXX ??? /tmp???
    assertThat(glob("/*/*"), is(paths("/2007/12", "/2008/01")));
    
    // bug?
    //assertThat(glob("/*/12/*"), is(paths("/2007/12/30", "/2007/12/31")));

    assertThat(glob("/200?"), is(paths("/2007", "/2008")));
    assertThat(glob("/200[78]"), is(paths("/2007", "/2008")));
    assertThat(glob("/200[7-8]"), is(paths("/2007", "/2008")));
    assertThat(glob("/200[^01234569]"), is(paths("/2007", "/2008"))); // XXX are globs just regular expressions, no FileSystem.globStatus accepts a subset of regex

    assertThat(glob("/*/*/{31,01}"), is(paths("/2007/12/31", "/2008/01/01")));
    assertThat(glob("/*/*/3{0,1}"), is(paths("/2007/12/30", "/2007/12/31")));

    assertThat(glob("/*/{12/31,01/01}"), is(paths("/2007/12/31", "/2008/01/01"))); // XXX glob with alternatives

    // bug?
    //assertThat(glob("/2007/12/30/data\\[2007-12-30\\]"), is(paths("/2007/12/30/data[2007-12-30]")));

  }
  
  @Test
  public void regexIncludes() throws Exception {
    assertThat(glob("/*", new RegexPathFilter("^.*/2007$")), is(paths("/2007")));
    
    // bug?
    //assertThat(glob("/*/*/*", new RegexPathFilter("^.*/2007/12/31$")), is(paths("/2007/12/31")));
    // this works but shouldn't be necessary? see https://issues.apache.org/jira/browse/HADOOP-3497
    assertThat(glob("/*/*/*", new RegexPathFilter("^.*/2007(/12(/31)?)?$")), is(paths("/2007/12/31")));
  }
  
  @Test
  public void regexExcludes() throws Exception {
    assertThat(glob("/*", new RegexPathFilter("^.*/2007$", false)), is(paths("/2008")));
    assertThat(glob("/2007/*/*", new RegexPathFilter("^.*/2007/12/31$", false)), is(paths("/2007/12/30")));
  }
  
  @Test
  public void regexExcludesWithRegexExcludePathFilter() throws Exception {
    assertThat(glob("/*", new RegexExcludePathFilter("^.*/2007$")), is(paths("/2008"))); // XXX
    assertThat(glob("/2007/*/*", new RegexExcludePathFilter("^.*/2007/12/31$")), is(paths("/2007/12/30")));
  }
	  
  public void testDateRange() throws Exception {
    DateRangePathFilter filter = new DateRangePathFilter(date("2007/12/31"),
        date("2008/01/01"));    // XXX DateRangePathFilter
    assertThat(glob("/*/*/*", filter), is(paths("/2007/12/31", "/2008/01/01")));  
  } 
  
  private Path[] glob(String pattern) throws IOException {
    return FileUtil.stat2Paths(fs.globStatus(new Path(BASE_PATH + pattern))); // XXX 
  }
  
  private Path[] glob(String pattern, PathFilter pathFilter) throws IOException {
    return FileUtil.stat2Paths(fs.globStatus(new Path(BASE_PATH + pattern), pathFilter)); // XXX
  }
  
  private Path[] paths(String... pathStrings) { // XXX String...
    Path[] paths = new Path[pathStrings.length];
    for (int i = 0; i < paths.length; i++) {
      paths[i] = new Path("file:" + BASE_PATH + pathStrings[i]);
    }
    return paths;
  }
  
  private Date date(String date) throws ParseException {
    return new SimpleDateFormat("yyyy/MM/dd").parse(date); // XXX
  }
}

//=*=*=*=*
//./ch03/src/test/java/ShowFileStatusTest.java
// cc ShowFileStatusTest Demonstrates file status information
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.*;

// vv ShowFileStatusTest
public class ShowFileStatusTest {
  
  private MiniDFSCluster cluster; // use an in-process HDFS cluster for testing
  private FileSystem fs;

  @Before                       // XXX
  public void setUp() throws IOException {
    Configuration conf = new Configuration(); // XXX hadoop.conf.Configuration
    if (System.getProperty("test.build.data") == null) {
      System.setProperty("test.build.data", "/tmp");
    }
    cluster = new MiniDFSCluster(conf, 1, true, null); // XXX
    fs = cluster.getFileSystem();                      // XXX
    OutputStream out = fs.create(new Path("/dir/file")); // XXX hadoop.fs.Path
    out.write("content".getBytes("UTF-8"));
    out.close();
  }
  
  @After                        // XXX
  public void tearDown() throws IOException {
    if (fs != null) { fs.close(); }              // XXX
    if (cluster != null) { cluster.shutdown(); } // XXX
  }
  
  @Test(expected = FileNotFoundException.class) // XXX @TesT(expected = FNFE.class)
  public void throwsFileNotFoundForNonExistentFile() throws IOException {
    fs.getFileStatus(new Path("no-such-file"));
  }
  
  @Test
  public void fileStatusForFile() throws IOException {
    Path file = new Path("/dir/file"); // XXX new Path creates the file
    FileStatus stat = fs.getFileStatus(file);
    assertThat(stat.getPath().toUri().getPath(), is("/dir/file")); // XXX FileStatus.getPath().toUri() -> URI .getPath()
    assertThat(stat.isDir(), is(false));                           // XXX assertThat(actual, Matcher) Matcher provides matches method
    assertThat(stat.getLen(), is(7L));
    assertThat(stat.getModificationTime(),
        is(lessThanOrEqualTo(System.currentTimeMillis())));
    assertThat(stat.getReplication(), is((short) 1)); // XXX Matcher<Short> is(Short value) -> o.h.core.Is.<Short>is(Short) static factory method from oh.core.Is
    // XXX which calls the constructor is(Matcher<Short> matcher) with the matcher equalTo(Short) hamcrest-all-1.3-source.java:Is.java:65
    assertThat(stat.getBlockSize(), is(64 * 1024 * 1024L));
    assertThat(stat.getOwner(), is(System.getProperty("user.name")));
    assertThat(stat.getGroup(), is("supergroup"));
    assertThat(stat.getPermission().toString(), is("rw-r--r--"));
  }
  
  @Test
  public void fileStatusForDirectory() throws IOException {
    Path dir = new Path("/dir"); // XXX new Path creates the directory
    FileStatus stat = fs.getFileStatus(dir);
    assertThat(stat.getPath().toUri().getPath(), is("/dir"));
    assertThat(stat.isDir(), is(true));
    assertThat(stat.getLen(), is(0L));
    assertThat(stat.getModificationTime(),
        is(lessThanOrEqualTo(System.currentTimeMillis())));
    assertThat(stat.getReplication(), is((short) 0));
    assertThat(stat.getBlockSize(), is(0L));
    assertThat(stat.getOwner(), is(System.getProperty("user.name")));
    assertThat(stat.getGroup(), is("supergroup"));
    assertThat(stat.getPermission().toString(), is("rwxr-xr-x"));
  }
  
}
// ^^ ShowFileStatusTest

//=*=*=*=*
//./ch04/src/main/examples/FileDecompressor.java.input.txt
hadoop FileDecompressor file.gz
//=*=*=*=*
//./ch04/src/main/examples/MapFileWriteDemo.java.input.txt
hadoop MapFileWriteDemo numbers.map
//=*=*=*=*
//./ch04/src/main/examples/SequenceFileMapReduceSort.java.input.txt
hadoop jar $HADOOP_INSTALL/hadoop-*-examples.jar sort -r 1 \
  -inFormat org.apache.hadoop.mapred.SequenceFileInputFormat \
  -outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat \
  -outKey org.apache.hadoop.io.IntWritable \
  -outValue org.apache.hadoop.io.Text \
  numbers.seq sorted
//=*=*=*=*
//./ch04/src/main/examples/SequenceFileMapReduceSortResults.java.input.txt
hadoop fs -text sorted/part-00000 | head
//=*=*=*=*
//./ch04/src/main/examples/SequenceFileMapReduceSortResults.java.output.txt
1       Nine, ten, a big fat hen
2       Seven, eight, lay them straight
3       Five, six, pick up sticks
4       Three, four, shut the door
5       One, two, buckle my shoe
6       Nine, ten, a big fat hen
7       Seven, eight, lay them straight
8       Five, six, pick up sticks
9       Three, four, shut the door
10      One, two, buckle my shoe
//=*=*=*=*
//./ch04/src/main/examples/SequenceFileMapReduceSortResults.java.pre.sh
# Produce sorted seq file
hadoop SequenceFileWriteDemo numbers.seq

hadoop jar $HADOOP_INSTALL/hadoop-*-examples.jar sort -r 1 \
  -inFormat org.apache.hadoop.mapred.SequenceFileInputFormat \
  -outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat \
  -outKey org.apache.hadoop.io.IntWritable \
  -outValue org.apache.hadoop.io.Text \
  numbers.seq sorted
//=*=*=*=*
//./ch04/src/main/examples/SequenceFileReadDemo.java.input.txt
hadoop SequenceFileReadDemo numbers.seq
//=*=*=*=*
//./ch04/src/main/examples/SequenceFileReadDemo.java.output.txt
[128]   100     One, two, buckle my shoe
[173]   99      Three, four, shut the door
[220]   98      Five, six, pick up sticks
[264]   97      Seven, eight, lay them straight
[314]   96      Nine, ten, a big fat hen
[359]   95      One, two, buckle my shoe
[404]   94      Three, four, shut the door
[451]   93      Five, six, pick up sticks
[495]   92      Seven, eight, lay them straight
[545]   91      Nine, ten, a big fat hen
[590]   90      One, two, buckle my shoe
...
[1976]  60      One, two, buckle my shoe
[2021*] 59      Three, four, shut the door
[2088]  58      Five, six, pick up sticks
[2132]  57      Seven, eight, lay them straight
[2182]  56      Nine, ten, a big fat hen
...
[4557]  5       One, two, buckle my shoe
[4602]  4       Three, four, shut the door
[4649]  3       Five, six, pick up sticks
[4693]  2       Seven, eight, lay them straight
[4743]  1       Nine, ten, a big fat hen
//=*=*=*=*
//./ch04/src/main/examples/SequenceFileReadDemo.java.pre.sh
# Make sure file is there to be read
hadoop SequenceFileWriteDemo numbers.seq
//=*=*=*=*
//./ch04/src/main/examples/SequenceFileToMapFileConverter-fix.java.input.txt
hadoop MapFileFixer numbers.map
//=*=*=*=*
//./ch04/src/main/examples/SequenceFileToMapFileConverter-mv.java.input.txt
hadoop fs -mv numbers.map/part-00000 numbers.map/data
//=*=*=*=*
//./ch04/src/main/examples/SequenceFileToMapFileConverter-sort.java.input.txt
hadoop jar $HADOOP_INSTALL/hadoop-*-examples.jar sort -r 1 \
  -inFormat org.apache.hadoop.mapred.SequenceFileInputFormat \
  -outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat \
  -outKey org.apache.hadoop.io.IntWritable \
  -outValue org.apache.hadoop.io.Text \
  numbers.seq numbers.map
//=*=*=*=*
//./ch04/src/main/examples/SequenceFileWriteDemo.java.input.txt
hadoop SequenceFileWriteDemo numbers.seq
//=*=*=*=*
//./ch04/src/main/examples/SequenceFileWriteDemo.java.output.txt
[128]   100     One, two, buckle my shoe
[173]   99      Three, four, shut the door
[220]   98      Five, six, pick up sticks
[264]   97      Seven, eight, lay them straight
[314]   96      Nine, ten, a big fat hen
[359]   95      One, two, buckle my shoe
[404]   94      Three, four, shut the door
[451]   93      Five, six, pick up sticks
[495]   92      Seven, eight, lay them straight
[545]   91      Nine, ten, a big fat hen
...
[1976]  60      One, two, buckle my shoe
[2021]  59      Three, four, shut the door
[2088]  58      Five, six, pick up sticks
[2132]  57      Seven, eight, lay them straight
[2182]  56      Nine, ten, a big fat hen
...
[4557]  5       One, two, buckle my shoe
[4602]  4       Three, four, shut the door
[4649]  3       Five, six, pick up sticks
[4693]  2       Seven, eight, lay them straight
[4743]  1       Nine, ten, a big fat hen
//=*=*=*=*
//./ch04/src/main/examples/StreamCompressor.java.input.txt
echo "Text" | hadoop StreamCompressor org.apache.hadoop.io.compress.GzipCodec \
  | gunzip -
//=*=*=*=*
//./ch04/src/main/examples/StreamCompressor.java.output.txt
Text
//=*=*=*=*
//./ch04/src/main/examples/TextIterator.java.input.txt
hadoop TextIterator
//=*=*=*=*
//./ch04/src/main/examples/TextIterator.java.output.txt
41
df
6771
10400
//=*=*=*=*
//./ch04/src/main/java/FileDecompressor.java
// cc FileDecompressor A program to decompress a compressed file using a codec inferred from the file's extension
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

// vv FileDecompressor
public class FileDecompressor {

  public static void main(String[] args) throws Exception {
    String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    
    Path inputPath = new Path(uri);
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    CompressionCodec codec = factory.getCodec(inputPath);
    if (codec == null) {
      System.err.println("No codec found for " + uri);
      System.exit(1);
    }

    String outputUri =
      CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());

    InputStream in = null;
    OutputStream out = null;
    try {
      in = codec.createInputStream(fs.open(inputPath));
      out = fs.create(new Path(outputUri));
      IOUtils.copyBytes(in, out, conf);
    } finally {
      IOUtils.closeStream(in);
      IOUtils.closeStream(out);
    }
  }
}
// ^^ FileDecompressor

//=*=*=*=*
//./ch04/src/main/java/IntPair.java
import java.io.*;

import org.apache.hadoop.io.*;

public class IntPair implements WritableComparable<IntPair> {

  private int first;
  private int second;
  
  public IntPair() {
  }
  
  public IntPair(int first, int second) {
    set(first, second);
  }
  
  public void set(int first, int second) {
    this.first = first;
    this.second = second;
  }
  
  public int getFirst() {
    return first;
  }

  public int getSecond() {
    return second;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(first);
    out.writeInt(second);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first = in.readInt();
    second = in.readInt();
  }
  
  @Override
  public int hashCode() {
    return first * 163 + second;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof IntPair) {
      IntPair ip = (IntPair) o;
      return first == ip.first && second == ip.second;
    }
    return false;
  }

  @Override
  public String toString() {
    return first + "\t" + second;
  }
  
  @Override
  public int compareTo(IntPair ip) {
    int cmp = compare(first, ip.first);
    if (cmp != 0) {
      return cmp;
    }
    return compare(second, ip.second);
  }
  
  /**
   * Convenience method for comparing two ints.
   */
  public static int compare(int a, int b) {
    return (a < b ? -1 : (a == b ? 0 : 1));
  }
  
}

//=*=*=*=*
//./ch04/src/main/java/MapFileFixer.java
// cc MapFileFixer Re-creates the index for a MapFile
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

// vv MapFileFixer
public class MapFileFixer {

  public static void main(String[] args) throws Exception {
    String mapUri = args[0];
    
    Configuration conf = new Configuration();
    
    FileSystem fs = FileSystem.get(URI.create(mapUri), conf);
    Path map = new Path(mapUri);
    Path mapData = new Path(map, MapFile.DATA_FILE_NAME);
    
    // Get key and value types from data sequence file
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, mapData, conf);
    Class keyClass = reader.getKeyClass();
    Class valueClass = reader.getValueClass();
    reader.close();
    
    // Create the map file index file
    long entries = MapFile.fix(fs, map, keyClass, valueClass, false, conf);
    System.out.printf("Created MapFile %s with %d entries\n", map, entries);
  }
}
// ^^ MapFileFixer

//=*=*=*=*
//./ch04/src/main/java/MapFileWriteDemo.java
// cc MapFileWriteDemo Writing a MapFile
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

// vv MapFileWriteDemo
public class MapFileWriteDemo {
  
  private static final String[] DATA = {
    "One, two, buckle my shoe",
    "Three, four, shut the door",
    "Five, six, pick up sticks",
    "Seven, eight, lay them straight",
    "Nine, ten, a big fat hen"
  };
  
  public static void main(String[] args) throws IOException {
    String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);

    IntWritable key = new IntWritable();
    Text value = new Text();
    MapFile.Writer writer = null;
    try {
      writer = new MapFile.Writer(conf, fs, uri,
          key.getClass(), value.getClass());
      
      for (int i = 0; i < 1024; i++) {
        key.set(i + 1);
        value.set(DATA[i % DATA.length]);
        writer.append(key, value);
      }
    } finally {
      IOUtils.closeStream(writer);
    }
  }
}
// ^^ MapFileWriteDemo

//=*=*=*=*
//./ch04/src/main/java/MaxTemperatureWithCompression.java
// cc MaxTemperatureWithCompression Application to run the maximum temperature job producing compressed output
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//vv MaxTemperatureWithCompression
public class MaxTemperatureWithCompression {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithCompression <input path> " +
        "<output path>");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(MaxTemperature.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    /*[*/FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);/*]*/
    
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducer.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
//^^ MaxTemperatureWithCompression

//=*=*=*=*
//./ch04/src/main/java/MaxTemperatureWithMapOutputCompression.java
// == MaxTemperatureWithMapOutputCompression
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperatureWithMapOutputCompression {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithMapOutputCompression " +
        "<input path> <output path>");
      System.exit(-1);
    }

    // vv MaxTemperatureWithMapOutputCompression
    Configuration conf = new Configuration();
    conf.setBoolean("mapred.compress.map.output", true);
    conf.setClass("mapred.map.output.compression.codec", GzipCodec.class,
        CompressionCodec.class);
    Job job = new Job(conf);
    // ^^ MaxTemperatureWithMapOutputCompression
    job.setJarByClass(MaxTemperature.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducer.class);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

//=*=*=*=*
//./ch04/src/main/java/PooledStreamCompressor.java
// cc PooledStreamCompressor A program to compress data read from standard input and write it to standard output using a pooled compressor
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;

// vv PooledStreamCompressor
public class PooledStreamCompressor {

  public static void main(String[] args) throws Exception {
    String codecClassname = args[0];
    Class<?> codecClass = Class.forName(codecClassname);
    Configuration conf = new Configuration();
    CompressionCodec codec = (CompressionCodec)
      ReflectionUtils.newInstance(codecClass, conf);
    /*[*/Compressor compressor = null;
    try {
      compressor = CodecPool.getCompressor(codec);/*]*/
      CompressionOutputStream out =
        codec.createOutputStream(System.out, /*[*/compressor/*]*/);
      IOUtils.copyBytes(System.in, out, 4096, false);
      out.finish();
    /*[*/} finally {
      CodecPool.returnCompressor(compressor);
    }/*]*/
  }
}
// ^^ PooledStreamCompressor

//=*=*=*=*
//./ch04/src/main/java/SequenceFileReadDemo.java
// cc SequenceFileReadDemo Reading a SequenceFile
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

// vv SequenceFileReadDemo
public class SequenceFileReadDemo {
  
  public static void main(String[] args) throws IOException {
    String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    Path path = new Path(uri);

    SequenceFile.Reader reader = null;
    try {
      reader = new SequenceFile.Reader(fs, path, conf);
      Writable key = (Writable)
        ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Writable value = (Writable)
        ReflectionUtils.newInstance(reader.getValueClass(), conf);
      long position = reader.getPosition();
      while (reader.next(key, value)) {
        String syncSeen = reader.syncSeen() ? "*" : "";
        System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
        position = reader.getPosition(); // beginning of next record
      }
    } finally {
      IOUtils.closeStream(reader);
    }
  }
}
// ^^ SequenceFileReadDemo
//=*=*=*=*
//./ch04/src/main/java/SequenceFileWriteDemo.java
// cc SequenceFileWriteDemo Writing a SequenceFile
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

// vv SequenceFileWriteDemo
public class SequenceFileWriteDemo {
  
  private static final String[] DATA = {
    "One, two, buckle my shoe",
    "Three, four, shut the door",
    "Five, six, pick up sticks",
    "Seven, eight, lay them straight",
    "Nine, ten, a big fat hen"
  };
  
  public static void main(String[] args) throws IOException {
    String uri = args[0];
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(uri), conf);
    Path path = new Path(uri);

    IntWritable key = new IntWritable();
    Text value = new Text();
    SequenceFile.Writer writer = null;
    try {
      writer = SequenceFile.createWriter(fs, conf, path,
          key.getClass(), value.getClass());
      
      for (int i = 0; i < 100; i++) {
        key.set(100 - i);
        value.set(DATA[i % DATA.length]);
        System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
        writer.append(key, value);
      }
    } finally {
      IOUtils.closeStream(writer);
    }
  }
}
// ^^ SequenceFileWriteDemo

//=*=*=*=*
//./ch04/src/main/java/StreamCompressor.java
// cc StreamCompressor A program to compress data read from standard input and write it to standard output
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

// vv StreamCompressor
public class StreamCompressor {

  public static void main(String[] args) throws Exception {
    String codecClassname = args[0];
    Class<?> codecClass = Class.forName(codecClassname);
    Configuration conf = new Configuration();
    CompressionCodec codec = (CompressionCodec)
      ReflectionUtils.newInstance(codecClass, conf);
    
    CompressionOutputStream out = codec.createOutputStream(System.out);
    IOUtils.copyBytes(System.in, out, 4096, false);
    out.finish();
  }
}
// ^^ StreamCompressor

//=*=*=*=*
//./ch04/src/main/java/TextArrayWritable.java
// == TextArrayWritable
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

// vv TextArrayWritable
public class TextArrayWritable extends ArrayWritable {
  public TextArrayWritable() {
    super(Text.class);
  }
}
// ^^ TextArrayWritable

//=*=*=*=*
//./ch04/src/main/java/TextIterator.java
// cc TextIterator Iterating over the characters in a Text object
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;

// vv TextIterator
public class TextIterator {
  
  public static void main(String[] args) {    
    Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
    
    ByteBuffer buf = ByteBuffer.wrap(t.getBytes(), 0, t.getLength());
    int cp;
    while (buf.hasRemaining() && (cp = Text.bytesToCodePoint(buf)) != -1) {
      System.out.println(Integer.toHexString(cp));
    }
  }  
}
// ^^ TextIterator

//=*=*=*=*
//./ch04/src/main/java/TextPair.java
// cc TextPair A Writable implementation that stores a pair of Text objects
// cc TextPairComparator A RawComparator for comparing TextPair byte representations
// cc TextPairFirstComparator A custom RawComparator for comparing the first field of TextPair byte representations
// vv TextPair
import java.io.*;

import org.apache.hadoop.io.*;

public class TextPair implements WritableComparable<TextPair> {

  private Text first;
  private Text second;
  
  public TextPair() {
    set(new Text(), new Text());
  }
  
  public TextPair(String first, String second) {
    set(new Text(first), new Text(second));
  }
  
  public TextPair(Text first, Text second) {
    set(first, second);
  }
  
  public void set(Text first, Text second) {
    this.first = first;
    this.second = second;
  }
  
  public Text getFirst() {
    return first;
  }

  public Text getSecond() {
    return second;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    first.write(out);
    second.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first.readFields(in);
    second.readFields(in);
  }
  
  @Override
  public int hashCode() {
    return first.hashCode() * 163 + second.hashCode();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof TextPair) {
      TextPair tp = (TextPair) o;
      return first.equals(tp.first) && second.equals(tp.second);
    }
    return false;
  }

  @Override
  public String toString() {
    return first + "\t" + second;
  }
  
  @Override
  public int compareTo(TextPair tp) {
    int cmp = first.compareTo(tp.first);
    if (cmp != 0) {
      return cmp;
    }
    return second.compareTo(tp.second);
  }
  // ^^ TextPair
  
  // vv TextPairComparator
  public static class Comparator extends WritableComparator {
    
    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
    
    public Comparator() {
      super(TextPair.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      
      try {
        int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
        int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
        int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
        if (cmp != 0) {
          return cmp;
        }
        return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                                       b2, s2 + firstL2, l2 - firstL2);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  static {
    WritableComparator.define(TextPair.class, new Comparator());
  }
  // ^^ TextPairComparator
  
  // vv TextPairFirstComparator
  public static class FirstComparator extends WritableComparator {
    
    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
    
    public FirstComparator() {
      super(TextPair.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      
      try {
        int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
        int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
        return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
    
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      if (a instanceof TextPair && b instanceof TextPair) {
        return ((TextPair) a).first.compareTo(((TextPair) b).first);
      }
      return super.compare(a, b);
    }
  }
  // ^^ TextPairFirstComparator
  
// vv TextPair
}
// ^^ TextPair

//=*=*=*=*
//./ch04/src/main/java/oldapi/IntPair.java
package oldapi;
import java.io.*;

import org.apache.hadoop.io.*;

public class IntPair implements WritableComparable<IntPair> {

  private int first;
  private int second;
  
  public IntPair() {
  }
  
  public IntPair(int first, int second) {
    set(first, second);
  }
  
  public void set(int first, int second) {
    this.first = first;
    this.second = second;
  }
  
  public int getFirst() {
    return first;
  }

  public int getSecond() {
    return second;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(first);
    out.writeInt(second);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first = in.readInt();
    second = in.readInt();
  }
  
  @Override
  public int hashCode() {
    return first * 163 + second;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof IntPair) {
      IntPair ip = (IntPair) o;
      return first == ip.first && second == ip.second;
    }
    return false;
  }

  @Override
  public String toString() {
    return first + "\t" + second;
  }
  
  @Override
  public int compareTo(IntPair ip) {
    int cmp = compare(first, ip.first);
    if (cmp != 0) {
      return cmp;
    }
    return compare(second, ip.second);
  }
  
  /**
   * Convenience method for comparing two ints.
   */
  public static int compare(int a, int b) {
    return (a < b ? -1 : (a == b ? 0 : 1));
  }
  
}

//=*=*=*=*
//./ch04/src/main/java/oldapi/MaxTemperatureWithCompression.java
package oldapi;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class MaxTemperatureWithCompression {

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithCompression <input path> " +
      		"<output path>");
      System.exit(-1);
    }
    
    JobConf conf = new JobConf(MaxTemperatureWithCompression.class);
    conf.setJobName("Max temperature with output compression");

    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    
    /*[*/FileOutputFormat.setCompressOutput(conf, true);
    FileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);/*]*/

    conf.setMapperClass(MaxTemperatureMapper.class);
    conf.setCombinerClass(MaxTemperatureReducer.class);
    conf.setReducerClass(MaxTemperatureReducer.class);

    JobClient.runJob(conf);
  }
}

//=*=*=*=*
//./ch04/src/main/java/oldapi/MaxTemperatureWithMapOutputCompression.java
// == OldMaxTemperatureWithMapOutputCompression
package oldapi;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class MaxTemperatureWithMapOutputCompression {

  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: MaxTemperatureWithMapOutputCompression " +
      		"<input path> <output path>");
      System.exit(-1);
    }
    
    JobConf conf = new JobConf(MaxTemperatureWithCompression.class);
    conf.setJobName("Max temperature with map output compression");

    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    
    // vv OldMaxTemperatureWithMapOutputCompression
    conf.setCompressMapOutput(true);
    conf.setMapOutputCompressorClass(GzipCodec.class);
    // ^^ OldMaxTemperatureWithMapOutputCompression

    conf.setMapperClass(MaxTemperatureMapper.class);
    conf.setCombinerClass(MaxTemperatureReducer.class);
    conf.setReducerClass(MaxTemperatureReducer.class);

    JobClient.runJob(conf);
  }

}

//=*=*=*=*
//./ch04/src/main/java/oldapi/TextPair.java
package oldapi;
// cc TextPair A Writable implementation that stores a pair of Text objects
// cc TextPairComparator A RawComparator for comparing TextPair byte representations
// cc TextPairFirstComparator A custom RawComparator for comparing the first field of TextPair byte representations
// vv TextPair
import java.io.*;

import org.apache.hadoop.io.*;

public class TextPair implements WritableComparable<TextPair> {

  private Text first;
  private Text second;
  
  public TextPair() {
    set(new Text(), new Text());
  }
  
  public TextPair(String first, String second) {
    set(new Text(first), new Text(second));
  }
  
  public TextPair(Text first, Text second) {
    set(first, second);
  }
  
  public void set(Text first, Text second) {
    this.first = first;
    this.second = second;
  }
  
  public Text getFirst() {
    return first;
  }

  public Text getSecond() {
    return second;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    first.write(out);
    second.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first.readFields(in);
    second.readFields(in);
  }
  
  @Override
  public int hashCode() {
    return first.hashCode() * 163 + second.hashCode();
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof TextPair) {
      TextPair tp = (TextPair) o;
      return first.equals(tp.first) && second.equals(tp.second);
    }
    return false;
  }

  @Override
  public String toString() {
    return first + "\t" + second;
  }
  
  @Override
  public int compareTo(TextPair tp) {
    int cmp = first.compareTo(tp.first);
    if (cmp != 0) {
      return cmp;
    }
    return second.compareTo(tp.second);
  }
  // ^^ TextPair
  
  // vv TextPairComparator
  public static class Comparator extends WritableComparator {
    
    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
    
    public Comparator() {
      super(TextPair.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      
      try {
        int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
        int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
        int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
        if (cmp != 0) {
          return cmp;
        }
        return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                                       b2, s2 + firstL2, l2 - firstL2);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  static {
    WritableComparator.define(TextPair.class, new Comparator());
  }
  // ^^ TextPairComparator
  
  // vv TextPairFirstComparator
  public static class FirstComparator extends WritableComparator {
    
    private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
    
    public FirstComparator() {
      super(TextPair.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      
      try {
        int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
        int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
        return TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
    
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      if (a instanceof TextPair && b instanceof TextPair) {
        return ((TextPair) a).first.compareTo(((TextPair) b).first);
      }
      return super.compare(a, b);
    }
  }
  // ^^ TextPairFirstComparator
  
// vv TextPair
}
// ^^ TextPair

//=*=*=*=*
//./ch04/src/test/java/ArrayWritableTest.java
// == ArrayWritableTest
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.Test;

public class ArrayWritableTest extends WritableTestBase {
  
  @Test
  public void test() throws IOException {
    // vv ArrayWritableTest
    ArrayWritable writable = new ArrayWritable(Text.class);
    // ^^ ArrayWritableTest
    writable.set(new Text[] { new Text("cat"), new Text("dog") });
    
    TextArrayWritable dest = new TextArrayWritable();
    WritableUtils.cloneInto(dest, writable);
    assertThat(dest.get().length, is(2));
    // TODO: fix cast, also use single assert
    assertThat((Text) dest.get()[0], is(new Text("cat")));
    assertThat((Text) dest.get()[1], is(new Text("dog")));
    
    Text[] copy = (Text[]) dest.toArray();
    assertThat(copy[0], is(new Text("cat")));
    assertThat(copy[1], is(new Text("dog")));
  }
}

//=*=*=*=*
//./ch04/src/test/java/BinaryOrTextWritable.java
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class BinaryOrTextWritable extends GenericWritable {
  private static Class[] TYPES = { BytesWritable.class, Text.class };

  @Override
  protected Class<? extends Writable>[] getTypes() {
    return TYPES;
  }
  
}

//=*=*=*=*
//./ch04/src/test/java/BooleanWritableTest.java
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.BooleanWritable;
import org.junit.Test;

public class BooleanWritableTest extends WritableTestBase {
  
  @Test
  public void test() throws IOException {
    BooleanWritable src = new BooleanWritable(true);
    BooleanWritable dest = new BooleanWritable();
    assertThat(writeTo(src, dest), is("01"));
    assertThat(dest.get(), is(src.get()));
  }
}

//=*=*=*=*
//./ch04/src/test/java/BytesWritableTest.java
// == BytesWritableTest
// == BytesWritableTest-Capacity
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

public class BytesWritableTest extends WritableTestBase {
  
  @Test
  public void test() throws IOException {
    // vv BytesWritableTest
    BytesWritable b = new BytesWritable(new byte[] { 3, 5 });
    byte[] bytes = serialize(b);
    assertThat(StringUtils.byteToHexString(bytes), is("000000020305"));
    // ^^ BytesWritableTest
    
    // vv BytesWritableTest-Capacity
    b.setCapacity(11);
    assertThat(b.getLength(), is(2));
    assertThat(b.getBytes().length, is(11));
    // ^^ BytesWritableTest-Capacity
  }
}

//=*=*=*=*
//./ch04/src/test/java/FileDecompressorTest.java
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.*;
import java.util.Scanner;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class FileDecompressorTest {

  @Test
  public void decompressesGzippedFile() throws Exception {
    File file = File.createTempFile("file", ".gz");
    file.deleteOnExit();
    InputStream in = this.getClass().getResourceAsStream("/file.gz");
    IOUtils.copyBytes(in, new FileOutputStream(file), 4096, true);
    
    String path = file.getAbsolutePath();
    FileDecompressor.main(new String[] { path });
    
    String decompressedPath = path.substring(0, path.length() - 3);
    assertThat(readFile(new File(decompressedPath)), is("Text\n"));
  }
  
  private String readFile(File file) throws IOException {
    return new Scanner(file).useDelimiter("\\A").next();
  }
}

//=*=*=*=*
//./ch04/src/test/java/GenericWritableTest.java
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.Test;

public class GenericWritableTest extends WritableTestBase {
  
  @Test
  public void test() throws IOException {
    BinaryOrTextWritable src = new BinaryOrTextWritable();
    src.set(new Text("text"));
    BinaryOrTextWritable dest = new BinaryOrTextWritable();
    WritableUtils.cloneInto(dest, src);
    assertThat((Text) dest.get(), is(new Text("text")));
    
    src.set(new BytesWritable(new byte[] {3, 5}));
    WritableUtils.cloneInto(dest, src);
    assertThat(((BytesWritable) dest.get()).getLength(), is(2)); // TODO proper assert
  }
}

//=*=*=*=*
//./ch04/src/test/java/IntPairTest.java
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.junit.Test;

public class IntPairTest extends WritableTestBase {
  
  private IntPair ip1 = new IntPair(1, 2);
  private IntPair ip2 = new IntPair(2, 1);
  private IntPair ip3 = new IntPair(1, 12);
  private IntPair ip4 = new IntPair(11, 2);
  private IntPair ip5 = new IntPair(Integer.MAX_VALUE, 2);
  private IntPair ip6 = new IntPair(Integer.MAX_VALUE, Integer.MAX_VALUE);

  
  @Test
  public void testComparator() throws IOException {
    check(ip1, ip1, 0);
    check(ip1, ip2, -1);
    check(ip3, ip4, -1);
    check(ip2, ip4, -1);
    check(ip3, ip5, -1);
    check(ip5, ip6, -1);
  }
  
  private void check(IntPair ip1, IntPair ip2, int c) throws IOException {
    check(WritableComparator.get(IntPair.class), ip1, ip2, c);
  }
  
  private void check(RawComparator comp, IntPair ip1, IntPair ip2, int c) throws IOException {
    checkOnce(comp, ip1, ip2, c);
    checkOnce(comp, ip2, ip1, -c);
  }

  private void checkOnce(RawComparator comp, IntPair ip1, IntPair ip2, int c) throws IOException {
    assertThat("Object", signum(comp.compare(ip1, ip2)), is(c));
    byte[] out1 = serialize(ip1);
    byte[] out2 = serialize(ip2);
    assertThat("Raw", signum(comp.compare(out1, 0, out1.length, out2, 0, out2.length)), is(c));
  }
  
  private int signum(int i) {
    return i < 0 ? -1 : (i == 0 ? 0 : 1);
  }

}

//=*=*=*=*
//./ch04/src/test/java/IntWritableTest.java
// == IntWritableTest
// == IntWritableTest-ValueConstructor
// == IntWritableTest-SerializedLength
// == IntWritableTest-SerializedBytes
// == IntWritableTest-Deserialization
// == IntWritableTest-Comparator
// == IntWritableTest-ObjectComparison
// == IntWritableTest-BytesComparison
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

public class IntWritableTest extends WritableTestBase {
  
  @Test
  public void walkthroughWithNoArgsConstructor() throws IOException {
    // vv IntWritableTest
    IntWritable writable = new IntWritable();
    writable.set(163);
    // ^^ IntWritableTest
    checkWalkthrough(writable);
  }

  @Test
  public void walkthroughWithValueConstructor() throws IOException {
    // vv IntWritableTest-ValueConstructor
    IntWritable writable = new IntWritable(163);
    // ^^ IntWritableTest-ValueConstructor
    checkWalkthrough(writable);
  }

  private void checkWalkthrough(IntWritable writable) throws IOException {
    // vv IntWritableTest-SerializedLength
    byte[] bytes = serialize(writable);
    assertThat(bytes.length, is(4));
    // ^^ IntWritableTest-SerializedLength
    
    // vv IntWritableTest-SerializedBytes
    assertThat(StringUtils.byteToHexString(bytes), is("000000a3"));
    // ^^ IntWritableTest-SerializedBytes
    
    // vv IntWritableTest-Deserialization
    IntWritable newWritable = new IntWritable();
    deserialize(newWritable, bytes);
    assertThat(newWritable.get(), is(163));
    // ^^ IntWritableTest-Deserialization
  }
  
  @Test
  public void comparator() throws IOException {
    // vv IntWritableTest-Comparator
    RawComparator<IntWritable> comparator = WritableComparator.get(IntWritable.class);
    // ^^ IntWritableTest-Comparator
    
    // vv IntWritableTest-ObjectComparison
    IntWritable w1 = new IntWritable(163);
    IntWritable w2 = new IntWritable(67);
    assertThat(comparator.compare(w1, w2), greaterThan(0));
    // ^^ IntWritableTest-ObjectComparison
    
    // vv IntWritableTest-BytesComparison
    byte[] b1 = serialize(w1);
    byte[] b2 = serialize(w2);
    assertThat(comparator.compare(b1, 0, b1.length, b2, 0, b2.length),
        greaterThan(0));
    // ^^ IntWritableTest-BytesComparison
  }
  
  @Test
  public void test() throws IOException {
    IntWritable src = new IntWritable(163);
    IntWritable dest = new IntWritable();
    assertThat(writeTo(src, dest), is("000000a3"));
    assertThat(dest.get(), is(src.get()));
  }

}

//=*=*=*=*
//./ch04/src/test/java/MapFileSeekTest.java
// == MapFileSeekTest
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.*;

public class MapFileSeekTest {
  
  private static final String MAP_URI = "test.numbers.map";
  private FileSystem fs;
  private MapFile.Reader reader;
  private WritableComparable<?> key;
  private Writable value;

  @Before
  public void setUp() throws IOException {
    MapFileWriteDemo.main(new String[] { MAP_URI });

    Configuration conf = new Configuration();
    fs = FileSystem.get(URI.create(MAP_URI), conf);

    reader = new MapFile.Reader(fs, MAP_URI, conf);
    key = (WritableComparable<?>)
      ReflectionUtils.newInstance(reader.getKeyClass(), conf);
    value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
  }

  @After
  public void tearDown() throws IOException {
    fs.delete(new Path(MAP_URI), true);
  }
  
  @Test
  public void get() throws Exception {
    // vv MapFileSeekTest
    Text value = new Text();
    reader.get(new IntWritable(496), value);
    assertThat(value.toString(), is("One, two, buckle my shoe"));
    // ^^ MapFileSeekTest
  }
  
  @Test
  public void seek() throws Exception {
    assertThat(reader.seek(new IntWritable(496)), is(true));
    assertThat(reader.next(key, value), is(true));
    assertThat(((IntWritable) key).get(), is(497));
    assertThat(((Text) value).toString(), is("Three, four, shut the door"));
  }
  
  

}

//=*=*=*=*
//./ch04/src/test/java/MapWritableTest.java
// == MapWritableTest
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.Test;

public class MapWritableTest extends WritableTestBase {
  
  @Test
  public void mapWritable() throws IOException {
    // vv MapWritableTest
    MapWritable src = new MapWritable();
    src.put(new IntWritable(1), new Text("cat"));
    src.put(new VIntWritable(2), new LongWritable(163));
    
    MapWritable dest = new MapWritable();
    WritableUtils.cloneInto(dest, src);
    assertThat((Text) dest.get(new IntWritable(1)), is(new Text("cat")));
    assertThat((LongWritable) dest.get(new VIntWritable(2)), is(new LongWritable(163)));
    // ^^ MapWritableTest
  }

  @Test
  public void setWritableEmulation() throws IOException {
    MapWritable src = new MapWritable();
    src.put(new IntWritable(1), NullWritable.get());
    src.put(new IntWritable(2), NullWritable.get());
    
    MapWritable dest = new MapWritable();
    WritableUtils.cloneInto(dest, src);
    assertThat(dest.containsKey(new IntWritable(1)), is(true));
  }
}

//=*=*=*=*
//./ch04/src/test/java/NullWritableTest.java
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.junit.Test;

public class NullWritableTest extends WritableTestBase {
  
  @Test
  public void test() throws IOException {
    NullWritable writable = NullWritable.get();
    assertThat(serialize(writable).length, is(0));
  }
}

//=*=*=*=*
//./ch04/src/test/java/ObjectWritableTest.java
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.Test;

public class ObjectWritableTest extends WritableTestBase {
  
  @Test
  public void test() throws IOException {
    ObjectWritable src = new ObjectWritable(Integer.TYPE, 163);
    ObjectWritable dest = new ObjectWritable();
    WritableUtils.cloneInto(dest, src);
    assertThat((Integer) dest.get(), is(163));
  }
}

//=*=*=*=*
//./ch04/src/test/java/SequenceFileSeekAndSyncTest.java
// == SequenceFileSeekAndSyncTest
// == SequenceFileSeekAndSyncTest-SeekNonRecordBoundary
// == SequenceFileSeekAndSyncTest-SyncNonRecordBoundary
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.*;

public class SequenceFileSeekAndSyncTest {
  
  private static final String SF_URI = "test.numbers.seq";
  private FileSystem fs;
  private SequenceFile.Reader reader;
  private Writable key;
  private Writable value;

  @Before
  public void setUp() throws IOException {
    SequenceFileWriteDemo.main(new String[] { SF_URI });
    
    Configuration conf = new Configuration();
    fs = FileSystem.get(URI.create(SF_URI), conf);
    Path path = new Path(SF_URI);

    reader = new SequenceFile.Reader(fs, path, conf);
    key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
    value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
  }
  
  @After
  public void tearDown() throws IOException {
    fs.delete(new Path(SF_URI), true);
  }

  @Test
  public void seekToRecordBoundary() throws IOException {
    // vv SequenceFileSeekAndSyncTest
    reader.seek(359);
    assertThat(reader.next(key, value), is(true));
    assertThat(((IntWritable) key).get(), is(95));
    // ^^ SequenceFileSeekAndSyncTest
  }
  
  @Test(expected=IOException.class)
  public void seekToNonRecordBoundary() throws IOException {
    // vv SequenceFileSeekAndSyncTest-SeekNonRecordBoundary
    reader.seek(360);
    reader.next(key, value); // fails with IOException
    // ^^ SequenceFileSeekAndSyncTest-SeekNonRecordBoundary
  }
  
  @Test
  public void syncFromNonRecordBoundary() throws IOException {
    // vv SequenceFileSeekAndSyncTest-SyncNonRecordBoundary
    reader.sync(360);
    assertThat(reader.getPosition(), is(2021L));
    assertThat(reader.next(key, value), is(true));
    assertThat(((IntWritable) key).get(), is(59));
    // ^^ SequenceFileSeekAndSyncTest-SyncNonRecordBoundary
  }
  
  @Test
  public void syncAfterLastSyncPoint() throws IOException {
    reader.sync(4557);
    assertThat(reader.getPosition(), is(4788L));
    assertThat(reader.next(key, value), is(false));
  }

}

//=*=*=*=*
//./ch04/src/test/java/StringTextComparisonTest.java
// cc StringTextComparisonTest Tests showing the differences between the String and Text classes
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.*;

import org.apache.hadoop.io.Text;
import org.junit.Test;

// vv StringTextComparisonTest
public class StringTextComparisonTest {

  @Test
  public void string() throws UnsupportedEncodingException {
    
    String s = "\u0041\u00DF\u6771\uD801\uDC00";
    assertThat(s.length(), is(5));
    assertThat(s.getBytes("UTF-8").length, is(10));
    
    assertThat(s.indexOf("\u0041"), is(0));
    assertThat(s.indexOf("\u00DF"), is(1));
    assertThat(s.indexOf("\u6771"), is(2));
    assertThat(s.indexOf("\uD801\uDC00"), is(3));
    
    assertThat(s.charAt(0), is('\u0041'));
    assertThat(s.charAt(1), is('\u00DF'));
    assertThat(s.charAt(2), is('\u6771'));
    assertThat(s.charAt(3), is('\uD801'));
    assertThat(s.charAt(4), is('\uDC00'));
    
    assertThat(s.codePointAt(0), is(0x0041));
    assertThat(s.codePointAt(1), is(0x00DF));
    assertThat(s.codePointAt(2), is(0x6771));
    assertThat(s.codePointAt(3), is(0x10400));
  }
  
  @Test
  public void text() {
    
    Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
    assertThat(t.getLength(), is(10));
    
    assertThat(t.find("\u0041"), is(0));
    assertThat(t.find("\u00DF"), is(1));
    assertThat(t.find("\u6771"), is(3));
    assertThat(t.find("\uD801\uDC00"), is(6));

    assertThat(t.charAt(0), is(0x0041));
    assertThat(t.charAt(1), is(0x00DF));
    assertThat(t.charAt(3), is(0x6771));
    assertThat(t.charAt(6), is(0x10400));
  }  
}
// ^^ StringTextComparisonTest

//=*=*=*=*
//./ch04/src/test/java/TextPairTest.java
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.junit.Test;


public class TextPairTest extends WritableTestBase {
  
  private TextPair tp1 = new TextPair("a", "b");
  private TextPair tp2 = new TextPair("b", "a");
  private TextPair tp3 = new TextPair("a", "ab");
  private TextPair tp4 = new TextPair("aa", "b");
  private TextPair tp5 = new TextPair(nTimes("a", 128), "b");
  private TextPair tp6 = new TextPair(nTimes("a", 128), nTimes("a", 128));
  private TextPair tp7 = new TextPair(nTimes("a", 128), nTimes("b", 128));
  
  private static String nTimes(String s, int n) {
    StringBuilder sb = new StringBuilder(n);
    for (int i = 0; i < n; i++) {
      sb.append(s);
    }
    return sb.toString();
  }
  
  @Test
  public void testComparator() throws IOException {
    check(tp1, tp1, 0);
    check(tp1, tp2, -1);
    check(tp3, tp4, -1);
    check(tp2, tp4, 1);
    check(tp3, tp5, -1);
    check(tp5, tp6, 1);
    check(tp5, tp7, -1);
  }

  @Test
  public void testFirstComparator() throws IOException {
    RawComparator comp = new TextPair.FirstComparator();
    check(comp, tp1, tp1, 0);
    check(comp, tp1, tp2, -1);
    check(comp, tp3, tp4, -1);
    check(comp, tp2, tp4, 1);
    check(comp, tp3, tp5, -1);
    check(comp, tp5, tp6, 0);
    check(comp, tp5, tp7, 0);
  }
  
  private void check(TextPair tp1, TextPair tp2, int c) throws IOException {
    check(WritableComparator.get(TextPair.class), tp1, tp2, c);
  }
  
  private void check(RawComparator comp, TextPair tp1, TextPair tp2, int c) throws IOException {
    checkOnce(comp, tp1, tp2, c);
    checkOnce(comp, tp2, tp1, -c);
  }

  private void checkOnce(RawComparator comp, TextPair tp1, TextPair tp2, int c) throws IOException {
    assertThat("Object", signum(comp.compare(tp1, tp2)), is(c));
    byte[] out1 = serialize(tp1);
    byte[] out2 = serialize(tp2);
    assertThat("Raw", signum(comp.compare(out1, 0, out1.length, out2, 0, out2.length)), is(c));
  }
  
  private int signum(int i) {
    return i < 0 ? -1 : (i == 0 ? 0 : 1);
  }

}

//=*=*=*=*
//./ch04/src/test/java/TextTest.java
// == TextTest
// == TextTest-Find
// == TextTest-Mutability
// == TextTest-ByteArrayNotShortened
// == TextTest-ToString
// == TextTest-Comparison
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.junit.Test;

public class TextTest extends WritableTestBase {
  
  @Test
  public void test() throws IOException {
    // vv TextTest
    Text t = new Text("hadoop");
    assertThat(t.getLength(), is(6));
    assertThat(t.getBytes().length, is(6));
    
    assertThat(t.charAt(2), is((int) 'd'));
    assertThat("Out of bounds", t.charAt(100), is(-1));
    // ^^ TextTest
  }    

  @Test
  public void find() throws IOException {
    // vv TextTest-Find
    Text t = new Text("hadoop");
    assertThat("Find a substring", t.find("do"), is(2));
    assertThat("Finds first 'o'", t.find("o"), is(3));
    assertThat("Finds 'o' from position 4 or later", t.find("o", 4), is(4));
    assertThat("No match", t.find("pig"), is(-1));
    // ^^ TextTest-Find
  }
  
  @Test
  public void mutability() throws IOException {
    // vv TextTest-Mutability
    Text t = new Text("hadoop");
    t.set("pig");
    assertThat(t.getLength(), is(3));
    assertThat(t.getBytes().length, is(3));
    // ^^ TextTest-Mutability
  }
  
  @Test
  public void byteArrayNotShortened() throws IOException {
    // vv TextTest-ByteArrayNotShortened
    Text t = new Text("hadoop");
    t.set(/*[*/new Text("pig")/*]*/);
    assertThat(t.getLength(), is(3));
    assertThat("Byte length not shortened", t.getBytes().length, /*[*/is(6)/*]*/);
    // ^^ TextTest-ByteArrayNotShortened
  }

  @Test
  public void toStringMethod() throws IOException {
    // vv TextTest-ToString
    assertThat(new Text("hadoop").toString(), is("hadoop"));
    // ^^ TextTest-ToString
  }
  
  @Test
  public void comparison() throws IOException {
    // vv TextTest-Comparison
    assertThat("\ud800\udc00".compareTo("\ue000"), lessThan(0));
    assertThat(new Text("\ud800\udc00").compareTo(new Text("\ue000")), greaterThan(0));
    // ^^ TextTest-Comparison
  }
  
  @Test
  public void withSupplementaryCharacters() throws IOException {
    
    String s = "\u0041\u00DF\u6771\uD801\uDC00";
    assertThat(s.length(), is(5));
    assertThat(s.getBytes("UTF-8").length, is(10));
    
    assertThat(s.indexOf('\u0041'), is(0));
    assertThat(s.indexOf('\u00DF'), is(1));
    assertThat(s.indexOf('\u6771'), is(2));
    assertThat(s.indexOf('\uD801'), is(3));
    assertThat(s.indexOf('\uDC00'), is(4));
    
    assertThat(s.charAt(0), is('\u0041'));
    assertThat(s.charAt(1), is('\u00DF'));
    assertThat(s.charAt(2), is('\u6771'));
    assertThat(s.charAt(3), is('\uD801'));
    assertThat(s.charAt(4), is('\uDC00'));
    
    Text t = new Text("\u0041\u00DF\u6771\uD801\uDC00");
    
    assertThat(serializeToString(t), is("0a41c39fe69db1f0909080"));
    
    assertThat(t.charAt(t.find("\u0041")), is(0x0041));
    assertThat(t.charAt(t.find("\u00DF")), is(0x00DF));
    assertThat(t.charAt(t.find("\u6771")), is(0x6771));
    assertThat(t.charAt(t.find("\uD801\uDC00")), is(0x10400));
    
  }    

}

//=*=*=*=*
//./ch04/src/test/java/VIntWritableTest.java
// == VIntWritableTest
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

public class VIntWritableTest extends WritableTestBase {
  
  @Test
  public void test() throws IOException {
    // vv VIntWritableTest
    byte[] data = serialize(new VIntWritable(163));
    assertThat(StringUtils.byteToHexString(data), is("8fa3"));
    // ^^ VIntWritableTest
  }
  
  @Test
  public void testSizes() throws IOException {
    assertThat(serializeToString(new VIntWritable(1)), is("01")); // 1 byte
    assertThat(serializeToString(new VIntWritable(-112)), is("90")); // 1 byte
    assertThat(serializeToString(new VIntWritable(127)), is("7f")); // 1 byte
    assertThat(serializeToString(new VIntWritable(128)), is("8f80")); // 2 byte
    assertThat(serializeToString(new VIntWritable(163)), is("8fa3")); // 2 byte
    assertThat(serializeToString(new VIntWritable(Integer.MAX_VALUE)),
        is("8c7fffffff")); // 5 byte
    assertThat(serializeToString(new VIntWritable(Integer.MIN_VALUE)),
        is("847fffffff")); // 5 byte
  }
}

//=*=*=*=*
//./ch04/src/test/java/VLongWritableTest.java
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.io.VLongWritable;
import org.junit.Test;

public class VLongWritableTest extends WritableTestBase {
  
  @Test
  public void test() throws IOException {
    assertThat(serializeToString(new VLongWritable(1)), is("01")); // 1 byte
    assertThat(serializeToString(new VLongWritable(127)), is("7f")); // 1 byte
    assertThat(serializeToString(new VLongWritable(128)), is("8f80")); // 2 byte
    assertThat(serializeToString(new VLongWritable(163)), is("8fa3")); // 2 byte
    assertThat(serializeToString(new VLongWritable(Long.MAX_VALUE)), is("887fffffffffffffff")); // 9 byte
    assertThat(serializeToString(new VLongWritable(Long.MIN_VALUE)), is("807fffffffffffffff")); // 9 byte
  }
}

//=*=*=*=*
//./ch04/src/test/java/WritableTestBase.java
// == WritableTestBase
// == WritableTestBase-Deserialize
import java.io.*;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

public class WritableTestBase {
  
  // vv WritableTestBase
  public static byte[] serialize(Writable writable) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    writable.write(dataOut);
    dataOut.close();
    return out.toByteArray();
  }
  // ^^ WritableTestBase
  
  // vv WritableTestBase-Deserialize
  public static byte[] deserialize(Writable writable, byte[] bytes)
      throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    DataInputStream dataIn = new DataInputStream(in);
    writable.readFields(dataIn);
    dataIn.close();
    return bytes;
  }
  // ^^ WritableTestBase-Deserialize
  
  public static String serializeToString(Writable src) throws IOException {
    return StringUtils.byteToHexString(serialize(src));
  }
  
  public static String writeTo(Writable src, Writable dest) throws IOException {
    byte[] data = deserialize(dest, serialize(src));
    return StringUtils.byteToHexString(data);
  }

}

//=*=*=*=*
//./ch04-avro/src/main/java/AvroGenericMaxTemperature.java
// cc AvroGenericMaxTemperature MapReduce program to find the maximum temperature, creating Avro output

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroUtf8InputFormat;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//vv AvroGenericMaxTemperature
public class AvroGenericMaxTemperature extends Configured implements Tool {
  
  private static final Schema SCHEMA = new Schema.Parser().parse(
      "{" +
      "  \"type\": \"record\"," +
      "  \"name\": \"WeatherRecord\"," +
      "  \"doc\": \"A weather reading.\"," +
      "  \"fields\": [" +
      "    {\"name\": \"year\", \"type\": \"int\"}," +
      "    {\"name\": \"temperature\", \"type\": \"int\"}," +
      "    {\"name\": \"stationId\", \"type\": \"string\"}" +
      "  ]" +
      "}"
  );

  public static class MaxTemperatureMapper
      extends AvroMapper<Utf8, Pair<Integer, GenericRecord>> {
    private NcdcRecordParser parser = new NcdcRecordParser();
    private GenericRecord record = new GenericData.Record(SCHEMA);
    @Override
    public void map(Utf8 line,
        AvroCollector<Pair<Integer, GenericRecord>> collector,
        Reporter reporter) throws IOException {
      parser.parse(line.toString());
      if (parser.isValidTemperature()) {
        record.put("year", parser.getYearInt());
        record.put("temperature", parser.getAirTemperature());
        record.put("stationId", parser.getStationId());
        collector.collect(
            new Pair<Integer, GenericRecord>(parser.getYearInt(), record));
      }
    }
  }
  
  public static class MaxTemperatureReducer
      extends AvroReducer<Integer, GenericRecord, GenericRecord> {

    @Override
    public void reduce(Integer key, Iterable<GenericRecord> values,
        AvroCollector<GenericRecord> collector, Reporter reporter)
        throws IOException {
      GenericRecord max = null;
      for (GenericRecord value : values) {
        if (max == null || 
            (Integer) value.get("temperature") > (Integer) max.get("temperature")) {
          max = newWeatherRecord(value);
        }
      }
      collector.collect(max);
    }
    private GenericRecord newWeatherRecord(GenericRecord value) {
      GenericRecord record = new GenericData.Record(SCHEMA);
      record.put("year", value.get("year"));
      record.put("temperature", value.get("temperature"));
      record.put("stationId", value.get("stationId"));
      return record;
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("Max temperature");
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    AvroJob.setInputSchema(conf, Schema.create(Schema.Type.STRING));
    AvroJob.setMapOutputSchema(conf,
        Pair.getPairSchema(Schema.create(Schema.Type.INT), SCHEMA));
    AvroJob.setOutputSchema(conf, SCHEMA);
    
    conf.setInputFormat(AvroUtf8InputFormat.class);

    AvroJob.setMapperClass(conf, MaxTemperatureMapper.class);
    AvroJob.setReducerClass(conf, MaxTemperatureReducer.class);

    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new AvroGenericMaxTemperature(), args);
    System.exit(exitCode);
  }
}
// ^^ AvroGenericMaxTemperature

//=*=*=*=*
//./ch04-avro/src/main/java/AvroProjection.java
import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroProjection extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    
    if (args.length != 3) {
      System.err.printf("Usage: %s [generic options] <input> <output> <schema-file>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    String input = args[0];
    String output = args[1];
    String schemaFile = args[2];

    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("Avro projection");
    
    FileInputFormat.addInputPath(conf, new Path(input));
    FileOutputFormat.setOutputPath(conf, new Path(output));
    
    Schema schema = new Schema.Parser().parse(new File(schemaFile));
    AvroJob.setInputSchema(conf, schema);
    AvroJob.setMapOutputSchema(conf, schema);
    AvroJob.setOutputSchema(conf, schema);
    conf.setNumReduceTasks(0);

    JobClient.runJob(conf); 
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new AvroProjection(), args);
    System.exit(exitCode);
  }

}

//=*=*=*=*
//./ch04-avro/src/main/java/AvroSort.java
// cc AvroSort A MapReduce program to sort an Avro data file

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//vv AvroSort
public class AvroSort extends Configured implements Tool {

  static class SortMapper<K> extends AvroMapper<K, Pair<K, K>> {
    public void map(K datum, AvroCollector<Pair<K, K>> collector,
        Reporter reporter) throws IOException {
      collector.collect(new Pair<K, K>(datum, null, datum, null));
    }
  }

  static class SortReducer<K> extends AvroReducer<K, K, K> {
    public void reduce(K key, Iterable<K> values,
        AvroCollector<K> collector,
        Reporter reporter) throws IOException {
      for (K value : values) {
        collector.collect(value);
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    
    if (args.length != 3) {
      System.err.printf(
        "Usage: %s [generic options] <input> <output> <schema-file>\n",
        getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    String input = args[0];
    String output = args[1];
    String schemaFile = args[2];

    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("Avro sort");
    
    FileInputFormat.addInputPath(conf, new Path(input));
    FileOutputFormat.setOutputPath(conf, new Path(output));
    
    Schema schema = new Schema.Parser().parse(new File(schemaFile));
    AvroJob.setInputSchema(conf, schema);
    Schema intermediateSchema = Pair.getPairSchema(schema, schema);
    AvroJob.setMapOutputSchema(conf, intermediateSchema);
    AvroJob.setOutputSchema(conf, schema);
    
    AvroJob.setMapperClass(conf, SortMapper.class);
    AvroJob.setReducerClass(conf, SortReducer.class);
  
    JobClient.runJob(conf); 
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new AvroSort(), args);
    System.exit(exitCode);
  }
}
// ^^ AvroSort

//=*=*=*=*
//./ch04-avro/src/main/java/AvroSpecificMaxTemperature.java
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroUtf8InputFormat;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import specific.WeatherRecord;

public class AvroSpecificMaxTemperature extends Configured implements Tool {
  
  public static class MaxTemperatureMapper
      extends AvroMapper<Utf8, Pair<Integer, WeatherRecord>> {
    private NcdcRecordParser parser = new NcdcRecordParser();
    private WeatherRecord record = new WeatherRecord();
    @Override
    public void map(Utf8 line,
        AvroCollector<Pair<Integer, WeatherRecord>> collector,
        Reporter reporter) throws IOException {
      parser.parse(line.toString());
      if (parser.isValidTemperature()) {
        record.year = parser.getYearInt();
        record.temperature = parser.getAirTemperature();
        record.stationId = new Utf8(parser.getStationId());
        collector.collect(
            new Pair<Integer, WeatherRecord>(parser.getYearInt(), record));
      }
    }
  }
  
  public static class MaxTemperatureReducer extends
      AvroReducer<Integer, WeatherRecord, WeatherRecord> {

    @Override
    public void reduce(Integer key, Iterable<WeatherRecord> values,
        AvroCollector<WeatherRecord> collector,
        Reporter reporter) throws IOException {
      WeatherRecord max = null;
      for (WeatherRecord value : values) {
        if (max == null || value.temperature > max.temperature) {
          max = newWeatherRecord(value);
        }
      }
      collector.collect(max);
    }
  }

  public static class MaxTemperatureCombiner extends
      AvroReducer<Integer, WeatherRecord, Pair<Integer, WeatherRecord>> {
    
    @Override
    public void reduce(Integer key, Iterable<WeatherRecord> values,
        AvroCollector<Pair<Integer, WeatherRecord>> collector,
        Reporter reporter) throws IOException {
      WeatherRecord max = null;
      for (WeatherRecord value : values) {
        if (max == null || value.temperature > max.temperature) {
          max = newWeatherRecord(value);
        }
      }
      collector.collect(new Pair<Integer, WeatherRecord>(key, max));
    }
  }

  private static WeatherRecord newWeatherRecord(WeatherRecord value) {
    WeatherRecord record = new WeatherRecord();
    record.year = value.year;
    record.temperature = value.temperature;
    record.stationId = value.stationId;
    return record;
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("Max temperature");
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    AvroJob.setInputSchema(conf, Schema.create(Schema.Type.STRING));
    AvroJob.setMapOutputSchema(conf, Pair.getPairSchema(
        Schema.create(Schema.Type.INT), WeatherRecord.SCHEMA$));
    AvroJob.setOutputSchema(conf, WeatherRecord.SCHEMA$);
    
    conf.setInputFormat(AvroUtf8InputFormat.class);

    AvroJob.setMapperClass(conf, MaxTemperatureMapper.class);
    AvroJob.setCombinerClass(conf, MaxTemperatureCombiner.class);
    AvroJob.setReducerClass(conf, MaxTemperatureReducer.class);

    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new AvroSpecificMaxTemperature(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch04-avro/src/main/java/NcdcRecordParser.java
import java.text.*;
import java.util.Date;

import org.apache.hadoop.io.Text;

public class NcdcRecordParser {
  
  private static final int MISSING_TEMPERATURE = 9999;
  
  private static final DateFormat DATE_FORMAT =
    new SimpleDateFormat("yyyyMMddHHmm");
  
  private String stationId;
  private String observationDateString;
  private String year;
  private String airTemperatureString;
  private int airTemperature;
  private boolean airTemperatureMalformed;
  private String quality;
  
  public void parse(String record) {
    stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
    observationDateString = record.substring(15, 27);
    year = record.substring(15, 19);
    airTemperatureMalformed = false;
    // Remove leading plus sign as parseInt doesn't like them
    if (record.charAt(87) == '+') {
      airTemperatureString = record.substring(88, 92);
      airTemperature = Integer.parseInt(airTemperatureString);
    } else if (record.charAt(87) == '-') {
      airTemperatureString = record.substring(87, 92);
      airTemperature = Integer.parseInt(airTemperatureString);
    } else {
      airTemperatureMalformed = true;
    }
    airTemperature = Integer.parseInt(airTemperatureString);
    quality = record.substring(92, 93);
  }
  
  public void parse(Text record) {
    parse(record.toString());
  }
  
  public boolean isValidTemperature() {
    return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE
        && quality.matches("[01459]");
  }
  
  public boolean isMalformedTemperature() {
    return airTemperatureMalformed;
  }
  
  public boolean isMissingTemperature() {
    return airTemperature == MISSING_TEMPERATURE;
  }
  
  public String getStationId() {
    return stationId;
  }
  
  public Date getObservationDate() {
    try {
      System.out.println(observationDateString);
      return DATE_FORMAT.parse(observationDateString);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public String getYear() {
    return year;
  }

  public int getYearInt() {
    return Integer.parseInt(year);
  }

  public int getAirTemperature() {
    return airTemperature;
  }
  
  public String getAirTemperatureString() {
    return airTemperatureString;
  }

  public String getQuality() {
    return quality;
  }

}

//=*=*=*=*
//./ch04-avro/src/test/java/AvroTest.java
// == AvroParseSchema
// == AvroGenericRecordCreation
// == AvroGenericRecordSerialization
// == AvroGenericRecordDeserialization
// == AvroSpecificStringPair
// == AvroDataFileCreation
// == AvroDataFileGetSchema
// == AvroDataFileRead
// == AvroDataFileIterator
// == AvroDataFileShortIterator
// == AvroSchemaResolution
// == AvroSchemaResolutionWithDataFile
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.junit.Ignore;
import org.junit.Test;

public class AvroTest {

  @Test
  public void testInt() throws IOException {
    Schema schema = new Schema.Parser().parse("\"int\"");
    
    int datum = 163;

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<Integer> writer = new GenericDatumWriter<Integer>(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
    writer.write(datum, encoder); // boxed
    encoder.flush();
    out.close();
    
    DatumReader<Integer> reader = new GenericDatumReader<Integer>(schema); // have to tell it the schema - it's not in the data stream!
    Decoder decoder = DecoderFactory.get()
      .binaryDecoder(out.toByteArray(), null /* reuse */);
    Integer result = reader.read(null /* reuse */, decoder);
    assertThat(result, is(163));
    
    try {
      reader.read(null, decoder);
      fail("Expected EOFException");
    } catch (EOFException e) {
      // expected
    }
  }
  
  @Test
  @Ignore("Requires Avro 1.6.0 or later")
  public void testGenericString() throws IOException {
    Schema schema = new Schema.Parser().parse("{\"type\": \"string\", \"avro.java.string\": \"String\"}");
    
    String datum = "foo";

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<String> writer = new GenericDatumWriter<String>(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
    writer.write(datum, encoder); // boxed
    encoder.flush();
    out.close();
    
    DatumReader<String> reader = new GenericDatumReader<String>(schema);
    Decoder decoder = DecoderFactory.get()
      .binaryDecoder(out.toByteArray(), null /* reuse */);
    String result = reader.read(null /* reuse */, decoder);
    assertThat(result, equalTo("foo"));
    
    try {
      reader.read(null, decoder);
      fail("Expected EOFException");
    } catch (EOFException e) {
      // expected
    }
  }
  
  @Test
  public void testPairGeneric() throws IOException {
// vv AvroParseSchema
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(getClass().getResourceAsStream("StringPair.avsc"));
// ^^ AvroParseSchema
    
// vv AvroGenericRecordCreation
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", "L");
    datum.put("right", "R");
// ^^ AvroGenericRecordCreation
    
// vv AvroGenericRecordSerialization
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(datum, encoder);
    encoder.flush();
    out.close();
// ^^ AvroGenericRecordSerialization
    
// vv AvroGenericRecordDeserialization
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
    GenericRecord result = reader.read(null, decoder);
    assertThat(result.get("left").toString(), is("L"));
    assertThat(result.get("right").toString(), is("R"));
// ^^ AvroGenericRecordDeserialization
  }

  @Test
  public void testPairSpecific() throws IOException {
    
// vv AvroSpecificStringPair
    /*[*/StringPair datum = new StringPair();
    datum.left = "L";
    datum.right = "R";/*]*/

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    /*[*/DatumWriter<StringPair> writer =
      new SpecificDatumWriter<StringPair>(StringPair.class);/*]*/
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(datum, encoder);
    encoder.flush();
    out.close();
    
    /*[*/DatumReader<StringPair> reader =
      new SpecificDatumReader<StringPair>(StringPair.class);/*]*/
    Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
    StringPair result = reader.read(null, decoder);
    assertThat(result./*[*/left/*]*/.toString(), is("L"));
    assertThat(result./*[*/right/*]*/.toString(), is("R"));
// ^^ AvroSpecificStringPair
  }

  @Test
  public void testDataFile() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
    
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", "L");
    datum.put("right", "R");

// vv AvroDataFileCreation
    File file = new File("data.avro");
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter =
      new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(schema, file);
    dataFileWriter.append(datum);
    dataFileWriter.close();
// ^^ AvroDataFileCreation
    
// vv AvroDataFileGetSchema
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> dataFileReader =
      new DataFileReader<GenericRecord>(file, reader);
    assertThat("Schema is the same", schema, is(dataFileReader.getSchema()));
// ^^ AvroDataFileGetSchema
    
// vv AvroDataFileRead
    assertThat(dataFileReader.hasNext(), is(true));
    GenericRecord result = dataFileReader.next();
    assertThat(result.get("left").toString(), is("L"));
    assertThat(result.get("right").toString(), is("R"));
    assertThat(dataFileReader.hasNext(), is(false));
// ^^ AvroDataFileRead
    
    file.delete();
  }
  
  @Test
  public void testDataFileIteration() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
    
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", "L");
    datum.put("right", "R");

    File file = new File("data.avro");
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter =
      new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(schema, file);
    dataFileWriter.append(datum);
    datum.put("right", new Utf8("r"));
    dataFileWriter.append(datum);
    dataFileWriter.close();
    
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> dataFileReader =
      new DataFileReader<GenericRecord>(file, reader);
    assertThat("Schema is the same", schema, is(dataFileReader.getSchema()));
    
    int count = 0;
// vv AvroDataFileIterator
    GenericRecord record = null;
    while (dataFileReader.hasNext()) {
      record = dataFileReader.next(record);
      // process record
// ^^ AvroDataFileIterator
      count++;
      assertThat(record.get("left").toString(), is("L"));
      if (count == 1) {
        assertThat(record.get("right").toString(), is("R"));
      } else {
        assertThat(record.get("right").toString(), is("r"));        
      }
// vv AvroDataFileIterator      
    }
// ^^ AvroDataFileIterator    
    
    assertThat(count, is(2));
    file.delete();
  }
  
  @Test
  public void testDataFileIterationShort() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
    
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", "L");
    datum.put("right", "R");

    File file = new File("data.avro");
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter =
      new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(schema, file);
    dataFileWriter.append(datum);
    datum.put("right", new Utf8("r"));
    dataFileWriter.append(datum);
    dataFileWriter.close();
    
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> dataFileReader =
      new DataFileReader<GenericRecord>(file, reader);
    assertThat("Schema is the same", schema, is(dataFileReader.getSchema()));
    
    int count = 0;
// vv AvroDataFileShortIterator
    for (GenericRecord record : dataFileReader) {
      // process record
// ^^ AvroDataFileShortIterator
      count++;
      assertThat(record.get("left").toString(), is("L"));
      if (count == 1) {
        assertThat(record.get("right").toString(), is("R"));
      } else {
        assertThat(record.get("right").toString(), is("r"));        
      }
// vv AvroDataFileShortIterator      
    }
// ^^ AvroDataFileShortIterator    
    
    assertThat(count, is(2));
    file.delete();
  }
  
  @Test
  public void testSchemaResolution() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
    Schema newSchema = new Schema.Parser().parse(getClass().getResourceAsStream("NewStringPair.avsc"));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
    GenericRecord datum = new GenericData.Record(schema); // no description
    datum.put("left", "L");
    datum.put("right", "R");
    writer.write(datum, encoder);
    encoder.flush();
    
// vv AvroSchemaResolution
    DatumReader<GenericRecord> reader =
      /*[*/new GenericDatumReader<GenericRecord>(schema, newSchema);/*]*/
    Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
    GenericRecord result = reader.read(null, decoder);
    assertThat(result.get("left").toString(), is("L"));
    assertThat(result.get("right").toString(), is("R"));
    /*[*/assertThat(result.get("description").toString(), is(""));/*]*/
// ^^ AvroSchemaResolution
  }
  
  @Test
  public void testSchemaResolutionWithAliases() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
    Schema newSchema = new Schema.Parser().parse(getClass().getResourceAsStream("AliasedStringPair.avsc"));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", "L");
    datum.put("right", "R");
    writer.write(datum, encoder);
    encoder.flush();
    
    DatumReader<GenericRecord> reader =
      new GenericDatumReader<GenericRecord>(schema, newSchema);
    Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
    GenericRecord result = reader.read(null, decoder);
    assertThat(result.get("first").toString(), is("L"));
    assertThat(result.get("second").toString(), is("R"));

    // old field names don't work
    assertThat(result.get("left"), is((Object) null));
    assertThat(result.get("right"), is((Object) null));
  }
  
  @Test
  public void testSchemaResolutionWithNull() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
    Schema newSchema = new Schema.Parser().parse(getClass().getResourceAsStream("NewStringPairWithNull.avsc"));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
    GenericRecord datum = new GenericData.Record(schema); // no description
    datum.put("left", "L");
    datum.put("right", "R");
    writer.write(datum, encoder);
    encoder.flush();
    
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema, newSchema); // write schema, read schema
    Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
    GenericRecord result = reader.read(null, decoder);
    assertThat(result.get("left").toString(), is("L"));
    assertThat(result.get("right").toString(), is("R"));
    assertThat(result.get("description"), is((Object) null));
  }
  
  @Test
  public void testIncompatibleSchemaResolution() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
    Schema newSchema = new Schema.Parser().parse("{\"type\": \"array\", \"items\": \"string\"}");
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null /* reuse */);
    GenericRecord datum = new GenericData.Record(schema); // no description
    datum.put("left", "L");
    datum.put("right", "R");
    writer.write(datum, encoder);
    encoder.flush();
    
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema, newSchema); // write schema, read schema
    Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
    try {
      reader.read(null, decoder);
      fail("Expected AvroTypeException");
    } catch (AvroTypeException e) {
      // expected
    }

  }
  
  @Test
  public void testSchemaResolutionWithDataFile() throws IOException {
    Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("StringPair.avsc"));
    Schema newSchema = new Schema.Parser().parse(getClass().getResourceAsStream("NewStringPair.avsc"));
    
    File file = new File("data.avro");
    
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter =
      new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(schema, file);
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("left", "L");
    datum.put("right", "R");
    dataFileWriter.append(datum);
    dataFileWriter.close();
    
// vv AvroSchemaResolutionWithDataFile
    DatumReader<GenericRecord> reader =
      new GenericDatumReader<GenericRecord>(/*[*/null/*]*/, newSchema);
// ^^ AvroSchemaResolutionWithDataFile
    DataFileReader<GenericRecord> dataFileReader =
      new DataFileReader<GenericRecord>(file, reader);
    assertThat(schema, is(dataFileReader.getSchema())); // schema is the actual (write) schema
    
    assertThat(dataFileReader.hasNext(), is(true));
    GenericRecord result = dataFileReader.next();
    assertThat(result.get("left").toString(), is("L"));
    assertThat(result.get("right").toString(), is("R"));
    assertThat(result.get("description").toString(), is(""));
    assertThat(dataFileReader.hasNext(), is(false));
    
    file.delete();
  }

}

//=*=*=*=*
//./ch05/src/main/examples/ConfigurationPrinterSystem.java.input.txt
hadoop -Dcolor=yellow ConfigurationPrinter | grep color
//=*=*=*=*
//./ch05/src/main/examples/ConfigurationPrinterWithConf.java.input.txt
hadoop ConfigurationPrinter -conf conf/hadoop-localhost.xml \
  | grep mapred.job.tracker=
//=*=*=*=*
//./ch05/src/main/examples/ConfigurationPrinterWithConf.java.output.txt
mapred.job.tracker=localhost:8021

//=*=*=*=*
//./ch05/src/main/examples/ConfigurationPrinterWithConfAndD.java.input.txt
hadoop ConfigurationPrinter -conf conf/hadoop-localhost.xml \
  -D mapred.job.tracker=example.com:8021 \
  | grep mapred.job.tracker
//=*=*=*=*
//./ch05/src/main/examples/ConfigurationPrinterWithD.java.input.txt
hadoop ConfigurationPrinter -D color=yellow | grep color
//=*=*=*=*
//./ch05/src/main/examples/ConfigurationPrinterWithD.java.output.txt
color=yellow

//=*=*=*=*
//./ch05/src/main/examples/MaxTemperatureDriver.java.input.txt
hadoop jar hadoop-examples.jar v3.MaxTemperatureDriver -conf conf/hadoop-cluster.xml \
  input/ncdc/all max-temp
//=*=*=*=*
//./ch05/src/main/java/ConfigurationPrinter.java
// cc ConfigurationPrinter An example Tool implementation for printing the properties in a Configuration
import java.util.Map.Entry;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

// vv ConfigurationPrinter
public class ConfigurationPrinter extends Configured implements Tool {
  
  static {
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    for (Entry<String, String> entry: conf) {
      System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
    }
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
    System.exit(exitCode);
  }
}
// ^^ ConfigurationPrinter

//=*=*=*=*
//./ch05/src/main/java/LoggingDriver.java
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LoggingDriver extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    Job job = new Job(getConf(), "Logging job");
    job.setJarByClass(getClass());

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(LoggingIdentityMapper.class);
    job.setNumReduceTasks(0);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new LoggingDriver(), args);
    System.exit(exitCode);
  }
}
//=*=*=*=*
//./ch05/src/main/java/LoggingIdentityMapper.java
//cc LoggingIdentityMapper An identity mapper that writes to standard output and also uses the Apache Commons Logging API
import java.io.IOException;

//vv LoggingIdentityMapper
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Mapper;

public class LoggingIdentityMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
  extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  
  private static final Log LOG = LogFactory.getLog(LoggingIdentityMapper.class);
  
  @Override
  public void map(KEYIN key, VALUEIN value, Context context)
      throws IOException, InterruptedException {
    // Log to stdout file
    System.out.println("Map key: " + key);
    
    // Log to syslog file
    LOG.info("Map key: " + key);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Map value: " + value);
    }
    context.write((KEYOUT) key, (VALUEOUT) value);
  }
}
//^^ LoggingIdentityMapper
//=*=*=*=*
//./ch05/src/main/java/v1/MaxTemperatureMapper.java
package v1;
// cc MaxTemperatureMapperV1 First version of a Mapper that passes MaxTemperatureMapperTest
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
//vv MaxTemperatureMapperV1
public class MaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();
    String year = line.substring(15, 19);
    int airTemperature = Integer.parseInt(line.substring(87, 92));
    context.write(new Text(year), new IntWritable(airTemperature));
  }
}
//^^ MaxTemperatureMapperV1

//=*=*=*=*
//./ch05/src/main/java/v1/MaxTemperatureReducer.java
package v1;
//cc MaxTemperatureReducerV1 Reducer for maximum temperature example
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// vv MaxTemperatureReducerV1
public class MaxTemperatureReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
    
    int maxValue = Integer.MIN_VALUE;
    for (IntWritable value : values) {
      maxValue = Math.max(maxValue, value.get());
    }
    context.write(key, new IntWritable(maxValue));
  }
}
// ^^ MaxTemperatureReducerV1

//=*=*=*=*
//./ch05/src/main/java/v2/MaxTemperatureDriver.java
package v2;

// cc MaxTemperatureDriverV2 Application to find the maximum temperature
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import v1.MaxTemperatureReducer;

// vv MaxTemperatureDriverV2
public class MaxTemperatureDriver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    Job job = new Job(getConf(), "Max temperature");
    job.setJarByClass(getClass());

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
    System.exit(exitCode);
  }
}
// ^^ MaxTemperatureDriverV2

//=*=*=*=*
//./ch05/src/main/java/v2/MaxTemperatureMapper.java
package v2;
//== MaxTemperatureMapperV2

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
  
//vv MaxTemperatureMapperV2
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();
    String year = line.substring(15, 19);
    /*[*/String temp = line.substring(87, 92);
    if (!missing(temp)) {/*]*/
        int airTemperature = Integer.parseInt(temp);
        context.write(new Text(year), new IntWritable(airTemperature));
    /*[*/}/*]*/
  }
  
  /*[*/private boolean missing(String temp) {
    return temp.equals("+9999");
  }/*]*/
//^^ MaxTemperatureMapperV2
}

//=*=*=*=*
//./ch05/src/main/java/v3/MaxTemperatureDriver.java
package v3;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import v1.MaxTemperatureReducer;

// Identical to v2 except for v3 mapper
public class MaxTemperatureDriver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    Job job = new Job(getConf(), "Max temperature");
    job.setJarByClass(getClass());

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch05/src/main/java/v3/MaxTemperatureMapper.java
package v3;
// cc MaxTemperatureMapperV3 A Mapper that uses a utility class to parse records

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// vv MaxTemperatureMapperV3
public class MaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
  
  /*[*/private NcdcRecordParser parser = new NcdcRecordParser();/*]*/
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    /*[*/parser.parse(value);/*]*/
    if (/*[*/parser.isValidTemperature()/*]*/) {
      context.write(new Text(/*[*/parser.getYear()/*]*/),
          new IntWritable(/*[*/parser.getAirTemperature()/*]*/));
    }
  }
}
// ^^ MaxTemperatureMapperV3

//=*=*=*=*
//./ch05/src/main/java/v3/NcdcRecordParser.java
// cc NcdcRecordParserV3 A class for parsing weather records in NCDC format
package v3;
import org.apache.hadoop.io.Text;

// vv NcdcRecordParserV3
public class NcdcRecordParser {
  
  private static final int MISSING_TEMPERATURE = 9999;
  
  private String year;
  private int airTemperature;
  private String quality;
  
  public void parse(String record) {
    year = record.substring(15, 19);
    String airTemperatureString;
    // Remove leading plus sign as parseInt doesn't like them
    if (record.charAt(87) == '+') { 
      airTemperatureString = record.substring(88, 92);
    } else {
      airTemperatureString = record.substring(87, 92);
    }
    airTemperature = Integer.parseInt(airTemperatureString);
    quality = record.substring(92, 93);
  }
  
  public void parse(Text record) {
    parse(record.toString());
  }

  public boolean isValidTemperature() {
    return airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
  }
  
  public String getYear() {
    return year;
  }

  public int getAirTemperature() {
    return airTemperature;
  }
}
// ^^ NcdcRecordParserV3

//=*=*=*=*
//./ch05/src/main/java/v4/MaxTemperatureDriver.java
package v4;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import v1.MaxTemperatureReducer;

//Identical to v3 except for v4 mapper
public class MaxTemperatureDriver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    Job job = new Job(getConf(), "Max temperature");
    job.setJarByClass(getClass());

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch05/src/main/java/v4/MaxTemperatureMapper.java
// == MaxTemperatureMapperV4
package v4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import v3.NcdcRecordParser;

//vv MaxTemperatureMapperV4
public class MaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  /*[*/enum Temperature {
    OVER_100
  }/*]*/
  
  private NcdcRecordParser parser = new NcdcRecordParser();

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    parser.parse(value);
    if (parser.isValidTemperature()) {
      int airTemperature = parser.getAirTemperature();
      /*[*/if (airTemperature > 1000) {
        System.err.println("Temperature over 100 degrees for input: " + value);
        context.setStatus("Detected possibly corrupt record: see logs.");
        context.getCounter(Temperature.OVER_100).increment(1);
      }/*]*/
      context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
    }
  }
}
//^^ MaxTemperatureMapperV4


//=*=*=*=*
//./ch05/src/main/java/v5/MaxTemperatureDriver.java
package v5;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import v1.MaxTemperatureReducer;

//Identical to v4 except for v5 mapper
public class MaxTemperatureDriver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    Job job = new Job(getConf(), "Max temperature");
    job.setJarByClass(getClass());

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch05/src/main/java/v5/MaxTemperatureMapper.java
package v5;
// cc MaxTemperatureMapperV5 Mapper for maximum temperature example
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// vv MaxTemperatureMapperV5
public class MaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
  
  enum Temperature {
    MALFORMED
  }

  private NcdcRecordParser parser = new NcdcRecordParser();
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    parser.parse(value);
    if (parser.isValidTemperature()) {
      int airTemperature = parser.getAirTemperature();
      context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
    } else if (parser.isMalformedTemperature()) {
      System.err.println("Ignoring possibly corrupt input: " + value);
      context.getCounter(Temperature.MALFORMED).increment(1);
    }
  }
}
// ^^ MaxTemperatureMapperV5

//=*=*=*=*
//./ch05/src/main/java/v5/NcdcRecordParser.java
package v5;
import org.apache.hadoop.io.Text;

public class NcdcRecordParser {
  
  private static final int MISSING_TEMPERATURE = 9999;
  
  private String year;
  private int airTemperature;
  private boolean airTemperatureMalformed;
  private String quality;
  
  public void parse(String record) {
    year = record.substring(15, 19);
    airTemperatureMalformed = false;
    // Remove leading plus sign as parseInt doesn't like them
    if (record.charAt(87) == '+') { 
      airTemperature = Integer.parseInt(record.substring(88, 92));
    } else if (record.charAt(87) == '-') {
      airTemperature = Integer.parseInt(record.substring(87, 92));
    } else {
      airTemperatureMalformed = true;
    }
    quality = record.substring(92, 93);
  }
  
  public void parse(Text record) {
    parse(record.toString());
  }

  public boolean isValidTemperature() {
    return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE
        && quality.matches("[01459]");
  }
  
  public boolean isMalformedTemperature() {
    return airTemperatureMalformed;
  }
  
  public String getYear() {
    return year;
  }

  public int getAirTemperature() {
    return airTemperature;
  }
}

//=*=*=*=*
//./ch05/src/main/java/v6/MaxTemperatureDriver.java
package v6;
//== MaxTemperatureDriverV6

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import v1.MaxTemperatureReducer;
import v5.MaxTemperatureMapper;

//Identical to v5 except for profiling configuration
public class MaxTemperatureDriver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    //vv MaxTemperatureDriverV6
    Configuration conf = getConf();
    conf.setBoolean("mapred.task.profile", true);
    conf.set("mapred.task.profile.params", "-agentlib:hprof=cpu=samples," +
        "heap=sites,depth=6,force=n,thread=y,verbose=n,file=%s");
    conf.set("mapred.task.profile.maps", "0-2");
    conf.set("mapred.task.profile.reduces", ""); // no reduces
    Job job = new Job(conf, "Max temperature");
    //^^ MaxTemperatureDriverV6
    
    // Following alternative is only available in 0.21 onwards
//    conf.setBoolean(JobContext.TASK_PROFILE, true);
//    conf.set(JobContext.TASK_PROFILE_PARAMS, "-agentlib:hprof=cpu=samples," +
//        "heap=sites,depth=6,force=n,thread=y,verbose=n,file=%s");
//    conf.set(JobContext.NUM_MAP_PROFILES, "0-2");
//    conf.set(JobContext.NUM_REDUCE_PROFILES, "");
    
    job.setJarByClass(getClass());
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
    System.exit(exitCode);
  }
}


//=*=*=*=*
//./ch05/src/main/java/v7/MaxTemperatureDriver.java
package v7;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import v1.MaxTemperatureReducer;

//Identical to v5 except for v7 mapper
public class MaxTemperatureDriver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    Job job = new Job(getConf(), "Max temperature");
    job.setJarByClass(getClass());

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch05/src/main/java/v7/MaxTemperatureMapper.java
package v7;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import v5.NcdcRecordParser;

public class MaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  enum Temperature {
    MALFORMED
  }

  private NcdcRecordParser parser = new NcdcRecordParser();
  /*[*/private Text year = new Text();
  private IntWritable temp = new IntWritable();/*]*/
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    parser.parse(value);
    if (parser.isValidTemperature()) {
      /*[*/year.set(parser.getYear());
      temp.set(parser.getAirTemperature());
      context.write(year, temp);/*]*/
    } else if (parser.isMalformedTemperature()) {
      System.err.println("Ignoring possibly corrupt input: " + value);
      context.getCounter(Temperature.MALFORMED).increment(1);
    }
  }
}

//=*=*=*=*
//./ch05/src/test/java/MultipleResourceConfigurationTest.java
// == MultipleResourceConfigurationTest
// == MultipleResourceConfigurationTest-Override
// == MultipleResourceConfigurationTest-Final
// == MultipleResourceConfigurationTest-Expansion
// == MultipleResourceConfigurationTest-SystemExpansion
// == MultipleResourceConfigurationTest-NoSystemByDefault
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class MultipleResourceConfigurationTest {
  
  @Test
  public void get() throws IOException {
    // Single test as an expedient for inclusion in the book
    
    // vv MultipleResourceConfigurationTest
    Configuration conf = new Configuration();
    conf.addResource("configuration-1.xml");
    conf.addResource("configuration-2.xml");
    // ^^ MultipleResourceConfigurationTest
    
    assertThat(conf.get("color"), is("yellow"));

    // override
    // vv MultipleResourceConfigurationTest-Override
    assertThat(conf.getInt("size", 0), is(12));
    // ^^ MultipleResourceConfigurationTest-Override

    // final properties cannot be overridden
    // vv MultipleResourceConfigurationTest-Final
    assertThat(conf.get("weight"), is("heavy"));
    // ^^ MultipleResourceConfigurationTest-Final

    // variable expansion
    // vv MultipleResourceConfigurationTest-Expansion
    assertThat(conf.get("size-weight"), is("12,heavy"));
    // ^^ MultipleResourceConfigurationTest-Expansion

    // variable expansion with system properties
    // vv MultipleResourceConfigurationTest-SystemExpansion
    System.setProperty("size", "14");
    assertThat(conf.get("size-weight"), is("14,heavy"));
    // ^^ MultipleResourceConfigurationTest-SystemExpansion

    // system properties are not picked up
    // vv MultipleResourceConfigurationTest-NoSystemByDefault
    System.setProperty("length", "2");
    assertThat(conf.get("length"), is((String) null));
    // ^^ MultipleResourceConfigurationTest-NoSystemByDefault

  }

}

//=*=*=*=*
//./ch05/src/test/java/SingleResourceConfigurationTest.java
// == SingleResourceConfigurationTest
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class SingleResourceConfigurationTest {
  
  @Test
  public void get() throws IOException {
    // vv SingleResourceConfigurationTest
    Configuration conf = new Configuration();
    conf.addResource("configuration-1.xml");
    assertThat(conf.get("color"), is("yellow"));
    assertThat(conf.getInt("size", 0), is(10));
    assertThat(conf.get("breadth", "wide"), is("wide"));
    // ^^ SingleResourceConfigurationTest
  }

}

//=*=*=*=*
//./ch05/src/test/java/v1/MaxTemperatureMapperTest.java
package v1;
// cc MaxTemperatureMapperTestV1 Unit test for MaxTemperatureMapper
// == MaxTemperatureMapperTestV1Missing
// vv MaxTemperatureMapperTestV1
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;

public class MaxTemperatureMapperTest {

  @Test
  public void processesValidRecord() throws IOException, InterruptedException {
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9-00111+99999999999");
                              // Temperature ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInputValue(value)
      .withOutput(new Text("1950"), new IntWritable(-11))
      .runTest();
  }
// ^^ MaxTemperatureMapperTestV1
  @Ignore // since we are showing a failing test in the book
// vv MaxTemperatureMapperTestV1Missing
  @Test
  public void ignoresMissingTemperatureRecord() throws IOException,
      InterruptedException {
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9+99991+99999999999");
                              // Temperature ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInputValue(value)
      .runTest();
  }
// ^^ MaxTemperatureMapperTestV1Missing
  @Test
  public void processesMalformedTemperatureRecord() throws IOException,
      InterruptedException {
    Text value = new Text("0335999999433181957042302005+37950+139117SAO  +0004" +
                                  // Year ^^^^
        "RJSN V02011359003150070356999999433201957010100005+353");
                              // Temperature ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInputValue(value)
      .withOutput(new Text("1957"), new IntWritable(1957))
      .runTest();
  }
// vv MaxTemperatureMapperTestV1
}
// ^^ MaxTemperatureMapperTestV1

//=*=*=*=*
//./ch05/src/test/java/v1/MaxTemperatureReducerTest.java
package v1;
// == MaxTemperatureReducerTestV1
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;

public class MaxTemperatureReducerTest {
  
  //vv MaxTemperatureReducerTestV1
  @Test
  public void returnsMaximumIntegerInValues() throws IOException,
      InterruptedException {
    new ReduceDriver<Text, IntWritable, Text, IntWritable>()
      .withReducer(new MaxTemperatureReducer())
      .withInputKey(new Text("1950"))
      .withInputValues(Arrays.asList(new IntWritable(10), new IntWritable(5)))
      .withOutput(new Text("1950"), new IntWritable(10))
      .runTest();
  }
  //^^ MaxTemperatureReducerTestV1
}

//=*=*=*=*
//./ch05/src/test/java/v2/MaxTemperatureMapperTest.java
package v2;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class MaxTemperatureMapperTest {

  @Test
  public void processesValidRecord() throws IOException, InterruptedException {
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9-00111+99999999999");
                              // Temperature ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInputValue(value)
      .withOutput(new Text("1950"), new IntWritable(-11))
      .runTest();
  }
  
  @Test
  public void ignoresMissingTemperatureRecord() throws IOException,
      InterruptedException {
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9+99991+99999999999");
                              // Temperature ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInputValue(value)
      .runTest();
  }
}

//=*=*=*=*
//./ch05/src/test/java/v3/MaxTemperatureDriverMiniTest.java
package v3;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;

// A test for MaxTemperatureDriver that runs in a "mini" HDFS and MapReduce cluster
public class MaxTemperatureDriverMiniTest extends ClusterMapReduceTestCase {
  
  public static class OutputLogFilter implements PathFilter {
    public boolean accept(Path path) {
      return !path.getName().startsWith("_");
    }
  }
  
  @Override
  protected void setUp() throws Exception {
    if (System.getProperty("test.build.data") == null) {
      System.setProperty("test.build.data", "/tmp");
    }
    if (System.getProperty("hadoop.log.dir") == null) {
      System.setProperty("hadoop.log.dir", "/tmp");
    }
    super.setUp();
  }
  
  // Not marked with @Test since ClusterMapReduceTestCase is a JUnit 3 test case
  public void test() throws Exception {
    Configuration conf = createJobConf();
    
    Path localInput = new Path("input/ncdc/micro");
    Path input = getInputDir();
    Path output = getOutputDir();
    
    // Copy input data into test HDFS
    getFileSystem().copyFromLocalFile(localInput, input);
    
    MaxTemperatureDriver driver = new MaxTemperatureDriver();
    driver.setConf(conf);
    
    int exitCode = driver.run(new String[] {
        input.toString(), output.toString() });
    assertThat(exitCode, is(0));
    
    // Check the output is as expected
    Path[] outputFiles = FileUtil.stat2Paths(
        getFileSystem().listStatus(output, new OutputLogFilter()));
    assertThat(outputFiles.length, is(1));
    
    InputStream in = getFileSystem().open(outputFiles[0]);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    assertThat(reader.readLine(), is("1949\t111"));
    assertThat(reader.readLine(), is("1950\t22"));
    assertThat(reader.readLine(), nullValue());
    reader.close();
  }
}

//=*=*=*=*
//./ch05/src/test/java/v3/MaxTemperatureDriverTest.java
package v3;
// cc MaxTemperatureDriverTestV3 A test for MaxTemperatureDriver that uses a local, in-process job runner
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.Test;

public class MaxTemperatureDriverTest {
  
  public static class OutputLogFilter implements PathFilter {
    public boolean accept(Path path) {
      return !path.getName().startsWith("_");
    }
  }
  
//vv MaxTemperatureDriverTestV3
  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    conf.set("mapred.job.tracker", "local");
    
    Path input = new Path("input/ncdc/micro");
    Path output = new Path("output");
    
    FileSystem fs = FileSystem.getLocal(conf);
    fs.delete(output, true); // delete old output
    
    MaxTemperatureDriver driver = new MaxTemperatureDriver();
    driver.setConf(conf);
    
    int exitCode = driver.run(new String[] {
        input.toString(), output.toString() });
    assertThat(exitCode, is(0));
    
    checkOutput(conf, output);
  }
//^^ MaxTemperatureDriverTestV3

  private void checkOutput(Configuration conf, Path output) throws IOException {
    FileSystem fs = FileSystem.getLocal(conf);
    Path[] outputFiles = FileUtil.stat2Paths(
        fs.listStatus(output, new OutputLogFilter()));
    assertThat(outputFiles.length, is(1));
    
    BufferedReader actual = asBufferedReader(fs.open(outputFiles[0]));
    BufferedReader expected = asBufferedReader(
        getClass().getResourceAsStream("/expected.txt"));
    String expectedLine;
    while ((expectedLine = expected.readLine()) != null) {
      assertThat(actual.readLine(), is(expectedLine));
    }
    assertThat(actual.readLine(), nullValue());
    actual.close();
    expected.close();
  }
  
  private BufferedReader asBufferedReader(InputStream in) throws IOException {
    return new BufferedReader(new InputStreamReader(in));
  }
}

//=*=*=*=*
//./ch05/src/test/java/v3/MaxTemperatureMapperTest.java
package v3;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class MaxTemperatureMapperTest {

  @Test
  public void processesValidRecord() throws IOException, InterruptedException {
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9-00111+99999999999");
                              // Temperature ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInputValue(value)
      .withOutput(new Text("1950"), new IntWritable(-11))
      .runTest();
  }
  
  @Test
  public void processesPositiveTemperatureRecord() throws IOException,
      InterruptedException {
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9+00111+99999999999");
                              // Temperature ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInputValue(value)
      .withOutput(new Text("1950"), new IntWritable(11))
      .runTest();
  }
  
  @Test
  public void ignoresMissingTemperatureRecord() throws IOException,
      InterruptedException {
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9+99991+99999999999");
                              // Temperature ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInputValue(value)
      .runTest();
  }
  
  @Test
  public void ignoresSuspectQualityRecord() throws IOException,
      InterruptedException {
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9+00112+99999999999");
                              // Temperature ^^^^^
                               // Suspect quality ^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInputValue(value)
      .runTest();
  }
  
}

//=*=*=*=*
//./ch05/src/test/java/v5/MaxTemperatureMapperTest.java
package v5;
// == MaxTemperatureMapperTestV5Malformed
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class MaxTemperatureMapperTest {

  @Test
  public void parsesValidRecord() throws IOException, InterruptedException {
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9-00111+99999999999");
                              // Temperature ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInputValue(value)
      .withOutput(new Text("1950"), new IntWritable(-11))
      .runTest();
  }
  
  @Test
  public void parsesMissingTemperature() throws IOException,
      InterruptedException {
    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
                                  // Year ^^^^
        "99999V0203201N00261220001CN9999999N9+99991+99999999999");
                              // Temperature ^^^^^
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInputValue(value)
      .runTest();
  }
//vv MaxTemperatureMapperTestV5Malformed
  @Test
  public void parsesMalformedTemperature() throws IOException,
      InterruptedException {
    Text value = new Text("0335999999433181957042302005+37950+139117SAO  +0004" +
                                  // Year ^^^^
        "RJSN V02011359003150070356999999433201957010100005+353");
                              // Temperature ^^^^^
    Counters counters = new Counters();
    new MapDriver<LongWritable, Text, Text, IntWritable>()
      .withMapper(new MaxTemperatureMapper())
      .withInputValue(value)
      .withCounters(counters)
      .runTest();
    Counter c = counters.findCounter(MaxTemperatureMapper.Temperature.MALFORMED);
    assertThat(c.getValue(), is(1L));
  }
// ^^ MaxTemperatureMapperTestV5Malformed
}

//=*=*=*=*
//./ch07/src/main/examples/MinimalMapReduce.java.input.txt
hadoop MinimalMapReduce "input/ncdc/all/190{1,2}.gz" output
//=*=*=*=*
//./ch07/src/main/examples/PartitionByStationUsingMultipleOutputFormat.java.input.txt
hadoop jar hadoop-examples.jar PartitionByStationUsingMultipleOutputFormat 'input/ncdc/all/190?.gz' output-part-by-station
//=*=*=*=*
//./ch07/src/main/examples/SmallFilesToSequenceFileConverter.java.input.txt
hadoop jar hadoop-examples.jar SmallFilesToSequenceFileConverter \
  -conf conf/hadoop-localhost.xml -D mapred.reduce.tasks=2 input/smallfiles output
//=*=*=*=*
//./ch07/src/main/java/MaxTemperatureWithMultipleInputs.java
// == MaxTemperatureWithMultipleInputs
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class MaxTemperatureWithMultipleInputs extends Configured
  implements Tool {
  
  static class MetOfficeMaxTemperatureMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {
  
    private MetOfficeRecordParser parser = new MetOfficeRecordParser();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      parser.parse(value);
      if (parser.isValidTemperature()) {
        context.write(new Text(parser.getYear()),
            new IntWritable(parser.getAirTemperature()));
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      JobBuilder.printUsage(this, "<ncdc input> <metoffice input> <output>");
      return -1;
    }
    
    Job job = new Job(getConf(), "Max temperature with multiple input formats");
    job.setJarByClass(getClass());
    
    Path ncdcInputPath = new Path(args[0]);
    Path metOfficeInputPath = new Path(args[1]);
    Path outputPath = new Path(args[2]);
    
// vv MaxTemperatureWithMultipleInputs    
    MultipleInputs.addInputPath(job, ncdcInputPath,
        TextInputFormat.class, MaxTemperatureMapper.class);
    MultipleInputs.addInputPath(job, metOfficeInputPath,
        TextInputFormat.class, MetOfficeMaxTemperatureMapper.class);
// ^^ MaxTemperatureWithMultipleInputs
    FileOutputFormat.setOutputPath(job, outputPath);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducer.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureWithMultipleInputs(),
        args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch07/src/main/java/MinimalMapReduce.java
// == MinimalMapReduce The simplest possible MapReduce driver, which uses the defaults
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv MinimalMapReduce
public class MinimalMapReduce extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    Job job = new Job(getConf());
    job.setJarByClass(getClass());
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MinimalMapReduce(), args);
    System.exit(exitCode);
  }
}
// ^^ MinimalMapReduce

//=*=*=*=*
//./ch07/src/main/java/MinimalMapReduceWithDefaults.java
// cc MinimalMapReduceWithDefaults A minimal MapReduce driver, with the defaults explicitly set
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//vv MinimalMapReduceWithDefaults
public class MinimalMapReduceWithDefaults extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }
    
    /*[*/job.setInputFormatClass(TextInputFormat.class);
    
    job.setMapperClass(Mapper.class);
    
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setPartitionerClass(HashPartitioner.class);
    
    job.setNumReduceTasks(1);
    job.setReducerClass(Reducer.class);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    job.setOutputFormatClass(TextOutputFormat.class);/*]*/
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MinimalMapReduceWithDefaults(), args);
    System.exit(exitCode);
  }
}
// ^^ MinimalMapReduceWithDefaults

//=*=*=*=*
//./ch07/src/main/java/NonSplittableTextInputFormat.java
// == NonSplittableTextInputFormat
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class NonSplittableTextInputFormat extends TextInputFormat {
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }
}

//=*=*=*=*
//./ch07/src/main/java/PartitionByStationUsingMultipleOutputs.java
// cc PartitionByStationUsingMultipleOutputs Partitions whole dataset into files named by the station ID using MultipleOutputs
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//vv PartitionByStationUsingMultipleOutputs
public class PartitionByStationUsingMultipleOutputs extends Configured
  implements Tool {
  
  static class StationMapper
    extends Mapper<LongWritable, Text, Text, Text> {
  
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      parser.parse(value);
      context.write(new Text(parser.getStationId()), value);
    }
  }
  
  /*[*/static class MultipleOutputsReducer
      extends Reducer<Text, Text, NullWritable, Text> {
    
    private MultipleOutputs<NullWritable, Text> multipleOutputs;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text value : values) {
        multipleOutputs.write(NullWritable.get(), value, key.toString());
      }
    }
    
    @Override
    protected void cleanup(Context context)
        throws IOException, InterruptedException {
      multipleOutputs.close();
    }
  }/*]*/

  @Override
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }
    
    job.setMapperClass(StationMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setReducerClass(MultipleOutputsReducer.class);
    job.setOutputKeyClass(NullWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new PartitionByStationUsingMultipleOutputs(),
        args);
    System.exit(exitCode);
  }
}
//^^ PartitionByStationUsingMultipleOutputs
//=*=*=*=*
//./ch07/src/main/java/PartitionByStationYearUsingMultipleOutputs.java
// == PartitionByStationYearUsingMultipleOutputs
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartitionByStationYearUsingMultipleOutputs extends Configured
  implements Tool {
  
  static class StationMapper
    extends Mapper<LongWritable, Text, Text, Text> {
  
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      parser.parse(value);
      context.write(new Text(parser.getStationId()), value);
    }
  }
  
  static class MultipleOutputsReducer
    extends Reducer<Text, Text, NullWritable, Text> {
    
    private MultipleOutputs<NullWritable, Text> multipleOutputs;
    private NcdcRecordParser parser = new NcdcRecordParser();

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
    }

// vv PartitionByStationYearUsingMultipleOutputs
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      for (Text value : values) {
        parser.parse(value);
        String basePath = String.format("%s/%s/part",
            parser.getStationId(), parser.getYear());
        multipleOutputs.write(NullWritable.get(), value, basePath);
      }
    }
// ^^ PartitionByStationYearUsingMultipleOutputs
    
    @Override
    protected void cleanup(Context context)
        throws IOException, InterruptedException {
      multipleOutputs.close();
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }
    
    job.setMapperClass(StationMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setReducerClass(MultipleOutputsReducer.class);
    job.setOutputKeyClass(NullWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new PartitionByStationYearUsingMultipleOutputs(),
        args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch07/src/main/java/SmallFilesToSequenceFileConverter.java
// cc SmallFilesToSequenceFileConverter A MapReduce program for packaging a collection of small files as a single SequenceFile
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//vv SmallFilesToSequenceFileConverter
public class SmallFilesToSequenceFileConverter extends Configured
    implements Tool {
  
  static class SequenceFileMapper
      extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    
    private Text filenameKey;
    
    @Override
    protected void setup(Context context) throws IOException,
        InterruptedException {
      InputSplit split = context.getInputSplit();
      Path path = ((FileSplit) split).getPath();
      filenameKey = new Text(path.toString());
    }
    
    @Override
    protected void map(NullWritable key, BytesWritable value, Context context)
        throws IOException, InterruptedException {
      context.write(filenameKey, value);
    }
    
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }
    
    job.setInputFormatClass(WholeFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);

    job.setMapperClass(SequenceFileMapper.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SmallFilesToSequenceFileConverter(), args);
    System.exit(exitCode);
  }
}
// ^^ SmallFilesToSequenceFileConverter
//=*=*=*=*
//./ch07/src/main/java/StationPartitioner.java
// == StationPartitioner
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//vv StationPartitioner
public class StationPartitioner extends Partitioner<LongWritable, Text> {
  
  private NcdcRecordParser parser = new NcdcRecordParser();
  
  @Override
  public int getPartition(LongWritable key, Text value, int numPartitions) {
    parser.parse(value);
    return getPartition(parser.getStationId());
  }

  private int getPartition(String stationId) {
    /*...*/
// ^^ StationPartitioner
    return 0;
// vv StationPartitioner
  }

}
//^^ StationPartitioner
//=*=*=*=*
//./ch07/src/main/java/WholeFileInputFormat.java
// cc WholeFileInputFormat An InputFormat for reading a whole file as a record
import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;

//vv WholeFileInputFormat
public class WholeFileInputFormat
    extends FileInputFormat<NullWritable, BytesWritable> {
  
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    return false;
  }

  @Override
  public RecordReader<NullWritable, BytesWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    WholeFileRecordReader reader = new WholeFileRecordReader();
    reader.initialize(split, context);
    return reader;
  }
}
//^^ WholeFileInputFormat

//=*=*=*=*
//./ch07/src/main/java/WholeFileRecordReader.java
// cc WholeFileRecordReader The RecordReader used by WholeFileInputFormat for reading a whole file as a record
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//vv WholeFileRecordReader
class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {
  
  private FileSplit fileSplit;
  private Configuration conf;
  private BytesWritable value = new BytesWritable();
  private boolean processed = false;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    this.fileSplit = (FileSplit) split;
    this.conf = context.getConfiguration();
  }
  
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!processed) {
      byte[] contents = new byte[(int) fileSplit.getLength()];
      Path file = fileSplit.getPath();
      FileSystem fs = file.getFileSystem(conf);
      FSDataInputStream in = null;
      try {
        in = fs.open(file);
        IOUtils.readFully(in, contents, 0, contents.length);
        value.set(contents, 0, contents.length);
      } finally {
        IOUtils.closeStream(in);
      }
      processed = true;
      return true;
    }
    return false;
  }
  
  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public BytesWritable getCurrentValue() throws IOException,
      InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException {
    return processed ? 1.0f : 0.0f;
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }
}
//^^ WholeFileRecordReader

//=*=*=*=*
//./ch07/src/main/java/oldapi/MaxTemperatureWithMultipleInputs.java
package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.*;

public class MaxTemperatureWithMultipleInputs extends Configured
  implements Tool {
  
  static class MetOfficeMaxTemperatureMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, IntWritable> {
  
    private MetOfficeRecordParser parser = new MetOfficeRecordParser();
    
    public void map(LongWritable key, Text value,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        output.collect(new Text(parser.getYear()),
            new IntWritable(parser.getAirTemperature()));
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      JobBuilder.printUsage(this, "<ncdc input> <metoffice input> <output>");
      return -1;
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("Max temperature with multiple input formats");
    
    Path ncdcInputPath = new Path(args[0]);
    Path metOfficeInputPath = new Path(args[1]);
    Path outputPath = new Path(args[2]);
    
    MultipleInputs.addInputPath(conf, ncdcInputPath,
        TextInputFormat.class, MaxTemperatureMapper.class);
    MultipleInputs.addInputPath(conf, metOfficeInputPath,
        TextInputFormat.class, MetOfficeMaxTemperatureMapper.class);
    FileOutputFormat.setOutputPath(conf, outputPath);
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    
    conf.setCombinerClass(MaxTemperatureReducer.class);
    conf.setReducerClass(MaxTemperatureReducer.class);

    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureWithMultipleInputs(),
        args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch07/src/main/java/oldapi/MinimalMapReduce.java
package oldapi;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class MinimalMapReduce extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MinimalMapReduce(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch07/src/main/java/oldapi/MinimalMapReduceWithDefaults.java
package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;

public class MinimalMapReduceWithDefaults extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws IOException {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }
    
    /*[*/conf.setInputFormat(TextInputFormat.class);
    
    conf.setNumMapTasks(1);
    conf.setMapperClass(IdentityMapper.class);
    conf.setMapRunnerClass(MapRunner.class);
    
    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(Text.class);
    
    conf.setPartitionerClass(HashPartitioner.class);
    
    conf.setNumReduceTasks(1);
    conf.setReducerClass(IdentityReducer.class);

    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setOutputFormat(TextOutputFormat.class);/*]*/
    
    JobClient.runJob(conf);
    return 0;
  }
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MinimalMapReduceWithDefaults(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch07/src/main/java/oldapi/NonSplittableTextInputFormat.java
package oldapi;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.TextInputFormat;

public class NonSplittableTextInputFormat extends TextInputFormat {
  @Override
  protected boolean isSplitable(FileSystem fs, Path file) {
    return false;
  }
}

//=*=*=*=*
//./ch07/src/main/java/oldapi/PartitionByStationUsingMultipleOutputFormat.java
package oldapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.*;

public class PartitionByStationUsingMultipleOutputFormat extends Configured
  implements Tool {
  
  static class StationMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text> {
  
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      
      parser.parse(value);
      output.collect(new Text(parser.getStationId()), value);
    }
  }
  
  static class StationReducer extends MapReduceBase
    implements Reducer<Text, Text, NullWritable, Text> {

    @Override
    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<NullWritable, Text> output, Reporter reporter)
        throws IOException {
      while (values.hasNext()) {
        output.collect(NullWritable.get(), values.next());
      }
    }
  }
  
  /*[*/static class StationNameMultipleTextOutputFormat
    extends MultipleTextOutputFormat<NullWritable, Text> {
    
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    protected String generateFileNameForKeyValue(NullWritable key, Text value,
        String name) {
      parser.parse(value);
      return parser.getStationId();
    }
  }/*]*/

  @Override
  public int run(String[] args) throws IOException {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }
    
    conf.setMapperClass(StationMapper.class);
    conf.setMapOutputKeyClass(Text.class);
    conf.setReducerClass(StationReducer.class);
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputFormat(StationNameMultipleTextOutputFormat.class);

    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(
        new PartitionByStationUsingMultipleOutputFormat(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch07/src/main/java/oldapi/PartitionByStationUsingMultipleOutputs.java
package oldapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;

public class PartitionByStationUsingMultipleOutputs extends Configured
  implements Tool {
  
  static class StationMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text> {
  
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      
      parser.parse(value);
      output.collect(new Text(parser.getStationId()), value);
    }
  }
  
  static class MultipleOutputsReducer extends MapReduceBase
    implements Reducer<Text, Text, NullWritable, Text> {
    
    private MultipleOutputs multipleOutputs;

    @Override
    public void configure(JobConf conf) {
      multipleOutputs = new MultipleOutputs(conf);
    }

    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<NullWritable, Text> output, Reporter reporter)
        throws IOException {
      
      OutputCollector collector = multipleOutputs.getCollector("station",
          key.toString().replace("-", ""), reporter);
      while (values.hasNext()) {
        collector.collect(NullWritable.get(), values.next());        
      }
    }
    
    @Override
    public void close() throws IOException {
      multipleOutputs.close();
    }
  }

  @Override
  public int run(String[] args) throws IOException {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }
    
    conf.setMapperClass(StationMapper.class);
    conf.setMapOutputKeyClass(Text.class);
    conf.setReducerClass(MultipleOutputsReducer.class);
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputFormat(NullOutputFormat.class); // suppress empty part file
    
    MultipleOutputs.addMultiNamedOutput(conf, "station", TextOutputFormat.class,
        NullWritable.class, Text.class);

    JobClient.runJob(conf);
    return 0;
  }
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new PartitionByStationUsingMultipleOutputs(),
        args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch07/src/main/java/oldapi/PartitionByStationYearUsingMultipleOutputFormat.java
package oldapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.*;

public class PartitionByStationYearUsingMultipleOutputFormat extends Configured
  implements Tool {
  
  static class StationMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text> {
  
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      
      parser.parse(value);
      output.collect(new Text(parser.getStationId()), value);
    }
  }
  
  static class StationReducer extends MapReduceBase
    implements Reducer<Text, Text, NullWritable, Text> {

    @Override
    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<NullWritable, Text> output, Reporter reporter)
        throws IOException {
      while (values.hasNext()) {
        output.collect(NullWritable.get(), values.next());
      }
    }
  }

  static class StationNameMultipleTextOutputFormat
    extends MultipleTextOutputFormat<NullWritable, Text> {
    
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    protected String generateFileNameForKeyValue(NullWritable key, Text value,
        String name) {
      parser.parse(value);
      return parser.getStationId() + "/" + parser.getYear();
    }
  }

  @Override
  public int run(String[] args) throws IOException {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }
    
    conf.setMapperClass(StationMapper.class);
    conf.setMapOutputKeyClass(Text.class);
    conf.setReducerClass(StationReducer.class);
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputFormat(StationNameMultipleTextOutputFormat.class);

    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(
        new PartitionByStationYearUsingMultipleOutputFormat(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch07/src/main/java/oldapi/SmallFilesToSequenceFileConverter.java
package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.*;

public class SmallFilesToSequenceFileConverter extends Configured
  implements Tool {
  
  static class SequenceFileMapper extends MapReduceBase
      implements Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    
    private JobConf conf;
    
    @Override
    public void configure(JobConf conf) {
      this.conf = conf;
    }

    @Override
    public void map(NullWritable key, BytesWritable value,
        OutputCollector<Text, BytesWritable> output, Reporter reporter)
        throws IOException {
      
      String filename = conf.get("map.input.file");
      output.collect(new Text(filename), value);
    }
    
  }

  @Override
  public int run(String[] args) throws IOException {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }
    
    conf.setInputFormat(WholeFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(BytesWritable.class);

    conf.setMapperClass(SequenceFileMapper.class);
    conf.setReducerClass(IdentityReducer.class);

    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SmallFilesToSequenceFileConverter(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch07/src/main/java/oldapi/StationPartitioner.java
package oldapi;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class StationPartitioner implements Partitioner<LongWritable, Text> {
  
  private NcdcRecordParser parser = new NcdcRecordParser();
  
  @Override
  public int getPartition(LongWritable key, Text value, int numPartitions) {
    parser.parse(value);
    return getPartition(parser.getStationId());
  }

  private int getPartition(String stationId) {
    return 0;
  }

  @Override
  public void configure(JobConf conf) { }
}

//=*=*=*=*
//./ch07/src/main/java/oldapi/WholeFileInputFormat.java
package oldapi;

import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WholeFileInputFormat
    extends FileInputFormat<NullWritable, BytesWritable> {
  
  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return false;
  }

  @Override
  public RecordReader<NullWritable, BytesWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {

    return new WholeFileRecordReader((FileSplit) split, job);
  }
}

//=*=*=*=*
//./ch07/src/main/java/oldapi/WholeFileRecordReader.java
package oldapi;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

class WholeFileRecordReader implements RecordReader<NullWritable, BytesWritable> {
  
  private FileSplit fileSplit;
  private Configuration conf;
  private boolean processed = false;
  
  public WholeFileRecordReader(FileSplit fileSplit, Configuration conf)
      throws IOException {
    this.fileSplit = fileSplit;
    this.conf = conf;
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public BytesWritable createValue() {
    return new BytesWritable();
  }

  @Override
  public long getPos() throws IOException {
    return processed ? fileSplit.getLength() : 0;
  }

  @Override
  public float getProgress() throws IOException {
    return processed ? 1.0f : 0.0f;
  }

  @Override
  public boolean next(NullWritable key, BytesWritable value) throws IOException {
    if (!processed) {
      byte[] contents = new byte[(int) fileSplit.getLength()];
      Path file = fileSplit.getPath();
      FileSystem fs = file.getFileSystem(conf);
      FSDataInputStream in = null;
      try {
        in = fs.open(file);
        IOUtils.readFully(in, contents, 0, contents.length);
        value.set(contents, 0, contents.length);
      } finally {
        IOUtils.closeStream(in);
      }
      processed = true;
      return true;
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }
}

//=*=*=*=*
//./ch07/src/test/java/TextInputFormatsTest.java
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.junit.*;


public class TextInputFormatsTest {
  
  private static final String BASE_PATH = "/tmp/" +
    TextInputFormatsTest.class.getSimpleName();
  
  private JobConf conf;
  private FileSystem fs;
  
  @Before
  public void setUp() throws Exception {
    conf = new JobConf();
    fs = FileSystem.get(conf);
  }
  
  @After
  public void tearDown() throws Exception {
    fs.delete(new Path(BASE_PATH), true);
  }

  @Test
  public void text() throws Exception {
    String input =
      "On the top of the Crumpetty Tree\n" +
      "The Quangle Wangle sat,\n" +
      "But his face you could not see,\n" +
      "On account of his Beaver Hat.";
    
    writeInput(input);
    
    TextInputFormat format = new TextInputFormat();
    format.configure(conf);
    InputSplit[] splits = format.getSplits(conf, 1);
    RecordReader<LongWritable, Text> recordReader =
      format.getRecordReader(splits[0], conf, Reporter.NULL);
    checkNextLine(recordReader, 0, "On the top of the Crumpetty Tree");
    checkNextLine(recordReader, 33, "The Quangle Wangle sat,");
    checkNextLine(recordReader, 57, "But his face you could not see,");
    checkNextLine(recordReader, 89, "On account of his Beaver Hat.");
  }

  @Test
  public void keyValue() throws Exception {
    String input =
      "line1\tOn the top of the Crumpetty Tree\n" +
      "line2\tThe Quangle Wangle sat,\n" +
      "line3\tBut his face you could not see,\n" +
      "line4\tOn account of his Beaver Hat.";
    
    writeInput(input);
    
    KeyValueTextInputFormat format = new KeyValueTextInputFormat();
    format.configure(conf);
    InputSplit[] splits = format.getSplits(conf, 1);
    RecordReader<Text, Text> recordReader =
      format.getRecordReader(splits[0], conf, Reporter.NULL);
    checkNextLine(recordReader, "line1", "On the top of the Crumpetty Tree");
    checkNextLine(recordReader, "line2", "The Quangle Wangle sat,");
    checkNextLine(recordReader, "line3", "But his face you could not see,");
    checkNextLine(recordReader, "line4", "On account of his Beaver Hat.");
  }
  
  @Test
  public void nLine() throws Exception {
    String input =
      "On the top of the Crumpetty Tree\n" +
      "The Quangle Wangle sat,\n" +
      "But his face you could not see,\n" +
      "On account of his Beaver Hat.";
    
    writeInput(input);
    
    conf.setInt("mapred.line.input.format.linespermap", 2);
    NLineInputFormat format = new NLineInputFormat();
    format.configure(conf);
    InputSplit[] splits = format.getSplits(conf, 2);
    RecordReader<LongWritable, Text> recordReader =
      format.getRecordReader(splits[0], conf, Reporter.NULL);
    checkNextLine(recordReader, 0, "On the top of the Crumpetty Tree");
    checkNextLine(recordReader, 33, "The Quangle Wangle sat,");
    recordReader = format.getRecordReader(splits[1], conf, Reporter.NULL);
    checkNextLine(recordReader, 57, "But his face you could not see,");
    checkNextLine(recordReader, 89, "On account of his Beaver Hat.");
  }
  
  private void writeInput(String input) throws IOException {
    OutputStream out = fs.create(new Path(BASE_PATH, "input"));
    out.write(input.getBytes());
    out.close();
    
    FileInputFormat.setInputPaths(conf, BASE_PATH);
  }
  
  private void checkNextLine(RecordReader<LongWritable, Text> recordReader,
      long expectedKey, String expectedValue) throws IOException {
    LongWritable key = new LongWritable();
    Text value = new Text();
    assertThat(expectedValue, recordReader.next(key, value), is(true));
    assertThat(key.get(), is(expectedKey));
    assertThat(value.toString(), is(expectedValue));
  }
  
  private void checkNextLine(RecordReader<Text, Text> recordReader,
      String expectedKey, String expectedValue) throws IOException {
    Text key = new Text();
    Text value = new Text();
    assertThat(expectedValue, recordReader.next(key, value), is(true));
    assertThat(key.toString(), is(expectedKey));
    assertThat(value.toString(), is(expectedValue));
  }
}

//=*=*=*=*
//./ch08/src/main/examples/LookupRecordByTemperature.java.input.txt
hadoop jar hadoop-examples.jar LookupRecordByTemperature output-hashmapsort -100
//=*=*=*=*
//./ch08/src/main/examples/LookupRecordByTemperature.java.output.txt
357460-99999    1956
//=*=*=*=*
//./ch08/src/main/examples/LookupRecordsByTemperature.java.input.txt
hadoop jar hadoop-examples.jar LookupRecordsByTemperature output-hashmapsort -100 \
  2> /dev/null | wc -l
//=*=*=*=*
//./ch08/src/main/examples/LookupRecordsByTemperature.java.output.txt
1489272
//=*=*=*=*
//./ch08/src/main/examples/MaxTemperatureByStationNameUsingDistributedCacheFile.java.input.txt
hadoop jar hadoop-examples.jar MaxTemperatureByStationNameUsingDistributedCacheFile \
  -files input/ncdc/metadata/stations-fixed-width.txt input/ncdc/all output
//=*=*=*=*
//./ch08/src/main/examples/MaxTemperatureWithCounters.java.input.txt
hadoop jar hadoop-examples.jar MaxTemperatureWithCounters input/ncdc/all output-counters

//=*=*=*=*
//./ch08/src/main/examples/MissingTemperatureFields.java.input.txt
hadoop jar hadoop-examples.jar MissingTemperatureFields job_200904200610_0003
//=*=*=*=*
//./ch08/src/main/examples/SortByTemperatureUsingHashPartitioner.java.input.txt
hadoop jar hadoop-examples.jar SortByTemperatureUsingHashPartitioner \
  -D mapred.reduce.tasks=30 input/ncdc/all-seq output-hashsort

//=*=*=*=*
//./ch08/src/main/examples/SortByTemperatureUsingTotalOrderPartitioner.java.input.txt
hadoop jar hadoop-examples.jar SortByTemperatureUsingTotalOrderPartitioner \
  -D mapred.reduce.tasks=30 input/ncdc/all-seq output-totalsort
//=*=*=*=*
//./ch08/src/main/examples/SortDataPreprocessor.java.input.txt
hadoop jar hadoop-examples.jar SortDataPreprocessor input/ncdc/all \
  input/ncdc/all-seq
//=*=*=*=*
//./ch08/src/main/java/JoinRecordMapper.java
// cc JoinRecordMapper Mapper for tagging weather records for a reduce-side join
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//vv JoinRecordMapper
public class JoinRecordMapper
    extends Mapper<LongWritable, Text, TextPair, Text> {
  private NcdcRecordParser parser = new NcdcRecordParser();
  
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    parser.parse(value);
    context.write(new TextPair(parser.getStationId(), "1"), value);
  }

}
//^^ JoinRecordMapper
//=*=*=*=*
//./ch08/src/main/java/JoinRecordWithStationName.java
// cc JoinRecordWithStationName Application to join weather records with station names
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

// vv JoinRecordWithStationName
public class JoinRecordWithStationName extends Configured implements Tool {
  
  public static class KeyPartitioner extends Partitioner<TextPair, Text> {
    @Override
    public int getPartition(/*[*/TextPair key/*]*/, Text value, int numPartitions) {
      return (/*[*/key.getFirst().hashCode()/*]*/ & Integer.MAX_VALUE) % numPartitions;
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      JobBuilder.printUsage(this, "<ncdc input> <station input> <output>");
      return -1;
    }
    
    Job job = new Job(getConf(), "Join weather records with station names");
    job.setJarByClass(getClass());
    
    Path ncdcInputPath = new Path(args[0]);
    Path stationInputPath = new Path(args[1]);
    Path outputPath = new Path(args[2]);
    
    MultipleInputs.addInputPath(job, ncdcInputPath,
        TextInputFormat.class, JoinRecordMapper.class);
    MultipleInputs.addInputPath(job, stationInputPath,
        TextInputFormat.class, JoinStationMapper.class);
    FileOutputFormat.setOutputPath(job, outputPath);
    
    /*[*/job.setPartitionerClass(KeyPartitioner.class);
    job.setGroupingComparatorClass(TextPair.FirstComparator.class);/*]*/
    
    job.setMapOutputKeyClass(TextPair.class);
    
    job.setReducerClass(JoinReducer.class);

    job.setOutputKeyClass(Text.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new JoinRecordWithStationName(), args);
    System.exit(exitCode);
  }
}
// ^^ JoinRecordWithStationName

//=*=*=*=*
//./ch08/src/main/java/JoinReducer.java
// cc JoinReducer Reducer for joining tagged station records with tagged weather records
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// vv JoinReducer
public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

  @Override
  protected void reduce(TextPair key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    Iterator<Text> iter = values.iterator();
    Text stationName = new Text(iter.next());
    while (iter.hasNext()) {
      Text record = iter.next();
      Text outValue = new Text(stationName.toString() + "\t" + record.toString());
      context.write(key.getFirst(), outValue);
    }
  }
}
// ^^ JoinReducer
//=*=*=*=*
//./ch08/src/main/java/JoinStationMapper.java
// cc JoinStationMapper Mapper for tagging station records for a reduce-side join
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

// vv JoinStationMapper
public class JoinStationMapper
    extends Mapper<LongWritable, Text, TextPair, Text> {
  private NcdcStationMetadataParser parser = new NcdcStationMetadataParser();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    if (parser.parse(value)) {
      context.write(new TextPair(parser.getStationId(), "0"),
          new Text(parser.getStationName()));
    }
  }
}
// ^^ JoinStationMapper
//=*=*=*=*
//./ch08/src/main/java/LookupRecordByTemperature.java
// cc LookupRecordByTemperature Retrieve the first entry with a given key from a collection of MapFiles
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.*;

// vv LookupRecordByTemperature
public class LookupRecordByTemperature extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      JobBuilder.printUsage(this, "<path> <key>");
      return -1;
    }
    Path path = new Path(args[0]);
    IntWritable key = new IntWritable(Integer.parseInt(args[1]));
    
    Reader[] readers = /*[*/MapFileOutputFormat.getReaders(path, getConf())/*]*/;
    Partitioner<IntWritable, Text> partitioner =
      new HashPartitioner<IntWritable, Text>();
    Text val = new Text();
    Writable entry =
      /*[*/MapFileOutputFormat.getEntry(readers, partitioner, key, val)/*]*/;
    if (entry == null) {
      System.err.println("Key not found: " + key);
      return -1;
    }
    NcdcRecordParser parser = new NcdcRecordParser();
    parser.parse(val.toString());
    System.out.printf("%s\t%s\n", parser.getStationId(), parser.getYear());
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new LookupRecordByTemperature(), args);
    System.exit(exitCode);
  }
}
// ^^ LookupRecordByTemperature

//=*=*=*=*
//./ch08/src/main/java/LookupRecordsByTemperature.java
// cc LookupRecordsByTemperature Retrieve all entries with a given key from a collection of MapFiles
// == LookupRecordsByTemperature-ReaderFragment
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.*;

// vv LookupRecordsByTemperature
public class LookupRecordsByTemperature extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      JobBuilder.printUsage(this, "<path> <key>");
      return -1;
    }
    Path path = new Path(args[0]);
    IntWritable key = new IntWritable(Integer.parseInt(args[1]));
    
    Reader[] readers = MapFileOutputFormat.getReaders(path, getConf());
    Partitioner<IntWritable, Text> partitioner =
      new HashPartitioner<IntWritable, Text>();
    Text val = new Text();
    
    // vv LookupRecordsByTemperature-ReaderFragment
    Reader reader = readers[partitioner.getPartition(key, val, readers.length)];
    // ^^ LookupRecordsByTemperature-ReaderFragment
    Writable entry = reader.get(key, val);
    if (entry == null) {
      System.err.println("Key not found: " + key);
      return -1;
    }
    NcdcRecordParser parser = new NcdcRecordParser();
    IntWritable nextKey = new IntWritable();
    do {
      parser.parse(val.toString());
      System.out.printf("%s\t%s\n", parser.getStationId(), parser.getYear());
    } while(reader.next(nextKey, val) && key.equals(nextKey));
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new LookupRecordsByTemperature(), args);
    System.exit(exitCode);
  }
}
// ^^ LookupRecordsByTemperature

//=*=*=*=*
//./ch08/src/main/java/MaxTemperatureByStationNameUsingDistributedCacheFile.java
// cc MaxTemperatureByStationNameUsingDistributedCacheFile Application to find the maximum temperature by station, showing station names from a lookup table passed as a distributed cache file
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv MaxTemperatureByStationNameUsingDistributedCacheFile
public class MaxTemperatureByStationNameUsingDistributedCacheFile
  extends Configured implements Tool {
  
  static class StationTemperatureMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

    private NcdcRecordParser parser = new NcdcRecordParser();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        context.write(new Text(parser.getStationId()),
            new IntWritable(parser.getAirTemperature()));
      }
    }
  }
  
  static class MaxTemperatureReducerWithStationLookup
    extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    /*[*/private NcdcStationMetadata metadata;/*]*/
    
    /*[*/@Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      metadata = new NcdcStationMetadata();
      metadata.initialize(new File("stations-fixed-width.txt"));
    }/*]*/

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
      
      /*[*/String stationName = metadata.getStationName(key.toString());/*]*/
      
      int maxValue = Integer.MIN_VALUE;
      for (IntWritable value : values) {
        maxValue = Math.max(maxValue, value.get());
      }
      context.write(new Text(/*[*/stationName/*]*/), new IntWritable(maxValue));
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(StationTemperatureMapper.class);
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducerWithStationLookup.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(
        new MaxTemperatureByStationNameUsingDistributedCacheFile(), args);
    System.exit(exitCode);
  }
}
// ^^ MaxTemperatureByStationNameUsingDistributedCacheFile

//=*=*=*=*
//./ch08/src/main/java/MaxTemperatureByStationNameUsingDistributedCacheFileApi.java
// == MaxTemperatureByStationNameUsingDistributedCacheFileApi
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MaxTemperatureByStationNameUsingDistributedCacheFileApi
  extends Configured implements Tool {
  
  static class StationTemperatureMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

    private NcdcRecordParser parser = new NcdcRecordParser();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        context.write(new Text(parser.getStationId()),
            new IntWritable(parser.getAirTemperature()));
      }
    }
  }
  
  static class MaxTemperatureReducerWithStationLookup
    extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private NcdcStationMetadata metadata;
    
 // vv MaxTemperatureByStationNameUsingDistributedCacheFileApi
    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      metadata = new NcdcStationMetadata();
      Path[] localPaths = context.getLocalCacheFiles();
      if (localPaths.length == 0) {
        throw new FileNotFoundException("Distributed cache file not found.");
      }
      File localFile = new File(localPaths[0].toString());
      metadata.initialize(localFile);
    }
 // ^^ MaxTemperatureByStationNameUsingDistributedCacheFileApi

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
      
      String stationName = metadata.getStationName(key.toString());
      
      int maxValue = Integer.MIN_VALUE;
      for (IntWritable value : values) {
        maxValue = Math.max(maxValue, value.get());
      }
      context.write(new Text(stationName), new IntWritable(maxValue));
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(StationTemperatureMapper.class);
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducerWithStationLookup.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(
        new MaxTemperatureByStationNameUsingDistributedCacheFileApi(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch08/src/main/java/MaxTemperatureUsingSecondarySort.java
// cc MaxTemperatureUsingSecondarySort Application to find the maximum temperature by sorting temperatures in the key
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv MaxTemperatureUsingSecondarySort
public class MaxTemperatureUsingSecondarySort
  extends Configured implements Tool {
  
  static class MaxTemperatureMapper
    extends Mapper<LongWritable, Text, IntPair, NullWritable> {
  
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    @Override
    protected void map(LongWritable key, Text value,
        Context context) throws IOException, InterruptedException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        /*[*/context.write(new IntPair(parser.getYearInt(),
            parser.getAirTemperature()), NullWritable.get());/*]*/
      }
    }
  }
  
  static class MaxTemperatureReducer
    extends Reducer<IntPair, NullWritable, IntPair, NullWritable> {
  
    @Override
    protected void reduce(IntPair key, Iterable<NullWritable> values,
        Context context) throws IOException, InterruptedException {
      
      /*[*/context.write(key, NullWritable.get());/*]*/
    }
  }
  
  public static class FirstPartitioner
    extends Partitioner<IntPair, NullWritable> {

    @Override
    public int getPartition(IntPair key, NullWritable value, int numPartitions) {
      // multiply by 127 to perform some mixing
      return Math.abs(key.getFirst() * 127) % numPartitions;
    }
  }
  
  public static class KeyComparator extends WritableComparator {
    protected KeyComparator() {
      super(IntPair.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      IntPair ip1 = (IntPair) w1;
      IntPair ip2 = (IntPair) w2;
      int cmp = IntPair.compare(ip1.getFirst(), ip2.getFirst());
      if (cmp != 0) {
        return cmp;
      }
      return -IntPair.compare(ip1.getSecond(), ip2.getSecond()); //reverse
    }
  }
  
  public static class GroupComparator extends WritableComparator {
    protected GroupComparator() {
      super(IntPair.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      IntPair ip1 = (IntPair) w1;
      IntPair ip2 = (IntPair) w2;
      return IntPair.compare(ip1.getFirst(), ip2.getFirst());
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }
    
    job.setMapperClass(MaxTemperatureMapper.class);
    /*[*/job.setPartitionerClass(FirstPartitioner.class);/*]*/
    /*[*/job.setSortComparatorClass(KeyComparator.class);/*]*/
    /*[*/job.setGroupingComparatorClass(GroupComparator.class);/*]*/
    job.setReducerClass(MaxTemperatureReducer.class);
    job.setOutputKeyClass(IntPair.class);
    job.setOutputValueClass(NullWritable.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureUsingSecondarySort(), args);
    System.exit(exitCode);
  }
}
// ^^ MaxTemperatureUsingSecondarySort

//=*=*=*=*
//./ch08/src/main/java/MaxTemperatureWithCounters.java
// cc MaxTemperatureWithCounters Application to run the maximum temperature job, including counting missing and malformed fields and quality codes
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv MaxTemperatureWithCounters
public class MaxTemperatureWithCounters extends Configured implements Tool {
  
  enum Temperature {
    MISSING,
    MALFORMED
  }
  
  static class MaxTemperatureMapperWithCounters
    extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private NcdcRecordParser parser = new NcdcRecordParser();
  
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        int airTemperature = parser.getAirTemperature();
        context.write(new Text(parser.getYear()),
            new IntWritable(airTemperature));
      } else if (parser.isMalformedTemperature()) {
        System.err.println("Ignoring possibly corrupt input: " + value);
        context.getCounter(Temperature.MALFORMED).increment(1);
      } else if (parser.isMissingTemperature()) {
        context.getCounter(Temperature.MISSING).increment(1);
      }
      
      // dynamic counter
      context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MaxTemperatureMapperWithCounters.class);
    job.setCombinerClass(MaxTemperatureReducer.class);
    job.setReducerClass(MaxTemperatureReducer.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureWithCounters(), args);
    System.exit(exitCode);
  }
}
// ^^ MaxTemperatureWithCounters

//=*=*=*=*
//./ch08/src/main/java/MissingTemperatureFields.java
// cc MissingTemperatureFields Application to calculate the proportion of records with missing temperature fields
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class MissingTemperatureFields extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      JobBuilder.printUsage(this, "<job ID>");
      return -1;
    }
    String jobID = args[0];
    JobClient jobClient = new JobClient(new JobConf(getConf()));
    RunningJob job = jobClient.getJob(JobID.forName(jobID));
    if (job == null) {
      System.err.printf("No job with ID %s found.\n", jobID);
      return -1;
    }
    if (!job.isComplete()) {
      System.err.printf("Job %s is not complete.\n", jobID);
      return -1;
    }
    
    Counters counters = job.getCounters();
    long missing = counters.getCounter(
        MaxTemperatureWithCounters.Temperature.MISSING);
    
    long total = counters.getCounter(Task.Counter.MAP_INPUT_RECORDS);

    System.out.printf("Records with missing temperature fields: %.2f%%\n",
        100.0 * missing / total);
    return 0;
  }
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MissingTemperatureFields(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch08/src/main/java/NewMissingTemperatureFields.java
// == NewMissingTemperatureFields Application to calculate the proportion of records with missing temperature fields

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.util.*;

public class NewMissingTemperatureFields extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      JobBuilder.printUsage(this, "<job ID>");
      return -1;
    }
    String jobID = args[0];
    // vv NewMissingTemperatureFields
    Cluster cluster = new Cluster(getConf());
    Job job = cluster.getJob(JobID.forName(jobID));
    // ^^ NewMissingTemperatureFields
    if (job == null) {
      System.err.printf("No job with ID %s found.\n", jobID);
      return -1;
    }
    if (!job.isComplete()) {
      System.err.printf("Job %s is not complete.\n", jobID);
      return -1;
    }
    
    // vv NewMissingTemperatureFields
    Counters counters = job.getCounters();
    long missing = counters.findCounter(
        MaxTemperatureWithCounters.Temperature.MISSING).getValue();
    long total = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
    // ^^ NewMissingTemperatureFields

    System.out.printf("Records with missing temperature fields: %.2f%%\n",
        100.0 * missing / total);
    return 0;
  }
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new NewMissingTemperatureFields(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch08/src/main/java/SortByTemperatureToMapFile.java
// cc SortByTemperatureToMapFile A MapReduce program for sorting a SequenceFile and producing MapFiles as output
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv SortByTemperatureToMapFile
public class SortByTemperatureToMapFile extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }
    
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    /*[*/job.setOutputFormatClass(MapFileOutputFormat.class);/*]*/
    SequenceFileOutputFormat.setCompressOutput(job, true);
    SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(job,
        CompressionType.BLOCK);

    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SortByTemperatureToMapFile(), args);
    System.exit(exitCode);
  }
}
// ^^ SortByTemperatureToMapFile

//=*=*=*=*
//./ch08/src/main/java/SortByTemperatureUsingHashPartitioner.java
// cc SortByTemperatureUsingHashPartitioner A MapReduce program for sorting a SequenceFile with IntWritable keys using the default HashPartitioner
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv SortByTemperatureUsingHashPartitioner
public class SortByTemperatureUsingHashPartitioner extends Configured
  implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }
    
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setCompressOutput(job, true);
    SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(job,
        CompressionType.BLOCK);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SortByTemperatureUsingHashPartitioner(),
        args);
    System.exit(exitCode);
  }
}
// ^^ SortByTemperatureUsingHashPartitioner

//=*=*=*=*
//./ch08/src/main/java/SortByTemperatureUsingTotalOrderPartitioner.java
// cc SortByTemperatureUsingTotalOrderPartitioner A MapReduce program for sorting a SequenceFile with IntWritable keys using the TotalOrderPartitioner to globally sort the data
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.*;

// vv SortByTemperatureUsingTotalOrderPartitioner
public class SortByTemperatureUsingTotalOrderPartitioner extends Configured
  implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }
    
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setCompressOutput(job, true);
    SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(job,
        CompressionType.BLOCK);

    job.setPartitionerClass(TotalOrderPartitioner.class);

    InputSampler.Sampler<IntWritable, Text> sampler =
      new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000, 10);
    
    InputSampler.writePartitionFile(job, sampler);

    // Add to DistributedCache
    Configuration conf = job.getConfiguration();
    String partitionFile =TotalOrderPartitioner.getPartitionFile(conf);
    URI partitionUri = new URI(partitionFile + "#" +
        TotalOrderPartitioner.DEFAULT_PATH);
    DistributedCache.addCacheFile(partitionUri, conf);
    DistributedCache.createSymlink(conf);

    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(
        new SortByTemperatureUsingTotalOrderPartitioner(), args);
    System.exit(exitCode);
  }
}
// ^^ SortByTemperatureUsingTotalOrderPartitioner

//=*=*=*=*
//./ch08/src/main/java/SortDataPreprocessor.java
// cc SortDataPreprocessor A MapReduce program for transforming the weather data into SequenceFile format
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// vv SortDataPreprocessor
public class SortDataPreprocessor extends Configured implements Tool {
  
  static class CleanerMapper
    extends Mapper<LongWritable, Text, IntWritable, Text> {
  
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        context.write(new IntWritable(parser.getAirTemperature()), value);
      }
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }

    job.setMapperClass(CleanerMapper.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setCompressOutput(job, true);
    SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(job,
        CompressionType.BLOCK);

    return job.waitForCompletion(true) ? 0 : 1;
  }
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SortDataPreprocessor(), args);
    System.exit(exitCode);
  }
}
// ^^ SortDataPreprocessor
//=*=*=*=*
//./ch08/src/main/java/TemperatureDistribution.java
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TemperatureDistribution extends Configured implements Tool {
  
  static class TemperatureCountMapper
    extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
  
    private static final LongWritable ONE = new LongWritable(1);
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        context.write(new IntWritable(parser.getAirTemperature() / 10), ONE);
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (job == null) {
      return -1;
    }
    
    job.setMapperClass(TemperatureCountMapper.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(LongSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(LongWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new TemperatureDistribution(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch08/src/main/java/oldapi/JoinRecordMapper.java
package oldapi;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class JoinRecordMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, TextPair, Text> {
  private NcdcRecordParser parser = new NcdcRecordParser();
  
  public void map(LongWritable key, Text value,
      OutputCollector<TextPair, Text> output, Reporter reporter)
      throws IOException {
    
    parser.parse(value);
    output.collect(new TextPair(parser.getStationId(), "1"), value);
  }
}

//=*=*=*=*
//./ch08/src/main/java/oldapi/JoinRecordWithStationName.java
package oldapi;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.*;

public class JoinRecordWithStationName extends Configured implements Tool {
  
  public static class KeyPartitioner implements Partitioner<TextPair, Text> {
    @Override
    public void configure(JobConf job) {}
    
    @Override
    public int getPartition(/*[*/TextPair key/*]*/, Text value, int numPartitions) {
      return (/*[*/key.getFirst().hashCode()/*]*/ & Integer.MAX_VALUE) % numPartitions;
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      JobBuilder.printUsage(this, "<ncdc input> <station input> <output>");
      return -1;
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("Join record with station name");
    
    Path ncdcInputPath = new Path(args[0]);
    Path stationInputPath = new Path(args[1]);
    Path outputPath = new Path(args[2]);
    
    MultipleInputs.addInputPath(conf, ncdcInputPath,
        TextInputFormat.class, JoinRecordMapper.class);
    MultipleInputs.addInputPath(conf, stationInputPath,
        TextInputFormat.class, JoinStationMapper.class);
    FileOutputFormat.setOutputPath(conf, outputPath);

    /*[*/conf.setPartitionerClass(KeyPartitioner.class);
    conf.setOutputValueGroupingComparator(TextPair.FirstComparator.class);/*]*/
    
    conf.setMapOutputKeyClass(TextPair.class);
    
    conf.setReducerClass(JoinReducer.class);

    conf.setOutputKeyClass(Text.class);
    
    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new JoinRecordWithStationName(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch08/src/main/java/oldapi/JoinReducer.java
package oldapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class JoinReducer extends MapReduceBase implements
    Reducer<TextPair, Text, Text, Text> {

  public void reduce(TextPair key, Iterator<Text> values,
      OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {

    Text stationName = new Text(values.next());
    while (values.hasNext()) {
      Text record = values.next();
      Text outValue = new Text(stationName.toString() + "\t" + record.toString());
      output.collect(key.getFirst(), outValue);
    }
  }
}

//=*=*=*=*
//./ch08/src/main/java/oldapi/JoinStationMapper.java
package oldapi;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class JoinStationMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, TextPair, Text> {
  private NcdcStationMetadataParser parser = new NcdcStationMetadataParser();

  public void map(LongWritable key, Text value,
      OutputCollector<TextPair, Text> output, Reporter reporter)
      throws IOException {

    if (parser.parse(value)) {
      output.collect(new TextPair(parser.getStationId(), "0"),
          new Text(parser.getStationName()));
    }
  }
}

//=*=*=*=*
//./ch08/src/main/java/oldapi/LookupRecordByTemperature.java
package oldapi;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.*;

public class LookupRecordByTemperature extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      JobBuilder.printUsage(this, "<path> <key>");
      return -1;
    }
    Path path = new Path(args[0]);
    IntWritable key = new IntWritable(Integer.parseInt(args[1]));
    FileSystem fs = path.getFileSystem(getConf());
    
    Reader[] readers = /*[*/MapFileOutputFormat.getReaders(fs, path, getConf())/*]*/;
    Partitioner<IntWritable, Text> partitioner =
      new HashPartitioner<IntWritable, Text>();
    Text val = new Text();
    Writable entry =
      /*[*/MapFileOutputFormat.getEntry(readers, partitioner, key, val)/*]*/;
    if (entry == null) {
      System.err.println("Key not found: " + key);
      return -1;
    }
    NcdcRecordParser parser = new NcdcRecordParser();
    parser.parse(val.toString());
    System.out.printf("%s\t%s\n", parser.getStationId(), parser.getYear());
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new LookupRecordByTemperature(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch08/src/main/java/oldapi/LookupRecordsByTemperature.java
package oldapi;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.*;

public class LookupRecordsByTemperature extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      JobBuilder.printUsage(this, "<path> <key>");
      return -1;
    }
    Path path = new Path(args[0]);
    IntWritable key = new IntWritable(Integer.parseInt(args[1]));
    FileSystem fs = path.getFileSystem(getConf());
    
    Reader[] readers = MapFileOutputFormat.getReaders(fs, path, getConf());
    Partitioner<IntWritable, Text> partitioner =
      new HashPartitioner<IntWritable, Text>();
    Text val = new Text();
    
    Reader reader = readers[partitioner.getPartition(key, val, readers.length)];
    Writable entry = reader.get(key, val);
    if (entry == null) {
      System.err.println("Key not found: " + key);
      return -1;
    }
    NcdcRecordParser parser = new NcdcRecordParser();
    IntWritable nextKey = new IntWritable();
    do {
      parser.parse(val.toString());
      System.out.printf("%s\t%s\n", parser.getStationId(), parser.getYear());
    } while(reader.next(nextKey, val) && key.equals(nextKey));
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new LookupRecordsByTemperature(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch08/src/main/java/oldapi/MaxTemperatureByStationNameUsingDistributedCacheFile.java
package oldapi;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class MaxTemperatureByStationNameUsingDistributedCacheFile
  extends Configured implements Tool {
  
  static class StationTemperatureMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, IntWritable> {

    private NcdcRecordParser parser = new NcdcRecordParser();
    
    public void map(LongWritable key, Text value,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        output.collect(new Text(parser.getStationId()),
            new IntWritable(parser.getAirTemperature()));
      }
    }
  }
  
  static class MaxTemperatureReducerWithStationLookup extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, IntWritable> {
    
    /*[*/private NcdcStationMetadata metadata;/*]*/
    
    /*[*/@Override
    public void configure(JobConf conf) {
      metadata = new NcdcStationMetadata();
      try {
        metadata.initialize(new File("stations-fixed-width.txt"));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }/*]*/

    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      
      /*[*/String stationName = metadata.getStationName(key.toString());/*]*/
      
      int maxValue = Integer.MIN_VALUE;
      while (values.hasNext()) {
        maxValue = Math.max(maxValue, values.next().get());
      }
      output.collect(new Text(/*[*/stationName/*]*/), new IntWritable(maxValue));
    }
  }

  @Override
  public int run(String[] args) throws IOException {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(StationTemperatureMapper.class);
    conf.setCombinerClass(MaxTemperatureReducer.class);
    conf.setReducerClass(MaxTemperatureReducerWithStationLookup.class);
    
    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(
        new MaxTemperatureByStationNameUsingDistributedCacheFile(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch08/src/main/java/oldapi/MaxTemperatureByStationNameUsingDistributedCacheFileApi.java
// == OldMaxTemperatureByStationNameUsingDistributedCacheFileApi
package oldapi;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class MaxTemperatureByStationNameUsingDistributedCacheFileApi
  extends Configured implements Tool {
  
  static class StationTemperatureMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, IntWritable> {

    private NcdcRecordParser parser = new NcdcRecordParser();
    
    public void map(LongWritable key, Text value,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        output.collect(new Text(parser.getStationId()),
            new IntWritable(parser.getAirTemperature()));
      }
    }
  }
  
  static class MaxTemperatureReducerWithStationLookup extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, IntWritable> {
    
    private NcdcStationMetadata metadata;
    
    // vv OldMaxTemperatureByStationNameUsingDistributedCacheFileApi
    @Override
    public void configure(JobConf conf) {
      metadata = new NcdcStationMetadata();
      try {
        Path[] localPaths = /*[*/DistributedCache.getLocalCacheFiles(conf);/*]*/
        if (localPaths.length == 0) {
          throw new FileNotFoundException("Distributed cache file not found.");
        }
        File localFile = new File(localPaths[0].toString());
        metadata.initialize(localFile);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    // ^^ OldMaxTemperatureByStationNameUsingDistributedCacheFileApi

    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      
      String stationName = metadata.getStationName(key.toString());
      
      int maxValue = Integer.MIN_VALUE;
      while (values.hasNext()) {
        maxValue = Math.max(maxValue, values.next().get());
      }
      output.collect(new Text(stationName), new IntWritable(maxValue));
    }
  }

  @Override
  public int run(String[] args) throws IOException {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(StationTemperatureMapper.class);
    conf.setCombinerClass(MaxTemperatureReducer.class);
    conf.setReducerClass(MaxTemperatureReducerWithStationLookup.class);
    
    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(
        new MaxTemperatureByStationNameUsingDistributedCacheFileApi(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch08/src/main/java/oldapi/MaxTemperatureUsingSecondarySort.java
package oldapi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class MaxTemperatureUsingSecondarySort
  extends Configured implements Tool {
  
  static class MaxTemperatureMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, IntPair, NullWritable> {
  
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    public void map(LongWritable key, Text value,
        OutputCollector<IntPair, NullWritable> output, Reporter reporter)
        throws IOException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        /*[*/output.collect(new IntPair(parser.getYearInt(),
            + parser.getAirTemperature()), NullWritable.get());/*]*/
      }
    }
  }
  
  static class MaxTemperatureReducer extends MapReduceBase
    implements Reducer<IntPair, NullWritable, IntPair, NullWritable> {
  
    public void reduce(IntPair key, Iterator<NullWritable> values,
        OutputCollector<IntPair, NullWritable> output, Reporter reporter)
        throws IOException {
      
      /*[*/output.collect(key, NullWritable.get());/*]*/
    }
  }
  
  public static class FirstPartitioner
    implements Partitioner<IntPair, NullWritable> {

    @Override
    public void configure(JobConf job) {}
    
    @Override
    public int getPartition(IntPair key, NullWritable value, int numPartitions) {
      return Math.abs(key.getFirst() * 127) % numPartitions;
    }
  }
  
  public static class KeyComparator extends WritableComparator {
    protected KeyComparator() {
      super(IntPair.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      IntPair ip1 = (IntPair) w1;
      IntPair ip2 = (IntPair) w2;
      int cmp = IntPair.compare(ip1.getFirst(), ip2.getFirst());
      if (cmp != 0) {
        return cmp;
      }
      return -IntPair.compare(ip1.getSecond(), ip2.getSecond()); //reverse
    }
  }
  
  public static class GroupComparator extends WritableComparator {
    protected GroupComparator() {
      super(IntPair.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      IntPair ip1 = (IntPair) w1;
      IntPair ip2 = (IntPair) w2;
      return IntPair.compare(ip1.getFirst(), ip2.getFirst());
    }
  }

  @Override
  public int run(String[] args) throws IOException {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }
    
    conf.setMapperClass(MaxTemperatureMapper.class);
    /*[*/conf.setPartitionerClass(FirstPartitioner.class);/*]*/
    /*[*/conf.setOutputKeyComparatorClass(KeyComparator.class);/*]*/
    /*[*/conf.setOutputValueGroupingComparator(GroupComparator.class);/*]*/
    conf.setReducerClass(MaxTemperatureReducer.class);
    conf.setOutputKeyClass(IntPair.class);
    conf.setOutputValueClass(NullWritable.class);
    
    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureUsingSecondarySort(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch08/src/main/java/oldapi/MaxTemperatureWithCounters.java
package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class MaxTemperatureWithCounters extends Configured implements Tool {
  
  enum Temperature {
    MISSING,
    MALFORMED
  }
  
  static class MaxTemperatureMapperWithCounters extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, IntWritable> {
    
    private NcdcRecordParser parser = new NcdcRecordParser();
  
    public void map(LongWritable key, Text value,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        int airTemperature = parser.getAirTemperature();
        output.collect(new Text(parser.getYear()),
            new IntWritable(airTemperature));
      } else if (parser.isMalformedTemperature()) {
        System.err.println("Ignoring possibly corrupt input: " + value);
        reporter.incrCounter(Temperature.MALFORMED, 1);
      } else if (parser.isMissingTemperature()) {
        reporter.incrCounter(Temperature.MISSING, 1);
      }
      
      // dynamic counter
      reporter.incrCounter("TemperatureQuality", parser.getQuality(), 1);
      
    }
  }
  
  @Override
  public int run(String[] args) throws IOException {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(MaxTemperatureMapperWithCounters.class);
    conf.setCombinerClass(MaxTemperatureReducer.class);
    conf.setReducerClass(MaxTemperatureReducer.class);

    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MaxTemperatureWithCounters(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch08/src/main/java/oldapi/MissingTemperatureFields.java
package oldapi;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class MissingTemperatureFields extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      JobBuilder.printUsage(this, "<job ID>");
      return -1;
    }
    String jobID = args[0];
    JobClient jobClient = new JobClient(new JobConf(getConf()));
    RunningJob job = jobClient.getJob(JobID.forName(jobID));
    if (job == null) {
      System.err.printf("No job with ID %s found.\n", jobID);
      return -1;
    }
    if (!job.isComplete()) {
      System.err.printf("Job %s is not complete.\n", jobID);
      return -1;
    }
    
    Counters counters = job.getCounters();
    long missing = counters.getCounter(
        MaxTemperatureWithCounters.Temperature.MISSING);
    
    long total = counters.findCounter("org.apache.hadoop.mapred.Task$Counter",
        "MAP_INPUT_RECORDS").getCounter();

    System.out.printf("Records with missing temperature fields: %.2f%%\n",
        100.0 * missing / total);
    return 0;
  }
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new MissingTemperatureFields(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch08/src/main/java/oldapi/SortByTemperatureToMapFile.java
package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SortByTemperatureToMapFile extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws IOException {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }
    
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputKeyClass(IntWritable.class);
    /*[*/conf.setOutputFormat(MapFileOutputFormat.class);/*]*/
    SequenceFileOutputFormat.setCompressOutput(conf, true);
    SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(conf,
        CompressionType.BLOCK);

    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SortByTemperatureToMapFile(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch08/src/main/java/oldapi/SortByTemperatureUsingHashPartitioner.java
package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SortByTemperatureUsingHashPartitioner extends Configured
  implements Tool {
  
  @Override
  public int run(String[] args) throws IOException {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }
    
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setCompressOutput(conf, true);
    SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(conf,
        CompressionType.BLOCK);
    
    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SortByTemperatureUsingHashPartitioner(),
        args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch08/src/main/java/oldapi/SortByTemperatureUsingTotalOrderPartitioner.java
package oldapi;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;

public class SortByTemperatureUsingTotalOrderPartitioner extends Configured
  implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }
    
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setCompressOutput(conf, true);
    SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(conf,
        CompressionType.BLOCK);

    conf.setPartitionerClass(TotalOrderPartitioner.class);

    InputSampler.Sampler<IntWritable, Text> sampler =
      new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000, 10);
    
    Path input = FileInputFormat.getInputPaths(conf)[0];
    input = input.makeQualified(input.getFileSystem(conf));
    
    Path partitionFile = new Path(input, "_partitions");
    TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
    InputSampler.writePartitionFile(conf, sampler);

    // Add to DistributedCache
    URI partitionUri = new URI(partitionFile.toString() + "#_partitions");
    DistributedCache.addCacheFile(partitionUri, conf);
    DistributedCache.createSymlink(conf);

    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(
        new SortByTemperatureUsingTotalOrderPartitioner(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch08/src/main/java/oldapi/SortDataPreprocessor.java
package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SortDataPreprocessor extends Configured implements Tool {
  
  static class CleanerMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, IntWritable, Text> {
  
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    public void map(LongWritable key, Text value,
        OutputCollector<IntWritable, Text> output, Reporter reporter)
        throws IOException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        output.collect(new IntWritable(parser.getAirTemperature()), value);
      }
    }
  }
  
  @Override
  public int run(String[] args) throws IOException {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }

    conf.setMapperClass(CleanerMapper.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Text.class);
    conf.setNumReduceTasks(0);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setCompressOutput(conf, true);
    SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(conf,
        CompressionType.BLOCK);

    JobClient.runJob(conf);
    return 0;
  }
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SortDataPreprocessor(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch08/src/main/java/oldapi/TemperatureDistribution.java
package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.*;

public class TemperatureDistribution extends Configured implements Tool {
  
  static class TemperatureCountMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, IntWritable, LongWritable> {
  
    private static final LongWritable ONE = new LongWritable(1);
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    public void map(LongWritable key, Text value,
        OutputCollector<IntWritable, LongWritable> output, Reporter reporter)
        throws IOException {
      
      parser.parse(value);
      if (parser.isValidTemperature()) {
        output.collect(new IntWritable(parser.getAirTemperature() / 10), ONE);
      }
    }
  }

  @Override
  public int run(String[] args) throws IOException {
    JobConf conf = JobBuilder.parseInputAndOutput(this, getConf(), args);
    if (conf == null) {
      return -1;
    }
    
    conf.setMapperClass(TemperatureCountMapper.class);
    conf.setCombinerClass(LongSumReducer.class);
    conf.setReducerClass(LongSumReducer.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(LongWritable.class);

    JobClient.runJob(conf);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new TemperatureDistribution(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch08/src/test/java/KeyFieldBasedComparatorTest.java
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.KeyFieldBasedComparator;
import org.junit.Test;


public class KeyFieldBasedComparatorTest {

  Text line1 = new Text("2\t30");
  Text line2 = new Text("10\t4");
  Text line3 = new Text("10\t30");
  
  @Test
  public void firstKey() throws Exception {
    check("-k1,1", line1, line2, 1);
    check("-k1",   line1, line2, 1);
    check("-k1.1", line1, line2, 1);
    check("-k1n",  line1, line2, -1);
    check("-k1nr", line1, line2, 1);
  }

  @Test
  public void secondKey() throws Exception {
    check("-k2,2", line1, line2, -1);
    check("-k2",   line1, line2, -1);
    check("-k2.1", line1, line2, -1);
    check("-k2n",  line1, line2, 1);
    check("-k2nr", line1, line2, -1);
  }

  @Test
  public void firstThenSecondKey() throws Exception {
    check("-k1 -k2",   line1, line2, 1);
    check("-k1 -k2",   line2, line3, 1);
    //check("-k1 -k2n",  line2, line3, -1);
    check("-k1 -k2nr", line2, line3, 1);
  }
  
  private void check(String options, Text l1, Text l2, int c) throws IOException {
    JobConf conf = new JobConf();
    conf.setKeyFieldComparatorOptions(options);
    KeyFieldBasedComparator comp = new KeyFieldBasedComparator();
    comp.configure(conf);
    
    DataOutputBuffer out1 = serialize(l1);
    DataOutputBuffer out2 = serialize(l2);
    assertThat(options, comp.compare(out1.getData(), 0, out1.getLength(), out2.getData(), 0, out2.getLength()), is(c));
  }
  
  public static DataOutputBuffer serialize(Writable writable) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    DataOutputStream dataOut = new DataOutputStream(out);
    writable.write(dataOut);
    dataOut.close();
    return out;
  }
  
}

//=*=*=*=*
//./ch11/src/main/java/com/hadoopbook/pig/CutLoadFunc.java
//cc CutLoadFunc A LoadFunc UDF to load tuple fields as column ranges
package com.hadoopbook.pig;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

// vv CutLoadFunc
public class CutLoadFunc extends LoadFunc {

  private static final Log LOG = LogFactory.getLog(CutLoadFunc.class);

  private final List<Range> ranges;
  private final TupleFactory tupleFactory = TupleFactory.getInstance();
  private RecordReader reader;

  public CutLoadFunc(String cutPattern) {
    ranges = Range.parse(cutPattern);
  }
  
  @Override
  public void setLocation(String location, Job job)
      throws IOException {
    FileInputFormat.setInputPaths(job, location);
  }
  
  @Override
  public InputFormat getInputFormat() {
    return new TextInputFormat();
  }
  
  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) {
    this.reader = reader;
  }

  @Override
  public Tuple getNext() throws IOException {
    try {
      if (!reader.nextKeyValue()) {
        return null;
      }
      Text value = (Text) reader.getCurrentValue();
      String line = value.toString();
      Tuple tuple = tupleFactory.newTuple(ranges.size());
      for (int i = 0; i < ranges.size(); i++) {
        Range range = ranges.get(i);
        if (range.getEnd() > line.length()) {
          LOG.warn(String.format(
              "Range end (%s) is longer than line length (%s)",
              range.getEnd(), line.length()));
          continue;
        }
        tuple.set(i, new DataByteArray(range.getSubstring(line)));
      }
      return tuple;
    } catch (InterruptedException e) {
      throw new ExecException(e);
    }
  }
}
// ^^ CutLoadFunc

//=*=*=*=*
//./ch11/src/main/java/com/hadoopbook/pig/IsGoodQuality.java
//cc IsGoodQuality A FilterFunc UDF to remove records with unsatisfactory temperature quality readings
// == IsGoodQualityTyped
//vv IsGoodQuality
package com.hadoopbook.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.FilterFunc;
//^^ IsGoodQuality
import org.apache.pig.FuncSpec;
//vv IsGoodQuality  
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
//^^ IsGoodQuality
import org.apache.pig.impl.logicalLayer.schema.Schema;
//vv IsGoodQuality

public class IsGoodQuality extends FilterFunc {

  @Override
  public Boolean exec(Tuple tuple) throws IOException {
    if (tuple == null || tuple.size() == 0) {
      return false;
    }
    try {
      Object object = tuple.get(0);
      if (object == null) {
        return false;
      }
      int i = (Integer) object;
      return i == 0 || i == 1 || i == 4 || i == 5 || i == 9;
    } catch (ExecException e) {
      throw new IOException(e);
    }
  }
//^^ IsGoodQuality
//vv IsGoodQualityTyped
  @Override
  public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
    List<FuncSpec> funcSpecs = new ArrayList<FuncSpec>();
    funcSpecs.add(new FuncSpec(this.getClass().getName(),
        new Schema(new Schema.FieldSchema(null, DataType.INTEGER))));

    return funcSpecs;
  }
//^^ IsGoodQualityTyped  
//vv IsGoodQuality  
}
// ^^ IsGoodQuality
//=*=*=*=*
//./ch11/src/main/java/com/hadoopbook/pig/Range.java
package com.hadoopbook.pig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Range {
  private final int start;
  private final int end;

  public Range(int start, int end) {
    this.start = start;
    this.end = end;
  }

  public int getStart() {
    return start;
  }

  public int getEnd() {
    return end;
  }
  
  public String getSubstring(String line) {
    return line.substring(start - 1, end);
  }
  
  @Override
  public int hashCode() {
    return start * 37 + end;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Range)) {
      return false;
    }
    Range other = (Range) obj;
    return this.start == other.start && this.end == other.end;
  }

  public static List<Range> parse(String rangeSpec)
      throws IllegalArgumentException {
    if (rangeSpec.length() == 0) {
      return Collections.emptyList();
    }
    List<Range> ranges = new ArrayList<Range>();
    String[] specs = rangeSpec.split(",");
    for (String spec : specs) {
      String[] split = spec.split("-");
      try {
        ranges.add(new Range(Integer.parseInt(split[0]), Integer
            .parseInt(split[1])));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }
    return ranges;
  }

}

//=*=*=*=*
//./ch11/src/main/java/com/hadoopbook/pig/Trim.java
package com.hadoopbook.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

//cc Trim An EvalFunc UDF to trim leading and trailing whitespace from chararray values
//vv Trim
public class Trim extends EvalFunc<String> {

  @Override
  public String exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0) {
      return null;
    }
    try {
      Object object = input.get(0);
      if (object == null) {
        return null;
      }
      return ((String) object).trim();
    } catch (ExecException e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
    List<FuncSpec> funcList = new ArrayList<FuncSpec>();
    funcList.add(new FuncSpec(this.getClass().getName(), new Schema(
        new Schema.FieldSchema(null, DataType.CHARARRAY))));

    return funcList;
  }
}
// ^^ Trim
//=*=*=*=*
//./ch11/src/test/java/com/hadoopbook/pig/IsGoodQualityTest.java
package com.hadoopbook.pig;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.pig.data.*;
import org.junit.*;

public class IsGoodQualityTest {
    
    private IsGoodQuality func;
    
    @Before
    public void setUp() {
      func = new IsGoodQuality();
    }

    @Test
    public void nullTuple() throws IOException {
      assertThat(func.exec(null), is(false));
    }
    
    @Test
    public void emptyTuple() throws IOException {
      Tuple tuple = TupleFactory.getInstance().newTuple();
      assertThat(func.exec(tuple), is(false));
    }

    @Test
    public void tupleWithNullField() throws IOException {
      Tuple tuple = TupleFactory.getInstance().newTuple((Object) null);
      assertThat(func.exec(tuple), is(false));
    }
    
    @Test
    public void badQuality() throws IOException {
      Tuple tuple = TupleFactory.getInstance().newTuple(new Integer(2));
      assertThat(func.exec(tuple), is(false));
    }
    
    @Test
    public void goodQuality() throws IOException {
      Tuple tuple = TupleFactory.getInstance().newTuple(new Integer(1));
      assertThat(func.exec(tuple), is(true));
    }
    

}

//=*=*=*=*
//./ch11/src/test/java/com/hadoopbook/pig/RangeTest.java
package com.hadoopbook.pig;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.*;

public class RangeTest {

  @Test
  public void parsesEmptyRangeSpec() {
    assertThat(Range.parse("").size(), is(0));
  }

  @Test
  public void parsesSingleRangeSpec() {
    List<Range> ranges = Range.parse("1-3");
    assertThat(ranges.size(), is(1));
    assertThat(ranges.get(0), is(new Range(1, 3)));
  }

  @Test
  public void parsesMultipleRangeSpec() {
    List<Range> ranges = Range.parse("1-3,5-10");
    assertThat(ranges.size(), is(2));
    assertThat(ranges.get(0), is(new Range(1, 3)));
    assertThat(ranges.get(1), is(new Range(5, 10)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void failsOnInvalidSpec() {
    Range.parse("1-n");
  }
}

//=*=*=*=*
//./ch12/src/main/java/com/hadoopbook/hive/Maximum.java
package com.hadoopbook.hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

public class Maximum extends UDAF {

  public static class MaximumIntUDAFEvaluator implements UDAFEvaluator {
    
    private IntWritable result;
    
    public void init() {
      System.err.printf("%s %s\n", hashCode(), "init");
      result = null;
    }

    public boolean iterate(IntWritable value) {
      System.err.printf("%s %s %s\n", hashCode(), "iterate", value);
      if (value == null) {
        return true;
      }
      if (result == null) {
        result = new IntWritable(value.get());
      } else {
        result.set(Math.max(result.get(), value.get()));
      }
      return true;
    }

    public IntWritable terminatePartial() {
      System.err.printf("%s %s\n", hashCode(), "terminatePartial");
      return result;
    }

    public boolean merge(IntWritable other) {
      System.err.printf("%s %s %s\n", hashCode(), "merge", other);
      return iterate(other);
    }

    public IntWritable terminate() {
      System.err.printf("%s %s\n", hashCode(), "terminate");
      return result;
    }
  }
}

//=*=*=*=*
//./ch12/src/main/java/com/hadoopbook/hive/Mean.java
package com.hadoopbook.hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

public class Mean extends UDAF {

  public static class MeanDoubleUDAFEvaluator implements UDAFEvaluator {
    public static class PartialResult {
      double sum;
      long count;
    }
    
    private PartialResult partial;

    public void init() {
      partial = null;
    }

    public boolean iterate(DoubleWritable value) {
      if (value == null) {
        return true;
      }
      if (partial == null) {
        partial = new PartialResult();
      }
      partial.sum += value.get();
      partial.count++;
      return true;
    }

    public PartialResult terminatePartial() {
      return partial;
    }

    public boolean merge(PartialResult other) {
      if (other == null) {
        return true;
      }
      if (partial == null) {
        partial = new PartialResult();
      }
      partial.sum += other.sum;
      partial.count += other.count;
      return true;
    }

    public DoubleWritable terminate() {
      if (partial == null) {
        return null;
      }
      return new DoubleWritable(partial.sum / partial.count);
    }
  }
}

//=*=*=*=*
//./ch12/src/main/java/com/hadoopbook/hive/Strip.java
package com.hadoopbook.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Strip extends UDF {
  private Text result = new Text();
  
  public Text evaluate(Text str) {
    if (str == null) {
      return null;
    }
    result.set(StringUtils.strip(str.toString()));
    return result;
  }
  
  public Text evaluate(Text str, String stripChars) {
    if (str == null) {
      return null;
    }
    result.set(StringUtils.strip(str.toString(), stripChars));
    return result;
  }
}
//=*=*=*=*
//./ch13/src/main/java/HBaseStationCli.java
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseStationCli extends Configured implements Tool {
  static final byte [] INFO_COLUMNFAMILY = Bytes.toBytes("info");
  static final byte [] NAME_QUALIFIER = Bytes.toBytes("name");
  static final byte [] LOCATION_QUALIFIER = Bytes.toBytes("location");
  static final byte [] DESCRIPTION_QUALIFIER = Bytes.toBytes("description");

  public Map<String, String> getStationInfo(HTable table, String stationId)
      throws IOException {
    Get get = new Get(Bytes.toBytes(stationId));
    get.addColumn(INFO_COLUMNFAMILY);
    Result res = table.get(get);
    if (res == null) {
      return null;
    }
    Map<String, String> resultMap = new HashMap<String, String>();
    resultMap.put("name", getValue(res, INFO_COLUMNFAMILY, NAME_QUALIFIER));
    resultMap.put("location", getValue(res, INFO_COLUMNFAMILY, LOCATION_QUALIFIER));
    resultMap.put("description", getValue(res, INFO_COLUMNFAMILY, DESCRIPTION_QUALIFIER));
    return resultMap;
  }

  private static String getValue(Result res, byte [] cf, byte [] qualifier) {
    byte [] value = res.getValue(cf, qualifier);
    return value == null? "": Bytes.toString(value);
  }

  public int run(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: HBaseStationCli <station_id>");
      return -1;
    }

    HTable table = new HTable(new HBaseConfiguration(getConf()), "stations");
    Map<String, String> stationInfo = getStationInfo(table, args[0]);
    if (stationInfo == null) {
      System.err.printf("Station ID %s not found.\n", args[0]);
      return -1;
    }
    for (Map.Entry<String, String> station : stationInfo.entrySet()) {
      // Print the date, time, and temperature
      System.out.printf("%s\t%s\n", station.getKey(), station.getValue());
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new HBaseConfiguration(),
        new HBaseStationCli(), args);
    System.exit(exitCode);
  }
}
//=*=*=*=*
//./ch13/src/main/java/HBaseStationImporter.java
import java.io.*;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.*;

public class HBaseStationImporter extends Configured implements Tool {
  
  public int run(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: HBaseStationImporter <input>");
      return -1;
    }
    
    HTable table = new HTable(new HBaseConfiguration(getConf()), "stations");
    
    NcdcStationMetadata metadata = new NcdcStationMetadata();
    metadata.initialize(new File(args[0]));
    Map<String, String> stationIdToNameMap = metadata.getStationIdToNameMap();
    
    for (Map.Entry<String, String> entry : stationIdToNameMap.entrySet()) {
      Put put = new Put(Bytes.toBytes(entry.getKey()));
      put.add(HBaseStationCli.INFO_COLUMNFAMILY, HBaseStationCli.NAME_QUALIFIER,
        Bytes.toBytes(entry.getValue()));
      put.add(HBaseStationCli.INFO_COLUMNFAMILY, HBaseStationCli.DESCRIPTION_QUALIFIER,
        Bytes.toBytes("Description..."));
      put.add(HBaseStationCli.INFO_COLUMNFAMILY, HBaseStationCli.LOCATION_QUALIFIER,
        Bytes.toBytes("Location..."));
      table.put(put);
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new HBaseConfiguration(),
        new HBaseStationImporter(), args);
    System.exit(exitCode);
  }
}
//=*=*=*=*
//./ch13/src/main/java/HBaseTemperatureCli.java
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.*;

public class HBaseTemperatureCli extends Configured implements Tool {
  static final byte [] DATA_COLUMNFAMILY = Bytes.toBytes("data");
  static final byte [] AIRTEMP_QUALIFIER = Bytes.toBytes("airtemp");
  
  public NavigableMap<Long, Integer> getStationObservations(HTable table,
      String stationId, long maxStamp, int maxCount) throws IOException {
    byte[] startRow = RowKeyConverter.makeObservationRowKey(stationId, maxStamp);
    NavigableMap<Long, Integer> resultMap = new TreeMap<Long, Integer>();
    Scan scan = new Scan(startRow);
    scan.addColumn(DATA_COLUMNFAMILY, AIRTEMP_QUALIFIER);
    ResultScanner scanner = table.getScanner(scan);
    Result res = null;
    int count = 0;
    try {
      while ((res = scanner.next()) != null && count++ < maxCount) {
        byte[] row = res.getRow();
        byte[] value = res.getValue(DATA_COLUMNFAMILY, AIRTEMP_QUALIFIER);
        Long stamp = Long.MAX_VALUE -
          Bytes.toLong(row, row.length - Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);
        Integer temp = Bytes.toInt(value);
        resultMap.put(stamp, temp);
      }
    } finally {
      scanner.close();
    }
    return resultMap;
  }

  /**
   * Return the last ten observations.
   */
  public NavigableMap<Long, Integer> getStationObservations(HTable table,
      String stationId) throws IOException {
    return getStationObservations(table, stationId, Long.MAX_VALUE, 10);
  }

  public int run(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: HBaseTemperatureCli <station_id>");
      return -1;
    }
    
    HTable table = new HTable(new HBaseConfiguration(getConf()), "observations");
    NavigableMap<Long, Integer> observations =
      getStationObservations(table, args[0]).descendingMap();
    for (Map.Entry<Long, Integer> observation : observations.entrySet()) {
      // Print the date, time, and temperature
      System.out.printf("%1$tF %1$tR\t%2$s\n", observation.getKey(),
          observation.getValue());
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new HBaseConfiguration(),
        new HBaseTemperatureCli(), args);
    System.exit(exitCode);
  }
}

//=*=*=*=*
//./ch13/src/main/java/HBaseTemperatureImporter.java
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.*;

public class HBaseTemperatureImporter extends Configured implements Tool {
  
  // Inner-class for map
  static class HBaseTemperatureMapper<K, V> extends MapReduceBase implements
      Mapper<LongWritable, Text, K, V> {
    private NcdcRecordParser parser = new NcdcRecordParser();
    private HTable table;

    public void map(LongWritable key, Text value,
      OutputCollector<K, V> output, Reporter reporter)
    throws IOException {
      parser.parse(value.toString());
      if (parser.isValidTemperature()) {
        byte[] rowKey = RowKeyConverter.makeObservationRowKey(parser.getStationId(),
          parser.getObservationDate().getTime());
        Put p = new Put(rowKey);
        p.add(HBaseTemperatureCli.DATA_COLUMNFAMILY,
          HBaseTemperatureCli.AIRTEMP_QUALIFIER,
          Bytes.toBytes(parser.getAirTemperature()));
        table.put(p);
      }
    }

    public void configure(JobConf jc) {
      super.configure(jc);
      // Create the HBase table client once up-front and keep it around
      // rather than create on each map invocation.
      try {
        this.table = new HTable(new HBaseConfiguration(jc), "observations");
      } catch (IOException e) {
        throw new RuntimeException("Failed HTable construction", e);
      }
    }

    @Override
    public void close() throws IOException {
      super.close();
      table.close();
    }
  }

  public int run(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: HBaseTemperatureImporter <input>");
      return -1;
    }
    JobConf jc = new JobConf(getConf(), getClass());
    FileInputFormat.addInputPath(jc, new Path(args[0]));
    jc.setMapperClass(HBaseTemperatureMapper.class);
    jc.setNumReduceTasks(0);
    jc.setOutputFormat(NullOutputFormat.class);
    JobClient.runJob(jc);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new HBaseConfiguration(),
        new HBaseTemperatureImporter(), args);
    System.exit(exitCode);
  }
}
//=*=*=*=*
//./ch13/src/main/java/RowKeyConverter.java
import org.apache.hadoop.hbase.util.Bytes;

public class RowKeyConverter {

  private static final int STATION_ID_LENGTH = 12;

  /**
   * @return A row key whose format is: <station_id> <reverse_order_epoch>
   */
  public static byte[] makeObservationRowKey(String stationId,
      long observationTime) {
    byte[] row = new byte[STATION_ID_LENGTH + Bytes.SIZEOF_LONG];
    Bytes.putBytes(row, 0, Bytes.toBytes(stationId), 0, STATION_ID_LENGTH);
    long reverseOrderEpoch = Long.MAX_VALUE - observationTime;
    Bytes.putLong(row, STATION_ID_LENGTH, reverseOrderEpoch);
    return row;
  }
}

//=*=*=*=*
//./ch14/src/main/java/ActiveKeyValueStore.java
//== ActiveKeyValueStore
//== ActiveKeyValueStore-Read
//== ActiveKeyValueStore-Write

import java.nio.charset.Charset;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

// vv ActiveKeyValueStore
public class ActiveKeyValueStore extends ConnectionWatcher {

  private static final Charset CHARSET = Charset.forName("UTF-8");

//vv ActiveKeyValueStore-Write
  public void write(String path, String value) throws InterruptedException,
      KeeperException {
    Stat stat = zk.exists(path, false);
    if (stat == null) {
      zk.create(path, value.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
    } else {
      zk.setData(path, value.getBytes(CHARSET), -1);
    }
  }
//^^ ActiveKeyValueStore-Write
//^^ ActiveKeyValueStore
//vv ActiveKeyValueStore-Read
  public String read(String path, Watcher watcher) throws InterruptedException,
      KeeperException {
    byte[] data = zk.getData(path, watcher, null/*stat*/);
    return new String(data, CHARSET);
  }
//^^ ActiveKeyValueStore-Read
//vv ActiveKeyValueStore
}
//^^ ActiveKeyValueStore

//=*=*=*=*
//./ch14/src/main/java/ConfigUpdater.java
//cc ConfigUpdater An application that updates a property in ZooKeeper at random times
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

// vv ConfigUpdater
public class ConfigUpdater {
  
  public static final String PATH = "/config";
  
  private ActiveKeyValueStore store;
  private Random random = new Random();
  
  public ConfigUpdater(String hosts) throws IOException, InterruptedException {
    store = new ActiveKeyValueStore();
    store.connect(hosts);
  }
  
  public void run() throws InterruptedException, KeeperException {
    while (true) {
      String value = random.nextInt(100) + "";
      store.write(PATH, value);
      System.out.printf("Set %s to %s\n", PATH, value);
      TimeUnit.SECONDS.sleep(random.nextInt(10));
    }
  }
  
  public static void main(String[] args) throws Exception {
    ConfigUpdater configUpdater = new ConfigUpdater(args[0]);
    configUpdater.run();
  }
}
// ^^ ConfigUpdater

//=*=*=*=*
//./ch14/src/main/java/ConfigWatcher.java
//cc ConfigWatcher An application that watches for updates of a property in ZooKeeper and prints them to the console
import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

// vv ConfigWatcher
public class ConfigWatcher implements Watcher {
  
  private ActiveKeyValueStore store;
  
  public ConfigWatcher(String hosts) throws IOException, InterruptedException {
    store = new ActiveKeyValueStore();
    store.connect(hosts);
  }
  
  public void displayConfig() throws InterruptedException, KeeperException {
    String value = store.read(ConfigUpdater.PATH, this);
    System.out.printf("Read %s as %s\n", ConfigUpdater.PATH, value);
  }

  @Override
  public void process(WatchedEvent event) {
    if (event.getType() == EventType.NodeDataChanged) {
      try {
        displayConfig();
      } catch (InterruptedException e) {
        System.err.println("Interrupted. Exiting.");        
        Thread.currentThread().interrupt();
      } catch (KeeperException e) {
        System.err.printf("KeeperException: %s. Exiting.\n", e);        
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    ConfigWatcher configWatcher = new ConfigWatcher(args[0]);
    configWatcher.displayConfig();
    
    // stay alive until process is killed or thread is interrupted
    Thread.sleep(Long.MAX_VALUE);
  }
}
//^^ ConfigWatcher

//=*=*=*=*
//./ch14/src/main/java/ConnectionWatcher.java
//cc ConnectionWatcher A helper class that waits for the connection to ZooKeeper to be established
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

// vv ConnectionWatcher
public class ConnectionWatcher implements Watcher {
  
  private static final int SESSION_TIMEOUT = 5000;

  protected ZooKeeper zk;
  private CountDownLatch connectedSignal = new CountDownLatch(1);

  public void connect(String hosts) throws IOException, InterruptedException {
    zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
    connectedSignal.await();
  }
  
  @Override
  public void process(WatchedEvent event) {
    if (event.getState() == KeeperState.SyncConnected) {
      connectedSignal.countDown();
    }
  }
  
  public void close() throws InterruptedException {
    zk.close();
  }
}
// ^^ ConnectionWatcher
//=*=*=*=*
//./ch14/src/main/java/CreateGroup.java
//cc CreateGroup A program to create a znode representing a group in ZooKeeper

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;

// vv CreateGroup
public class CreateGroup implements Watcher {
  
  private static final int SESSION_TIMEOUT = 5000;
  
  private ZooKeeper zk;
  private CountDownLatch connectedSignal = new CountDownLatch(1);
  
  public void connect(String hosts) throws IOException, InterruptedException {
    zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
    connectedSignal.await();
  }
  
  @Override
  public void process(WatchedEvent event) { // Watcher interface
    if (event.getState() == KeeperState.SyncConnected) {
      connectedSignal.countDown();
    }
  }
  
  public void create(String groupName) throws KeeperException,
      InterruptedException {
    String path = "/" + groupName;
    String createdPath = zk.create(path, null/*data*/, Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    System.out.println("Created " + createdPath);
  }
  
  public void close() throws InterruptedException {
    zk.close();
  }

  public static void main(String[] args) throws Exception {
    CreateGroup createGroup = new CreateGroup();
    createGroup.connect(args[0]);
    createGroup.create(args[1]);
    createGroup.close();
  }
}
// ^^ CreateGroup

//=*=*=*=*
//./ch14/src/main/java/DeleteGroup.java
//cc DeleteGroup A program to delete a group and its members
import java.util.List;

import org.apache.zookeeper.KeeperException;

// vv DeleteGroup
public class DeleteGroup extends ConnectionWatcher {
    
  public void delete(String groupName) throws KeeperException,
      InterruptedException {
    String path = "/" + groupName;
    
    try {
      List<String> children = zk.getChildren(path, false);
      for (String child : children) {
        zk.delete(path + "/" + child, -1);
      }
      zk.delete(path, -1);
    } catch (KeeperException.NoNodeException e) {
      System.out.printf("Group %s does not exist\n", groupName);
      System.exit(1);
    }
  }
  
  public static void main(String[] args) throws Exception {
    DeleteGroup deleteGroup = new DeleteGroup();
    deleteGroup.connect(args[0]);
    deleteGroup.delete(args[1]);
    deleteGroup.close();
  }
}
// ^^ DeleteGroup

//=*=*=*=*
//./ch14/src/main/java/JoinGroup.java
//cc JoinGroup A program that joins a group

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

// vv JoinGroup
public class JoinGroup extends ConnectionWatcher {
  
  public void join(String groupName, String memberName) throws KeeperException,
      InterruptedException {
    String path = "/" + groupName + "/" + memberName;
    String createdPath = zk.create(path, null/*data*/, Ids.OPEN_ACL_UNSAFE,
      CreateMode.EPHEMERAL);
    System.out.println("Created " + createdPath);
  }
  
  public static void main(String[] args) throws Exception {
    JoinGroup joinGroup = new JoinGroup();
    joinGroup.connect(args[0]);
    joinGroup.join(args[1], args[2]);
    
    // stay alive until process is killed or thread is interrupted
    Thread.sleep(Long.MAX_VALUE);
  }
}
// ^^ JoinGroup

//=*=*=*=*
//./ch14/src/main/java/ListGroup.java
//cc ListGroup A program to list the members in a group
import java.util.List;

import org.apache.zookeeper.KeeperException;

// vv ListGroup
public class ListGroup extends ConnectionWatcher {
    
  public void list(String groupName) throws KeeperException,
      InterruptedException {
    String path = "/" + groupName;
    
    try {
      List<String> children = zk.getChildren(path, false);
      if (children.isEmpty()) {
        System.out.printf("No members in group %s\n", groupName);
        System.exit(1);
      }
      for (String child : children) {
        System.out.println(child);
      }
    } catch (KeeperException.NoNodeException e) {
      System.out.printf("Group %s does not exist\n", groupName);
      System.exit(1);
    }
  }
  
  public static void main(String[] args) throws Exception {
    ListGroup listGroup = new ListGroup();
    listGroup.connect(args[0]);
    listGroup.list(args[1]);
    listGroup.close();
  }
}
// ^^ ListGroup

//=*=*=*=*
//./ch14/src/main/java/ResilientActiveKeyValueStore.java
//== ResilientActiveKeyValueStore-Write

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ResilientActiveKeyValueStore extends ConnectionWatcher {

  private static final Charset CHARSET = Charset.forName("UTF-8");
  private static final int MAX_RETRIES = 5;
  private static final int RETRY_PERIOD_SECONDS = 10;

//vv ResilientActiveKeyValueStore-Write
  public void write(String path, String value) throws InterruptedException,
      KeeperException {
    int retries = 0;
    while (true) {
      try {
        Stat stat = zk.exists(path, false);
        if (stat == null) {
          zk.create(path, value.getBytes(CHARSET), Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT);
        } else {
          zk.setData(path, value.getBytes(CHARSET), stat.getVersion());
        }
        return;
      } catch (KeeperException.SessionExpiredException e) {
        throw e;
      } catch (KeeperException e) {
        if (retries++ == MAX_RETRIES) {
          throw e;
        }
        // sleep then retry
        TimeUnit.SECONDS.sleep(RETRY_PERIOD_SECONDS);
      }
    }
  }
//^^ ResilientActiveKeyValueStore-Write
  public String read(String path, Watcher watcher) throws InterruptedException,
      KeeperException {
    byte[] data = zk.getData(path, watcher, null/*stat*/);
    return new String(data, CHARSET);
  }
}

//=*=*=*=*
//./ch14/src/main/java/ResilientConfigUpdater.java
//== ResilientConfigUpdater
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

public class ResilientConfigUpdater {
  
  public static final String PATH = "/config";
  
  private ResilientActiveKeyValueStore store;
  private Random random = new Random();
  
  public ResilientConfigUpdater(String hosts) throws IOException,
      InterruptedException {
    store = new ResilientActiveKeyValueStore();
    store.connect(hosts);
  }
  
  public void run() throws InterruptedException, KeeperException {
    while (true) {
      String value = random.nextInt(100) + "";
      store.write(PATH, value);
      System.out.printf("Set %s to %s\n", PATH, value);
      TimeUnit.SECONDS.sleep(random.nextInt(10));
    }
  }
  
//vv ResilientConfigUpdater
  public static void main(String[] args) throws Exception {
    /*[*/while (true) {
      try {/*]*/
        ResilientConfigUpdater configUpdater =
          new ResilientConfigUpdater(args[0]);
        configUpdater.run();
      /*[*/} catch (KeeperException.SessionExpiredException e) {
        // start a new session
      } catch (KeeperException e) {
        // already retried, so exit
        e.printStackTrace();
        break;
      }
    }/*]*/
  }
//^^ ResilientConfigUpdater
}

//=*=*=*=*
//./ch15/src/main/java/MaxWidgetId.java

import java.io.IOException;

import com.cloudera.sqoop.lib.RecordParser.ParseError;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class MaxWidgetId extends Configured implements Tool {

  public static class MaxWidgetMapper
      extends Mapper<LongWritable, Text, LongWritable, Widget> {

    private Widget maxWidget = null;

    public void map(LongWritable k, Text v, Context context) {
      Widget widget = new Widget();
      try {
        widget.parse(v); // Auto-generated: parse all fields from text.
      } catch (ParseError pe) {
        // Got a malformed record. Ignore it.
        return;
      }

      Integer id = widget.get_id();
      if (null == id) {
        return;
      } else {
        if (maxWidget == null
            || id.intValue() > maxWidget.get_id().intValue()) {
          maxWidget = widget;
        }
      }
    }

    public void cleanup(Context context)
        throws IOException, InterruptedException {
      if (null != maxWidget) {
        context.write(new LongWritable(0), maxWidget);
      }
    }
  }

  public static class MaxWidgetReducer
      extends Reducer<LongWritable, Widget, Widget, NullWritable> {

    // There will be a single reduce call with key '0' which gets
    // the max widget from each map task. Pick the max widget from
    // this list.
    public void reduce(LongWritable k, Iterable<Widget> vals, Context context)
        throws IOException, InterruptedException {
      Widget maxWidget = null;

      for (Widget w : vals) {
        if (maxWidget == null
            || w.get_id().intValue() > maxWidget.get_id().intValue()) {
          try {
            maxWidget = (Widget) w.clone();
          } catch (CloneNotSupportedException cnse) {
            // Shouldn't happen; Sqoop-generated classes support clone().
            throw new IOException(cnse);
          }
        }
      }

      if (null != maxWidget) {
        context.write(maxWidget, NullWritable.get());
      }
    }
  }

  public int run(String [] args) throws Exception {
    Job job = new Job(getConf());

    job.setJarByClass(MaxWidgetId.class);

    job.setMapperClass(MaxWidgetMapper.class);
    job.setReducerClass(MaxWidgetReducer.class);

    FileInputFormat.addInputPath(job, new Path("widgets"));
    FileOutputFormat.setOutputPath(job, new Path("maxwidget"));

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Widget.class);

    job.setOutputKeyClass(Widget.class);
    job.setOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(1);

    if (!job.waitForCompletion(true)) {
      return 1; // error.
    }

    return 0;
  }

  public static void main(String [] args) throws Exception {
    int ret = ToolRunner.run(new MaxWidgetId(), args);
    System.exit(ret);
  }
}

//=*=*=*=*
//./ch15/src/main/java/MaxWidgetIdGenericAvro.java
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxWidgetIdGenericAvro extends Configured implements Tool {
  
  public static class MaxWidgetMapper extends AvroMapper<GenericRecord, Pair<Long, GenericRecord>> {
    
    private GenericRecord maxWidget;
    private AvroCollector<Pair<Long, GenericRecord>> collector;
    
    @Override
    public void map(GenericRecord widget,
        AvroCollector<Pair<Long, GenericRecord>> collector, Reporter reporter)
        throws IOException {
      this.collector = collector;
      Integer id = (Integer) widget.get("id");
      if (id != null) {
        if (maxWidget == null
            || id > (Integer) maxWidget.get("id")) {
          maxWidget = widget;
        }
      }
    }
    
    @Override
    public void close() throws IOException {
      if (maxWidget != null) {
        collector.collect(new Pair<Long, GenericRecord>(0L, maxWidget));
      }
      super.close();
    }
    
  }
  
  static GenericRecord copy(GenericRecord record) {
    Schema schema = record.getSchema();
    GenericRecord copy = new GenericData.Record(schema);
    for (Schema.Field f : schema.getFields()) {
      copy.put(f.name(), record.get(f.name()));
    }
    return copy;
  }
  
  public static class MaxWidgetReducer extends AvroReducer<Long, GenericRecord, GenericRecord> {
    
    @Override
    public void reduce(Long key, Iterable<GenericRecord> values,
        AvroCollector<GenericRecord> collector, Reporter reporter) throws IOException {
      GenericRecord maxWidget = null;

      for (GenericRecord w : values) {
        if (maxWidget == null
            || (Integer) w.get("id") > (Integer) maxWidget.get("id")) {
          maxWidget = copy(w);
        }
      }

      if (maxWidget != null) {
        collector.collect(maxWidget);
      }
    }
  }

  public int run(String [] args) throws Exception {
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("Max temperature");
    
    Path inputDir = new Path("widgets");
    FileInputFormat.addInputPath(conf, inputDir);
    FileOutputFormat.setOutputPath(conf, new Path("maxwidget"));
    
    Schema schema = readSchema(inputDir, conf);
    
    conf.setInputFormat(AvroInputFormat.class);
    conf.setOutputFormat(AvroOutputFormat.class);
        
    AvroJob.setInputSchema(conf, schema);
    AvroJob.setMapOutputSchema(conf,
        Pair.getPairSchema(Schema.create(Schema.Type.LONG), schema));
    AvroJob.setOutputSchema(conf, schema);

    AvroJob.setMapperClass(conf, MaxWidgetMapper.class);
    AvroJob.setReducerClass(conf, MaxWidgetReducer.class);

    conf.setNumReduceTasks(1);

    JobClient.runJob(conf);
    return 0;
  }
  
  /**
   * Read the Avro schema from the first file in the input directory.
   */
  private Schema readSchema(Path inputDir, Configuration conf) throws IOException {
    FsInput fsInput = null;
    FileReader<Object> reader = null;
    try {
      fsInput = new FsInput(new Path(inputDir, "part-m-00000.avro"), conf);
      reader = DataFileReader.openReader(fsInput, new GenericDatumReader<Object>());
      return reader.getSchema();
    } finally {
      IOUtils.closeStream(fsInput);
      IOUtils.closeStream(reader);
    }
  }

  public static void main(String [] args) throws Exception {
    int ret = ToolRunner.run(new MaxWidgetIdGenericAvro(), args);
    System.exit(ret);
  }
}

//=*=*=*=*
//./ch15/src/main/java/Widget.java
// ORM class for widgets
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Widget extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  protected ResultSet __cur_result_set;
  private Integer id;
  public Integer get_id() {
    return id;
  }
  public void set_id(Integer id) {
    this.id = id;
  }
  public Widget with_id(Integer id) {
    this.id = id;
    return this;
  }
  private String widget_name;
  public String get_widget_name() {
    return widget_name;
  }
  public void set_widget_name(String widget_name) {
    this.widget_name = widget_name;
  }
  public Widget with_widget_name(String widget_name) {
    this.widget_name = widget_name;
    return this;
  }
  private java.math.BigDecimal price;
  public java.math.BigDecimal get_price() {
    return price;
  }
  public void set_price(java.math.BigDecimal price) {
    this.price = price;
  }
  public Widget with_price(java.math.BigDecimal price) {
    this.price = price;
    return this;
  }
  private java.sql.Date design_date;
  public java.sql.Date get_design_date() {
    return design_date;
  }
  public void set_design_date(java.sql.Date design_date) {
    this.design_date = design_date;
  }
  public Widget with_design_date(java.sql.Date design_date) {
    this.design_date = design_date;
    return this;
  }
  private Integer version;
  public Integer get_version() {
    return version;
  }
  public void set_version(Integer version) {
    this.version = version;
  }
  public Widget with_version(Integer version) {
    this.version = version;
    return this;
  }
  private String design_comment;
  public String get_design_comment() {
    return design_comment;
  }
  public void set_design_comment(String design_comment) {
    this.design_comment = design_comment;
  }
  public Widget with_design_comment(String design_comment) {
    this.design_comment = design_comment;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Widget)) {
      return false;
    }
    Widget that = (Widget) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.widget_name == null ? that.widget_name == null : this.widget_name.equals(that.widget_name));
    equal = equal && (this.price == null ? that.price == null : this.price.equals(that.price));
    equal = equal && (this.design_date == null ? that.design_date == null : this.design_date.equals(that.design_date));
    equal = equal && (this.version == null ? that.version == null : this.version.equals(that.version));
    equal = equal && (this.design_comment == null ? that.design_comment == null : this.design_comment.equals(that.design_comment));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.widget_name = JdbcWritableBridge.readString(2, __dbResults);
    this.price = JdbcWritableBridge.readBigDecimal(3, __dbResults);
    this.design_date = JdbcWritableBridge.readDate(4, __dbResults);
    this.version = JdbcWritableBridge.readInteger(5, __dbResults);
    this.design_comment = JdbcWritableBridge.readString(6, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(id, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(widget_name, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(price, 3 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeDate(design_date, 4 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeInteger(version, 5 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(design_comment, 6 + __off, 12, __dbStmt);
    return 6;
  }
  public void readFields(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.id = null;
    } else {
    this.id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.widget_name = null;
    } else {
    this.widget_name = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.price = null;
    } else {
    this.price = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.design_date = null;
    } else {
    this.design_date = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.version = null;
    } else {
    this.version = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.design_comment = null;
    } else {
    this.design_comment = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.id);
    }
    if (null == this.widget_name) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, widget_name);
    }
    if (null == this.price) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.price, __dataOut);
    }
    if (null == this.design_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.design_date.getTime());
    }
    if (null == this.version) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.version);
    }
    if (null == this.design_comment) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, design_comment);
    }
  }
  private final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(widget_name==null?"null":widget_name, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(price==null?"null":"" + price, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(design_date==null?"null":"" + design_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(version==null?"null":"" + version, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(design_comment==null?"null":design_comment, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  private final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str;
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.widget_name = null; } else {
      this.widget_name = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.price = null; } else {
      this.price = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.design_date = null; } else {
      this.design_date = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.version = null; } else {
      this.version = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.design_comment = null; } else {
      this.design_comment = __cur_str;
    }

  }

  public Object clone() throws CloneNotSupportedException {
    Widget o = (Widget) super.clone();
    o.design_date = (o.design_date != null) ? (java.sql.Date) o.design_date.clone() : null;
    return o;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("widget_name", this.widget_name);
    __sqoop$field_map.put("price", this.price);
    __sqoop$field_map.put("design_date", this.design_date);
    __sqoop$field_map.put("version", this.version);
    __sqoop$field_map.put("design_comment", this.design_comment);
    return __sqoop$field_map;
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("id".equals(__fieldName)) {
      this.id = (Integer) __fieldVal;
    }
    else    if ("widget_name".equals(__fieldName)) {
      this.widget_name = (String) __fieldVal;
    }
    else    if ("price".equals(__fieldName)) {
      this.price = (java.math.BigDecimal) __fieldVal;
    }
    else    if ("design_date".equals(__fieldName)) {
      this.design_date = (java.sql.Date) __fieldVal;
    }
    else    if ("version".equals(__fieldName)) {
      this.version = (Integer) __fieldVal;
    }
    else    if ("design_comment".equals(__fieldName)) {
      this.design_comment = (String) __fieldVal;
    }
    else {
      throw new RuntimeException("No such field: " + __fieldName);
    }
  }
}

//=*=*=*=*
//./ch16/src/main/java/fm/last/hadoop/io/records/TrackStats.java
// File generated by hadoop record compiler. Do not edit.
package fm.last.hadoop.io.records;

public class TrackStats extends org.apache.hadoop.record.Record {
  private static final org.apache.hadoop.record.meta.RecordTypeInfo _rio_recTypeInfo;
  private static org.apache.hadoop.record.meta.RecordTypeInfo _rio_rtiFilter;
  private static int[] _rio_rtiFilterFields;
  static {
    _rio_recTypeInfo = new org.apache.hadoop.record.meta.RecordTypeInfo("TrackStats");
    _rio_recTypeInfo.addField("listeners", org.apache.hadoop.record.meta.TypeID.IntTypeID);
    _rio_recTypeInfo.addField("plays", org.apache.hadoop.record.meta.TypeID.IntTypeID);
    _rio_recTypeInfo.addField("scrobbles", org.apache.hadoop.record.meta.TypeID.IntTypeID);
    _rio_recTypeInfo.addField("radioPlays", org.apache.hadoop.record.meta.TypeID.IntTypeID);
    _rio_recTypeInfo.addField("skips", org.apache.hadoop.record.meta.TypeID.IntTypeID);
  }
  
  private int listeners;
  private int plays;
  private int scrobbles;
  private int radioPlays;
  private int skips;
  public TrackStats() { }
  public TrackStats(
    final int listeners,
    final int plays,
    final int scrobbles,
    final int radioPlays,
    final int skips) {
    this.listeners = listeners;
    this.plays = plays;
    this.scrobbles = scrobbles;
    this.radioPlays = radioPlays;
    this.skips = skips;
  }
  public static org.apache.hadoop.record.meta.RecordTypeInfo getTypeInfo() {
    return _rio_recTypeInfo;
  }
  public static void setTypeFilter(org.apache.hadoop.record.meta.RecordTypeInfo rti) {
    if (null == rti) return;
    _rio_rtiFilter = rti;
    _rio_rtiFilterFields = null;
  }
  private static void setupRtiFields()
  {
    if (null == _rio_rtiFilter) return;
    // we may already have done this
    if (null != _rio_rtiFilterFields) return;
    int _rio_i, _rio_j;
    _rio_rtiFilterFields = new int [_rio_rtiFilter.getFieldTypeInfos().size()];
    for (_rio_i=0; _rio_i<_rio_rtiFilterFields.length; _rio_i++) {
      _rio_rtiFilterFields[_rio_i] = 0;
    }
    java.util.Iterator<org.apache.hadoop.record.meta.FieldTypeInfo> _rio_itFilter = _rio_rtiFilter.getFieldTypeInfos().iterator();
    _rio_i=0;
    while (_rio_itFilter.hasNext()) {
      org.apache.hadoop.record.meta.FieldTypeInfo _rio_tInfoFilter = _rio_itFilter.next();
      java.util.Iterator<org.apache.hadoop.record.meta.FieldTypeInfo> _rio_it = _rio_recTypeInfo.getFieldTypeInfos().iterator();
      _rio_j=1;
      while (_rio_it.hasNext()) {
        org.apache.hadoop.record.meta.FieldTypeInfo _rio_tInfo = _rio_it.next();
        if (_rio_tInfo.equals(_rio_tInfoFilter)) {
          _rio_rtiFilterFields[_rio_i] = _rio_j;
          break;
        }
        _rio_j++;
      }
      _rio_i++;
    }
  }
  public int getListeners() {
    return listeners;
  }
  public void setListeners(final int listeners) {
    this.listeners=listeners;
  }
  public int getPlays() {
    return plays;
  }
  public void setPlays(final int plays) {
    this.plays=plays;
  }
  public int getScrobbles() {
    return scrobbles;
  }
  public void setScrobbles(final int scrobbles) {
    this.scrobbles=scrobbles;
  }
  public int getRadioPlays() {
    return radioPlays;
  }
  public void setRadioPlays(final int radioPlays) {
    this.radioPlays=radioPlays;
  }
  public int getSkips() {
    return skips;
  }
  public void setSkips(final int skips) {
    this.skips=skips;
  }
  public void serialize(final org.apache.hadoop.record.RecordOutput _rio_a, final String _rio_tag)
  throws java.io.IOException {
    _rio_a.startRecord(this,_rio_tag);
    _rio_a.writeInt(listeners,"listeners");
    _rio_a.writeInt(plays,"plays");
    _rio_a.writeInt(scrobbles,"scrobbles");
    _rio_a.writeInt(radioPlays,"radioPlays");
    _rio_a.writeInt(skips,"skips");
    _rio_a.endRecord(this,_rio_tag);
  }
  private void deserializeWithoutFilter(final org.apache.hadoop.record.RecordInput _rio_a, final String _rio_tag)
  throws java.io.IOException {
    _rio_a.startRecord(_rio_tag);
    listeners=_rio_a.readInt("listeners");
    plays=_rio_a.readInt("plays");
    scrobbles=_rio_a.readInt("scrobbles");
    radioPlays=_rio_a.readInt("radioPlays");
    skips=_rio_a.readInt("skips");
    _rio_a.endRecord(_rio_tag);
  }
  public void deserialize(final org.apache.hadoop.record.RecordInput _rio_a, final String _rio_tag)
  throws java.io.IOException {
    if (null == _rio_rtiFilter) {
      deserializeWithoutFilter(_rio_a, _rio_tag);
      return;
    }
    // if we're here, we need to read based on version info
    _rio_a.startRecord(_rio_tag);
    setupRtiFields();
    for (int _rio_i=0; _rio_i<_rio_rtiFilter.getFieldTypeInfos().size(); _rio_i++) {
      if (1 == _rio_rtiFilterFields[_rio_i]) {
        listeners=_rio_a.readInt("listeners");
      }
      else if (2 == _rio_rtiFilterFields[_rio_i]) {
        plays=_rio_a.readInt("plays");
      }
      else if (3 == _rio_rtiFilterFields[_rio_i]) {
        scrobbles=_rio_a.readInt("scrobbles");
      }
      else if (4 == _rio_rtiFilterFields[_rio_i]) {
        radioPlays=_rio_a.readInt("radioPlays");
      }
      else if (5 == _rio_rtiFilterFields[_rio_i]) {
        skips=_rio_a.readInt("skips");
      }
      else {
        java.util.ArrayList<org.apache.hadoop.record.meta.FieldTypeInfo> typeInfos = (java.util.ArrayList<org.apache.hadoop.record.meta.FieldTypeInfo>)(_rio_rtiFilter.getFieldTypeInfos());
        org.apache.hadoop.record.meta.Utils.skip(_rio_a, typeInfos.get(_rio_i).getFieldID(), typeInfos.get(_rio_i).getTypeID());
      }
    }
    _rio_a.endRecord(_rio_tag);
  }
  public int compareTo (final Object _rio_peer_) throws ClassCastException {
    if (!(_rio_peer_ instanceof TrackStats)) {
      throw new ClassCastException("Comparing different types of records.");
    }
    TrackStats _rio_peer = (TrackStats) _rio_peer_;
    int _rio_ret = 0;
    _rio_ret = (listeners == _rio_peer.listeners)? 0 :((listeners<_rio_peer.listeners)?-1:1);
    if (_rio_ret != 0) return _rio_ret;
    _rio_ret = (plays == _rio_peer.plays)? 0 :((plays<_rio_peer.plays)?-1:1);
    if (_rio_ret != 0) return _rio_ret;
    _rio_ret = (scrobbles == _rio_peer.scrobbles)? 0 :((scrobbles<_rio_peer.scrobbles)?-1:1);
    if (_rio_ret != 0) return _rio_ret;
    _rio_ret = (radioPlays == _rio_peer.radioPlays)? 0 :((radioPlays<_rio_peer.radioPlays)?-1:1);
    if (_rio_ret != 0) return _rio_ret;
    _rio_ret = (skips == _rio_peer.skips)? 0 :((skips<_rio_peer.skips)?-1:1);
    if (_rio_ret != 0) return _rio_ret;
    return _rio_ret;
  }
  public boolean equals(final Object _rio_peer_) {
    if (!(_rio_peer_ instanceof TrackStats)) {
      return false;
    }
    if (_rio_peer_ == this) {
      return true;
    }
    TrackStats _rio_peer = (TrackStats) _rio_peer_;
    boolean _rio_ret = false;
    _rio_ret = (listeners==_rio_peer.listeners);
    if (!_rio_ret) return _rio_ret;
    _rio_ret = (plays==_rio_peer.plays);
    if (!_rio_ret) return _rio_ret;
    _rio_ret = (scrobbles==_rio_peer.scrobbles);
    if (!_rio_ret) return _rio_ret;
    _rio_ret = (radioPlays==_rio_peer.radioPlays);
    if (!_rio_ret) return _rio_ret;
    _rio_ret = (skips==_rio_peer.skips);
    if (!_rio_ret) return _rio_ret;
    return _rio_ret;
  }
  public Object clone() throws CloneNotSupportedException {
    TrackStats _rio_other = new TrackStats();
    _rio_other.listeners = this.listeners;
    _rio_other.plays = this.plays;
    _rio_other.scrobbles = this.scrobbles;
    _rio_other.radioPlays = this.radioPlays;
    _rio_other.skips = this.skips;
    return _rio_other;
  }
  public int hashCode() {
    int _rio_result = 17;
    int _rio_ret;
    _rio_ret = (int)listeners;
    _rio_result = 37*_rio_result + _rio_ret;
    _rio_ret = (int)plays;
    _rio_result = 37*_rio_result + _rio_ret;
    _rio_ret = (int)scrobbles;
    _rio_result = 37*_rio_result + _rio_ret;
    _rio_ret = (int)radioPlays;
    _rio_result = 37*_rio_result + _rio_ret;
    _rio_ret = (int)skips;
    _rio_result = 37*_rio_result + _rio_ret;
    return _rio_result;
  }
  public static String signature() {
    return "LTrackStats(iiiii)";
  }
  public static class Comparator extends org.apache.hadoop.record.RecordComparator {
    public Comparator() {
      super(TrackStats.class);
    }
    static public int slurpRaw(byte[] b, int s, int l) {
      try {
        int os = s;
        {
          int i = org.apache.hadoop.record.Utils.readVInt(b, s);
          int z = org.apache.hadoop.record.Utils.getVIntSize(i);
          s+=z; l-=z;
        }
        {
          int i = org.apache.hadoop.record.Utils.readVInt(b, s);
          int z = org.apache.hadoop.record.Utils.getVIntSize(i);
          s+=z; l-=z;
        }
        {
          int i = org.apache.hadoop.record.Utils.readVInt(b, s);
          int z = org.apache.hadoop.record.Utils.getVIntSize(i);
          s+=z; l-=z;
        }
        {
          int i = org.apache.hadoop.record.Utils.readVInt(b, s);
          int z = org.apache.hadoop.record.Utils.getVIntSize(i);
          s+=z; l-=z;
        }
        {
          int i = org.apache.hadoop.record.Utils.readVInt(b, s);
          int z = org.apache.hadoop.record.Utils.getVIntSize(i);
          s+=z; l-=z;
        }
        return (os - s);
      } catch(java.io.IOException e) {
        throw new RuntimeException(e);
      }
    }
    static public int compareRaw(byte[] b1, int s1, int l1,
                                   byte[] b2, int s2, int l2) {
      try {
        int os1 = s1;
        {
          int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
          int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
          if (i1 != i2) {
            return ((i1-i2) < 0) ? -1 : 0;
          }
          int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);
          int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);
          s1+=z1; s2+=z2; l1-=z1; l2-=z2;
        }
        {
          int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
          int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
          if (i1 != i2) {
            return ((i1-i2) < 0) ? -1 : 0;
          }
          int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);
          int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);
          s1+=z1; s2+=z2; l1-=z1; l2-=z2;
        }
        {
          int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
          int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
          if (i1 != i2) {
            return ((i1-i2) < 0) ? -1 : 0;
          }
          int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);
          int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);
          s1+=z1; s2+=z2; l1-=z1; l2-=z2;
        }
        {
          int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
          int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
          if (i1 != i2) {
            return ((i1-i2) < 0) ? -1 : 0;
          }
          int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);
          int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);
          s1+=z1; s2+=z2; l1-=z1; l2-=z2;
        }
        {
          int i1 = org.apache.hadoop.record.Utils.readVInt(b1, s1);
          int i2 = org.apache.hadoop.record.Utils.readVInt(b2, s2);
          if (i1 != i2) {
            return ((i1-i2) < 0) ? -1 : 0;
          }
          int z1 = org.apache.hadoop.record.Utils.getVIntSize(i1);
          int z2 = org.apache.hadoop.record.Utils.getVIntSize(i2);
          s1+=z1; s2+=z2; l1-=z1; l2-=z2;
        }
        return (os1 - s1);
      } catch(java.io.IOException e) {
        throw new RuntimeException(e);
      }
    }
    public int compare(byte[] b1, int s1, int l1,
                         byte[] b2, int s2, int l2) {
      int ret = compareRaw(b1,s1,l1,b2,s2,l2);
      return (ret == -1)? -1 : ((ret==0)? 1 : 0);}
  }
  
  static {
    org.apache.hadoop.record.RecordComparator.define(TrackStats.class, new Comparator());
  }
}

//=*=*=*=*
//./ch16/src/main/java/fm/last/hadoop/programs/labs/trackstats/TrackStatisticsProgram.java
/*
 * Copyright 2008 Last.fm.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package fm.last.hadoop.programs.labs.trackstats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import fm.last.hadoop.io.records.TrackStats;

/**
 * Program that calculates various track-related statistics from raw listening data.
 */
public class TrackStatisticsProgram {

  public static final Log log = LogFactory.getLog(TrackStatisticsProgram.class);

  // values below indicate position in raw data for each value

  private static final int COL_USERID = 0;
  private static final int COL_TRACKID = 1;
  private static final int COL_SCROBBLES = 2;
  private static final int COL_RADIO = 3;
  private static final int COL_SKIP = 4;

  private Configuration conf;

  /**
   * Constructs a new TrackStatisticsProgram, using a default Configuration.
   */
  public TrackStatisticsProgram() {
    this.conf = new Configuration();
  }

  /**
   * Enumeration for Hadoop error counters.
   */
  private enum COUNTER_KEYS {
    INVALID_LINES, NOT_LISTEN
  };

  /**
   * Mapper that takes in raw listening data and outputs the number of unique listeners per track.
   */
  public static class UniqueListenersMapper extends MapReduceBase implements
      Mapper<LongWritable, Text, IntWritable, IntWritable> {

    public void map(LongWritable position, Text rawLine, OutputCollector<IntWritable, IntWritable> output,
        Reporter reporter) throws IOException {

      String line = (rawLine).toString();
      if (line.trim().isEmpty()) { // if the line is empty, report error and ignore
        reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
        return;
      }

      String[] parts = line.split(" "); // raw data is whitespace delimited
      try {
        int scrobbles = Integer.parseInt(parts[TrackStatisticsProgram.COL_SCROBBLES]);
        int radioListens = Integer.parseInt(parts[TrackStatisticsProgram.COL_RADIO]);
        if (scrobbles <= 0 && radioListens <= 0) {
          // if track somehow is marked with zero plays, report error and ignore
          reporter.incrCounter(COUNTER_KEYS.NOT_LISTEN, 1);
          return;
        }
        // if we get to here then user has listened to track, so output user id against track id
        IntWritable trackId = new IntWritable(Integer.parseInt(parts[TrackStatisticsProgram.COL_TRACKID]));
        IntWritable userId = new IntWritable(Integer.parseInt(parts[TrackStatisticsProgram.COL_USERID]));
        output.collect(trackId, userId);
      } catch (NumberFormatException e) {
        reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
        reporter.setStatus("Invalid line in listening data: " + rawLine);
        return;
      }
    }
  }

  /**
   * Combiner that improves efficiency by removing duplicate user ids from mapper output.
   */
  public static class UniqueListenersCombiner extends MapReduceBase implements
      Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void reduce(IntWritable trackId, Iterator<IntWritable> values,
        OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
      
      Set<IntWritable> userIds = new HashSet<IntWritable>();
      while (values.hasNext()) {
        IntWritable userId = values.next();
        if (!userIds.contains(userId)) {
          // if this user hasn't already been marked as listening to the track, add them to set and output them
          userIds.add(new IntWritable(userId.get()));
          output.collect(trackId, userId);
        }
      }
    }
  }

  /**
   * Reducer that outputs only unique listener ids per track (i.e. it removes any duplicated). Final output is number of
   * unique listeners per track.
   */
  public static class UniqueListenersReducer extends MapReduceBase implements
      Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void reduce(IntWritable trackId, Iterator<IntWritable> values,
        OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
      
      Set<Integer> userIds = new HashSet<Integer>();
      // add all userIds to the set, duplicates automatically removed (set contract)
      while (values.hasNext()) {
        IntWritable userId = values.next();
        userIds.add(Integer.valueOf(userId.get()));
      }
      // output trackId -> number of unique listeners per track
      output.collect(trackId, new IntWritable(userIds.size()));
    }

  }

  /**
   * Mapper that summarizes various statistics per track. Input is raw listening data, output is a partially filled in
   * TrackStatistics object per track id.
   */
  public static class SumMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, TrackStats> {

    public void map(LongWritable position, Text rawLine, OutputCollector<IntWritable, TrackStats> output,
        Reporter reporter) throws IOException {

      String line = (rawLine).toString();
      if (line.trim().isEmpty()) { // ignore empty lines
        reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
        return;
      }

      String[] parts = line.split(" ");
      try {
        int trackId = Integer.parseInt(parts[TrackStatisticsProgram.COL_TRACKID]);
        int scrobbles = Integer.parseInt(parts[TrackStatisticsProgram.COL_SCROBBLES]);
        int radio = Integer.parseInt(parts[TrackStatisticsProgram.COL_RADIO]);
        int skip = Integer.parseInt(parts[TrackStatisticsProgram.COL_SKIP]);
        // set number of listeners to 0 (this is calculated later) and other values as provided in text file
        TrackStats trackstat = new TrackStats(0, scrobbles + radio, scrobbles, radio, skip);
        output.collect(new IntWritable(trackId), trackstat);
      } catch (NumberFormatException e) {
        reporter.incrCounter(COUNTER_KEYS.INVALID_LINES, 1);
        log.warn("Invalid line in listening data: " + rawLine);
      }
    }
  }

  /**
   * Sum up the track statistics per track. Output is a TrackStatistics object per track id.
   */
  public static class SumReducer extends MapReduceBase implements
      Reducer<IntWritable, TrackStats, IntWritable, TrackStats> {

    @Override
    public void reduce(IntWritable trackId, Iterator<TrackStats> values,
        OutputCollector<IntWritable, TrackStats> output, Reporter reporter) throws IOException {
      
      TrackStats sum = new TrackStats(); // holds the totals for this track
      while (values.hasNext()) {
        TrackStats trackStats = (TrackStats) values.next();
        sum.setListeners(sum.getListeners() + trackStats.getListeners());
        sum.setPlays(sum.getPlays() + trackStats.getPlays());
        sum.setSkips(sum.getSkips() + trackStats.getSkips());
        sum.setScrobbles(sum.getScrobbles() + trackStats.getScrobbles());
        sum.setRadioPlays(sum.getRadioPlays() + trackStats.getRadioPlays());
      }
      output.collect(trackId, sum);
    }
  }

  /**
   * Mapper that takes the number of listeners for a track and converts this to a TrackStats object which is output
   * against each track id.
   */
  public static class MergeListenersMapper extends MapReduceBase implements
      Mapper<IntWritable, IntWritable, IntWritable, TrackStats> {

    public void map(IntWritable trackId, IntWritable uniqueListenerCount,
        OutputCollector<IntWritable, TrackStats> output, Reporter reporter) throws IOException {
      
      TrackStats trackStats = new TrackStats();
      trackStats.setListeners(uniqueListenerCount.get());
      output.collect(trackId, trackStats);
    }
  }

  /**
   * Create a JobConf for a Job that will calculate the number of unique listeners per track.
   * 
   * @param inputDir The path to the folder containing the raw listening data files.
   * @return The unique listeners JobConf.
   */
  private JobConf getUniqueListenersJobConf(Path inputDir) {
    log.info("Creating configuration for unique listeners Job");

    // output results to a temporary intermediate folder, this will get deleted by start() method
    Path uniqueListenersOutput = new Path("uniqueListeners");

    JobConf conf = new JobConf(TrackStatisticsProgram.class);
    conf.setOutputKeyClass(IntWritable.class); // track id
    conf.setOutputValueClass(IntWritable.class); // number of unique listeners
    conf.setInputFormat(TextInputFormat.class); // raw listening data
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setMapperClass(UniqueListenersMapper.class);
    conf.setCombinerClass(UniqueListenersCombiner.class);
    conf.setReducerClass(UniqueListenersReducer.class);

    FileInputFormat.addInputPath(conf, inputDir);
    FileOutputFormat.setOutputPath(conf, uniqueListenersOutput);
    conf.setJobName("uniqueListeners");
    return conf;
  }

  /**
   * Creates a JobConf for a Job that will sum up the TrackStatistics per track.
   * 
   * @param inputDir The path to the folder containing the raw input data files.
   * @return The sum JobConf.
   */
  private JobConf getSumJobConf(Path inputDir) {
    log.info("Creating configuration for sum job");
    // output results to a temporary intermediate folder, this will get deleted by start() method
    Path playsOutput = new Path("sum");

    JobConf conf = new JobConf(TrackStatisticsProgram.class);
    conf.setOutputKeyClass(IntWritable.class); // track id
    conf.setOutputValueClass(TrackStats.class); // statistics for a track
    conf.setInputFormat(TextInputFormat.class); // raw listening data
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setMapperClass(SumMapper.class);
    conf.setCombinerClass(SumReducer.class);
    conf.setReducerClass(SumReducer.class);

    FileInputFormat.addInputPath(conf, inputDir);
    FileOutputFormat.setOutputPath(conf, playsOutput);
    conf.setJobName("sum");
    return conf;
  }

  /**
   * Creates a JobConf for a Job that will merge the unique listeners and track statistics.
   * 
   * @param outputPath The path for the results to be output to.
   * @param sumInputDir The path containing the data from the sum Job.
   * @param listenersInputDir The path containing the data from the unique listeners job.
   * @return The merge JobConf.
   */
  private JobConf getMergeConf(Path outputPath, Path sumInputDir, Path listenersInputDir) {
    log.info("Creating configuration for merge job");
    JobConf conf = new JobConf(TrackStatisticsProgram.class);
    conf.setOutputKeyClass(IntWritable.class); // track id
    conf.setOutputValueClass(TrackStats.class); // overall track statistics
    conf.setCombinerClass(SumReducer.class); // safe to re-use reducer as a combiner here
    conf.setReducerClass(SumReducer.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileOutputFormat.setOutputPath(conf, outputPath);

    MultipleInputs.addInputPath(conf, sumInputDir, SequenceFileInputFormat.class, IdentityMapper.class);
    MultipleInputs.addInputPath(conf, listenersInputDir, SequenceFileInputFormat.class, MergeListenersMapper.class);
    conf.setJobName("merge");
    return conf;
  }

  /**
   * Start the program.
   * 
   * @param inputDir The path to the folder containing the raw listening data files.
   * @param outputPath The path for the results to be output to.
   * @throws IOException If an error occurs retrieving data from the file system or an error occurs running the job.
   */
  public void start(Path inputDir, Path outputDir) throws IOException {
    FileSystem fs = FileSystem.get(this.conf);

    JobConf uniqueListenersConf = getUniqueListenersJobConf(inputDir);
    Path listenersOutputDir = FileOutputFormat.getOutputPath(uniqueListenersConf);
    Job listenersJob = new Job(uniqueListenersConf);
    // delete any output that might exist from a previous run of this job
    if (fs.exists(FileOutputFormat.getOutputPath(uniqueListenersConf))) {
      fs.delete(FileOutputFormat.getOutputPath(uniqueListenersConf), true);
    }

    JobConf sumConf = getSumJobConf(inputDir);
    Path sumOutputDir = FileOutputFormat.getOutputPath(sumConf);
    Job sumJob = new Job(sumConf);
    // delete any output that might exist from a previous run of this job
    if (fs.exists(FileOutputFormat.getOutputPath(sumConf))) {
      fs.delete(FileOutputFormat.getOutputPath(sumConf), true);
    }

    // the merge job depends on the other two jobs
    ArrayList<Job> mergeDependencies = new ArrayList<Job>();
    mergeDependencies.add(listenersJob);
    mergeDependencies.add(sumJob);
    JobConf mergeConf = getMergeConf(outputDir, sumOutputDir, listenersOutputDir);
    Job mergeJob = new Job(mergeConf, mergeDependencies);
    // delete any output that might exist from a previous run of this job
    if (fs.exists(FileOutputFormat.getOutputPath(mergeConf))) {
      fs.delete(FileOutputFormat.getOutputPath(mergeConf), true);
    }

    // store the output paths of the intermediate jobs so this can be cleaned up after a successful run
    List<Path> deletePaths = new ArrayList<Path>();
    deletePaths.add(FileOutputFormat.getOutputPath(uniqueListenersConf));
    deletePaths.add(FileOutputFormat.getOutputPath(sumConf));

    JobControl control = new JobControl("TrackStatisticsProgram");
    control.addJob(listenersJob);
    control.addJob(sumJob);
    control.addJob(mergeJob);

    // execute the jobs
    try {
      Thread jobControlThread = new Thread(control, "jobcontrol");
      jobControlThread.start();
      while (!control.allFinished()) {
        Thread.sleep(1000);
      }
      if (control.getFailedJobs().size() > 0) {
        throw new IOException("One or more jobs failed");
      }
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for job control to finish", e);
    }

    // remove intermediate output paths
    for (Path deletePath : deletePaths) {
      fs.delete(deletePath, true);
    }
  }

  /**
   * Set the Configuration used by this Program.
   * 
   * @param conf The new Configuration to use by this program.
   */
  public void setConf(Configuration conf) {
    this.conf = conf; // this will usually only be set by unit test.
  }

  /**
   * Gets the Configuration used by this program.
   * 
   * @return This program's Configuration.
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Main method used to run the TrackStatisticsProgram from the command line. This takes two parameters - first the
   * path to the folder containing the raw input data; and second the path for the data to be output to.
   * 
   * @param args Command line arguments.
   * @throws IOException If an error occurs running the program.
   */
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      log.info("Args: <input directory> <output directory>");
      return;
    }

    Path inputPath = new Path(args[0]);
    Path outputDir = new Path(args[1]);
    log.info("Running on input directories: " + inputPath);
    TrackStatisticsProgram listeners = new TrackStatisticsProgram();
    listeners.start(inputPath, outputDir);
  }

}

//=*=*=*=*
//./common/src/main/java/JobBuilder.java
// == JobBuilder
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

public class JobBuilder {
  
  private final Class<?> driverClass;
  private final Job job;
  private final int extraArgCount;
  private final String extrArgsUsage;
  
  private String[] extraArgs;
  
  public JobBuilder(Class<?> driverClass) throws IOException {
    this(driverClass, 0, "");
  }
  
  public JobBuilder(Class<?> driverClass, int extraArgCount, String extrArgsUsage) throws IOException {
    this.driverClass = driverClass;
    this.extraArgCount = extraArgCount;
    this.job = new Job();
    this.job.setJarByClass(driverClass);
    this.extrArgsUsage = extrArgsUsage;
  }

  // vv JobBuilder
  public static Job parseInputAndOutput(Tool tool, Configuration conf,
      String[] args) throws IOException {
    
    if (args.length != 2) {
      printUsage(tool, "<input> <output>");
      return null;
    }
    Job job = new Job(conf);
    job.setJarByClass(tool.getClass());
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job;
  }

  public static void printUsage(Tool tool, String extraArgsUsage) {
    System.err.printf("Usage: %s [genericOptions] %s\n\n",
        tool.getClass().getSimpleName(), extraArgsUsage);
    GenericOptionsParser.printGenericCommandUsage(System.err);
  }
  // ^^ JobBuilder
  
  public JobBuilder withCommandLineArgs(String... args) throws IOException {
    Configuration conf = job.getConfiguration();
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    String[] otherArgs = parser.getRemainingArgs();
    if (otherArgs.length < 2 && otherArgs.length > 3 + extraArgCount) {
      System.err.printf("Usage: %s [genericOptions] [-overwrite] <input path> <output path> %s\n\n",
          driverClass.getSimpleName(), extrArgsUsage);
      GenericOptionsParser.printGenericCommandUsage(System.err);
      System.exit(-1);
    }
    int index = 0;
    boolean overwrite = false;
    if (otherArgs[index].equals("-overwrite")) {
      overwrite = true;
      index++;
    }
    Path input = new Path(otherArgs[index++]);
    Path output = new Path(otherArgs[index++]);
    
    if (index < otherArgs.length) {
      extraArgs = new String[otherArgs.length - index];
      System.arraycopy(otherArgs, index, extraArgs, 0, otherArgs.length - index);
    }
    
    if (overwrite) {
      output.getFileSystem(conf).delete(output, true);
    }
    
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);
    return this;
  }
  
  public Job build() {
    return job;
  }
  
  public String[] getExtraArgs() {
    return extraArgs;
  }
}

//=*=*=*=*
//./common/src/main/java/MetOfficeRecordParser.java
import java.math.*;
import org.apache.hadoop.io.Text;

public class MetOfficeRecordParser {
  
  private String year;
  private String airTemperatureString;
  private int airTemperature;
  private boolean airTemperatureValid;
  
  public void parse(String record) {
    if (record.length() < 18) {
      return;
    }
    year = record.substring(3, 7);
    if (isValidRecord(year)) {
      airTemperatureString = record.substring(13, 18);
      if (!airTemperatureString.trim().equals("---")) {
        BigDecimal temp = new BigDecimal(airTemperatureString.trim());
        temp = temp.multiply(new BigDecimal(BigInteger.TEN));
        airTemperature = temp.intValueExact();
        airTemperatureValid = true;
      }
    }
  }
  
  private boolean isValidRecord(String year) {
    try {
      Integer.parseInt(year);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public void parse(Text record) {
    parse(record.toString());
  }
  
  public String getYear() {
    return year;
  }

  public int getAirTemperature() {
    return airTemperature;
  }
  
  public String getAirTemperatureString() {
    return airTemperatureString;
  }

  public boolean isValidTemperature() {
    return airTemperatureValid;
  }

}

//=*=*=*=*
//./common/src/main/java/NcdcRecordParser.java
import java.text.*;
import java.util.Date;

import org.apache.hadoop.io.Text;

public class NcdcRecordParser {
  
  private static final int MISSING_TEMPERATURE = 9999;
  
  private static final DateFormat DATE_FORMAT =
    new SimpleDateFormat("yyyyMMddHHmm");
  
  private String stationId;
  private String observationDateString;
  private String year;
  private String airTemperatureString;
  private int airTemperature;
  private boolean airTemperatureMalformed;
  private String quality;
  
  public void parse(String record) {
    stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
    observationDateString = record.substring(15, 27);
    year = record.substring(15, 19);
    airTemperatureMalformed = false;
    // Remove leading plus sign as parseInt doesn't like them
    if (record.charAt(87) == '+') {
      airTemperatureString = record.substring(88, 92);
      airTemperature = Integer.parseInt(airTemperatureString);
    } else if (record.charAt(87) == '-') {
      airTemperatureString = record.substring(87, 92);
      airTemperature = Integer.parseInt(airTemperatureString);
    } else {
      airTemperatureMalformed = true;
    }
    airTemperature = Integer.parseInt(airTemperatureString);
    quality = record.substring(92, 93);
  }
  
  public void parse(Text record) {
    parse(record.toString());
  }
  
  public boolean isValidTemperature() {
    return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE
        && quality.matches("[01459]");
  }
  
  public boolean isMalformedTemperature() {
    return airTemperatureMalformed;
  }
  
  public boolean isMissingTemperature() {
    return airTemperature == MISSING_TEMPERATURE;
  }
  
  public String getStationId() {
    return stationId;
  }
  
  public Date getObservationDate() {
    try {
      System.out.println(observationDateString);
      return DATE_FORMAT.parse(observationDateString);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public String getYear() {
    return year;
  }

  public int getYearInt() {
    return Integer.parseInt(year);
  }

  public int getAirTemperature() {
    return airTemperature;
  }
  
  public String getAirTemperatureString() {
    return airTemperatureString;
  }

  public String getQuality() {
    return quality;
  }

}

//=*=*=*=*
//./common/src/main/java/NcdcStationMetadata.java
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IOUtils;

public class NcdcStationMetadata {
  
  private Map<String, String> stationIdToName = new HashMap<String, String>();

  public void initialize(File file) throws IOException {
    BufferedReader in = null;
    try {
      in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
      NcdcStationMetadataParser parser = new NcdcStationMetadataParser();
      String line;
      while ((line = in.readLine()) != null) {
        if (parser.parse(line)) {
          stationIdToName.put(parser.getStationId(), parser.getStationName());
        }
      }
    } finally {
      IOUtils.closeStream(in);
    }
  }

  public String getStationName(String stationId) {
    String stationName = stationIdToName.get(stationId);
    if (stationName == null || stationName.trim().length() == 0) {
      return stationId; // no match: fall back to ID
    }
    return stationName;
  }
  
  public Map<String, String> getStationIdToNameMap() {
    return Collections.unmodifiableMap(stationIdToName);
  }
  
}

//=*=*=*=*
//./common/src/main/java/NcdcStationMetadataParser.java
import org.apache.hadoop.io.Text;

public class NcdcStationMetadataParser {
  
  private String stationId;
  private String stationName;

  public boolean parse(String record) {
    if (record.length() < 42) { // header
      return false;
    }
    String usaf = record.substring(0, 6);
    String wban = record.substring(7, 12);
    stationId = usaf + "-" + wban;
    stationName = record.substring(13, 42);
    try {
      Integer.parseInt(usaf); // USAF identifiers are numeric
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
  
  public boolean parse(Text record) {
    return parse(record.toString());
  }
  
  public String getStationId() {
    return stationId;
  }

  public String getStationName() {
    return stationName;
  }
  
}

//=*=*=*=*
//./common/src/main/java/oldapi/JobBuilder.java
package oldapi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class JobBuilder {
  
  private final Class<?> driverClass;
  private final JobConf conf;
  private final int extraArgCount;
  private final String extrArgsUsage;
  
  private String[] extraArgs;
  
  public JobBuilder(Class<?> driverClass) {
    this(driverClass, 0, "");
  }
  
  public JobBuilder(Class<?> driverClass, int extraArgCount, String extrArgsUsage) {
    this.driverClass = driverClass;
    this.extraArgCount = extraArgCount;
    this.conf = new JobConf(driverClass);
    this.extrArgsUsage = extrArgsUsage;
  }

  public static JobConf parseInputAndOutput(Tool tool, Configuration conf,
      String[] args) {
    
    if (args.length != 2) {
      printUsage(tool, "<input> <output>");
      return null;
    }
    JobConf jobConf = new JobConf(conf, tool.getClass());
    FileInputFormat.addInputPath(jobConf, new Path(args[0]));
    FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));
    return jobConf;
  }

  public static void printUsage(Tool tool, String extraArgsUsage) {
    System.err.printf("Usage: %s [genericOptions] %s\n\n",
        tool.getClass().getSimpleName(), extraArgsUsage);
    GenericOptionsParser.printGenericCommandUsage(System.err);
  }
  
  public JobBuilder withCommandLineArgs(String... args) throws IOException {
    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
    String[] otherArgs = parser.getRemainingArgs();
    if (otherArgs.length < 2 && otherArgs.length > 3 + extraArgCount) {
      System.err.printf("Usage: %s [genericOptions] [-overwrite] <input path> <output path> %s\n\n",
          driverClass.getSimpleName(), extrArgsUsage);
      GenericOptionsParser.printGenericCommandUsage(System.err);
      System.exit(-1);
    }
    int index = 0;
    boolean overwrite = false;
    if (otherArgs[index].equals("-overwrite")) {
      overwrite = true;
      index++;
    }
    Path input = new Path(otherArgs[index++]);
    Path output = new Path(otherArgs[index++]);
    
    if (index < otherArgs.length) {
      extraArgs = new String[otherArgs.length - index];
      System.arraycopy(otherArgs, index, extraArgs, 0, otherArgs.length - index);
    }
    
    if (overwrite) {
      output.getFileSystem(conf).delete(output, true);
    }
    
    FileInputFormat.addInputPath(conf, input);
    FileOutputFormat.setOutputPath(conf, output);
    return this;
  }
  
  public JobConf build() {
    return conf;
  }
  
  public String[] getExtraArgs() {
    return extraArgs;
  }
}

//=*=*=*=*
//./common/src/main/java/oldapi/MetOfficeRecordParser.java
package oldapi;

import java.math.*;
import org.apache.hadoop.io.Text;

public class MetOfficeRecordParser {
  
  private String year;
  private String airTemperatureString;
  private int airTemperature;
  private boolean airTemperatureValid;
  
  public void parse(String record) {
    if (record.length() < 18) {
      return;
    }
    year = record.substring(3, 7);
    if (isValidRecord(year)) {
      airTemperatureString = record.substring(13, 18);
      if (!airTemperatureString.trim().equals("---")) {
        BigDecimal temp = new BigDecimal(airTemperatureString.trim());
        temp = temp.multiply(new BigDecimal(BigInteger.TEN));
        airTemperature = temp.intValueExact();
        airTemperatureValid = true;
      }
    }
  }
  
  private boolean isValidRecord(String year) {
    try {
      Integer.parseInt(year);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public void parse(Text record) {
    parse(record.toString());
  }
  
  public String getYear() {
    return year;
  }

  public int getAirTemperature() {
    return airTemperature;
  }
  
  public String getAirTemperatureString() {
    return airTemperatureString;
  }

  public boolean isValidTemperature() {
    return airTemperatureValid;
  }

}

//=*=*=*=*
//./common/src/main/java/oldapi/NcdcRecordParser.java
package oldapi;

import java.text.*;
import java.util.Date;

import org.apache.hadoop.io.Text;

public class NcdcRecordParser {
  
  private static final int MISSING_TEMPERATURE = 9999;
  
  private static final DateFormat DATE_FORMAT =
    new SimpleDateFormat("yyyyMMddHHmm");
  
  private String stationId;
  private String observationDateString;
  private String year;
  private String airTemperatureString;
  private int airTemperature;
  private boolean airTemperatureMalformed;
  private String quality;
  
  public void parse(String record) {
    stationId = record.substring(4, 10) + "-" + record.substring(10, 15);
    observationDateString = record.substring(15, 27);
    year = record.substring(15, 19);
    airTemperatureMalformed = false;
    // Remove leading plus sign as parseInt doesn't like them
    if (record.charAt(87) == '+') {
      airTemperatureString = record.substring(88, 92);
      airTemperature = Integer.parseInt(airTemperatureString);
    } else if (record.charAt(87) == '-') {
      airTemperatureString = record.substring(87, 92);
      airTemperature = Integer.parseInt(airTemperatureString);
    } else {
      airTemperatureMalformed = true;
    }
    airTemperature = Integer.parseInt(airTemperatureString);
    quality = record.substring(92, 93);
  }
  
  public void parse(Text record) {
    parse(record.toString());
  }
  
  public boolean isValidTemperature() {
    return !airTemperatureMalformed && airTemperature != MISSING_TEMPERATURE
        && quality.matches("[01459]");
  }
  
  public boolean isMalformedTemperature() {
    return airTemperatureMalformed;
  }
  
  public boolean isMissingTemperature() {
    return airTemperature == MISSING_TEMPERATURE;
  }
  
  public String getStationId() {
    return stationId;
  }
  
  public Date getObservationDate() {
    try {
      System.out.println(observationDateString);
      return DATE_FORMAT.parse(observationDateString);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public String getYear() {
    return year;
  }

  public int getYearInt() {
    return Integer.parseInt(year);
  }

  public int getAirTemperature() {
    return airTemperature;
  }
  
  public String getAirTemperatureString() {
    return airTemperatureString;
  }

  public String getQuality() {
    return quality;
  }

}

//=*=*=*=*
//./common/src/main/java/oldapi/NcdcStationMetadata.java
package oldapi;

import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IOUtils;

public class NcdcStationMetadata {
  
  private Map<String, String> stationIdToName = new HashMap<String, String>();

  public void initialize(File file) throws IOException {
    BufferedReader in = null;
    try {
      in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
      NcdcStationMetadataParser parser = new NcdcStationMetadataParser();
      String line;
      while ((line = in.readLine()) != null) {
        if (parser.parse(line)) {
          stationIdToName.put(parser.getStationId(), parser.getStationName());
        }
      }
    } finally {
      IOUtils.closeStream(in);
    }
  }

  public String getStationName(String stationId) {
    String stationName = stationIdToName.get(stationId);
    if (stationName == null || stationName.trim().length() == 0) {
      return stationId; // no match: fall back to ID
    }
    return stationName;
  }
  
  public Map<String, String> getStationIdToNameMap() {
    return Collections.unmodifiableMap(stationIdToName);
  }
  
}

//=*=*=*=*
//./common/src/main/java/oldapi/NcdcStationMetadataParser.java
package oldapi;

import org.apache.hadoop.io.Text;

public class NcdcStationMetadataParser {
  
  private String stationId;
  private String stationName;

  public boolean parse(String record) {
    if (record.length() < 42) { // header
      return false;
    }
    String usaf = record.substring(0, 6);
    String wban = record.substring(7, 12);
    stationId = usaf + "-" + wban;
    stationName = record.substring(13, 42);
    try {
      Integer.parseInt(usaf); // USAF identifiers are numeric
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
  
  public boolean parse(Text record) {
    return parse(record.toString());
  }
  
  public String getStationId() {
    return stationId;
  }

  public String getStationName() {
    return stationName;
  }
  
}

//=*=*=*=*
//./common/src/test/java/MetOfficeRecordParserTest.java
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.*;

public class MetOfficeRecordParserTest {
  
  private MetOfficeRecordParser parser;

  @Before
  public void setUp() {
    parser = new MetOfficeRecordParser();
  }
  
  @Test
  public void parsesValidRecord() {
    parser.parse("   1978   1    7.5     2.0       6   134.1    64.7");
    assertThat(parser.getYear(), is("1978"));
    assertThat(parser.getAirTemperature(), is(75));
    assertThat(parser.getAirTemperatureString(), is("  7.5"));
    assertThat(parser.isValidTemperature(), is(true));
  }
  
  @Test
  public void parsesNegativeTemperature() {
    parser.parse("   1978   1  -17.5     2.0       6   134.1    64.7");
    assertThat(parser.getYear(), is("1978"));
    assertThat(parser.getAirTemperature(), is(-175));
    assertThat(parser.getAirTemperatureString(), is("-17.5"));
    assertThat(parser.isValidTemperature(), is(true));
  }
  
  @Test
  public void parsesMissingTemperature() {
    parser.parse("   1853   1    ---     ---     ---    57.3     ---");
    assertThat(parser.getAirTemperatureString(), is("  ---"));
    assertThat(parser.isValidTemperature(), is(false));
  }
  
  @Test
  public void parsesHeaderLine() {
    parser.parse("Cardiff Bute Park");
    assertThat(parser.isValidTemperature(), is(false));
  }
  
  @Test(expected=NumberFormatException.class)
  public void cannotParseMalformedTemperature() {
    parser.parse("   1978   1    X.5     2.0       6   134.1    64.7");
  }
}

//=*=*=*=*
//./common/src/test/java/NcdcRecordParserTest.java
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.*;

public class NcdcRecordParserTest {
  
  private NcdcRecordParser parser;

  @Before
  public void setUp() {
    parser = new NcdcRecordParser();
  }
  
  @Test
  public void parsesValidRecord() {
    parser.parse("0043011990999991950051512004+68750+023550FM-12+038299999V0203201N00671220001CN9999999N9+00221+99999999999");
    assertThat(parser.getStationId(), is("011990-99999"));
    assertThat(parser.getYear(), is("1950"));
    assertThat(parser.getAirTemperature(), is(22));
    assertThat(parser.getAirTemperatureString(), is("0022"));
    assertThat(parser.isValidTemperature(), is(true));
    assertThat(parser.getQuality(), is("1"));
  }
  
  @Test
  public void parsesMissingTemperature() {
    parser.parse("0043011990999991950051512004+68750+023550FM-12+038299999V0203201N00671220001CN9999999N9+99991+99999999999");
    assertThat(parser.getAirTemperature(), is(9999));
    assertThat(parser.getAirTemperatureString(), is("9999"));
    assertThat(parser.isValidTemperature(), is(false));
  }
  
  @Test(expected=NumberFormatException.class)
  public void cannotParseMalformedTemperature() {
    parser.parse("0043011990999991950051512004+68750+023550FM-12+038299999V0203201N00671220001CN9999999N9+XXXX1+99999999999");
  }
}

//=*=*=*=*
//./common/src/test/java/NcdcStationMetadataParserTest.java
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.*;

public class NcdcStationMetadataParserTest {
  
  private NcdcStationMetadataParser parser;

  @Before
  public void setUp() {
    parser = new NcdcStationMetadataParser();
  }
  
  @Test
  public void parsesValidRecord() {
    assertThat(parser.parse("715390 99999 MOOSE JAW CS                  CN CA SA CZMJ  +50317 -105550 +05770"), is(true));
    assertThat(parser.getStationId(), is("715390-99999"));
    assertThat(parser.getStationName().trim(), is("MOOSE JAW CS"));
  }
  
  @Test
  public void parsesHeader() {
    assertThat(parser.parse("Integrated Surface Database Station History, November 2007"), is(false));
  }
  
  public void parsesBlankLine() {
    assertThat(parser.parse(""), is(false));
  }
  
}

//=*=*=*=*
//./experimental/src/test/java/FileInputFormatTest.java
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.*;


public class FileInputFormatTest {
  private static final String BASE_PATH = "/Users/tom/workspace/htdg/input/fileinput";
  
  @Test(expected = IOException.class)
  @Ignore("See HADOOP-5588")
  public void directoryWithSubdirectory() throws Exception {
    JobConf conf = new JobConf();
    
    Path path = new Path(BASE_PATH, "dir");
    FileInputFormat.addInputPath(conf, path);

    conf.getInputFormat().getSplits(conf, 1);
  }
  
  @Test
  @Ignore("See HADOOP-5588")
  public void directoryWithSubdirectoryUsingGlob() throws Exception {
    JobConf conf = new JobConf();
    
    Path path = new Path(BASE_PATH, "dir/a*");
    FileInputFormat.addInputPath(conf, path);

    InputSplit[] splits = conf.getInputFormat().getSplits(conf, 1);
    assertThat(splits.length, is(1));
  }
  
  @Test
  public void inputPathProperty() throws Exception {
    JobConf conf = new JobConf();
    FileInputFormat.setInputPaths(conf, new Path("/{a,b}"), new Path("/{c,d}"));
    assertThat(conf.get("mapred.input.dir"), is("file:/{a\\,b},file:/{c\\,d}"));
  }
  
}

//=*=*=*=*
//./experimental/src/test/java/SplitTest.java
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.junit.*;

/**
 * Create a file with 4k blocksize, 3 blocks, lines 1023 bytes + 1 byte nl
 *   Make each line begin with its line number 01 to 12
 * Then expect to get 3 splits (one per block), and 4 records per split
 *   * each split corresponds exactly to one block
 * If lines are 1024 * 1.5 bytes long (in nl), then what do we get for each record?
 *   * Do we lose records?
 *   * If not then in general need to get end from another block?
 *   
 * How does compression fit in!?
 */
/*

Line
0   0
1   1024
2   2048
3   

 */
public class SplitTest {
  
  private static final Random r = new Random();
  
  private static final String[] lines1 = new String[120];
  static {
    for (int i = 0; i < lines1.length; i++) {
      char[] c = new char[1023];
      c[0] = Integer.toHexString(i % 16).charAt(0);
      for (int j = 1; j < c.length; j++) {
        c[j] = (char) (r.nextInt(26) + (int) 'a');
      }
      lines1[i] = new String(c);
    }
  }
  
  private static final String[] lines2 = new String[12];
  static {
    for (int i = 0; i < lines2.length; i++) {
      char[] c = new char[1023 + 512];
      c[0] = Integer.toHexString(i % 16).charAt(0);
      for (int j = 1; j < c.length; j++) {
        c[j] = (char) (r.nextInt(26) + (int) 'a');
      }
      lines2[i] = new String(c);
    }
  }
  
  private static MiniDFSCluster cluster; // use an in-process HDFS cluster for testing
  private static FileSystem fs;

  @BeforeClass
  public static void setUp() throws IOException {
    Configuration conf = new Configuration();
    if (System.getProperty("test.build.data") == null) {
      System.setProperty("test.build.data", "/tmp");
    }
    cluster = new MiniDFSCluster(conf, 1, true, null);
    fs = cluster.getFileSystem();
  }
  
  @AfterClass
  public static void tearDown() throws IOException {
    fs.close();
    cluster.shutdown();
  }
  
  @Test
  @Ignore("Needs more investigation")
  public void recordsCoincideWithBlocks() throws IOException {
    int recordLength = 1024;
    Path input = new Path("input");
    createFile(input, 12, recordLength);
    
    JobConf job = new JobConf();
    job.set("fs.default.name", fs.getUri().toString());
    FileInputFormat.addInputPath(job, input);
    InputFormat<LongWritable, Text> inputFormat = job.getInputFormat();
    InputSplit[] splits = inputFormat.getSplits(job, job.getNumMapTasks());
    
    assertThat(splits.length, is(3));
    checkSplit(splits[0], 0, 4096);
    checkSplit(splits[1], 4096, 4096);
    checkSplit(splits[2], 8192, 4096);
    
    checkRecordReader(inputFormat, splits[0], job, recordLength, 0, 4);
    checkRecordReader(inputFormat, splits[1], job, recordLength, 4, 8);
    checkRecordReader(inputFormat, splits[2], job, recordLength, 8, 12);
  }
  
  @Test
  public void recordsDontCoincideWithBlocks() throws IOException {
    int recordLength = 1024 + 512;
    Path input = new Path("input");
    createFile(input, 8, recordLength);
    
    JobConf job = new JobConf();
    job.set("fs.default.name", fs.getUri().toString());
    FileInputFormat.addInputPath(job, input);
    InputFormat<LongWritable, Text> inputFormat = job.getInputFormat();
    InputSplit[] splits = inputFormat.getSplits(job, job.getNumMapTasks());
    
    System.out.println(Arrays.asList(splits));
    checkSplit(splits[0], 0, 4096);
    checkSplit(splits[1], 4096, 4096);
    checkSplit(splits[2], 8192, 4096);
    
    checkRecordReader(inputFormat, splits[0], job, recordLength, 0, 3);
    checkRecordReader(inputFormat, splits[1], job, recordLength, 3, 6);
    checkRecordReader(inputFormat, splits[2], job, recordLength, 6, 8);

  }
  
  @Test
  @Ignore("Needs more investigation")
  public void compression() throws IOException {
    int recordLength = 1024;
    Path input = new Path("input.bz2");
    createFile(input, 24, recordLength);
    System.out.println(">>>>>>" + fs.getLength(input));
    
    JobConf job = new JobConf();
    job.set("fs.default.name", fs.getUri().toString());
    FileInputFormat.addInputPath(job, input);
    InputFormat<LongWritable, Text> inputFormat = job.getInputFormat();
    InputSplit[] splits = inputFormat.getSplits(job, job.getNumMapTasks());
    
    System.out.println(Arrays.asList(splits));
    assertThat(splits.length, is(2));
    checkSplit(splits[0], 0, 4096);
    checkSplit(splits[1], 4096, 4096);
    
    checkRecordReader(inputFormat, splits[0], job, recordLength, 0, 4);
    checkRecordReader(inputFormat, splits[1], job, recordLength, 5, 12);

  }


  private void checkSplit(InputSplit split, long start, long length) {
    assertThat(split, instanceOf(FileSplit.class));
    FileSplit fileSplit = (FileSplit) split;
    assertThat(fileSplit.getStart(), is(start));
    assertThat(fileSplit.getLength(), is(length));
  }
  
  private void checkRecord(int record, RecordReader<LongWritable, Text> recordReader, long expectedKey, String expectedValue)
      throws IOException {
    LongWritable key = new LongWritable();
    Text value = new Text();
    assertThat(recordReader.next(key, value), is(true));
    assertThat("Record " + record, value.toString(), is(expectedValue));
    assertThat("Record " + record, key.get(), is(expectedKey));
  }
  
  private void checkRecordReader(InputFormat<LongWritable, Text> inputFormat,
      InputSplit split, JobConf job, long recordLength, int startLine, int endLine) throws IOException {
    RecordReader<LongWritable, Text> recordReader =
      inputFormat.getRecordReader(split, job, Reporter.NULL);
    for (int i = startLine; i < endLine; i++) {
      checkRecord(i, recordReader, i * recordLength, line(i, recordLength));
    }
    assertThat(recordReader.next(new LongWritable(), new Text()), is(false));
  }

  private void createFile(Path input, int records, int recordLength) throws IOException {
    long fileSize = 4096;
    OutputStream out = fs.create(input, true, 4096, (short) 1, fileSize);
    CompressionCodecFactory codecFactory = new CompressionCodecFactory(new Configuration());
    CompressionCodec codec = codecFactory.getCodec(input);
    if (codec != null) {
      out = codec.createOutputStream(out);
    }
    Writer writer = new OutputStreamWriter(out);
    try {
      for (int n = 0; n < records; n++) {
        writer.write(line(n, recordLength));
        writer.write("\n");
      }
    } finally {
      writer.close();
    }
  }

  private String line(int i, long recordLength) {
    return recordLength == 1024 ? lines1[i] : lines2[i];
  }

}

//=*=*=*=*
//./experimental/src/test/java/crunch/CogroupCrunchTest.java
package crunch;
import static com.cloudera.crunch.type.writable.Writables.strings;
import static com.cloudera.crunch.type.writable.Writables.tableOf;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

import org.junit.Test;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.lib.Cogroup;
import com.cloudera.crunch.lib.Join;
import com.google.common.base.Splitter;

public class CogroupCrunchTest implements Serializable {
  
  @Test
  public void test() throws IOException {
    Pipeline pipeline = new MRPipeline(CogroupCrunchTest.class);
    PCollection<String> a = pipeline.readTextFile("join/A");
    PCollection<String> b = pipeline.readTextFile("join/B");
    
    PTable<String, String> aTable = a.parallelDo(new DoFn<String, Pair<String, String>>() {
		@Override
		public void process(String input, Emitter<Pair<String, String>> emitter) {
			Iterator<String> split = Splitter.on('\t').split(input).iterator();
			emitter.emit(Pair.of(split.next(), split.next()));
		}
	}, tableOf(strings(),strings()));

    PTable<String, String> bTable = b.parallelDo(new DoFn<String, Pair<String, String>>() {
		@Override
		public void process(String input, Emitter<Pair<String, String>> emitter) {
			Iterator<String> split = Splitter.on('\t').split(input).iterator();
			String l = split.next();
			String r = split.next();
			emitter.emit(Pair.of(r, l));
		}
	}, tableOf(strings(),strings()));
    
    PTable<String, Pair<Collection<String>, Collection<String>>> cogroup = Cogroup.cogroup(aTable, bTable);
    
    pipeline.writeTextFile(cogroup, "output-cogrouped");
    pipeline.run();
  }

}

//=*=*=*=*
//./experimental/src/test/java/crunch/JoinCrunchTest.java
package crunch;
import static com.cloudera.crunch.type.writable.Writables.strings;
import static com.cloudera.crunch.type.writable.Writables.tableOf;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

import org.junit.Test;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.lib.Join;
import com.google.common.base.Splitter;

public class JoinCrunchTest implements Serializable {
  
  @Test
  public void test() throws IOException {
    Pipeline pipeline = new MRPipeline(JoinCrunchTest.class);
    PCollection<String> a = pipeline.readTextFile("join/A");
    PCollection<String> b = pipeline.readTextFile("join/B");
    
    PTable<String, String> aTable = a.parallelDo(new DoFn<String, Pair<String, String>>() {
		@Override
		public void process(String input, Emitter<Pair<String, String>> emitter) {
			Iterator<String> split = Splitter.on('\t').split(input).iterator();
			emitter.emit(Pair.of(split.next(), split.next()));
		}
	}, tableOf(strings(),strings()));

    PTable<String, String> bTable = b.parallelDo(new DoFn<String, Pair<String, String>>() {
		@Override
		public void process(String input, Emitter<Pair<String, String>> emitter) {
			Iterator<String> split = Splitter.on('\t').split(input).iterator();
			String l = split.next();
			String r = split.next();
			emitter.emit(Pair.of(r, l));
		}
	}, tableOf(strings(),strings()));
    
    PTable<String, Pair<String, String>> join = Join.join(aTable, bTable);
    
    pipeline.writeTextFile(join, "output-joined");
    pipeline.run();
  }

}

//=*=*=*=*
//./experimental/src/test/java/crunch/MaxTemperatureCrunchTest.java
package crunch;
import static com.cloudera.crunch.type.writable.Writables.ints;
import static com.cloudera.crunch.type.writable.Writables.strings;
import static com.cloudera.crunch.type.writable.Writables.tableOf;

import java.io.IOException;

import org.junit.Test;

import com.cloudera.crunch.CombineFn;
import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;

public class MaxTemperatureCrunchTest {
  
  private static final int MISSING = 9999;
  
  @Test
  public void test() throws IOException {
    Pipeline pipeline = new MRPipeline(MaxTemperatureCrunchTest.class);
    PCollection<String> records = pipeline.readTextFile("input");
    
    PTable<String, Integer> maxTemps = records
      .parallelDo(toYearTempPairsFn(), tableOf(strings(), ints()))
      .groupByKey()
      .combineValues(CombineFn.<String> MAX_INTS());
    
    pipeline.writeTextFile(maxTemps, "output");
    pipeline.run();
  }

  private static DoFn<String, Pair<String, Integer>> toYearTempPairsFn() {
    return new DoFn<String, Pair<String, Integer>>() {
      @Override
      public void process(String input, Emitter<Pair<String, Integer>> emitter) {
        String line = input.toString();
        String year = line.substring(15, 19);
        int airTemperature;
        if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
          airTemperature = Integer.parseInt(line.substring(88, 92));
        } else {
          airTemperature = Integer.parseInt(line.substring(87, 92));
        }
        String quality = line.substring(92, 93);
        if (airTemperature != MISSING && quality.matches("[01459]")) {
          emitter.emit(Pair.of(year, airTemperature));
        }
      }
    };
  }

}

//=*=*=*=*
//./experimental/src/test/java/crunch/SortCrunchTest.java
package crunch;
import static com.cloudera.crunch.lib.Sort.ColumnOrder.by;
import static com.cloudera.crunch.lib.Sort.Order.ASCENDING;
import static com.cloudera.crunch.lib.Sort.Order.DESCENDING;
import static com.cloudera.crunch.type.writable.Writables.ints;
import static com.cloudera.crunch.type.writable.Writables.pairs;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

import org.junit.Test;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.lib.Sort;
import com.google.common.base.Splitter;

public class SortCrunchTest implements Serializable {
  
  @Test
  public void test() throws IOException {
    Pipeline pipeline = new MRPipeline(SortCrunchTest.class);
    PCollection<String> records = pipeline.readTextFile("sort/A");
    
    PCollection<Pair<Integer, Integer>> pairs = records.parallelDo(new DoFn<String, Pair<Integer, Integer>>() {
      @Override
      public void process(String input, Emitter<Pair<Integer, Integer>> emitter) {
        Iterator<String> split = Splitter.on('\t').split(input).iterator();
        String l = split.next();
        String r = split.next();
        emitter.emit(Pair.of(Integer.parseInt(l), Integer.parseInt(r)));
      }
    }, pairs(ints(), ints()));
    
    PCollection<Pair<Integer, Integer>> sorted = Sort.sortPairs(pairs, by(1, ASCENDING), by(2, DESCENDING));
    
    pipeline.writeTextFile(sorted, "output-sorted");
    pipeline.run();
  }

}

//=*=*=*=*
//./experimental/src/test/java/crunch/ToYearTempPairsFn.java
package crunch;
import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.Pair;

public class ToYearTempPairsFn extends DoFn<String, Pair<String, Integer>> {

    private static final int MISSING = 9999;
	  

    @Override
    public void process(String input, Emitter<Pair<String, Integer>> emitter) {
      String line = input.toString();
      String year = line.substring(15, 19);
      int airTemperature;
      if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
        airTemperature = Integer.parseInt(line.substring(88, 92));
      } else {
        airTemperature = Integer.parseInt(line.substring(87, 92));
      }
      String quality = line.substring(92, 93);
      if (airTemperature != MISSING && quality.matches("[01459]")) {
        emitter.emit(Pair.of(year, airTemperature));
      }
    }

}

//=*=*=*=*
//./snippet/src/test/java/ExamplesIT.java
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import junitx.framework.FileAssert;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.NotFileFilter;
import org.apache.commons.io.filefilter.OrFileFilter;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * This test runs the examples and checks that they produce the expected output.
 * It takes each input.txt file and runs it as a script, then tests that the
 * output produced is the same as all the files in output.
 */
@RunWith(Parameterized.class)
public class ExamplesIT {

  private static final File PROJECT_BASE_DIR =
    new File(System.getProperty("hadoop.book.basedir",
        "/Users/tom/book-workspace/hadoop-book"));
  
  private static final String MODE_PROPERTY = "example.mode";
  private static final String MODE_DEFAULT = "local";
  
  private static final String EXAMPLE_CHAPTERS_PROPERTY = "example.chapters";
  private static final String EXAMPLE_CHAPTERS_DEFAULT = "ch02,ch04,ch04-avro,ch05,ch07,ch08";

  private static final IOFileFilter HIDDEN_FILE_FILTER =
    new OrFileFilter(HiddenFileFilter.HIDDEN, new PrefixFileFilter("_"));
  private static final IOFileFilter NOT_HIDDEN_FILE_FILTER =
    new NotFileFilter(HIDDEN_FILE_FILTER);
  
  @Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> data = new ArrayList<Object[]>();
    String exampleDirs = System.getProperty(EXAMPLE_CHAPTERS_PROPERTY,
        EXAMPLE_CHAPTERS_DEFAULT);
    int i = 0;
    for (String dirName : Splitter.on(',').split(exampleDirs)) {
      File dir = new File(new File(PROJECT_BASE_DIR, dirName),
          "src/main/examples");
      if (!dir.exists()) {
        fail(dir + " does not exist");
      }
      for (File file : dir.listFiles()) {
        if (file.isDirectory()) {
          data.add(new Object[] { file });
          // so we can see which test corresponds to which file
          System.out.printf("%s: %s\n", i++, file);
        }
      }
    }
    return data;
  }
  
  private File example; // parameter
  private File actualOutputDir = new File(PROJECT_BASE_DIR, "output");
  private static Map<String, String> env;
  private static String version;
  private static String mode;
  
  public ExamplesIT(File example) {
    this.example = example;
  }
  
  @SuppressWarnings("unchecked")
  @BeforeClass
  public static void setUpClass() throws IOException {
    mode = System.getProperty(MODE_PROPERTY, MODE_DEFAULT);
    System.out.printf("mode=%s\n", mode);

    String hadoopHome = System.getenv("HADOOP_HOME");
    assertNotNull("Export the HADOOP_HOME environment variable " +
        "to run the snippet tests", hadoopHome);
    env = new HashMap<String, String>(EnvironmentUtils.getProcEnvironment());
    env.put("HADOOP_HOME", hadoopHome);
    env.put("PATH", env.get("HADOOP_HOME") + "/bin" + ":" + env.get("PATH"));
    env.put("HADOOP_CONF_DIR", "snippet/conf/" + mode);
    env.put("HADOOP_USER_CLASSPATH_FIRST", "true");
    env.put("HADOOP_CLASSPATH", "hadoop-examples.jar:avro-examples.jar");
    
    System.out.printf("HADOOP_HOME=%s\n", hadoopHome);
    
    String versionOut = execute(hadoopHome + "/bin/hadoop version");
    for (String line : Splitter.on("\n").split(versionOut)) {
      Matcher matcher = Pattern.compile("^Hadoop (.+)+$").matcher(line);
      if (matcher.matches()) {
        version = matcher.group(1);
      }
    }
    assertNotNull("Version not found", version);
    System.out.printf("version=%s\n", version);
    
  }
  
  @Before
  public void setUp() throws IOException {
    assumeTrue(!example.getPath().endsWith(".ignore"));
    execute(new File("src/test/resources/setup.sh").getAbsolutePath());
  }
  
  @Test
  public void test() throws Exception {
    System.out.println("Running " + example);
    
    File exampleDir = findBaseExampleDirectory(example);
    File inputFile = new File(exampleDir, "input.txt");
    System.out.println("Running input " + inputFile);
    
    String systemOut = execute(inputFile.getAbsolutePath());
    System.out.println(systemOut);
    
    execute(new File("src/test/resources/copyoutput.sh").getAbsolutePath());
    
    File expectedOutputDir = new File(exampleDir, "output");
    if (!expectedOutputDir.exists()) {
      FileUtils.copyDirectory(actualOutputDir, expectedOutputDir);
      fail(expectedOutputDir  + " does not exist - creating.");
    }
    
    List<File> expectedParts = Lists.newArrayList(
        FileUtils.listFiles(expectedOutputDir, NOT_HIDDEN_FILE_FILTER,
            NOT_HIDDEN_FILE_FILTER));
    List<File> actualParts = Lists.newArrayList(
        FileUtils.listFiles(actualOutputDir, NOT_HIDDEN_FILE_FILTER,
            NOT_HIDDEN_FILE_FILTER));
    assertEquals("Number of parts (got " + actualParts + ")",
        expectedParts.size(), actualParts.size());
    
    for (int i = 0; i < expectedParts.size(); i++) {
      File expectedFile = expectedParts.get(i);
      File actualFile = actualParts.get(i);
      if (expectedFile.getPath().endsWith(".gz")) {
        File expectedDecompressed = decompress(expectedFile);
        File actualDecompressed = decompress(actualFile);
        FileAssert.assertEquals(expectedFile.toString(),
            expectedDecompressed, actualDecompressed);
      } else if (expectedFile.getPath().endsWith(".avro")) {
        // Avro files have a random sync marker
        // so just check lengths for the moment
        assertEquals("Avro file length", expectedFile.length(),
            actualFile.length());
      } else {
        FileAssert.assertEquals(expectedFile.toString(),
            expectedFile, actualFile);
      }
    }
    System.out.println("Completed " + example);
  }
  
  private File findBaseExampleDirectory(File example) {
    // Look in base/<version>/<mode> then base/<version> then base/<mode>
    File[] candidates = {
        new File(new File(example, version), mode),
        new File(example, version),
        new File(example, mode),
    };
    for (File candidate : candidates) {
      if (candidate.exists()) {
        File inputFile = new File(candidate, "input.txt");
        // if no input file then skip test
        assumeTrue(inputFile.exists());
        return candidate;
      }
    }
    return example;
  }

  private static String execute(String commandLine) throws ExecuteException, IOException {
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    PumpStreamHandler psh = new PumpStreamHandler(stdout);
    CommandLine cl = CommandLine.parse("/bin/bash " + commandLine);
    DefaultExecutor exec = new DefaultExecutor();
    exec.setWorkingDirectory(PROJECT_BASE_DIR);
    exec.setStreamHandler(psh);
    try {
      exec.execute(cl, env);
    } catch (ExecuteException e) {
      System.out.println(stdout.toString());
      throw e;
    } catch (IOException e) {
      System.out.println(stdout.toString());
      throw e;
    }
    return stdout.toString();
  }

  private File decompress(File file) throws IOException {
    File decompressed = File.createTempFile(getClass().getSimpleName(), ".txt");
    decompressed.deleteOnExit();
    final GZIPInputStream in = new GZIPInputStream(new FileInputStream(file));
    try {
      Files.copy(new InputSupplier<InputStream>() {
          public InputStream getInput() throws IOException {
            return in;
          }
      }, decompressed);
    } finally {
      in.close();
    }
    return decompressed;
  }

}

//=*=*=*=*
