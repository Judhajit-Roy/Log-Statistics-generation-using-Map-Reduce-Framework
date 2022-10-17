class task1 {

}
/**
 * This map reduce job takes in log files in a folder as input and outputs the number of messages of each log type
 * (INFO, DEBUG, WARN, ERROR) divided across time intervals of n seconds where n is passed as a parameter while
 * running the program.
 *
 */
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.*
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*
import scala.util.matching.Regex
import java.util.*
import java.time.*
import com.typesafe.config.{Config, ConfigFactory}
import HelperFunc.{CreateLogger}

object runtask1:
  val logger = CreateLogger(classOf[task1])
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    val config = ConfigFactory.load()
    private final val one = new IntWritable(1)
    private val word = new Text()

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      val type_pattern = config.getString("para.type_pattern").r
      val start_time = LocalTime.parse(config.getString("para.start_time"))
      val end_time = LocalTime.parse(config.getString("para.end_time"))
      if (line != "" & (LocalTime.parse(line.split(" ")(0)).isAfter(start_time) & LocalTime.parse(line.split(" ")(0)).isBefore(end_time) )) {
        line.split(" ").foreach {
          case token@type_pattern() => word.set(token)
            output.collect(word, one)
          case _ => "skip"
        }
      }

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key,  new IntWritable(sum.get()))
  
  @main def main1(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.set("mapred.textoutputformat.separator", ",")
    conf.setJobName("Task1")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    logger.info("Starting Job1, to calculate sum of the number of times the message type occurs in the interval")
    JobClient.runJob(conf)
