class task4 {

}
/**
 * This task involves counting the number of characters in the longest message of each log type which matches the pattern given.
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.typesafe.config.{Config, ConfigFactory}
//import HelperFunc.{CreateLogger}

object runtask4:
//  val logger = CreateLogger(classOf[task4])
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, Text]:
    val config = ConfigFactory.load()

    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, Text], reporter: Reporter): Unit =
      val line: String = value.toString
      val type_pattern = Pattern.compile(config.getString("para.type_pattern"))
      val content_pattern = Pattern.compile(config.getString("para.content_pattern"))
      val type_matcher = type_pattern.matcher(line)
      val content_matcher = content_pattern.matcher(line)

      if (type_matcher.find() & content_matcher.find()) {
        val msg_type = type_matcher.group(0)
        val matched_string = content_matcher.group(0)
        output.collect(new Text(msg_type), new Text(matched_string))
      }

  class Reduce extends MapReduceBase with Reducer[Text, Text, Text, IntWritable]:
    def maxstring(s1: Text, s2: Text): Text =
      if s1.toString.length > s2.toString.length then s1 else s2

    override def reduce(key: Text, values: util.Iterator[Text], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
        val msg_len = values.asScala.reduce((value1,value2) => maxstring(value1,value2))
        output.collect(new Text(key.toString), new IntWritable(msg_len.toString.length))

  @main def main4(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("Task4")
    conf.set("mapred.textoutputformat.separator", ",")
    conf.setMapOutputKeyClass(classOf[Text])
    conf.setMapOutputValueClass(classOf[Text])
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map])
//    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
//    logger.info("Starting Job1, to calculate the length of longest matched string for the message types")
    JobClient.runJob(conf)
