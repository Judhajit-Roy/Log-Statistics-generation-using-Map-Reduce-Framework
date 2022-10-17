//package scala
class task2 {

}
/**
 * This map reduce job takes in log files in a folder as input and outputs the number of error messages
 * divided across time intervals of n seconds where n is passed as a parameter while
 * running the program. The output is sorted in descending order of number of messages.
 **/
import HelperFunc.{CreateLogger}
import com.typesafe.config.ConfigFactory
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
import java.util.regex.Matcher
import java.util.regex.Pattern
import java.text.SimpleDateFormat

object runtask2:
  val logger = CreateLogger(classOf[task2])
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    private final val one = new IntWritable(1)
    val config = ConfigFactory.load()

    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      val type_pattern = Pattern.compile(config.getString("para.type_pattern"))
      val content_pattern = Pattern.compile(config.getString("para.content_pattern"))
      val timeRegex = Pattern.compile(config.getString("para.timeRegex"))
      val interval = config.getInt("para.interval")
      val type_matcher = type_pattern.matcher(line)
      val content_matcher = content_pattern.matcher(line)
      if (type_matcher.find() & content_matcher.find()) {
        if(type_matcher.group(0) == "ERROR") {
          val dateFormatter = new SimpleDateFormat("HH:mm:ss.SSS")
          val time_matcher = timeRegex.matcher(line)
          if (time_matcher.find()) {
            val f_time = dateFormatter.parse(time_matcher.group(0))
            output.collect(new Text((f_time.getTime.toInt / (1000 * interval)).toString), one)
          }
        }
      }

  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))

  class Map1 extends MapReduceBase with Mapper[LongWritable, Text, IntWritable,Text] :
    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[IntWritable,Text], reporter: Reporter): Unit =
      val line1: String = value.toString
      val instances = line1.split(",")
      output.collect(new IntWritable(instances(1).toInt), new Text(instances(0)))

  class Descending_sorter() extends WritableComparator(classOf[IntWritable], true) :
      override def compare(w1: WritableComparable[_], w2: WritableComparable[_]): Int =
        val k1 = w1.asInstanceOf[IntWritable]
        val k2 = w2.asInstanceOf[IntWritable]
        -1 * k1.compareTo(k2)

  class Reduce1 extends MapReduceBase with Reducer[IntWritable,Text, Text, IntWritable] :
    val config = ConfigFactory.load()
    override def reduce(key: IntWritable, values: util.Iterator[Text], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val dateFormatter = new SimpleDateFormat("HH:mm")
      val interval = config.getInt("para.interval")
      values.forEachRemaining(ep => {
        val timeinterval = dateFormatter.format(ep.toString.toInt*1000*interval) + "-"+  dateFormatter.format(ep.toString.toInt*1000*interval + 1000*interval)
        output.collect(new Text(timeinterval),key)
      })


  def main(inputPath: String,interPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("First Stage")
    conf.set("mapred.textoutputformat.separator", ",")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[Map])
    conf.setCombinerClass(classOf[Reduce])
    conf.setReducerClass(classOf[Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(interPath))
    logger.info("Starting Job1, to calculate sum of matched messages in the interval")
    val current_job = JobClient.runJob(conf)
    if current_job.isSuccessful then
      logger.info("Job1 Successful starting Stage 2")
      runMapReducefinal2(interPath,outputPath)

    def runMapReducefinal2(inputPath: String, outputPath: String) =
      val conf1: JobConf = new JobConf(this.getClass)
      conf1.setJobName("Second Stage")
      conf1.set("mapred.textoutputformat.separator", ",")
      conf1.set("mapreduce.job.maps", "1")
      conf1.set("mapreduce.job.reduces", "1")
      conf1.setMapperClass(classOf[Map1])
      conf1.setMapOutputKeyClass(classOf[IntWritable])
      conf1.setMapOutputValueClass(classOf[Text])
      conf1.setOutputKeyComparatorClass(classOf[Descending_sorter])
      conf1.setReducerClass(classOf[Reduce1])
      conf1.setOutputKeyClass(classOf[Text])
      conf1.setOutputValueClass(classOf[IntWritable])
      conf1.setInputFormat(classOf[TextInputFormat])
      conf1.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
      FileInputFormat.addInputPath(conf1, new Path(inputPath))
      FileOutputFormat.setOutputPath(conf1, new Path(outputPath))
      logger.info("Starting Job2, sorting the sum according to descending order")
      JobClient.runJob(conf1)


