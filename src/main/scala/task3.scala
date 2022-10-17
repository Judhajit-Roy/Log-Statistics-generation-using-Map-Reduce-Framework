class task3 {

}
/**
 * This task involves counting the number of messages belonging to each message type.
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

object runtask3:
  val logger = CreateLogger(classOf[task3])
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]:
    val config = ConfigFactory.load()
    private final val one = new IntWritable(1)
    private val word = new Text()

    @throws[IOException]
    def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line: String = value.toString
      val type_pattern = config.getString("para.type_pattern").r

      line.split(" ").foreach {
        case token@type_pattern() => word.set(token)
          output.collect(word, one)
        case _ => "skip"
      }


  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]:
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key,  new IntWritable(sum.get()))

  def main(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("Task3")
    conf.set("mapreduce.output.textoutputformat.separator", ",")
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
    logger.info("Starting Job1, to calculate the number of instances for the message types")
    JobClient.runJob(conf)
