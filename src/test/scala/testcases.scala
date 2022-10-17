
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.text.SimpleDateFormat
import java.time.LocalTime
import java.util.regex.Pattern

class testcases extends AnyFlatSpec with Matchers {

  val config = ConfigFactory.load()
  val logger = LoggerFactory.getLogger(getClass)

  it should "Checking if the content pattern matches" in {
    val test_msg = "16:47:01.530 [scala-execution-context-global-13] ERROR - Q5icf0cg1bg2ag0bf1E7faf1"
    val pattern = Pattern.compile(config.getString("para.content_pattern"))
    val content_matcher = pattern.matcher(test_msg)
    content_matcher.find() shouldBe (true)
  }

  it should "Checking if the time interval is properly working" in {
    val test_msg = "19:57:01.530 [scala-execution-context-global-13] ERROR - /US|e`bzLKW#8\\W1_:Az'Yc{d~"
    val start_time = LocalTime.parse(config.getString("para.start_time"))
    val end_time = LocalTime.parse(config.getString("para.end_time"))
    val check = (LocalTime.parse(test_msg.split(" ")(0)).isAfter(start_time) & LocalTime.parse(test_msg.split(" ")(0)).isBefore(end_time))
    check shouldBe (true)
  }

  it should "Match the time in the log entry and check if it matches properly" in {
    val example_msg = "12:47:01.530 [scala-execution-context-global-13] ERROR - /US|e`bzLKW#8\\W1_:Az'Yc{d~"
    val timeRegex = Pattern.compile(config.getString("para.timeRegex"))
    val time_matcher = timeRegex.matcher(example_msg)
    time_matcher.find() shouldBe (true)
  }

  it should "Match time and convert to epoch then back to time format and check" in {
    val example_msg = "12:47:01.530 [scala-execution-context-global-13] ERROR - /US|e`bzLKW#8\\W1_:Az'Yc{d~"
    val type_pattern = Pattern.compile(config.getString("para.type_pattern"))
    val matcher = type_pattern.matcher(example_msg)
    matcher.find()
    val mtype = matcher.group(0)
    val check = (mtype == "INFO")
    check shouldBe (false)
  }

  it should "Match the message type in the log entry " in {
    val example_msg = "11:22:01.530 [scala-execution-context-global-13] ERROR - /US|e`bzLKW#8\\W1_:Az'Yc{d~"
    val type_pattern = Pattern.compile(config.getString("para.type_pattern"))
    val matcher = type_pattern.matcher(example_msg)
    matcher.find() shouldBe (true)
  }

}

