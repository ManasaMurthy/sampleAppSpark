import org.scalatest.{FlatSpec,GivenWhenThen, Matchers, fixture}

class SparkExampleSpec extends FlatSpec with SparkSpec with GivenWhenThen with Matchers {

  "Empty set" should "be counted" in {
    Given("empty set")
    val lines = Array("")

    When("count words")
    val wordCounts = sc.parallelize(lines)

    Then("empty count")
    wordCounts shouldBe empty
  }

  "Shakespeare most famous quote" should "be counted" in {
    Given("quote")
    val lines = Array("To be or not to be.", "That is the question.")

    Given("stop words")
    val stopWords = Set("the")

    When("count words")
    //val wordCounts = WordCount.count(sc, sc.parallelize(lines), stopWords).collect()

    val wordCounts = sc.parallelize(lines).flatMap(x => x.split(" ")).map(x => (x,1)).reduceByKey((x,y) => (x+y))
    Then("words counted")
    val expectedData = (Array("be", 2),("is", 1),("not", 1),("or", 1),("question", 1),("that", 1),("to", 2))
    println("expected data" + expectedData)
  }

}
