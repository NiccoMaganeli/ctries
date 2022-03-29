import scala.collection.concurrent.TrieMap
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Random
import ExecutionContext.Implicits.global

trait Operation
case class Insert(value: (Int, Int)) extends Operation
case class Remove(value: Int) extends Operation

object SameStructure {

  def populateTrie(operations: Seq[Operation]): Future[TrieMap[Int, Int]] = {
    Future {
      val trie = TrieMap[Int, Int]()
      operations foreach { op =>
        op match {
          case Insert(value) => trie += value
          case Remove(value) => trie.remove(value)
        }

      }
      trie
    }
  }

  def main(args: Array[String]) = {

    val length = 100

    var validInt: Set[Int] = Set()
    val operations: Seq[Operation] = (0 until length) map { id =>
      val isInsert = Random.nextBoolean
      if (isInsert) {
        validInt += id
        Insert(id, Random.nextInt(1000))
      } else {
        val isValidInt = Random.nextBoolean
        if (validInt.size > 0 && isValidInt) {
          val removedInt = validInt.toVector(Random.nextInt(validInt.size))
          validInt -= removedInt
          Remove(removedInt)
        } else {
          Remove(Random.nextInt(length + 1000))
        }
      }
    }

    println(length)
    // println(operations)
    val firstFuture = populateTrie(operations)
    val secondFuture = populateTrie(operations)

    val aggFut = for {
      firstResult <- firstFuture
      secondResult <- secondFuture
    } yield (firstResult, secondResult)

    // aggFut.onComplete { x =>
    //     val results = x.get
    //     val firstTrie = results._1
    //     val secondTrie = results._2
    //     println("%d %d".format(firstTrie.size, secondTrie.size))
    //     println(firstTrie)
    //     println(secondTrie)

    //     val firstSnap = firstTrie.snapshot
    //     val secondSnap = secondTrie.snapshot

    //     println(firstSnap.zip(secondSnap).filter(x => x._1 == x._2).size)
    // }

    val something = Await.ready(aggFut, 3600.seconds)

    val tries = something.value.get.get
    val firstTrie = tries._1
    val secondTrie = tries._2

    println("%d %d".format(firstTrie.size, secondTrie.size))
    // println(firstTrie)
    // println(secondTrie)
    println(firstTrie == secondTrie)

  }

}
