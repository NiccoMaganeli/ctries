import scala.collection.concurrent.TrieMap
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Random
import ExecutionContext.Implicits.global
import scala.collection.mutable.ArrayBuffer

trait Operation
case class Insert(value: (Int, Int)) extends Operation
case class Remove(value: Int) extends Operation
case class Snapshot() extends Operation

object Main {

  def generateOperations(
      length: Int,
      intLimit: Int,
      percentOfSnapshots: Float
  ): Seq[Operation] = {

    val snapshotsAmount = (length * percentOfSnapshots).ceil.toInt
    val rest = 1 - percentOfSnapshots
    val restAmount = length * (rest / 2)

    val snapshots = (0 until snapshotsAmount) map { _ => Snapshot() }

    var validInt: Set[Int] = Set()
    val inserts = (0 until restAmount.ceil.toInt) map { _ =>
      val key = Random.nextInt(intLimit)
      validInt += key
      Insert(key, Random.nextInt(intLimit))
    }

    val removes = (0 until restAmount.floor.toInt) map { _ =>
      val isValidInt = Random.nextBoolean
      if (validInt.size > 0 && isValidInt) {
        val removedInt = validInt.toVector(Random.nextInt(validInt.size))
        validInt -= removedInt
        Remove(removedInt)
      } else {
        Remove(Random.nextInt(length + intLimit))
      }
    }

    // printf(
    //   "%d + %d + %d = %d\n",
    //   snapshots.length,
    //   inserts.length,
    //   removes.length,
    //   snapshots.length + inserts.length + removes.length
    // )

    Random.shuffle(snapshots ++ inserts ++ removes)
  }

  def populateTrie(
      operations: Seq[Operation]
  ): Future[ArrayBuffer[TrieMap[Int, Int]]] = {
    Future {
      val trie = TrieMap[Int, Int]()
      val snapshots = ArrayBuffer[TrieMap[Int, Int]]()

      operations foreach { op =>
        op match {
          case Insert(value) => trie += value
          case Remove(value) => trie.remove(value)
          case Snapshot()    => snapshots += trie.snapshot()
        }

      }
      snapshots += trie
      snapshots
    }
  }

  def main(args: Array[String]) = {

    val length = 10000
    val intLimit = 1000
    val snapshotPercent: Float = .1

    val operations = generateOperations(length, intLimit, snapshotPercent)

    val firstFuture = populateTrie(operations)
    val secondFuture = populateTrie(operations)

    val aggFut = for {
      firstResult <- firstFuture
      secondResult <- secondFuture
    } yield (firstResult, secondResult)

    val something = Await.ready(aggFut, 3600.seconds)

    val tries = something.value.get.get
    val firstBuffer = tries._1
    val secondBuffer = tries._2

    val numberOfSnapshots = firstBuffer.length
    val equalTries = firstBuffer
      .zip(secondBuffer)
      .filter { pair =>
        val (left, right) = pair
        left == right
      }
      .length

    printf(
      "Number of operations: %d\nSnapshots length: %d\nEqual tries: %d\n",
      length,
      numberOfSnapshots,
      equalTries
    )

  }

}
