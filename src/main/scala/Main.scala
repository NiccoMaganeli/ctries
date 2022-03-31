import scala.collection.concurrent.TrieMap
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Random
import ExecutionContext.Implicits.global
import scala.collection.mutable.ArrayBuffer
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.FileOutputStream
import java.io.FileInputStream
import java.io.File

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

  def saveAndReadTrie(
      trie: TrieMap[Int, Int]
  ): TrieMap[Int, Int] = {

    val targetPath = "./trie"

    val oos = new ObjectOutputStream(new FileOutputStream(targetPath))
    oos.writeObject(trie)
    oos.close

    val ois = new ObjectInputStream(new FileInputStream(targetPath))
    val fileTrie = ois.readObject.asInstanceOf[TrieMap[Int, Int]]
    ois.close

    new File(targetPath).delete()

    fileTrie
  }

  def compareAllTries(
      left: ArrayBuffer[TrieMap[Int, Int]],
      right: ArrayBuffer[TrieMap[Int, Int]]
  ): Boolean = {
    left
      .zip(right)
      .forall(pair => {
        val (l, r) = pair
        l == r
      })
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
    val equalTries = compareAllTries(firstBuffer, secondBuffer)

    val deserializedTries = firstBuffer map saveAndReadTrie
    val serializedEqualTries = compareAllTries(firstBuffer, deserializedTries)

    printf(
      "Number of operations: %d\nSnapshots length: %d\nEqual tries: %b\nSerialized is equal: %b\n",
      length,
      numberOfSnapshots,
      equalTries,
      serializedEqualTries
    )

  }

}
