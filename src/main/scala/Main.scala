import scala.collection.concurrent.TrieMap
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Random
import ExecutionContext.Implicits.global
import scala.collection.mutable.ArrayBuffer
import java.io.{
  ObjectInputStream,
  ObjectOutputStream,
  FileOutputStream,
  FileInputStream,
  File,
  ByteArrayInputStream
}
import java.nio.charset.StandardCharsets.ISO_8859_1
import java.nio.charset.Charset
import java.nio.ByteBuffer

trait Operation
case class Insert(value: (Int, Int)) extends Operation
case class Remove(value: Int) extends Operation
case class Snapshot() extends Operation

object Main {

  def generateOperations(
      length: Int,
      start: Int,
      end: Int,
      percentOfSnapshots: Float
  ): Seq[Operation] = {

    val snapshotsAmount = (length * percentOfSnapshots).ceil.toInt
    val rest = 1 - percentOfSnapshots
    val restAmount = length * (rest / 2)

    val snapshots = (0 until snapshotsAmount) map { _ => Snapshot() }

    var validInt: Set[Int] = Set()
    val inserts = (0 until restAmount.ceil.toInt) map { _ =>
      val key = Random.between(start, end)
      validInt += key
      Insert(key, Random.between(start, end))
    }

    val removes = (0 until restAmount.floor.toInt) map { _ =>
      val isValidInt = Random.nextBoolean
      if (validInt.size > 0 && isValidInt) {
        val removedInt = validInt.toVector(Random.nextInt(validInt.size))
        validInt -= removedInt
        Remove(removedInt) // Value that is inside tree
      } else {
        Remove(-1) // Value that is never inside the tree
      }
    }

    Random.shuffle(snapshots ++ inserts ++ removes)
  }

  def populateTrie(
      trie: TrieMap[Int, Int],
      operations: Seq[Operation]
  ): Future[ArrayBuffer[TrieMap[Int, Int]]] = {
    Future {
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

  def createDir(dirPath: String): Unit = {
    val dir = new File(dirPath)
    if (!dir.exists()) dir.mkdir()
  }

  def deleteFile(filePath: String): Boolean = new File(filePath).delete()

  def saveAndReadTrie(
      id: Int,
      trie: TrieMap[Int, Int],
      delFile: Boolean = true
  ): TrieMap[Int, Int] = {

    val dirPath = "./tries/"
    val targetFile = "trie-" + id
    val completePath = dirPath.concat(targetFile)

    createDir(dirPath)

    val oos = new ObjectOutputStream(new FileOutputStream(completePath))
    oos.writeObject(trie)
    oos.close

    val ois = new ObjectInputStream(new FileInputStream(completePath))
    val fileTrie = ois.readObject.asInstanceOf[TrieMap[Int, Int]]
    ois.close

    if (delFile) deleteFile(completePath)

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

  def readFromMultipleSourcesTrie(
      trie: TrieMap[Int, Int],
      id: Int
  ): TrieMap[Int, Int] = {
    val filePath = "./tries/trie-" + id

    val serializedRaw =
      io.Source.fromFile(filePath, "ISO8859-1").mkString
    val serializedLength = serializedRaw.length

    val (firstPart, secondPart) = serializedRaw.splitAt(serializedLength / 2)

    val bytes =
      Charset
        .forName("ISO-8859-1")
        .newDecoder()
        .decode(ByteBuffer.wrap((firstPart + secondPart).getBytes(ISO_8859_1)))
    val ois = new ObjectInputStream(
      new ByteArrayInputStream(bytes.array().map { c => c.toByte })
    )
    val newTrie = ois.readObject.asInstanceOf[TrieMap[Int, Int]]
    ois.close

    newTrie
  }

  def main(args: Array[String]) = {

    val amountOfGroups = 10
    val length = 10000
    val snapshotPercent: Float = .1

    var operationsGroups: Seq[Seq[Operation]] = Seq();
    for (i <- 0 to amountOfGroups) {
      operationsGroups = operationsGroups :+ generateOperations(
        length,
        i * length,
        i * length + length,
        snapshotPercent
      )
    }

    val firstTrie = TrieMap[Int, Int]()
    val secondTrie = TrieMap[Int, Int]()

    var firstFutures = Seq[Future[ArrayBuffer[TrieMap[Int, Int]]]]()
    var secondFutures = Seq[Future[ArrayBuffer[TrieMap[Int, Int]]]]()

    for (operations <- operationsGroups) {
      firstFutures = firstFutures :+ populateTrie(firstTrie, operations)
      secondFutures = secondFutures :+ populateTrie(secondTrie, operations)
    }

    val seqFirstTrieBuffers =
      Await.result(Future.sequence(firstFutures), 3600.seconds)
    val seqSecondTrieBuffers =
      Await.result(Future.sequence(secondFutures), 3600.seconds)

    println("Are concurrent snapshots equal?")
    for (
      (firstBuffer, secondBuffer) <-
        seqFirstTrieBuffers zip seqSecondTrieBuffers
    ) {
      println(compareAllTries(firstBuffer, secondBuffer))
    }

    println("Final tries are equal? " + (firstTrie == secondTrie).toString)
  }

}
