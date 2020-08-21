import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}


object GraphQuerier extends App {

  // Load node and edge data from TSVs into 2d arrays
  val nodeRows: Array[Array[String]] = extractDataFromTSV("nodes.tsv", 3)
  val edgeRows: Array[Array[String]] = extractDataFromTSV("edges.tsv", 4)

  // Build some node maps for easier lookups
  val nodeMap: Map[String, Array[String]] = nodeRows.map(row => row.head -> row).toMap
  val nodesByType: Map[String, Array[Array[String]]] = nodeRows.groupBy(_(2))
  val nodeIDsByType: Map[String, Set[String]] = nodesByType.map(t => t._1 -> t._2.map(_(0)).toSet)

  if (args.length > 0) {
    // Run query based on input given in sbt shell
    val inputNodeID: String = args(0)
    val numHops: Int = if (args.length > 1) args(1).toInt else 1
    val outputNodeType: String = if (args.length > 2) args(2) else ""
    val answerNodeIDs: Set[String] = runQuery(inputNodeID, outputNodeType, numHops, edgeRows, nodeIDsByType)
    reportFindings(answerNodeIDs, inputNodeID)
  } else {
    // Run some example queries in parallel
    val curieList: Array[String] = Array("PR:30", "HP:9", "CUI:111")
    println(s"Kicking off queries for nodes in this order: ${curieList.mkString(", ")}")
    curieList.foreach(nodeID => Future {
      runQuery(nodeID, "", 1, edgeRows, nodeIDsByType)
    }.onComplete({
      case Success(value) => reportFindings(value, nodeID)
      case Failure(error) => error.printStackTrace
    }))
    println("This will be printed before query results are, even though it comes after them in the code..")
  }


  def runQuery(inputNodeID: String, outputNodeType: String = "", numHops: Int, edgeRows: Array[Array[String]], nodeIDsByType: Map[String, Set[String]]): Set[String] = {
    val allConnectedNodeIDs: Set[String] = findConnectedNodes(inputNodeID, numHops, edgeRows)
    val nodeIDsOfProperType: Set[String] = if (outputNodeType == "") allConnectedNodeIDs else allConnectedNodeIDs.intersect(nodeIDsByType.getOrElse(outputNodeType, Set()))
    nodeIDsOfProperType
  }

  def findConnectedNodes(inputNodeID: String, numHops: Int, edgeRows: Array[Array[String]]): Set[String] = {
    if (numHops <= 0) {
      Set()
    } else {
      val directNeighbors: Set[String] = findDirectNeighbors(inputNodeID, edgeRows)
      if (numHops == 1) {
        directNeighbors
      } else {
        // Remove edges we've already seen to prevent backtracking (this is not efficient, but works for now)
        val remainingEdgeRows: Array[Array[String]] = edgeRows.filterNot(row => row(1) == inputNodeID || row(2) == inputNodeID)
        directNeighbors.flatMap(nodeID => findConnectedNodes(nodeID, numHops - 1, remainingEdgeRows))
      }
    }
  }

  def findDirectNeighbors(inputNodeID: String, edgeRows: Array[Array[String]]): Set[String] = {
    val edgesUsingNode = edgeRows.filter(row => row(1) == inputNodeID || row(2) == inputNodeID)
    val connectedNodeIDs = edgesUsingNode.map(row => row.slice(1, 3)).flatten.toSet.diff(Set(inputNodeID)) // Why can't I get flatMap to work here?
    connectedNodeIDs
  }

  def reportFindings(answerNodeIDs: Set[String], inputNodeID: String): Unit = {
    if (answerNodeIDs.isEmpty) {
      println(s"No answers found for node $inputNodeID.")
    } else {
      val numAnswers: Int = answerNodeIDs.size
      println(s"Found $numAnswers answers for node $inputNodeID: ${answerNodeIDs.mkString(", ")}")
    }
  }

  def extractDataFromTSV(fileName: String, numColumns: Int): Array[Array[String]] = {
    // Thank you https://alvinalexander.com/scala/csv-file-how-to-process-open-read-parse-in-scala/
    // TODO: dynamically determine the number of columns
    val numRows = io.Source.fromFile(fileName).getLines.size
    val source = io.Source.fromFile(fileName)
    val rows = Array.ofDim[String](numRows, numColumns)
    for ((line, count) <- source.getLines.zipWithIndex) {
      rows(count) = line.split("\t").map(_.trim)
    }
    source.close
    rows
  }
}
