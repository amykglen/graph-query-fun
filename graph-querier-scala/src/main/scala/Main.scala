object GraphQuerier extends App {

  // Load node and edge data from TSVs into 2d arrays
  val nodeRows: Array[Array[String]] = extractDataFromTSV("nodes.tsv", 3)
  val edgeRows: Array[Array[String]] = extractDataFromTSV("edges.tsv", 4)

  // Build some node maps for easier lookups
  val nodeMap: Map[String, Array[String]] = nodeRows.map(row => row.head -> row).toMap
  val nodesByType: Map[String, Array[Array[String]]] = nodeRows.groupBy(_(2))
  val nodeIDsByType: Map[String, Set[String]] = nodesByType.map(t => t._1 -> t._2.map(_(0)).toSet)

  // Search for nodes connected to specified node (input via sbt shell)
  val inputNodeID: String = if (args.length >= 1) args(0) else "CUI:111"
  val outputNodeType: String = if (args.length > 1) args(1) else ""
  val edgeType: String = if (args.length > 2) args(2) else ""
  val answerNodeIDs: Set[String] = findConnectedNodes(inputNodeID, outputNodeType, edgeType, edgeRows, nodeIDsByType)

  // Report our findings
  if (answerNodeIDs.isEmpty) {
    println("No answers found.")
  } else {
    val numAnswers: Int = answerNodeIDs.size
    println(s"Found $numAnswers answers:")
    answerNodeIDs.foreach(nodeID => println(nodeID))
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
    return rows
  }

  def findConnectedNodes(inputNodeID: String, outputNodeType: String, edgeType: String = "", edgeRows: Array[Array[String]], nodeIDsByType: Map[String, Set[String]]): Set[String] = {
    // TODO: make search more efficient by using more complex data structures
    val edgesUsingNode = edgeRows.filter(row => row(1) == inputNodeID || row(2) == inputNodeID)
    val relevantEdges = if (edgeType == "") edgesUsingNode else edgesUsingNode.filter(row => row(3) == edgeType)
    val connectedNodeIDs = getNodeIDsUsedByEdges(relevantEdges).diff(Set(inputNodeID))
    val relevantNodeIDs = if (outputNodeType == "") connectedNodeIDs else connectedNodeIDs.intersect(nodeIDsByType.getOrElse(outputNodeType, Set()))
    return relevantNodeIDs
  }

  def getNodeIDsUsedByEdges(edgeRows: Array[Array[String]]): Set[String] = {
    return edgeRows.map(row => row.slice(1, 3)).flatten.toSet  // Why can't I get flatMap to work here?
  }
}
