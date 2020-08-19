object GraphQuerier extends App {

  // Load node and edge data from TSVs into 2d arrays
  val nodeRows = extractDataFromTSV("nodes.tsv", 3)
  val edgeRows = extractDataFromTSV("edges.tsv", 4)

  // Build node lookup map
  val nodeMap = nodeRows.map(row => row.head -> row).toMap

  // Search for nodes connected to specified node (input via sbt shell)
  val inputNodeID = if (args.length >= 1) args(0) else "CUI:111"
  val edgeType = if (args.length > 1) args(1) else ""
  val answerNodeIDs = findConnectedNodes(inputNodeID, edgeRows, edgeType)
  if (answerNodeIDs.isEmpty) {
    println("No connected nodes found.")
  } else {
    println("Connected node IDs are:")
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

  def findConnectedNodes(inputNodeID: String, edgeRows: Array[Array[String]], edgeType: String = ""): Set[String] = {
    // TODO: make search more efficient by using more complex data structures
    val edgesUsingNode = edgeRows.filter(row => row(1) == inputNodeID || row(2) == inputNodeID)
    if (edgeType == "") {
      return getNodeIDsUsedByEdges(edgesUsingNode).diff(Set(inputNodeID))
    } else {
      val relevantEdges = edgesUsingNode.filter(row => row(3) == edgeType)
      return getNodeIDsUsedByEdges(relevantEdges).diff(Set(inputNodeID))
    }
  }

  def getNodeIDsUsedByEdges(edgeRows: Array[Array[String]]): Set[String] = {
    return edgeRows.map(row => row.slice(1, 3)).flatten.toSet  // Why can't I get flatMap to work here?
  }
}
