// Use the named values (val) below whenever your need to
// read/write inputs and outputs in your program. 
val inputFilePath  = "data/sample_input.txt"
val outputDirPath = "outputData/"

// Write your solution here

// read payload string and output a Double with unit corrections
def strToDouble(str: String) : Double = {
	val lastTwoChar = str.slice(str.length() - 2 , str.length())
	if(lastTwoChar == "MB"){
		return str.slice(0, str.length() - 2).toDouble * 1000000.0
	}else if(lastTwoChar == "KB"){
		return str.slice(0, str.length() - 2).toDouble * 1000.0
	}else{
		return str.slice(0, str.length() - 1).toDouble * 1.0
	}
}

// format Double ( in the unit B ) to be String with the format required 
def doubleToString(value: Double) : String = {
	return f"$value%1.0f" + "B"
}

// square the input Double
def square(v:Double) : Double = {
	return v*v
}

// take the floor of the input and pass it to doubleToString 
def formatInt(v: Double ) : String = {
	return doubleToString(math.floor(v))
}

// read from file
val file = sc.textFile(inputFilePath, 1)
// make list from data
val file_list = file.map(line=>line.split(","))
// extract desired data, i.e. url and payload
val filteredfileList = file_list.map(arr => (arr(0), (strToDouble(arr(3)))))
// save this RDD
filteredfileList.persist()

// find min for each url
val mins = filteredfileList.reduceByKey(math.min(_, _))
// find max for each url
val maxs = filteredfileList.reduceByKey(math.max(_, _))

// find count for each url
val counts = filteredfileList.groupBy(tuple => tuple._1).mapValues((_.size))
counts.persist()
// find sum of payload for each url 
val sums = filteredfileList.reduceByKey( _ + _ )
// use sums and counts to find the mean for each url
val means = sums.join(counts).mapValues{ case (_sum, _count) => _sum/_count }

// square the payload and sum them for each url
val square_sums = filteredfileList.map(v => (v._1, square(v._2))).reduceByKey( _ + _ )
// calculate the mean square of payload for each url
val mean_square = square_sums.join(counts).mapValues((v) => v._1/v._2)
// get variance by calculating (mean-square - square-mean)
val variances = mean_square.join(means).mapValues((v) => v._1 - square(v._2))

// join the result
val nestedOutput = mins.join(maxs).join(means).join(variances)
// format the result 
val output = nestedOutput.map {
	case (url, (((_min, _max), _mean),_var)) => (
		Seq(url, formatInt(_min), formatInt(_max), formatInt(_mean), formatInt(_var)).mkString(",")
	)
}

// save the result into the designated Path
output.saveAsTextFile(outputDirPath)


