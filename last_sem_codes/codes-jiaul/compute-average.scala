// This code produces average time spent by a customer in a shop. 

// reading data from file
val data = sc.textFile("average.txt")

// transformation operation 
val pairs = data.map(line => {val s = line.split("\\s+") 
    (s(0), (s(1).toDouble,1))
 }
)

// performs sum over time and number of visits
val interout = pairs.reduceByKey{case ((t1,v1), (t2,v2)) => (t1+t2, v1+v2)}

// producing final output (average)
val output = interout.map(r => (r._1, r._2._1/r._2._2))


