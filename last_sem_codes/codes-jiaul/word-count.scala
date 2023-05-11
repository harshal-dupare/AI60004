// This spark code does word counting. It reads from a text file  

//reading data from file. This creates an RDD
val data = sc.textFile("text.txt")

// transforme data into word and count format
val words = data.flatMap(line => line.split("\\s+"))
val pair = words.map(w => (w,1))

// reducing to perform word and its total count
val wordcount = pair.reduceByKey(_ + _)

//printing RDD content
wordcount.collect().foreach(println)

// saving data
wordcount.saveAsTextFile("output")

// setting number of reduce tasks
val wordcount = pair.reduceByKey(_ + _, 4)
