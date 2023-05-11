/* This code produces mean vector of the words in a document
   Data format (one line for each document or word):
   	1. Document: Space separated fields. The first field document id and the rests are the words.
   	2. Word: Space separated fields. The first field is the word, and the rests are the values.
*/   


import breeze.linalg.{Vector, DenseVector, squaredDistance}
val textDocVec = sc.textFile("docs")

// reading documents and creating word nd freq table
val pdocdata = textDocVec.map(line => {
	val s = line.split("\\s+")
	val map = scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
	for(i<-1 until s.length) {
		map.update(s(i), map(s(i))+1)
	}
	(s(0), map)
    })
 
// three column data: docid, word, freq
  
val splitDocRdd = pdocdata.flatMap {case (id, m) => m.toList.map {
    		case (w, freq) => (w, (id, freq))                                 // k=word, s=docid, v=freq
        }
} 
    
    val wordVec = sc.textFile("word-vector.txt")
    val wordVecRdd = wordVec.map(line => {
		val s = line.split("\\s+")
		val len = s.length
		var w = new Array[Double](len-1)
		for(i<-1 until len) {
			w(i-1) = s(i).toDouble
		}
		(s(0), DenseVector(w))
       }
    )
    
    val joinedRdd = splitDocRdd.join(wordVecRdd)
    val newjoinedRdd = joinedRdd.map(t => (t._2._1._1, (t._2._2 * (1.0 * t._2._1._2), t._2._1._2)))
    val reducedRdd = newjoinedRdd.reduceByKey{case ((v1, c1), (v2, c2)) => (v1 + v2, c1 + c2)}
    val finalDocVec = reducedRdd.map(t => { var len = t._2._2
    					if(len == 0)
    						len = 500000
    					val v = t._2._1 * (1.0 / len)
    					
    					(t._1, v.toArray.mkString(" "))
    		      }) 
    
    finalDocVec.saveAsTextFile("docvec.out")
