// This code performs K-nearest neighbour search over a set of vectors for a given query vector. 

val doc = sc.textFile("knn-vec")
    val procDocs = doc.map(line => {
    		var temp = line.split("\\s+")
    		var docid = temp(0)
    		var len = temp.length
    		var elements = new Array[Double](len-1)
    		for(i <- 1 to (len-1) ) {elements(i-1) = temp(i).toDouble}
    		(docid, elements)
    	}
    ).cache
    val dcount = procDocs.count
    
    
    val query = sc.textFile("knn-query")
    val procQuery = query.map(line => {
    		var temp = line.split("\\s+")
    		var qid = temp(0).toInt
    		var len = temp.length
    		var elements = new Array[Double](len-1)
    		for(i <- 1 to (len-1) ) {elements(i-1) = temp(i).toDouble}
    		(qid, elements)
    	}
    ).cache

    var qcount = procQuery.count
    val allq = procQuery.collect /* collect queries in the driver an an array of objects */
    
    val numq = allq.length
    for(j <- 0 to (numq-1)) {
    		var qno = allq(j)._1
    		var qvec = allq(j)._2
    		procDocs.map(t => {
	    		var sc = 0.0
	    		var l1 = 0.0
	    		var l2 = 0.0
	    		var dim = qvec.length
	    		for(i <- 0 to (dim-1)) {
	    			sc += qvec(i) * t._2(i)
	    			l1 += qvec(i) * qvec(i)
	    			l2 += t._2(i) * t._2(i)
	    		}
	    		/* if(l2 == 0.0)
	    		  l2 = 1000000.0
	    		sc = sc/(Math.sqrt(l1) * Math.sqrt(l2)) */
	    		(sc, qno, t._1)
    		    }
    		).top(1000).foreach(println)
    }
    
