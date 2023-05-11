// This code joins two multi-column files based on the common key. 

import breeze.linalg.{Vector, DenseVector, squaredDistance}


val tab1 = sc.textFile("tab1")
val tab1pair = tab1.map(line => {
		val s = line.split("\\s+")
		val len = s.length
		var w = new Array[String](len-1)
		for(i<-1 until len) {
			w(i-1) = s(i)
		}
		(s(0), DenseVector(w))
       }
)

val tab2 = sc.textFile("tab2")
val tab2pair = tab2.map(line => {
		val s = line.split("\\s+")
		val len = s.length
		var w = new Array[String](len-1)
		for(i<-1 until len) {
			w(i-1) = s(i)
		}
		(s(0), DenseVector(w))
       }
)

