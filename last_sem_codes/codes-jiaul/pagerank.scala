// Computing pagerank score of each node of a graph

val lines = sc.textFile("pagerank.txt")

val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()

var ranks = links.mapValues(v => 1.0)

val iters = 10

for (i <- 1 to iters) 
{
          val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
            val size = urls.size
            urls.map(url => (url, rank / size))
          }

      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
}
ranks.collect().foreach(println)

