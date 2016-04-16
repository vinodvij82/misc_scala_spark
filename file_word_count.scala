



val maplineRDD = sc.textFile("/home/hduser/IdeaProjects/files/ham.txt")
                // Split each line to individual words and create an list of all words in file
                .flatMap(_.split("\\W+"))     
                // Filter out the empty words
                .filter(_.trim().length > 0)  
                // Groupby word so now each row will be word1, Iterable(word1,word1,word1), cosidering word1 occurs 3 times
                .groupBy( w => w)             
                // Map each row to (word, frequency of word)
                .map( w => (w._1, w._2.size))
                // Sort by word frequency in descending order and take only top 10 frequency words
                .sortBy(_._2,false)
                .collect
                .take(10)

// Another way using reduceByKey
val maplineRRD = sc.textFile("/home/hduser/IdeaProjects/files/ham.txt")
                .flatMap(_.split("\\W+"))
                .filter(_.trim.length >0)
                .map( w => (w,1))
                .reduceByKey(_ + _)
                .sortBy(_._2,false)
                .collect
                .take(10)
