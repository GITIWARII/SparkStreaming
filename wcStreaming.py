import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":

    sc = SparkContext("local[3]", appName="wordCountStreaming")
    ssc = StreamingContext(sc, 10)
    lines = ssc.socketTextStream("localhost", 9999)
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word,1))\
                  .reduceByKey(lambda a,b: a+b)
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()