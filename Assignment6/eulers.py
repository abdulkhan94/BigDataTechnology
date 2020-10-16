from pyspark import SparkContext
import math

sc = SparkContext(appName = "eulers-approximate")
sc.setLogLevel("WARN")

data = list(range(500)) #Setting up terms

rdd = sc.parallelize(data) # Creating rdd

rdd2 = rdd.map(lambda x: (1**x)/math.factorial(x)) # Applying eulers formula

answer = rdd2.reduce(lambda x,y: x+y) # Using to reduce function to get answer


print("********OUTPUT********")  # Added this to easily spot output in shell
print(f"The approximate value of e with {len(data)} terms is {answer}")


