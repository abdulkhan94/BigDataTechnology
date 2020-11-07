from pyspark import SparkContext
from numpy import random

sc = SparkContext(appName = "pi-approximate")
sc.setLogLevel("WARN")

n=500000

def sample(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1 

# pick random points in the unit square (0, 0) to (1,1) and see how
# many fall in the unit circle.  The fraction should be pi/4

count = sc.parallelize(range(n)).filter(sample).count()
print("Pi is roughly {}".format(4.0*count/n))
