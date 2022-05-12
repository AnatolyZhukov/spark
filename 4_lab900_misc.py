"""
* Hello world.
  let's display 50,000
  hello world: it takes about the same time as creating 7.5m datapoints
  using Spark, just for the fun of it...
"""
import time

t0 = int(round(time.time() * 1000))
for i in range(5000):
    print("Hello, world")

t1 = int(round(time.time() * 1000))
print("1. Hello world ... {}-{} = {}".format(t1, t0, t1 - t0))