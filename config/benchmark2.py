from benchmark_creator import *
from random import *

if __name__ == '__main__':
  num_tuples1 = 50000
  num_fields1 = 100
  t1_tuples = [ tuple([randint(0, 100) for i in xrange(num_fields1)]) for j in xrange(num_tuples1) ]

  num_tuples2 = 20000
  num_fields2 = 200
  t2_tuples = [ tuple([randint(200, 300) for i in xrange(num_fields2)]) for j in xrange(num_tuples2) ]

  tuples_of_each_table = [ t1_tuples, t2_tuples ]
  partitioning_of_each_tuple = [ [num_tuples1/2, num_tuples1/2], [num_tuples2/2, num_tuples2/2] ]
  create_benchmark(tuples_of_each_table, partitioning_of_each_tuple)
