from benchmark_creator import *
from random import *

if __name__ == '__main__':
  num_tuples1 = 3000000
  num_fields1 = 2
  t1_tuples = [ tuple([randint(0, 100) for i in xrange(num_fields1)]) for j in xrange(num_tuples1) ]

  tuples_of_each_table = [ t1_tuples ]
  partitioning_of_each_tuple = [ [num_tuples1/2, num_tuples1/2] ]
  create_benchmark(tuples_of_each_table, partitioning_of_each_tuple)
