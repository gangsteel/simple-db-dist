from benchmark_creator import *
from random import *

if __name__ == '__main__':
  num_tuples1 = 100000
  num_fields1 = 1
  t1_tuples = [ tuple([randint(0, 100) for i in xrange(num_fields1)]) for j in xrange(num_tuples1) ]

  tuples_of_each_table = [ t1_tuples ]
  num_partitions = 4
  partitioning_of_each_tuple = [ [num_tuples1/num_partitions for i in xrange(num_partitions)] ]
  create_benchmark(tuples_of_each_table, partitioning_of_each_tuple)
