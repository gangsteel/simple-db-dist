from benchmark_creator import *

if __name__ == '__main__':
  tuples_of_each_table = [[(1, 2, 3), (2, 3, 4)]]
  partitioning_of_each_tuple = [[1,1]]
  create_benchmark(tuples_of_each_table, partitioning_of_each_tuple)
