# use to test hash partition
if __name__ == '__main__':
    num_tuples1 = 100
    num_fields1 = 10
    t1_tuples = [ tuple([randint(0,0) for i in xrange(num_fields1)]) for j in xrange(num_tuples1) ]

    num_tuples2 = 100
    num_fields2 = 10
    t2_tuples = [ tuple([randint(0,0) for i in xrange(num_fields2)]) for j in xrange(num_tuples2) ]

    num_tuples3 = 100
    num_fields3 = 10
    t3_tuples = [ tuple([randint(0,0) for i in xrange(num_fields3)]) for j in xrange(num_tuples3) ]

    num_tuples4 = 100
    num_fields4 = 10
    t4_tuples = [ tuple([randint(0,0) for i in xrange(num_fields4)]) for j in xrange(num_tuples3) ]

    tuples_of_each_table = [ t1_tuples, t2_tuples, t3_tuples, t4_tuples ]
    partitioning_of_each_tuple = [ [num_tuples1/2, num_tuples1/2], [num_tuples2/2, num_tuples2/2], [num_tuples3/2, num_tuples3/2], [num_tuples4/2, num_tuples4/2] ]
    create_benchmark(tuples_of_each_table, partitioning_of_each_tuple, 1)