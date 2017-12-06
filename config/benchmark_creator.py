from heap_file_creator import *
from random import shuffle
from glob import glob
from shutil import rmtree
import os

my_path = os.path.abspath(os.path.dirname(__file__))

def create_benchmark(tuples_of_each_table, partitioning_of_each_table, randomize=True):
  '''
  Creates a benchmark. You MUST make sure that this method is called in the /config directory.

  @param tuples_of_each_table
          a list containing the tuples in each table
  @param partitioning_of_each_table
          a list containing the partitioning of each table. Note that each partitioning is again
	  a list containing the number of tuples in a partition. Thus, the size of a partitioning
	  is also the number of ports
  @param randomize
          whether or not to randomize the tuples in a table before partitioning
  '''
  num_tables = len(tuples_of_each_table)
  starting_port = 8001
  unpartitioned_port = 9999

  # First clean up and delete everything in child
  for f in glob(os.path.join(my_path, '/child/*')): os.system("rm -rf %s" % f)

  # Determine the number of ports
  if len(partitioning_of_each_table) == 0: raise 'There must be at least one partitioning!'
  num_ports = len(partitioning_of_each_table[0])

  # Create local.txt in /head. Will overwrite if exists
  with open(os.path.join(my_path, 'head/local.txt'), 'w') as f:
    for i in xrange(num_ports): f.write("127.0.0.1:%d\n" % (i + starting_port))

  for tidx in xrange(num_tables):
    # Determine the properties of each table
    port = starting_port + tidx
    tuples = tuples_of_each_table[tidx]
    partitioning = partitioning_of_each_table[tidx]
    if len(partitioning) != num_ports: raise RuntimeError('The partitioning size must equa the number of ports')
    if len(tuples) == 0: raise RuntimeError('You must have at least one tuple per table')
    num_fields = len(tuples[0])
    
    # Validate that the number of tuples is the sum of the partitioning
    if len(tuples) != sum(partitioning): raise RuntimeError('The number of tuples must match partitioning')

    # Create unpartitioned file first
    unpartitioned_dir = os.path.join(my_path, "child/%d" % unpartitioned_port)
    _make_dir_if_not_exists(unpartitioned_dir)
    unpartitioned_heapfile = create_heap_file(tuples, num_fields, fn=os.path.join(unpartitioned_dir, "test.%d.dat" % tidx))
    # Also write/append to catalog.txt
    with open(os.path.join(unpartitioned_dir, 'catalog.txt'), 'a') as f:
      f.write('%s (%s)\n' % ('test.%d' % tidx, ', '.join(map(lambda i: 'f%d int' % i, xrange(1, num_fields+1)))))

    # Randomize the tuples if necessary before partitioning
    if randomize: shuffle(tuples)

    # Create partitioned files
    offset = 0
    for pidx in xrange(num_ports):
      num_rows = partitioning[pidx]
      partition_tuples = tuples[offset:offset+num_rows]
      offset += num_rows
      partition_dir = os.path.join(my_path, "child/%d" % (starting_port + pidx))
      table_name = "test.%d" % tidx
      fn = "%s.dat" % table_name
      _make_dir_if_not_exists(partition_dir)
      partitioned_heapfile = create_heap_file(partition_tuples, num_fields, fn=os.path.join(partition_dir, fn))

      # Also write/append to catalog.txt
      with open(os.path.join(partition_dir, 'catalog.txt'), 'a') as f:
        f.write('%s (%s)\n' % (table_name, ', '.join(map(lambda i: 'f%d int' % i, xrange(1,num_fields+1)))))

def _make_dir_if_not_exists(d):
  if not os.path.exists(d):
    try:
      os.makedirs(d)
    except OSError as exc:
      if exc.errno != errno.EEXIST:
        raise
