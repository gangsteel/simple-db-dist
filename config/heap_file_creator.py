import math
import struct

def create_heap_file(tuples, num_fields, page_size=4096, fn='heapfile.dat'):
  '''
  Creates a heap file supporting only tuples with int field types. Tuples are represented as an array of equal length arrays.
  Each array element is either a list of ints or None, where None means that the tuple does not exist or is invalid.
  '''
  if __debug__:
    for t in tuples:
      if t == None: continue
      assert len(t) == num_fields, 'Each tuple should have the same length'

  tuple_size = 4 * num_fields # int is 4 bytes
  num_tuples_per_page = (page_size * 8) / (tuple_size * 8 + 1)
  header_size = int(math.ceil(num_tuples_per_page / 8.0))
  num_pages = int(math.ceil(float(len(tuples)) / num_tuples_per_page))

  num_bytes_added = 0
  num_record_bytes_added = 0
  # Create a file
  with open(fn, 'wb') as f:
    # Create each page in the file
    for i in xrange(num_pages):
      num_page_bytes_added = 0
      page_tuples = tuples[i*num_tuples_per_page:min(len(tuples),(i+1)*num_tuples_per_page)]
      # Go through 8 tuples at a time and create the header bytes
      num_header_bytes_added = 0
      for j in xrange(0, len(page_tuples), 8):
        end = min(len(page_tuples), j+8)
        group_tuples = page_tuples[j:end]
        b = 0
        # Compute the byte based on these tuples
        for i in xrange(len(group_tuples)):
          t = group_tuples[i]
          if t != None:
            b = b | (1 << i)
        # Add the byte
        f.write(struct.pack('>B',b))
        num_header_bytes_added += 1
        num_page_bytes_added += 1
        num_bytes_added += 1
      # Add on more bytes for the header in case there were not many tuples
      for i in xrange(header_size - num_header_bytes_added):
        num_page_bytes_added += 1
        f.write(struct.pack('>B',0))
      # Now create the bytes for each tuple
      for t in page_tuples:
        if t == None:
          for i in xrange(num_fields):
            num_bytes_added += 4
            num_record_bytes_added += 4
            num_page_bytes_added += 4
            f.write(struct.pack('>I', 0xFFFFFFFF))
        else:
          for i in t:
            num_bytes_added += 4
            num_record_bytes_added += 4
            num_page_bytes_added += 4
            f.write(struct.pack('>I',i))
      # Add on additional bytes until we match page size
      for i in xrange(page_size - num_page_bytes_added): f.write(struct.pack('>B',0))
  print 'Created heap file at %s with %d tuples each with %d fields' % (fn, len(tuples), num_fields)

if __name__ == '__main__':
  import random 

  # Create tuples, where the descriptor only has 1 int field
  tuples = []

  # First create 2048 where there is 50% chance of there being a tuple
  for i in xrange(2048):
    if i % 3 == 0: tuples.append([1,])
    else: tuples.append(None)

  # Create 2048 more tuples where all tuples do not exist
  for i in xrange(2048):
    tuples.append(None)

  # Create 2000 more tuples where 10% of tuples exist
  for i in xrange(2048):
    if i % 10 == 0: tuples.append([2,])
    else: tuples.append(None)

  # Create 16000 more tuples where all tuples do not exist
  for i in xrange(16000):
    tuples.append(None)

  # Create 3 more tuples where all tuples exist
  for i in xrange(3):
    tuples.append([3,])

  # Create 5000 more tuples all empty:
  for i in xrange(5000):
    tuples.append(None)

  create_heap_file(tuples, 4096, 1)
