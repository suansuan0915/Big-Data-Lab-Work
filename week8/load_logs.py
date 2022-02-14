import sys
import os
import gzip
import re
from uuid import uuid4
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import *

# first, create a keyspace and a table in node
line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')


def main(input_dir, keyspace, table_name):
    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect(keyspace)

    count = 0
    insert_words = session.prepare("INSERT INTO " + table_name + " (host, id, date_time, r_path, bytes) VALUES (?, ?, ?, ?, ?)")
    batch = BatchStatement()  # default replication factor: 3

    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
			
            for line in logfile:
                # split a line
                # e_words = line_re.split(line)  # first and last elements are empty
                words = line_re.split(line)
                if len(words) == 6:
                    if count > 300:
                        count = 0
                        session.execute(batch)  # execute the batch before clear the batch
                        batch.clear()
						
                    count += 1
                    host_name = str(words[1])
                    id_ = uuid4()
                    date_time = datetime.strptime(words[2], '%d/%b/%Y:%H:%M:%S')
                    path_r = str(words[3])
                    byte = int(words[4])
                    batch.add(insert_words, (host_name, id_, date_time, path_r, byte))  # data type should match table
    session.execute(batch)


if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]
    main(input_dir, keyspace, table_name)