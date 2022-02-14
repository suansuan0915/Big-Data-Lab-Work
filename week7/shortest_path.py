from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary
def get_pair(line):
    lt = line.split(':')
    key_node = lt[0]
    outgoing_nodes = lt[1].split()
    return (key_node, outgoing_nodes)

def get_elem(pair):
    for i in pair[1][1]:
        k_v = (i, (pair[0], pair[1][0][1] + 1))
        yield k_v
    
def find_max(a, b):
    if a > b:
        return b
    return a


def main(inputs, output, source, destination):
    # main logic starts here
    text = sc.textFile(inputs)
    pair_t = text.map(get_pair).cache()
    head = sc.parallelize([(source, ('', 0))])  # source replace '1'
    node = head
    paths = []

    for i in range(6):
        pairs = node.join(pair_t).cache()
        k_v_pair = pairs.flatMap(get_elem).cache()
        k_v_h_pair = head.union(k_v_pair).cache()
        k_v_h_pair_r = k_v_h_pair.reduceByKey(lambda a, b: find_max(a,b)).cache()
        node = k_v_h_pair_r
        node.saveAsTextFile(output + '/iter-' + str(i))
        if len(k_v_h_pair_r.lookup(destination)) > 0:
            break
        

    node.take(10)

    # find path backwards
    paths.append(destination)
    destination_node = destination
    run = True

    while run:
        source_node = node.lookup(destination_node)  # (source_node, distance)  -> destination
        if source_node[0][0] != '': #len(node.lookup(destination)) > 0:
            destination_node = source_node[0][0]
            paths.append(destination_node)
        else:
            run = False
    paths.reverse()
    finalpath = sc.parallelize(paths)
    finalpath.saveAsTextFile(output + '/path')



if __name__ == '__main__':
    conf = SparkConf().setAppName(' shortest paths code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = sys.argv[3]
    destination = sys.argv[4]
    main(inputs, output, source, destination)