import logging
import os
from multiprocessing import Pool

import gensim
import pandas as pd
from py2neo import Graph, Node, Relationship

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)



def neo_node_creator(arxiv_id, graph):
        cypher = graph.begin()
        print("Runing Cypher")
        cypher.run('create (id:paper {arxiv_id:"%s"})' % arxiv_id)
        print("yess,  running")
        cypher.commit() 
        print("commited again !!!")
        print("Done ",arxiv_id)    

if __name__ == "__main__":
    pool = Pool(1)
    try: 
        graph = Graph(password="1234")
    except Exception as e:
        print(e)
    filenames_base_list = []
    for (dirpath, dirnames, filenames) in os.walk("data/pdf/"):
            filenames_base_list += [os.path.join(dirpath, file) for file in filenames]
    arxiv_id_filenames_base = [os.path.basename(i) for i in filenames_base_list[1:]]
    arxiv_id = [os.path.splitext(i)[0] for i in arxiv_id_filenames_base]
    print("done arxiv ID")
    try:
        pool.map(neo_node_creator,arxiv_id)
    except Exception as e:
        print(e)
        pool.close()
        pool.join()
    finally:
        pool.close()
        pool.join()

    