import matplotlib.pyplot as plt
import numpy as np
import csv
import math
import copy

from containers import *


simu_topic_size = 4  # number of letters that form a topic
lock1 = threading.Lock()

def Topic_generate(r, c, level):
    # generate r*c topic and store them into a csv file. level is the level of this topic: 1/2/3/...
    f = open("topic_pool_" + str(level) + ".csv", 'w', encoding='utf-8', newline="")  # newline is used to avoid empty lines between data
    f_writer = csv.writer(f)
    # ASC-II for A-Z: 65-90; a-z: 97-122
    topic_pool = []
    i = 0
    while i < r*c:
        tmp_topic = ""
        for j in range(simu_topic_size - level + 1):
            tmp_topic += chr(random.randint(65,90))
        if tmp_topic not in topic_pool:
            topic_pool.append(str(int(i/c)) + tmp_topic + str(level))
            i += 1

    # write the topics to the csv file topic_pool.csv
    pointer = 0
    for i in range(r):
        f_writer.writerow(topic_pool[pointer:pointer + c])
        pointer += c

    f.close()


def topic_pool_read(filename):
    # return a list[r][c]
    with open(filename) as f:
        f_reader = csv.reader(f)
        res = []
        for t in f_reader:
            res.append(t)
    return res


def draw_topology(graph, edges):
    # edges is [(start, end, weight(latency)), ]
    for e in edges:
        try:
            graph[e[0]][e[1]] = e[2]
            graph[e[1]][e[0]] = -1 * e[2]
        except IndexError:
            print(str(e) + " is not a legal connection in the network")

if __name__ == '__main__':
    num_of_brokers = 3
    num_of_candidate = 3
    topic_structure_level = 3  # do not easily change it
    read_from_existed = False
    # generate topics and all_Topics
    all_Topics = []
    if read_from_existed:
        for i in range(topic_structure_level):
            all_Topics.append(topic_pool_read("topic_pool_" + str(i + 1) + ".csv"))
    else:
        for i in range(topic_structure_level):  # 3 -- level of topic.
            Topic_generate(num_of_brokers, num_of_candidate, i+1)
            all_Topics.append(topic_pool_read("topic_pool_" + str(i+1) + ".csv"))

    # simulation start
    all_topic_pool = [[] for i in range(num_of_brokers)]
    atp_lock = threading.Lock()
    broker_graph = [[0 for j in range(num_of_brokers)] for i in range(num_of_brokers)]
    #edges = [(0, 1, 1), (0, 2, 1), (2, 3, 1), (2, 4, 1)]\
    edges = [(0, 1, 1), (1, 2, 1)] # simple 0 -> 1 -> 2 model for test functions
    draw_topology(broker_graph, edges)

    label_pool = ["XXthisXSubXisXfromXaXbrokerXX", "XXthisXPubXisXfromXanotherXbrokerXX"]

    for i in range(num_of_brokers):
        Broker_i = Broker(all_topic_pool, atp_lock, broker_graph, label_pool, i, all_Topics)
        Thi = threading.Thread(target=Broker_i.start_simu, args=(2, 1, )) # args: local topics, other topics
        Thi.start()

