import matplotlib.pyplot as plt
import numpy as np
import random
import csv
import math
import copy
import threading
import time
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


def some_rand(x, a, b):
    # generate x different ints in [a, b)
    if b - a < x:
        print("Invalid range and number! Cannot generate %d numbers from %d to %d !" %(x, a, b))
    res = []
    while len(res) != x:
        t = random.randint(a, b-1)
        if t not in res:
            res.append(t)
    res.sort()
    #print(res)
    return res


def pack_publication(topic, message):
    dict = {'topic': topic, 'message': message}
    return dict


def publish(pool, speed, period, simul, broker_topics):
    start = 0
    start_time = time.time()
    # simul: max number of publication at a moment
    while start < period:
        # select topics from pool
        number_of_topic = random.randint(1, simul)
        topic_index = some_rand(number_of_topic, 0, len(pool))
        lock1.acquire()
        for ti in topic_index:
            broker_topics.append(pack_publication(pool[ti], str(time.time() - start_time)))
        lock1.release()
        print(" === publish %d topics ! ====" % number_of_topic)

        time.sleep(speed)
        start += speed


def broadcast(name, pool, speed, broker_topics, neighbor_topics, neighbors):
    print("The broker " + name + " has these subscriptions: ", pool)
    # neighbor_topics: a dict where keys are topics and values are brokers
    # neighbors should be indice list of neighbor brokers
    while True:
        if len(broker_topics) == 0:
            time.sleep(speed * 3)
        else:
            lock1.acquire()
            pub = broker_topics[0]
            broker_topics.remove(broker_topics[0])
            lock1.release()

            if pub['topic'] in pool:
                print("Source: " + name + "  Topic: " + pub['topic'] + "  Message: " + pub['message'])

            if pub['topic'] in neighbor_topics.keys():
                for i in neighbor_topics[pub['topic']]:
                    lock1.acquire()
                    all_brokers[i].append(pub)
                    lock1.release()

            time.sleep(speed)


if __name__ == '__main__':
    num_of_b = 2 # number of brokers
    Topic_generate(num_of_b, 3, 1)
    Topic_generate(num_of_b, 3, 2)
    Topic_generate(num_of_b, 3, 3)
    whole_pool = topic_pool_read("topic_pool.csv")
    print("All topics are: ", whole_pool)

    all_brokers = [[] for i in range(num_of_b)]
    n_t = {}
    for i in whole_pool[1][0:2]:
        n_t[i] = [1]

    t1 = threading.Thread(target=publish, args=(whole_pool[0] + whole_pool[1], 1, 100, 3, all_brokers[0],))
    t2 = threading.Thread(target=broadcast, args=("BROKER1", whole_pool[0][0:3], 0.2, all_brokers[0], n_t, [1], ))
    t3 = threading.Thread(target=broadcast, args=("BROKER2", whole_pool[1][0:3], 0.2, all_brokers[1], {}, [0],))


    t1.start()
    t2.start()
    t3.start()