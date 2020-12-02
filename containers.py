import numpy as np
import random
import time
import threading

TYPE = {0: "empty", 1: "Broker", 2:"Subscriber", 3:"Publisher"}


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


def addToDict(dict, topic, user):
    try:
        temp_list = dict[topic]
    except:
        temp_list = []
    if user not in temp_list:
        temp_list.append(user)
    dict[topic] = temp_list
    return dict


class Device: # currently seems no need to make this father class
    def __init__(self, name, ip, type):
        self.name = name
        self.ip = ip
        self.type = TYPE[type]

    def info(self):
        print("This is %s <%s>, whose IP address is %s" %(self.type, self.name, self.ip))


# class Broker(Device):
#     def loop(self):
#         timer = 0 # second
#         timer_step = 1e-3
#         timer_lim = int(1 / timer_step)
#         print(timer_lim)
#         timer_count = 0
#         while 1:
#             if timer_count == 0 :
#                 print("%s inside timer: %d" % (self.name, timer))
#             timer_count += 1
#             if timer_count >= timer_lim:
#                 timer_count = 0
#                 timer += 1
#             time.sleep(timer_step)


class Broker():
    def __init__(self, all_topic_pool, atp_lock, broker_graph, label_pool, label, ALL_TOPICS):
        # atp: a 2D list whose row is equal to broker number. Each broker has a row to show its received publications
        # atp_lock: protect the atp
        # broker_graph: matrix shows the relationship among brokers.  G[u][v] > 0: u is closer to the root
        # label_pool: labels to show whether a topic is from a broker or a publisher
        # label: number of this broker
        self.atp = all_topic_pool
        self.lock = atp_lock
        self.bG = broker_graph
        self.lP = label_pool
        self.topic_pool = self.atp[label]
        self.subscription_pool = {}

        self.ATs = ALL_TOPICS  # a file have all possible topics. Only for simulation. 3D [level][label][number]

    def subscribe(self, init_number, locality):
        # init the subscription_pool. THIS MAY UPDATE later
        # init_number: randomly choose in topics from the ATs
        # locality: randomly choose locality topics from other brokers
        topics1 = some_rand(init_number, 0, len(self.ATs[0]))
        for i in topics1:
            addToDict(self.subscription_pool, self.topic_pool[i], -1)

        # subscribe some other




class Publisher():
    def publish(self):
        pass


