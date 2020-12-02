import numpy as np
import random
import time
import threading

TYPE = {0: "empty", 1: "Broker", 2:"Subscriber", 3:"Publisher"}
LEVEL = 3


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
    # TODO: detect whether topics are belongs to each other
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
    def __init__(self, all_topic_pool, atp_lock, broker_graph, label_pool, name, ALL_TOPICS):
        # atp: a 2D list whose row is equal to broker number. Each broker has a row to show its received publications
        # atp_lock: protect the atp
        # broker_graph: matrix shows the relationship among brokers.  G[u][v] > 0: u is closer to the root
        # label_pool: labels to show whether a topic is from a broker or a publisher
        # name: number of this broker
        self.atp = all_topic_pool
        self.lock = atp_lock
        self.bG = broker_graph
        self.lP = label_pool
        self.name = name

        self.topic_pool = self.atp[self.name] # where the broker read publications. and subscription from other broker*
        self.subscription_pool = {} # all subscription information of this broker
        self.sp_lock = threading.Lock() # protect the subscription pool
        self.subscription_queue = []    # this is for subscription flooding. (topic, name)
        self.sq_lock = threading.Lock() # protect the subscription queue
        self.sf_flag = False

        self.pub_flag= False
        self.pub_speed = 0.3    # every 0.3 seconds publish somethings
        self.pub_sub = 3    # one time 3 publications max
        self.pub_cnt = 0    # number of total publication generated
        self.process_speed = 0.1    # process every 0.1 second

        self.ATs = ALL_TOPICS  # a file have all possible topics. Only for simulation. 3D [level][name][number]
        self.time = time.time()

    def subscribe_init(self, init_number, locality):
        # init the subscription_pool. THIS MAY UPDATE later
        # init_number: number of init topics
        # locality: randomly choose locality topics from other brokers. 0 - no others
        # TODO: take wildcard into consideration

        topics0 = []
        max_length = len(self.ATs[0][self.name])
        for i in range(init_number):
            tmp_topic = self.ATs[0][self.name][init_number%max_length]
            for j in range(len(self.ATs[0]) - 1): # select branches
                tmp_topic += "/" + self.ATs[j+1][self.name][random.randint(0, max_length-1)]
            topics0.append(tmp_topic)

        # subscribe some other
        other_cnt = 0
        while other_cnt < locality:
            target = random.randint(0, max_length)
            if target != self.name:
                # select topic from target broker
                topics0.append(self.ATs[0][target][random.randint(0, max_length-1)] + "/" +
                               self.ATs[1][target][random.randint(0, max_length-1)] + "/" +
                               self.ATs[2][target][random.randint(0, max_length-1)])
                other_cnt += 1

        for i in topics0:
            addToDict(self.subscription_pool, i, -1)
            self.subscription_queue.append((i, self.name))

        print("Broker " + str(self.name) + " init subscription successfully!", self.subscription_pool)

    def subscribe_flooding(self):
        # TODO: should this be an extra list? this depends on the protocol used to transfer subscription info
        while self.sf_flag:
            if len(self.subscription_queue) == 0:
                time.sleep(0.01)
            else:
                self.sq_lock.acquire()
                tmp_sub = self.subscription_queue[0]    # (topic, name)
                self.subscription_queue = self.subscription_queue[1:]
                self.sq_lock.release()

                for i in range(len(self.atp)):  # find a neighbor
                    if self.bG[self.name][i] != 0 and i != tmp_sub[1]:
                        self.lock.acquire()
                        self.atp[i] = [{"topic": self.lP[0] + str(self.name) + "/" +  tmp_sub[0],
                                        "message": tmp_sub[0] + "from the broker " + str(self.name)}] + self.atp[i]
                        self.lock.release()
                        print("Broker %d finished the SF of topic %s to %d !" % (self.name, tmp_sub[0], i))

    def publish(self):
        # randomly publish something at random speed to the pool
        while self.pub_flag:
            # generate topics. Message: current time
            targets = [[random.randint(0, len(self.ATs[0][self.name])-1) for i in range(LEVEL)]
                       for j in range(random.randint(1, self.pub_sub))] # [[2,4,1] * n]

            # transform to acceptable form
            tmp_list = []
            for i in targets:
                tmp_list.append({"topic": self.ATs[0][self.name][i[0]] + "/" +
                                self.ATs[1][self.name][i[1]] + "/" +
                                self.ATs[2][self.name][i[2]],
                                 "message": str(time.time() - self.time)
                                 })
            # send to the pool
            self.lock.acquire()
            self.atp[self.name] += tmp_list
            self.lock.release()

            self.pub_cnt += len(targets)
            time.sleep(self.pub_speed)

    def work_loop(self):
        # read publications from the pool and do broadcast
        iteration = 10000
        for i in range(iteration):
            while len(self.atp[self.name]) == 0:
                continue
            # abstract a topic
            self.lock.acquire()
            tmp = self.atp[self.name][0]
            self.atp[self.name] = self.atp[self.name][1:]
            self.lock.release()

            # judge its header
            header = tmp["topic"].split("/")[0]
            if header[0:len(self.lP[0])] == self.lP[0]:
                source = int(header[self.lP[0]:])   # name of the source broker
                # add to subscripion queue
                self.sq_lock.acquire()
                self.subscription_queue.attend((tmp["topic"][len(header)+1:], source))
                self.sq_lock.release()
                # save to subscription pool
                self.sp_lock.acquire()
                addToDict(self.subscription_pool, tmp["topic"][len(header)+1:], source)
                self.sp_lock.release()
            else:   # normal publication from clients
                # match with subscription pool
                try:
                    send_list = self.subscription_pool[tmp["topic"]]
                except:
                    send_list = []

                for j in send_list:
                    if j == -1: # itself
                        print("Broker %d send out to its own clients with topic <%s> and message: [%s]" % (self.name, tmp["topic"], tmp["message"]))
                    else:
                        # if neighbor broker, send it to its topic_pool
                        self.lock.acquire()
                        self.atp[j].append(tmp)
                        self.lock.release()
                        print("Broker %d transfer a publication <%s> to its neighbor %d!" % (self.name, tmp["topic"], j))

            time.sleep(self.process_speed)

    def stop(self):
        self.pub_flag = False
        self.sf_flag = True

    def start_simu(self):
        # start multi-threads for simulation
        self.subscribe_init(2, 0)
        self.pub_flag = True
        self.sf_flag = True
        th1 = threading.Thread(target=self.subscribe_flooding)
        th2 = threading.Thread(target=self.publish)
        th3 = threading.Thread(target=self.work_loop)
        th1.start()
        th2.start()
        th3.start()



