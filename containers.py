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
    try:
        temp_list = dict[topic]
    except:
        temp_list = []
    if user not in temp_list:
        temp_list.append(user)
    dict[topic] = temp_list
    return dict


def match_topics(pub, sub):
    # to see whether pub has a matched sub, and need to return which one is larger
    # usually pub will have no wildcard. But this function will also be used to match two sub topics.
    # 0 - not equal; 1- pub < sub; 2 - pub == sub; 3 - pub > sub
    if pub == sub:
        return 2
    p = pub.split("/")
    s = sub.split("/")
    length = min(len(p), len(s))
    res = 2
    for i in range(length):
        if p[i] == "#":
            if s[i] == "#":
                return 2
            else:
                return 3
        elif p[i] == "+":
            if s[i] == "#":
                return 1
            elif s[i] != "+":
                if res == 2:
                    res = 3
            continue
        else:
            if s[i] == "#":
                return 3
            elif s[i] == "+":
                if res == 2:
                    res = 1
                continue
            elif s[i] == p[i]:
                continue
            else:
                return 0
    return res



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
        self.pub_speed = 1    # every x seconds publish somethings
        self.pub_sub = 2    # one time x publications max
        self.pub_cnt = 0    # number of total publication generated
        self.process_speed = 0.1    # process every x second

        self.ATs = ALL_TOPICS  # a file have all possible topics. Only for simulation. 3D [level][name][number]
        self.time = time.time() # just for generating message

        # evaluation:
        self.time_sys = 0  # time from the start of the loop
        self.num_innertrans = 0  # number of publication among brokers
        self.num_innersubs = 0  # number of inner subscription info sharing




    def compress_adddict(self, topic, user):
        # made same subscription together
        current_keys = self.subscription_pool.keys()
        substopics = [] # possible topics that belongs to topic
        for i in current_keys:
            tmp = match_topics(topic, i)
            if tmp == 0:
                continue
            elif tmp <= 2: # topic < current i
                if user not in self.subscription_pool[i]:
                    self.subscription_pool[i].append(user)
                return
            elif tmp == 3:
                substopics.append(i)

        addToDict(self.subscription_pool, topic, user)
        for i in substopics:
            if user in self.subscription_pool[i]:
                self.subscription_pool[i].remove(user)
            if self.subscription_pool[i] == []:
                self.subscription_pool.pop(i)

    def subscribe_init(self, init_number, locality):
        # init the subscription_pool. THIS MAY UPDATE later
        # init_number: number of init topics
        # locality: randomly choose locality topics from other brokers. 0 - no others
        # TODO: take wildcard into consideration

        topics0 = []
        max_length = len(self.ATs[0][self.name])
        for i in range(init_number):
            tmp_topic = self.ATs[0][self.name][init_number%max_length]
            for j in range(LEVEL - 1): # select branches
                tmp_topic += "/" + self.ATs[j+1][self.name][random.randint(0, max_length-1)]
            topics0.append(tmp_topic)

        # subscribe some other
        other_cnt = 0
        while other_cnt < locality:
            target = random.randint(0, len(self.atp)-1)
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

    def subscribe_topic(self, level1, level2, level3):
        # 0~n stands for candidate index; -1: single level+; -2: multi level#
        res = ""
        input = [level1, level2, level3]
        if input.count(-1) + input.count(-2) > 1:
            print("Sorry, only support one wildcard each time right now.")
            return
        for i in range(LEVEL):
            if input[i] == -1:
                tmp = "+"
            elif input[i] == -2:
                tmp = "#"
                res += tmp
                break
            else:
                tmp = self.ATs[i][self.name][input[i]]
            res += tmp;
            if i != LEVEL-1:
                res += "/"

        #addToDict(self.subscription_pool, res, -1)
        self.compress_adddict(res, -1)
        self.subscription_queue.append((res, self.name))

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
            if header[0:len(self.lP[0])] == self.lP[0]: # subscription info from another broker
                source = int(header[len(self.lP[0]):])   # name of the source broker
                # add to subscripion queue
                self.sq_lock.acquire()
                self.subscription_queue.append((tmp["topic"][len(header)+1:], source))
                self.sq_lock.release()
                # save to subscription pool
                self.sp_lock.acquire()
                addToDict(self.subscription_pool, tmp["topic"][len(header)+1:], source)
                self.sp_lock.release()
            # elif header[0:len(self.lP[1])] == self.lP[1]:   # publication from another broker
            else:   # normal publication from clients
                # see whether it is from another broker
                source = -2
                if header[0:len(self.lP[1])] == self.lP[1]:
                    source = int(header[len(self.lP[1]):])
                    tmp["topic"] = tmp["topic"][len(header)+1:]

                send_list = []
                for j in self.subscription_pool.keys():
                    if match_topics(tmp["topic"], j) != 0:
                        send_list += self.subscription_pool[j]

                for j in send_list:
                    if j == -1: # itself
                        print("Broker %d send out to its own clients with topic <%s> and message: [%s]" % (self.name, tmp["topic"], tmp["message"]))
                    elif j != source:
                        # if neighbor broker, send it to its topic_pool
                        tmp["topic"] = self.lP[1] + str(self.name) + "/" + tmp["topic"]
                        self.lock.acquire()
                        self.atp[j].append(tmp)
                        self.lock.release()
                        print("Broker %d transfer a publication <%s> to its neighbor %d!" % (self.name, tmp["topic"], j))

            time.sleep(self.process_speed)

    def stop(self):
        self.pub_flag = False
        self.sf_flag = True

    def start_simu(self, init_number, locality):
        # start multi-threads for simulation
        self.subscribe_init(init_number, locality)
        self.pub_flag = True
        self.sf_flag = True

        th1 = threading.Thread(target=self.subscribe_flooding)
        th2 = threading.Thread(target=self.publish)
        th3 = threading.Thread(target=self.work_loop)
        th1.start()
        th2.start()
        th3.start()

        # if self.name == 0:
        #     time.sleep(2)
        #     print(self.subscription_pool)
        #     self.subscribe_topic(-2, 0, 0)
        #     print(self.subscription_pool)



