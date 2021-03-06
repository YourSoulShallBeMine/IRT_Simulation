import numpy as np
import random
import time
import threading
import copy

TYPE = {0: "empty", 1: "Broker", 2:"Subscriber", 3:"Publisher"}
LEVEL = 3
SF_SIZE = 2 + (5 + 4 + 3) + 30 + 8  # a length (Bytes) of a subscription info
PNP_SIZE = 2 + (5 + 4 + 3) + 30 # a length (Bytes) without payload of a transfer publication


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
                    return 0
        else:
            if s[i] == "#":
                return 1
            elif s[i] == "+":
                if res == 2:
                    res = 1
                    continue
                else:
                    return 0
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
        self.pub_sub = 3    # one time x publications max
        self.pub_cnt = 0    # number of total publication generated
        self.process_speed = 0.1    # process every x second
        self.sub_flag = False
        self.sub_speed = 0.31  # every x seconds receive a subscription

        self.ATs = ALL_TOPICS  # a file have all possible topics. Only for simulation. 3D [level][name][number]
        self.time = time.time() # just for generating message

        # evaluation:
        self.time_sys = 0  # time from the start of the loop
        self.num_innertrans = 0  # number of publication among brokers
        self.num_innersubs = 0  # number of inner subscription info sharing
        self.num_subinfo = []  # number of topics stored in the subinfo
        self.size_accumulate = 0 # total size that this broker has added to the network
        self.size_trend = [] # size at different round

        self.size_expectedpf = 0    # total size if using pf algorithm
        self.size_pftrend = []      # pf size at different round

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


    def subscribe_init_2(self, init_number, locality):
        # generate, but won't add back to the queue/pool
        # force to generate topics with wildcard
        if init_number < 2 or locality < 2:
            print("Both parameters should larger than 2 for demo2 use !")
        

    def subscribe_init(self, init_number, locality):
        # init the subscription_pool. THIS MAY UPDATE later
        # init_number: number of init topics
        # locality: randomly choose locality topics from other brokers. 0 - no others

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

    def subscribe_topic(self, input, name, method):
        # 0~n stands for candidate index; -1: single level+; -2: multi level#
        res = ""
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
                tmp = self.ATs[i][name][input[i]]
            res += tmp;
            if i != LEVEL-1:
                res += "/"

        if method == 0:
            addToDict(self.subscription_pool, res, -1)
        elif method == 1:
            self.compress_adddict(res, -1)
        self.subscription_queue.append((res, self.name))

    def get_subpool_size(self):
        size = 0
        for k in self.subscription_pool.values():
            size += len(k)
        return size

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
                        self.size_accumulate += SF_SIZE
                        self.lock.release()
                        print("Broker %d finished the SF of topic %s to %d !" % (self.name, tmp_sub[0], i))
                        time.sleep(self.process_speed)
                #self.sf_flag = False    # for demo2, cause no Subscription arrives after the initial
        #print("\n === Broker %d end the initial subscription flooding === \n" % self.name)  # for demo2

    def subscribers(self, method):
        # simulate the action of subscribers.
        # method: whether consider wildcard optimization. 1- consider; 0 - no consider
        candidate_1 = 3  # equal to the num_of_candidate in main.py -> main()
        while self.sub_flag:
            # randomly generate a topic
            topic = [0 for i in range(3)]   # 0~n stands for candidate index; -1: single level+; -2: multi level#
            candidates = np.linspace(-2, candidate_1-1, candidate_1 + 2)
            wildcard_chance = [0.1, 0.2] # possibility to select [#, +]
            other_chance = [(1 - wildcard_chance[0] - wildcard_chance[1]) / float(candidate_1) for i in range(candidate_1)]
            for i in range(len(topic)):
                topic[i] = int(np.random.choice(candidates, 1, p=wildcard_chance+other_chance)[0])
            # can generate on other brokers
            name = self.name
            if random.random() > 0.8:
                name = random.randint(0, len(self.atp)-1)
            self.subscribe_topic(topic, name, method)
            time.sleep(self.sub_speed)
            self.num_subinfo.append(self.get_subpool_size())

    def publish(self):
        # randomly publish something at random speed to the pool
        #iteration = 30 # for demo 2
        print("\n>>>Broker %d starts the publishes! <<<\n" % self.name)
        while self.pub_flag:
        #for ite in range(iteration):
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
            for t in tmp_list:
                self.size_expectedpf += (PNP_SIZE + int(t["message"][-2:])) * len(self.atp)
            time.sleep(self.pub_speed)
        #self.pub_flag = False

    def work_loop(self):
        # read publications from the pool and do broadcast
        iteration = 600
        for i in range(iteration):
            if i%20 == 0:
                print("-=-=-=-= iteration %d =-=-=-=-\n" % i)
            # if not self.sub_flag or not self.pub_flag or not self.sf_flag:
            #     print(">> Terminated because of the flag chagne <<<\n"
            #           + "sub_flag" + str(self.name) + " is " + str(self.sub_flag)
            #           + "pub_flag" + str(self.name) + " is " + str(self.pub_flag)
            #           + "sf_flag" + str(self.name) + " is " + str(self.sf_flag)
            #           )
            # while len(self.atp[self.name]) == 0:  # no publication received, then be idle TODO: spin lock is not so good
            #     if not self.pub_flag:
            #         if len(self.atp[self.name]) == 0:
            #             print("\n>> Broker %d Terminated because pub is end <<<\n" % self.name)
            #             return
            #     continue
            # abstract a topic
            if len(self.atp[self.name]) == 0:
                self.size_trend.append(self.size_accumulate)
                self.size_pftrend.append(self.size_expectedpf)
                time.sleep(self.process_speed)
                continue
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
                time.sleep(self.process_speed)
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
                        self.size_accumulate += PNP_SIZE + int(tmp["message"][-2:])
                        self.lock.release()
                        print("Broker %d transfer a publication <%s> to its neighbor %d!" % (self.name, tmp["topic"], j))
                    time.sleep(self.process_speed)

            self.size_trend.append(self.size_accumulate)
            self.size_pftrend.append(self.size_expectedpf)
        self.stop()
        print("Broker %d has totally generated %d messages." % (self.name, self.pub_cnt))

    def stop(self):
        self.pub_flag = False
        self.sf_flag = False
        self.sub_flag = False

    def demo2(self, init_number, locality, res0, res1):
        # start multi-threads for simulation
        self.subscribe_init(init_number, locality)
        self.pub_flag = True
        self.sf_flag = True
        self.sub_flag = True

        th1 = threading.Thread(target=self.subscribe_flooding)
        th3 = threading.Thread(target=self.work_loop)
        th1.start()
        th3.start()
        time.sleep(20)
        th2 = threading.Thread(target=self.publish)
        th2.start()

        th3.join()
        res0[self.name] = self.size_trend
        res1[self.name] = self.size_pftrend
        # print("Broker %d has totally generated %d messages." % (self.name, self.pub_cnt))

        #time.sleep(10)
        #self.stop()

    def demo1(self, init_number, method, res):
        # demo1: the comparison bewteen whether use the wildcard
        # init_number: how many topics at the beginning; method: 0 for traditional, 1 for optimized
        # return res of number of topics at each round

        # start multi-threads for simulation
        self.subscribe_init(init_number, 0)
        self.sf_flag = True
        self.sub_flag = True

        th1 = threading.Thread(target=self.subscribe_flooding)
        th2 = threading.Thread(target=self.subscribers, args=(method,))
        th1.start()
        th2.start()

        time.sleep(10)
        self.stop()
        print(self.subscription_pool)
        print(self.num_subinfo)
        self.lock.acquire()
        res[self.name] = self.num_subinfo
        self.lock.release()



