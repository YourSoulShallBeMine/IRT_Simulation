import random
import threading
import time


fortest = [1,2,4,5]
exitFlag = 0

class myThread(threading.Thread):
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter
    def run(self):
        print("Start thread >>" + self.name + "===" + str(time.time()))
        output(self.threadID, self.counter)
        print("xxx Terminate thread " + self.name + " xxxxx" + str(time.time()))

def output(id, counter):
    lop = 5
    while lop:
        if exitFlag:
            id.exit()
        time.sleep(counter)
        print("This is thread " + str(id) + ">< <> " + str(time.time()))
        lop -= 1


test = [[1,2,3,4,5], [4,5,5]]


# class addtest():
#     def __init__(self, list):
#         self.list = list
#
#     def modify(self, x):
#         self.list.append(x)
#         print("This is the list in the class: ", self.list)
#
#     def runit(self):
#         self.modify(99)
#
# trial = addtest(test[0])
# print(test)
# trial.runit()
# print("This is outside:", test)

#
# def change1(array):
#     array.append(3)
#     test2.append(1)
#     print(array)
#
# test2 = [3]
#
# change1(test[0])

# dict = {"test1": 1, "test2": 2}
#
# def addDict(dict, topic, user):
#     dict[topic] = [dict[topic]]
#     dict[topic].append(user)
#     return dict
#
# new_dict = addDict(dict, "test1", 3)
# print(new_dict)


# lock = threading.Lock()
# l = []
#
# class test1():
#     def __init__(self, n, lock):
#         self.num = n
#         self.lock = lock
#
#     def add(self):
#         self.lock.acquire()
#         l.append(self.num)
#         print(l)
#         self.lock.release()
#
#     def add_nl(self):
#         l.append(self.num)
#         print(l)
#
#
# def test2(n):
#     l.append(n)
#     print(l)
#
# for i in range(10):
#     #t = test1(i, lock)
#     th = threading.Thread(target=test2, args=(i,))
#     th.start()


# # test th in th
#
# class test():
#     def __init__(self, n):
#         self.num = n
#
#     def output(self):
#         print("This is <<%d>>" % self.num)
#
#     def run(self):
#         for i in range(self.num):
#             th = threading.Thread(target= self.output)
#             th.start()
#
#
# for i in range(3):
#     t1 = test(i + 1)
#     th = threading.Thread(target=t1.run)
#     th.start()

# # test read don't need lock
#
# L = [1,2,3,4,5]
# flag = True
#
# def add(n):
#     L.append(n)
#
# def set():
#     global flag
#     flag = False
#
# def read():
#     while flag:
#         continue
#     print(len(L))
#
# th = threading.Thread(target=read)
# th.start()
# t = 0
# time.sleep(3)
# set()

class test_globalinclass():
    def __init__(self):
        self.test = True

    def set(self):
        self.test = False

    def Loop(self):
        while self.test:
            continue
        print ("The variable is changed!")

    def run(self):
        th = threading.Thread(target=self.Loop)
        th.start()
        time.sleep(3)
        self.set()


# test the empty loop

test_emt = []
for i in test_emt:
    print("empty!")
print("out empty")