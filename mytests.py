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
#
# trial = addtest(test[0])
# print(test)
# trial.modify(99)
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

dict = {"test1": 1, "test2": 2}

def addDict(dict, topic, user):
    dict[topic] = [dict[topic]]
    dict[topic].append(user)
    return dict

new_dict = addDict(dict, "test1", 3)
print(new_dict)