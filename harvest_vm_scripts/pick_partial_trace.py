import matplotlib
matplotlib.use('Agg')
import csv
import os
import sys
import datetime
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import math
import random

MaxCore  = 32
TestTime = 1500 # seconds
ChangeFreq = 60

SelectedTracesByNodes  = {}
SelectedTraces = {}
TraceId = 0
CurNode = ""

class Record:
    def __init__(self, time, core):
        self.time = time
        # self.pure_time = pure_time
        self.core = core

    def show(self):
        print self.time, ' ', self.core

def check_trace(lines, start_point):
    global SelectedTracesByNodes
    global SelectedTraces
    global TestTime
    global TraceId
    global CurNode

    while start_point < len(lines) and ("-------" in lines[start_point] or " " not in lines[start_point]):
        if " " not in lines[start_point]:
            print "cur_node: ", CurNode
            CurNode = lines[start_point].replace("\n", "")
        start_point += 1

    if start_point >= len(lines):
        return -1

    cur_trace = []
    # print lines[start_point]
    cur_time = long(float(lines[start_point].split(' ')[0]))

    start_time = cur_time 

    while True:
        if start_point >= len(lines):
            return -1
        elif "-------" in lines[start_point] or " " not in lines[start_point]:
            if " " not in lines[start_point]:
                print "cur_node: ", CurNode
                CurNode = lines[start_point].replace("\n", "")
            return start_point + 1

        cur_time = long(float(lines[start_point].split(' ')[0]))
        core_num = int(lines[start_point].split(' ')[1])

        if cur_time - start_time > TestTime:
            break
        else:
            cur_trace += [Record(cur_time - start_time, core_num)]
            start_point += 1

    if len(cur_trace) >= int(math.ceil(TestTime*1.0/ChangeFreq)):
        abort = False
        for k in cur_trace:
            if k.core <= 0:
                abort = True
        if not abort:
            SelectedTraces[TraceId] = cur_trace
            if CurNode not in SelectedTracesByNodes:
                SelectedTracesByNodes[CurNode] = {}
            SelectedTracesByNodes[CurNode][len(SelectedTracesByNodes[CurNode])] = cur_trace
            TraceId = TraceId + 1
    return start_point

def main():
    global SelectedTracesByNodes
    global SelectedTraces

    file = sys.argv[1]
    with open(file, 'r') as f:
        lines = f.readlines()

        start_point = 0
        while start_point != -1:
            start_point = check_trace(lines, start_point)

        nodes = list(SelectedTracesByNodes.keys())

        print "total trace number: ", TraceId
        print "total node nubmer: ", len(nodes)
        print ""

        # keys = range(0, TraceId)
        # random.shuffle(keys)
        # print min(10, len(keys))
        # for i in range(0, min(10, len(keys))):
        #     tid = keys[i]
        #     print tid
        #     for k in SelectedTraces[tid]:
        #         k.show()

        #     print '------------------'

        random.shuffle(nodes)

        for i in range(0, min(10, len(nodes))):
            node = nodes[i]
            print node
            trace_id = random.randint(0, len(SelectedTracesByNodes[node]) - 1)

            seconds  = []
            cores    = []
            for k in SelectedTracesByNodes[node][trace_id]:
                seconds += [k.time]
                cores   += [k.core]
                k.show()

            fig = plt.figure(i)
            plt.step(seconds, cores)
            plt.xlabel("time (seconds)")
            plt.ylabel("cores")

            plt.savefig("trace_" + str(ChangeFreq) + "s_" + node + ".png")
            print '------------------'

if __name__ == "__main__":
    main()

