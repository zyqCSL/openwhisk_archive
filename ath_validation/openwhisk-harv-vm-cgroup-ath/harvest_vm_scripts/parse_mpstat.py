import sys
import os

CoreNum = 48 # virtual cores on each vm

def main():
    global CoreNum

    mpstat_directory = sys.argv[1]
    trace_directory  = sys.argv[2]
    run_time         = int(sys.argv[3])

    cpu_limit = {}
    for file in os.listdir(trace_directory):
        invoker = int(file.replace(".txt", ""))
        print invoker
        cpu_limit[invoker] = {} # indexed by time (second precision)
        file = trace_directory + "/" + file

        event_time = []
        event_core = []

        with open(file, 'r') as f:
            lines = f.readlines()
            for line in lines:
                if line == "\n" or line == "":
                    continue
                items = line.replace("\n", "").split(' ')
                items = [t for t in items if t != ""]
                print items
                # print items
                assert len(items) == 2
                if len(event_time) > 0:
                    assert event_time[-1] < int(items[0])

                # print items[0], " ", items[1]
                event_time += [int(items[0])]
                event_core += [int(items[1])]

            cur_ptr = 0
            for i in range(0, run_time + 1):
                # the trace shows changes in core number at specific time point
                # actual core number at each t should be at [cur_ptr - 1]
                if cur_ptr < len(event_time) - 1 and i >= event_time[cur_ptr + 1]:
                    cur_ptr += 1
                cpu_limit[invoker][i] = event_core[cur_ptr] * 2 # convert to virtual cores
                # print i, cpu_limit[invoker][i]

    usage = {}
    for file in os.listdir(mpstat_directory):
        invoker = -1
        if "invoker" not in file:
            continue
        invoker = int(file.replace("invoker", "").replace(".txt", ""))
        trace_invoker = invoker
        if trace_invoker not in cpu_limit:
            trace_invoker = 0
        file = mpstat_directory + "/" + file
        sum_cpu_util = 0
        counter = 0
        all_list = []
        partial_list = []
        with open(file, 'r') as f:
            lines = f.readlines()
            for line in lines:
                if 'all' in line:
                    items = line.split(' ')
                    items = [i for i in items if i != '']
                    util = (100 - float(items[-1])) * CoreNum/cpu_limit[trace_invoker][counter]

                    all_list += [util]
                    sum_cpu_util += util
                    counter += 1

        usage[invoker] = sum_cpu_util/counter

        # print 'time util'
        # for k in all_list:
        #     print k

    invokers = sorted(list(usage.keys()))
    print invokers
    for invoker in invokers:
        print usage[invoker]

if __name__ == '__main__':
    main()
