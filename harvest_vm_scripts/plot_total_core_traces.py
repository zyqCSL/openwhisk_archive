import matplotlib
matplotlib.use('Agg')
import csv
import os
import sys
import datetime
import matplotlib.pyplot as plt

def main():
    trace_dir    = sys.argv[1]
    trace_time   = int(sys.argv[2])
    baseline_fix_core = int(sys.argv[3])
    time = range(0, trace_time)
    cores = [0] * trace_time
    vm_num = 0
    for file in os.listdir(trace_dir):
        if '.txt' in file:
            vm_num += 1
            prev_ptr = 0
            with open(trace_dir + '/' + file, 'r') as f:
                lines = f.readlines()
                cur_c = 0
                cur_t = 0
                for line in lines:
                    data = [k for k in line.split(' ') if k != '']
                    t = int(data[0])
                    c = int(data[1])
                    cores[t] += c
                    for i in range(cur_t + 1, t):
                        cores[i] += cur_c
                    cur_t = t
                    cur_c = c

                for i in range(cur_t + 1, trace_time):
                    cores[i] += cur_c

    base_cores = [baseline_fix_core*vm_num] * trace_time
    plt.step(time, cores, label="harvest VM", markersize=10)
    plt.step(time, base_cores, label="baseline VM", markersize=10)
    plt.xticks(fontsize=24)
    plt.yticks(fontsize=24)
    plt.ylabel('Time (sec)', fontsize=24)
    plt.xlabel('Core Number', fontsize=24)
    plt.legend(prop={'size': 24})

    fig = plt.gcf()
    fig.set_size_inches(12, 7)
    fig.savefig(trace_dir.replace('/', '') + '.png', dpi=100)
    # plt.savefig(trace_dir.replace('/', '') + '.png')
                    
if __name__ == "__main__":
    main()