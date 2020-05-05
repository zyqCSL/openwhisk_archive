import os
import sys
import time
import subprocess
import multiprocessing

HypervFile = "/var/lib/hyperv/.kvp_pool_0"
HarvestVmCgroupDir = "/sys/fs/cgroup/cpu/cgroup_harvest_vm/"

def main():
    global HypervFile
    global HarvestVmCgroupDir

    duration = int(sys.argv[1])         # experiment duration in seconds
    file     = sys.argv[2]
    memory   = int(sys.argv[3]) * 1024  # gb to mb

    cfs_period = 100000

    event_time = []
    event_core = []

    with open(file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            if line == "\n" or line == "":
                continue
            items = line.replace("\n", "").split(' ')
            items = [t for t in items if t != ""]
            # print items
            assert len(items) == 2
            if len(event_time) > 0:
                assert event_time[-1] < int(items[0])

            print items[0], " ", items[1]
            event_time += [int(items[0])]
            event_core += [int(items[1])]

    # check cfs sched period
    with open(HarvestVmCgroupDir + "cpu.cfs_period_us", 'r') as f:
        lines = f.readlines()
        assert len(lines) == 1
        cfs_period = int(lines[0].replace("\n", ""))
        print "cpu.cfs_period_us = ", cfs_period

    cur_trace_time = 0
    start_time = time.time()
    cur_ptr  = 0

    while cur_ptr < len(event_time):
        cur_time = time.time()
        if cur_time - start_time >= event_time[cur_ptr]:
            print "cur_ptr: ", cur_ptr
            print "elapsed time: ", format(cur_time - start_time, ".2f")
            print "event_time: ", event_time[cur_ptr]
            
            # invoker is using virtual cores, so x2
            virtual_core_num = 2 * event_core[cur_ptr]
            # write to kvp_pool
            with open(HypervFile, "w") as f:
                f.write(str(virtual_core_num) + "\n")
                f.write(str(memory) + "\n")
                print HypervFile, " written"
            # change cgroup cpu limit
            with open(HarvestVmCgroupDir + "cpu.cfs_quota_us", "w") as f:
                f.write(str(cfs_period * virtual_core_num) + "\n")
                print HarvestVmCgroupDir, "cpu.cfs_quota_us written"
            print "Time: ", event_time[cur_ptr] , ", cores: ", event_core[cur_ptr], " virtual cores: ", 2*event_core[cur_ptr]
            print ""
            cur_ptr += 1
        elif event_time[cur_ptr] >= duration:
            # check if time exceeds running time
            break
        else:
            wait_time = event_time[cur_ptr] - (cur_time - start_time)
            time.sleep(wait_time)

if __name__ == "__main__":
    main()