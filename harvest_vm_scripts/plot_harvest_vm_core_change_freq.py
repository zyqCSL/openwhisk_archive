import matplotlib
matplotlib.use('Agg')
import csv
import os
import sys
import datetime
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

VMcore = {}
VMmem  = {}

# harvest vm type: AZAP_Harvest_Compute_2, AZAP_Harvest_Compute_2_STRIPE

class Event:
    def __init__(self, terminate, vm_type, datetime, start_datetime):
        self.time = 0
        self.terminate = terminate  # if this is a terminationation event
        self.vm_type   = vm_type
        self.datetime = datetime
        self.time = (self.datetime - start_datetime).total_seconds()
        self.core_num = 0

        assert self.time >= 0

    def __lt__(self, other):
        return self.time < other.time

    def show(self, previous_time):
        print "datetime = ", self.datetime, "time in minutes since previous = ", format((self.time - previous_time)/60.0, '.1f'), \
            " time in seconds = ", self.time, " vm type = ", self.vm_type, " terminate = ", self.terminate, \
            " core number = ", self.core_num

def parse_vm_type(file):
    global VMcore
    global VMmem
    with open(file, 'r') as f:
        csv_reader = csv.reader(f, delimiter=',')
        line_num = 0
        for row in csv_reader:
            if line_num == 0:
                # VM Type, Cores, Memory,Host CPU allocation in pCore
                print "vm_type layout:", row
                line_num += 1
            else:
                vm_type = row[0]
                # print vm_type
                if vm_type in VMcore:
                    print vm_type, " duplicated"
                VMcore[vm_type] = int(row[1])
                VMmem[vm_type]  = float(row[2])     # in GB
                line_num += 1

def str_to_datetime(datetime_str):
    return datetime.datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')

def parse_trace_csv(file):
    global VMcore
    global VMmem

    nodes_event = {}
    nodes_start_datetime = {}

    unknown_types = []

    with open(file, 'r') as f:
        csv_reader = csv.reader(f, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print "trace_csv layout: ", row
                # ['CreationTime', 'EndTime', 'IsAllocationSucceeded', 'ContainerId', 'NodeId', 'NodeNum', 'Priority', 'InstanceType', 'TerminationCategory', 'TerminationTime']
                line_count += 1
            else:
                line_count += 1
                vm_type = row[-3]

                # caculate all vms that use core, except harvest vm
                # IOMachine has 0 core (correct me if I'm wrong). 
                if vm_type == 'AZAP_Harvest_Compute_2' or vm_type == 'AZAP_Harvest_Compute_2_STRIPE' or vm_type == "IOwnMachine":
                    continue

                # if vm_type not in VMcore and vm_type not in unknown_types:
                #     print "unknown type: ", vm_type
                #     unknown_types += [vm_type]

                node_id = row[4]
                create_time = row[0]
                terminate_time = row[-1]

                if node_id not in nodes_start_datetime:
                    nodes_start_datetime[node_id] = str_to_datetime(create_time)
                    nodes_event[node_id] = []

                create_event    = Event(terminate = False, vm_type = vm_type, datetime = str_to_datetime(create_time), start_datetime = nodes_start_datetime[node_id])
                terminate_event = Event(terminate = True,  vm_type = vm_type, datetime = str_to_datetime(terminate_time), start_datetime = nodes_start_datetime[node_id])

                nodes_event[node_id] += [create_event, terminate_event]

    print len(list(nodes_event.keys()))
    top_10_longest_nodes = []
    # keep the list of 10 longest traces


    for node_id in nodes_event:
        nodes_event[node_id] = sorted(nodes_event[node_id])

        new_top_10_longest_nodes = []
        if len(top_10_longest_nodes) < 10:
            new_top_10_longest_nodes = top_10_longest_nodes + [node_id]
        else:
            top = False
            for top_id in top_10_longest_nodes:
                if len(nodes_event[top_id]) < len(nodes_event[node_id]):
                    top = True
                elif len(nodes_event[top_id]) == len(nodes_event[node_id]):
                    top = True
                    new_top_10_longest_nodes += [top_id]
                else:
                    new_top_10_longest_nodes += [top_id]

        top_10_longest_nodes = new_top_10_longest_nodes


    print top_10_longest_nodes

    all_time_in_seconds = []

    # nf = open(file.replace("csv", "txt"), "w+")
    # for node_id in top_10_longest_nodes:
    for node_id in nodes_event:
        print node_id
        eid = 0
        total_core = 48
        time_points = []
        core_num    = {}
        while eid < len(nodes_event[node_id]):
            vm_type = nodes_event[node_id][eid].vm_type
            if eid == 0:
                if nodes_event[node_id][eid].terminate:
                    nodes_event[node_id][eid].core_num = total_core + VMcore[vm_type]
                else:
                    nodes_event[node_id][eid].core_num = total_core - VMcore[vm_type]

                # if node_id in top_10_longest_nodes:
                #     nodes_event[node_id][eid].show(nodes_event[node_id][eid].time)
            else:
                if nodes_event[node_id][eid].terminate:
                    nodes_event[node_id][eid].core_num = nodes_event[node_id][eid - 1].core_num + VMcore[vm_type]
                else:
                    nodes_event[node_id][eid].core_num = nodes_event[node_id][eid - 1].core_num - VMcore[vm_type]

                # if node_id in top_10_longest_nodes:
                #     nodes_event[node_id][eid].show(nodes_event[node_id][eid - 1].time)

            if nodes_event[node_id][eid].time not in time_points:
                time_points += [nodes_event[node_id][eid].time]

            core_num[nodes_event[node_id][eid].time] = nodes_event[node_id][eid].core_num

            eid = eid + 1

        prev_t = time_points[0]
        for t in time_points:
            # if node_id in top_10_longest_nodes:
            #     print t, format((t-prev_t)/60.0, '.1f'), ": ", core_num[t]
            all_time_in_seconds += [t-prev_t]    
            prev_t = t
        # nf.write("-------------------\n")
    # nf.close()

    all_time_in_seconds = np.array(all_time_in_seconds)
    print "mean: ", np.mean(all_time_in_seconds)
    val = []
    cdf = []
    for i in np.arange(0.1, 100, 0.1):
        print i, "%: ", np.percentile(all_time_in_seconds, i)
        val += [np.percentile(all_time_in_seconds, i)]
        cdf += [i]
    # sns_plot = sns.distplot(all_time_in_seconds)
    # fig = sns_plot.get_figure()
    # fig.savefig(file.replace("csv", "png"))
    plt.plot(val, cdf)
    plt.xscale('log')

    plt.savefig(file.replace("csv", "png"))


if __name__ == "__main__":
    vm_type_file = sys.argv[1]
    trace_csv_file = sys.argv[2]
    parse_vm_type(vm_type_file)
    parse_trace_csv(trace_csv_file)


