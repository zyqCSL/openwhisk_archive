import os
import sys

def main():
    test_time    = int(sys.argv[1])
    dir_name     = sys.argv[2]

    accum_core   = 0
    accum_time   = 0
    for file in os.listdir(dir_name):
        file = dir_name + "/" + file

        event_time = []
        event_core = []

        accum_time += test_time
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

                print items[0], " ", items[1]
                event_time += [int(items[0])]
                event_core += [int(items[1])]

            cur_ptr = 0
            time_exceeded = False
            while cur_ptr + 1 < len(event_time):
                cur_time  = event_time[cur_ptr]
                next_time = event_time[cur_ptr + 1]
                if next_time >= test_time:
                    time_exceeded = True
                    accum_core += (test_time - cur_time) * event_core[cur_ptr]
                    break
                else:
                    accum_core += (next_time - cur_time) * event_core[cur_ptr]

                cur_ptr += 1

            # make up to full test_time
            if not time_exceeded:
                print "not time_exceeded"
                assert cur_ptr == len(event_time) - 1
                assert event_time[-1] < test_time
                accum_core += (test_time - event_time[-1]) * event_core[-1]

    print "accum_time: ", accum_time
    print "average physical cores: ",  format(float(accum_core)/accum_time, ".2f")
    print "average virtual cores: ",  format(2*float(accum_core)/accum_time, ".2f")

if __name__ == "__main__":
    main()