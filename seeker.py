#!/usr/bin/env python
# coding: utf-8

import argparse
import datetime 
import subprocess
import sys
import textwrap

DEBUG = False



def time_to_float(time):
    """ converts [dd-[hh:]]mm:ss time to seconds """
    days, hours = 0, 0

    if "-" in time:
        days = int(time.split("-")[0]) * 86400
        time = time.split("-")[1]
    time = time.split(":")

    if len(time) > 2:
        hours = int(time[0]) * 3600

    mins = int(time[-2]) * 60
    secs = float(time[-1])

    return days + hours + mins + secs


def float_to_time(secs):
    """ converts seconds to [dd-[hh:]]mm:ss """
    days = secs // 86400
    secs %= 86400
    hours = secs // 3600
    secs %= 3600
    mins = secs // 60
    secs %= 60

    secs = "{:05.2f}".format(secs) if (secs % 1 != 0) else "{:02.0f}".format(secs)

    if days > 0:
        return (
            "{:02.0f}".format(days)
            + "-"
            + "{:02.0f}".format(hours)
            + ":"
            + "{:02.0f}".format(mins)
            + ":"
            + secs
        )
    elif hours > 0:
        return (
            "{:02.0f}".format(hours)
            + ":"
            + "{:02.0f}".format(mins)
            + ":"
            + secs
        )

    return "{:02.0f}".format(mins) + ":" + secs


# convert megabytes to mem_str with units
def mb_to_str(num):
    num = float(num)
    if num > 10 ** 3:
        return str(round(num / 1000.0, 2)) + "GB"
    elif num < 1:
        return str(round(num * 1000.0, 2)) + "KB"
    return str(round(num, 2)) + "MB"


# convert mem str to number of megabytes
def str_to_mb(s, cores=1):
    if (s[-1] != "c") and (s[-1] != "n") and (s[-1] != "B"):
        s += "B"

    unit = s[-2:]
    num = float(s[:-2])
    if "c" in unit:
        num *= cores

    unit = unit.replace("c", "B").replace("n", "B")
    
    if unit == "GB":
        return round(num * 1000, 2)
    elif unit == "KB":
        return round(num / 1000, 2)
    return round(num, 2)


# convert a mem str to the right unit with
# num part between 1 <= x < 1000
def fix_mem_str(s, c=1):
    return mb_to_str(str_to_mb(s, c))


def get_percentage(num):
    return min(round(num * 100, 2), 100)


# def check_state(state):
#     return (state != "COMPLETED")


def parse_groups(groups, line, jobid):
    line = line.split("|")

    if not line[2] or not line[3]:
        return groups

    user, group = line[2], line[3]

    if group not in groups.keys():
        groups[group] = {user:[jobid]}
    elif user not in groups[group].keys():
        groups[group][user] = [jobid]
    elif jobid not in groups[group][user]:
        groups[group][user].append(jobid)

    return groups


def wasteful(stats, limits):
    # for eff in stats:
    #     if eff != None and eff < limit:
    #         return True
    # return False

    for stat, limit in zip(stats, limits):
        if stat != None and stat < limit:
            return True
    return False


# ASSUMES JOBID IS CONTIGUOUS FROM SACCT DATA
def add_stats(stats, jobid, stat):
    if jobid not in stats.keys():
        stats[jobid] = stat
    
    return stats


def parse_stats(lines):
    # print("single")
    mem, time, cpu = [], [], []
    req_mem, req_time, req_cpu = None, None, None
    for line in lines:
        line = line.split("|")

        if (line[4] != "") and (req_mem == None):
            req_mem = str_to_mb(line[4])
        if (line[6] != "") and (req_time == None):
            req_time = time_to_float(line[6])
        if (line[8] != "") and ((req_cpu == None) or (int(line[8] > req_cpu))):
            req_cpu = int(line[8])

        if line[5]:
            mem.append(str_to_mb(line[5]))
        if line[7] and not time:
            time.append(time_to_float(line[7]))
        if line[7] and line[9] and (time_to_float(line[7]) != 0) and (time_to_float(line[9]) != 0):
            cpu.append(time_to_float(line[9])/(time_to_float(line[7])*req_cpu))

    
    mem_eff = get_percentage(sum(mem)/req_mem) if (mem and req_mem) else None
    time_eff = get_percentage(sum(time)/req_time) if (time and req_time) else None
    cpu_eff = get_percentage(sum(cpu)/len(cpu)) if (cpu) else None

    return [mem_eff, time_eff, cpu_eff]



# returns list split by newline of sacct output
def query_sacct():

    if (DEBUG):
        file = open(sys.argv[1], "r")
        result = file.read()
    else:
        # get sacct output
        # took out Cluster, JobName, and ExitCode for now
        start_date = datetime.date.today().strftime("%m%d%y")
        end_date = (datetime.date.today() + datetime.timedelta(days=1)).strftime("%m%d%y")


        query = (
                "sacct -anP -s CD -S %s -E %s --format=JobID,State,User,Group,"
                "ReqMem,MaxRSS,Timelimit,Elapsed,ReqCPUS,TotalCPU" 
                % (start_date, end_date)
            )

        # catch no response from sacct error
        try:
            result = subprocess.check_output([query], shell=True)
        except subprocess.CalledProcessError:
            print("Error: sacct failed to respond, please try again later.")
            return None
            
    # use utf-8 encoding if possible
    if sys.version_info[0] >= 3:
        result = str(result, "utf-8")

    # split by line and remove all empty lines
    return [x for x in result.split("\n") if x != ""]
    

def parse_data(data, limit):
    groups, stats = {}, {}
    curr_id, prev_id = None, None
    isArray, tmpisArray = False, False
    start = 0
    # print("len", len(data))
    for i in range(len(data)):
        line = data[i].split("|")

        jobid_raw = line[0].split(".")[0].split("_")

        if (len(jobid_raw) == 2):
            tmpisArray = True
            curr_id, arr_num = jobid_raw
        else:
            curr_id, arr_num = jobid_raw[0], None


        # if check_state(line[1]):
        #     prev_id = curr_id
        #     continue

        if ((curr_id != prev_id) and (prev_id != None)):
            stat = parse_stats(data[start:i])

            if wasteful(stat, limit):
                groups = parse_groups(groups, data[start], prev_id)
                stats = add_stats(stats, prev_id, stat)
                # l = data[start].split("|")
                # if l[2] and l[2] == "crr49":
                #     print("start: ", data[start])
                #     for a in data[start:i]:
                #         print(a)

            isArray = tmpisArray
            start = i
        
        if (i == len(data) - 1):
            stat = parse_stats(data[start:])

            if wasteful(stat, limit):
                groups = parse_groups(groups, data[start], curr_id)
                stats = add_stats(stats, curr_id, stat)
            

        tmpisArray = False
        prev_id = curr_id

    return [groups, stats]
   
# def print_dict(d):
#     for k,i in d.items():
#         print(k, i)


def sort_groups(groups, stats):
    sums = {}
    for group, users in groups.items():
        count = 0
        for jobs in users.values():
            count += len(jobs)
        sums[group] = count
    
    return sorted(sums.items(), key=lambda x: x[1], reverse=True)


def sort_users(groups, stats):
    sums = {}
    for group, users in groups.items():
        for user, jobs in users.items():
            if user in sums.keys():
                sums[user] += len(jobs)
            else:
                sums[user] = len(jobs)
    
    return sorted(sums.items(), key=lambda x: x[1], reverse=True)


def print_list(l):
    for i in l:
        print("{:15s}  {:>5.0f}".format(i[0], i[1]))


def print_job(stats, job):
    stat = [(str(i) + "%" if i else "---") for i in stats] 
    print("{:>9s}|{:>7s}|{:>8s}|{:>7s}".format(job, stat[0], stat[1], stat[2]))

def get_user(groups, stats, username):
    for group, users in groups.items():
        for user, jobs in users.items():
            if user == username:
                print("-----------------------------------")
                print("User: " + user)
                print("-----------------------------------")
                print("  Jobid  |  Mem  |  Time  |  CPU  |")
                print("-----------------------------------")

                for job in jobs:
                    print_job(stats[job], job)

                print("-----------------------------------")
                print("Total number of wasteful jobs: " + str(len(jobs)))
                print("-----------------------------------")
                return
    print("No jobs to show for user " + str(username))


def get_group(groups, stats, group_name):
    for group, users in groups.items():
        if group == group_name:
            count = 0
            print("-----------------------------------")
            print("Group: " + group)
            print("-----------------------------------")
            print("  Jobid  |  Mem  |  Time  |  CPU  |")
            print("-----------------------------------")
            
            for user, jobs in users.items():
                print("User: " + user)
                count += len(jobs)
                for job in jobs:
                    print_job(stats[job], job)

            print("-----------------------------------")
            print("Total number of wasteful jobs: " + str(count))
            print("-----------------------------------")
            return
    print("No jobs to show for group " + str(group_name))

def main(limit_list, sort, group, user):
    # check threshold bounds
    for limit in limit_list:
        if (limit < 0) or (limit > 100):
            print("Invalid limit given.")
            return 

    # call sacct
    raw_data = query_sacct()

    # empty data or no response from sacct
    if (len(raw_data) == 0) or (raw_data == None):
        print("No data available for analysis.")
        return 

    # parse the data 
    groups, stats = parse_data(raw_data, limit_list)


    # call appropriate function to print stats
    if group or user:
        if group:
            get_group(groups, stats, group)
        if user:
            get_user(groups, stats, user)
    elif sort:
        print_list(sort_groups(groups, stats))
    else:
        print_list(sort_users(groups, stats))


    # print_dict(groups)
    # print_dict(stats)

    # for k,v in groups.items():
    #     print(k)
    #     for a,b in v.items():
    #         print(a, len(b))

    # print sorted by groups
    # print_list(sort_groups(groups, stats))

    # print sorted by users
    # print_list(sort_users(groups, stats))
    
    # get_user(groups, stats, "st474")
    # get_group(groups, stats, "zhao")
    # get_user(groups, stats, "none")
    # get_group(groups, stats, "none")



if __name__ == '__main__':
    desc = (
    """
    waste-seeker
    https://github.com/ycrc/waste-seeker
    ---------------
    An internal tool that can be used to highlight inefficient resource usage by scanning through all jobs within the past day. 

    To use waste-seeker, simply run "waste-seeker" and it will default to displaying the numbers of inefficient jobs sorted by user. 

    To explore more options, run "waste-seeker -h".

    For debugging, you can also use waste-seeker on a text file containing the results of the command:
        sacct -anP -s CD -S <start_date> -E <end_date> 
        --format=JobID,State,User,Group,ReqMem,MaxRSS,Timelimit,Elapsed,ReqCPUS,TotalCPU" 
    where date formats are specified within the sacct manual.
    Note: DEBUG flag must be set to True for debugging mode. 
    
    Default theshold limit is set to 50%. Waste-seeker accepts any float in the range [0-100].

    To specify a user, please use their netid. 
    -----------------
    """
    )

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(desc),
    )

    if DEBUG:
        parser.add_argument("file")

    parser.add_argument("-m", type=float, default=50, action="store", dest="mem", help="set memory threshold limit (percentage [0-100]")
    parser.add_argument("-t", type=float, default=50, action="store", dest="time", help="set time threshold limit (percentage [0-100]")
    parser.add_argument("-c", type=float, default=50, action="store", dest="cpu", help="set cpu threshold limit (percentage [0-100]")
    parser.add_argument("-s", "--sort", action="store_true", default=False, dest="sort", help="toggle between sorting by users to group")
    parser.add_argument("-g", "--group", action="store", dest="group", help="show wasteful jobs for a specific group")
    parser.add_argument("-u", "--user", action="store", dest="user", help="show wasteful jobs for a specific user (enter netid)")
    
    args = parser.parse_args()
    main([args.mem, args.time, args.cpu], args.sort, args.group, args.user)