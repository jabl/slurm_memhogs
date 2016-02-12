#!/usr/bin/python

"""
Copyright (c) 2014,2016 Janne Blomqvist

This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Find SLURM memory hogs, that is jobs that have used much less memory
than they have allocated, leading to unused cluster resources. Email
those users with a message suggesting they alter their ways.


"""

def str2int(s):
    """String to integer conversion for slurm style memory usage strings.

    Returns 0 if conversion didn't work. Returned unit is KiB.
    """
    if len(s) == 0:
        return 0
    if s[-1] == 'c' or s[-1] == 'n':
        # Memory per core or per node, treat equally for now
        s = s[:-1]
    if s[-1] == 'K':
        return int(s[:-1])
    elif s[-1] == 'M':
        return int(s[:-1]) * 1024
    elif s[-1] == 'G':
        return int(s[:-1]) * 1024 * 1024
    elif len(s) == 0:
        return 0
    else:
        return int(s)
    

def run_sacct(jobid, starttime, endtime):
    import subprocess
    cmd = 'sacct -pn -ouser,jobid,reqmem,maxrss'
    if jobid:
        cmd += ' -j %d' % int(jobid)
    if starttime:
        cmd += ' -S %s' % starttime
    if endtime:
        cmd += ' -E %s' % endtime

    p = subprocess.Popen(cmd, shell=True, bufsize=-1,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    badjobs = {} # username to list of job id's map
    jobusr = {} # Job id to username map
    for line in p.stdout:
        ls = line.split('|')
        j = ls[1]
        js = j.split('.')
        jobid = int(js[0])
        if len(ls[0]) > 0:
            jobusr[jobid] = ls[0]
        maxrss = str2int(ls[3])
        reqmem = str2int(ls[2])
        if maxrss > 0:
            if reqmem / maxrss > 2:
                u = jobusr[jobid]
                if u in badjobs:
                    badjobs[u].append(jobid)
                else:
                    badjobs[u] = [jobid]
    return badjobs

if __name__ == '__main__':
    from optparse import OptionParser

    usage = """%prog [options]

Find SLURM memory hogs, optionally send email to users."""
    parser = OptionParser(usage)
    parser.add_option('-j', '--jobs', dest='jobid',
                      help='Job ID to check')
    parser.add_option('-E', '--endtime', dest='endtime',
                      help='Select jobs that finished before this date')
    parser.add_option('-S', '--starttime', dest='starttime',
                      help='Select jobs that started adter this date')
    (options, args) = parser.parse_args()
    jobs = run_sacct(options.jobid, options.starttime, options.endtime)
    print jobs
