# mapreduce
An implementation of a MapReduce server in Python. This is e a single machine, multi-process, multi-threaded server that can execute user-submitted MapReduce jobs. It will run each job to completion, handling failures along the way, and write the output of the job to a given directory.

Majority of source code can be found in mapreduce/worker and mapreduce/master
