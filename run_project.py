#! /usr/env python
# Script for running project 2 on EC2

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from os import system
from re import findall
from urlparse import urljoin

# Setup the code
def setup_code():
    fout = open("cluster_6.bak", "w")
    fin = open("Proj2.java")
    for line in fin:
        if "private static final int NUMBER_OF_REDUCERS" in line:
            fout.write("    private static final int NUMBER_OF_REDUCERS = 24;\n")
        else:
            fout.write(line)

    fout = open("cluster_9.bak", "w")
    fin = open("Proj2.java")
    for line in fin:
        if "private static final int NUMBER_OF_REDUCERS" in line:
            fout.write("    private static final int NUMBER_OF_REDUCERS = 36;\n")
        else:
            fout.write(line)

    fout = open("cluster_12.bak", "w")
    fin = open("Proj2.java")
    for line in fin:
        if "private static final int NUMBER_OF_REDUCERS" in line:
            fout.write("    private static final int NUMBER_OF_REDUCERS = 48;\n")
        else:
            fout.write(line)

# Initialize the environment
system("yes | cp ~cs61c/proj/02-2/Proj2.java .")
system("yes | cp ~cs61c/proj/02-2/Makefile .")
system("bash ec2-init.sh")
system("source ~/ec2-environment.sh")
system("mkdir result")
setup_code()

# Run cluster size 6
system("mkdir result")
system("hadoop-ec2 launch-cluster --auto-shutdown=230 large 6 > log.txt")
log = open("log.txt").read()
name_node = findall("http.*", log)[0] + "dfshealth.jsp"
job_tracker = findall("http.*", log)[1] + "jobtracker.jsp"
system("hadoop-ec2 proxy large")
system("yes | cp cluster_6.bak Proj2.java")
system("make clean")
system("make")
system("make proj2-hadoop-clean")
system("make proj2-hadoop WIDTH=5 HEIGHT=5 CONNECT=4 2>result/exececution_log_6.txt")
system("curl " + name_node + " > result/name_node_6.html")
system("curl " + job_tracker + " > result/job_tracker_6.html")
tracker_detail = open("result/job_tracker_6.html").read()
job_detail = urljoin(job_tracker, findall(r'jobdetails.jsp.*?"', tracker_detail)[len(findall(r'jobdetails.jsp.*?"', tracker_detail)) - 1])
job_detail = job_detail[0:len(job_detail) - 1]
system("curl '" + job_detail + "' > result/job_detail_6.html")
system("make proj2-hadoop-clean")
system("echo yes | hadoop-ec2 terminate-cluster large")


# Run cluster size 9

system("hadoop-ec2 launch-cluster --auto-shutdown=230 large 9 > log.txt")
log = open("log.txt").read()
name_node = findall("http.*", log)[0] + "dfshealth.jsp"
job_tracker = findall("http.*", log)[1] + "jobtracker.jsp"
system("hadoop-ec2 proxy large")
system("yes | cp cluster_9.bak Proj2.java")
system("make clean")
system("make")
system("make proj2-hadoop-clean")
system("make proj2-hadoop WIDTH=5 HEIGHT=5 CONNECT=4 2>result/exececution_log_9.txt")
system("curl " + name_node + " > result/name_node_9.html")
system("curl " + job_tracker + " > result/job_tracker_9.html")
tracker_detail = open("result/job_tracker_9.html").read()
job_detail = urljoin(job_tracker, findall(r'jobdetails.jsp.*?"', tracker_detail)[len(findall(r'jobdetails.jsp.*?"', tracker_detail)) - 1])
job_detail = job_detail[0:len(job_detail) - 1]
system("curl '" + job_detail + "' > result/job_detail_9.html")
system("make proj2-hadoop-clean")
system("echo yes | hadoop-ec2 terminate-cluster large")


# Run cluster size 12
system("hadoop-ec2 launch-cluster --auto-shutdown=230 large 12 > log.txt")
log = open("log.txt").read()
name_node = findall("http.*", log)[0] + "dfshealth.jsp"
job_tracker = findall("http.*", log)[1] + "jobtracker.jsp"
system("hadoop-ec2 proxy large")
system("yes | cp cluster_12.bak Proj2.java")
system("make clean")
system("make")
system("make proj2-hadoop-clean")
system("make proj2-hadoop WIDTH=5 HEIGHT=5 CONNECT=4 2>result/exececution_log_12.txt")
system("curl " + name_node + " > result/name_node_12.html")
system("curl " + job_tracker + " > result/job_tracker_12.html")
tracker_detail = open("result/job_tracker_12.html").read()
job_detail = urljoin(job_tracker, findall(r'jobdetails.jsp.*?"', tracker_detail)[len(findall(r'jobdetails.jsp.*?"', tracker_detail)) - 1])
job_detail = job_detail[0:len(job_detail) - 1]
system("curl '" + job_detail + "' > result/job_detail_12.html")
system("make proj2-hadoop-clean")
system("echo yes | hadoop-ec2 terminate-cluster large")





