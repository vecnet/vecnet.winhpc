#!/usr/bin/env python
# This file is part of the vecnet.winhpc package.
# For copyright and licensing information about this package, see the
# NOTICE.txt and LICENSE.txt files in its top-level directory; they are
# available at https://github.com/vecnet/vecnet.winhpc
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License (MPL), version 2.0.  If a copy of the MPL was not distributed
# with this file, You can obtain one at http://mozilla.org/MPL/2.0/.


""" Submit a job specified on the command line to Windows HPC cluster
python -m vecnet.winhpc <hostname> <username> <password> "<command to submit>"
"""
import argparse
from vecnet.winhpc.webapi import WebAPI


def main(hostname, username, password, name, command, workdir, priority):
    server = WebAPI(hostname, username, password)
    job_id = server.create_job(name=name, priority=priority)
    if workdir is None:
        server.add_task(job_id,
                        name="Task created by vecnet.winhpc library",
                        commandLine=command)
    else:
        print workdir
        task_id = server.add_task(job_id,
                        name="Task created by vecnet.winhpc library",
                        commandLine=command,
                        WorkDirectory=workdir)
        if task_id is None:
            print "Task creation failed"
            print "Error: %s" % server.response

    if server.submit_job(job_id):
        print "Successfully submitted job %s" % job_id
    else:
        print "Job submission failed, job_id %s" % job_id
    return job_id

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--hostname")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--command")
    parser.add_argument("--name")
    parser.add_argument("--priority")
    parser.add_argument("--workdir")

    args = parser.parse_args()
    hostname = args.hostname
    username = args.username
    password = args.password
    command = args.command
    name = args.name or "Job submitted by vecnet.winhpc library"
    priority = args.priority or "Normal"
    workdir = args.workdir
    main(hostname, username, password, name, command, workdir, priority)
