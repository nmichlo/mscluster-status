import os
from dataclasses import dataclass
from pprint import pprint
from typing import List
from typing import Optional

import paramiko
from paramiko.ssh_exception import SSHException


# ========================================================================= #
# PARSE SINFO                                                               #
# ========================================================================= #


@dataclass
class PartitionStatus:
    name: str
    avail_msg: str
    timelimit: str
    status_alloc: int
    status_idle: int
    status_down: int
    status_total: int
    nodelist: str


def _parse_sinfo_summary(string):
    """
    EXAMPLE:
        PARTITION AVAIL  TIMELIMIT   NODES(A/I/O/T)  NODELIST
        batch*       up 3-00:00:00       22/0/26/48  mscluster[11-58]
        biggpu       up 3-00:00:00          1/2/0/3  mscluster[10,59-60]
        stampede     up 3-00:00:00        35/1/4/40  mscluster[61-100]
    """
    # parse the lines
    (heading, *statuses) = (line.strip() for line in string.splitlines() if line.strip())
    # return the status codes
    assert heading == 'PARTITION AVAIL  TIMELIMIT   NODES(A/I/O/T)  NODELIST'
    # parse the statuses
    partitions = []
    for status in statuses:
        partition, avail_msg, timelimit, status, nodelist = [seg for seg in status.split(' ') if seg]
        # parse the individual values
        partition = partition.rstrip('*')
        status_alloc, status_idle, status_down, status_total = (int(i) for i in status.split('/'))
        # done
        partitions.append(PartitionStatus(
            name=partition,
            avail_msg=avail_msg,
            timelimit=timelimit,
            status_alloc=status_alloc,
            status_idle=status_idle,
            status_down=status_down,
            status_total=status_total,
            nodelist=nodelist,
        ))
    return partitions


# ========================================================================= #
# SSH CONNECTION HANDLER                                                    #
# ========================================================================= #


class SshConnectionHandler(object):

    def __init__(self, hostname: str, username: str, password: str = None, port: int = 22, timeout: int = 10):
        self._hostname = hostname
        self._username = username
        self._password = password
        self._port = port
        self._timeout = timeout
        # ssh client
        self._ssh: Optional[paramiko.SSHClient] = None

    def __enter__(self):
        # initialize the ssh client
        self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # setup a connection to the server
        self._ssh.connect(
            hostname=self._hostname,
            port=self._port,
            username=self._username,
            password=self._password,
            timeout=self._timeout,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._ssh.close()
        self._ssh = None
        return self

    def poll_cluster_status(self) -> List[PartitionStatus]:
        ssh_stdin, ssh_stdout, ssh_stderr = self._ssh.exec_command('sinfo --summarize')
        # get the output
        status_code = ssh_stdout.channel.recv_exit_status()
        out = ssh_stdout.read().decode()
        err = ssh_stderr.read().decode()
        # check the exit code
        if status_code != 0:
            raise SSHException(f'sinfo command returned non-zero status code: {repr(status_code)}')
        # check the error output
        if err:
            raise SSHException(f'sinfo command returned error text: {repr(err)}')
        # parse the standard output
        return _parse_sinfo_summary(out)


# ========================================================================= #
# MAIN                                                                      #
# ========================================================================= #



if __name__ == '__main__':

    def main():
        # connect to the server and poll the number of nodes
        with SshConnectionHandler(hostname=os.environ['CLUSTER_HOST'], username=os.environ['CLUSTER_USER']) as handler:
            status = handler.poll_cluster_status()

        # update discord based on the polled information
        pprint(status)

    main()
