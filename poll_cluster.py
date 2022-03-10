import json
import os
import time
import warnings
from dataclasses import dataclass
from typing import List
from typing import NoReturn
from typing import Optional
from typing import Tuple

import fabric
from paramiko.ssh_exception import SSHException


# ========================================================================= #
# PARSE SINFO                                                               #
# ========================================================================= #

class Now(object):

    def __init__(self):
        self._time = int(time.time())

    @property
    def time(self) -> int:
        return self._time


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

    def to_dict(self) -> dict:
        return dict(
            name=self.name,
            avail_msg=self.avail_msg,
            timelimit=self.timelimit,
            status_alloc=self.status_alloc,
            status_idle=self.status_idle,
            status_down=self.status_down,
            status_total=self.status_total,
            nodelist=self.nodelist,
        )

    @staticmethod
    def from_dict(dat: dict) -> 'PartitionStatus':
        return PartitionStatus(**dat)


@dataclass
class ClusterStatus:
    partitions: Optional[Tuple[PartitionStatus, ...]]
    poll_time: int
    online: bool
    error_msg: Optional[str]

    def to_dict(self):
        return dict(
            partitions=tuple(p.to_dict() for p in self.partitions) if (self.partitions is not None) else None,
            poll_time=self.poll_time,
            online=self.online,
            error_msg=self.error_msg,
        )

    @staticmethod
    def from_dict(dat: dict) -> 'ClusterStatus':
        dat = dict(dat)
        dat['partitions'] = tuple(PartitionStatus.from_dict(p) for p in dat['partitions']) if (dat['partitions'] is not None) else None
        return ClusterStatus(**dat)


def parse_sinfo_partitions(sinfo_summary_string: str) -> Tuple[PartitionStatus]:
    """
    EXAMPLE:
        PARTITION AVAIL  TIMELIMIT   NODES(A/I/O/T)  NODELIST
        batch*       up 3-00:00:00       22/0/26/48  mscluster[11-58]
        biggpu       up 3-00:00:00          1/2/0/3  mscluster[10,59-60]
        stampede     up 3-00:00:00        35/1/4/40  mscluster[61-100]
    """
    # parse the lines
    (heading, *statuses) = (line.strip() for line in sinfo_summary_string.splitlines() if line.strip())
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
    return tuple(partitions)


# ========================================================================= #
# SSH CONNECTION HANDLER                                                    #
# ========================================================================= #


class SshConnectionHandler(object):

    def __init__(self, host: str, user: str, port: int = 22, connect_timeout: int = 10):
        self._host = host
        self._user = user
        self._port = port
        self._connect_timeout = connect_timeout
        # ssh client
        self._client: Optional[fabric.Connection] = None

    def __enter__(self):
        # initialize the ssh client
        self._client = fabric.Connection(
            host=self._host,
            user=self._user,
            port=self._port,
            connect_timeout=self._connect_timeout,
            config=fabric.Config(overrides=dict(run=dict(hide=True))),
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._client.close()
        self._client = None
        return self

    def poll_cluster_status(self) -> ClusterStatus:
        poll_time = Now()
        # try poll for the cluster status
        try:
            result: fabric.Result = self._client.run('sinfo --summarize')
            # check the result
            if result.failed:
                raise SSHException(f'sinfo command returned non-zero status code: {repr(result.return_code)}')
            if result.stderr:
                raise SSHException(f'sinfo command returned error text: {repr(result.stderr)}')
            # parse the standard output
            partitions = parse_sinfo_partitions(result.stdout)
            # make the cluster status
            return ClusterStatus(
                partitions=tuple(partitions),
                poll_time=poll_time.time,
                online=True,
                error_msg=None,
            )
        except Exception as e:
            # we failed to poll the cluster status
            return ClusterStatus(
                partitions=None,
                poll_time=poll_time.time,
                online=False,
                error_msg=str(e),
            )


# ========================================================================= #
# ARTEFACT HANDLING                                                         #
# ========================================================================= #


def load_artifact(artifact: str) -> List[dict]:
    dat = []
    if os.path.exists(artifact):
        try:
            with open(artifact, 'r') as fp:
                dat = json.load(fp)
        except Exception as e:
            print('Failed to load artifact:')
    return dat


def save_artifact(artifact: str, dat: List[dict]) -> NoReturn:
    with open(artifact, 'w') as fp:
        json.dump(dat, fp)


def append_status(entries: List[ClusterStatus], status: ClusterStatus, max_age: int = 60 * 60 * 24) -> List[ClusterStatus]:
    # make sure that the status we are appending is newer than everything else
    assert all(status.poll_time > entry.poll_time for entry in entries)
    # filter the old values
    entries = [entry for entry in entries if (status.poll_time - entry.poll_time < max_age)]
    # append the item
    entries.append(status)
    # sort entries in ascending order of poll_time, this means the oldest entries are first
    entries = sorted(entries, key=lambda entry: entry.poll_time)
    # done!
    return entries


def convert_and_drop_invalid_statuses(entries: List[dict]) -> List[ClusterStatus]:
    converted = []
    for status in entries:
        try:
            converted.append(ClusterStatus.from_dict(status))
        except Exception as e:
            warnings.warn(f'dropped invalid entry: {status}, reason: {e}')
    return converted

# ========================================================================= #
# UPDATE HANDLER                                                            #
# ========================================================================= #


class Notifier(object):

    def force_update_state(self, curr: ClusterStatus):
        raise NotImplementedError

    def update_state_to_online(self, curr: ClusterStatus, prev: ClusterStatus):
        raise NotImplementedError

    def update_state_to_offline(self, curr: ClusterStatus, prev: ClusterStatus):
        raise NotImplementedError

    def generic_update_state(self, curr: ClusterStatus, prev: ClusterStatus):
        raise NotImplementedError

    def dispatch(self, entries: List[ClusterStatus]):
        # checks
        if len(entries) < 0:
            raise RuntimeError('This should never happen!')
        # handle the correct case
        curr = entries[-1]
        if len(entries) == 1:
            self.force_update_state(curr)
        else:
            prev = entries[-2]
            if curr.online != prev.online:
                if curr.online:
                    self.update_state_to_online(curr=curr, prev=prev)
                else:
                    self.update_state_to_offline(curr=curr, prev=prev)
            else:
                self.generic_update_state(curr=curr, prev=prev)


class ConsoleNotifier(Notifier):

    def force_update_state(self, curr: ClusterStatus):
        if curr.online:
            print(f'started polling, cluster is: ONLINE')
        else:
            print(f'started polling, cluster is: OFFLINE ({curr.error_msg})')

    def update_state_to_online(self, curr: ClusterStatus, prev: ClusterStatus):
        print(f'cluster is now: ONLINE')

    def update_state_to_offline(self, curr: ClusterStatus, prev: ClusterStatus):
        print(f'cluster is now: OFFLINE ({curr.error_msg})')

    def generic_update_state(self, curr: ClusterStatus, prev: ClusterStatus):
        if curr.online:
            print(f'no change in cluster status: ONLINE')
        else:
            print(f'no change in cluster status: OFFLINE ({curr.error_msg})')


class DiscordNotifier(ConsoleNotifier):
    pass


# ========================================================================= #
# MAIN                                                                      #
# ========================================================================= #


if __name__ == '__main__':

    def poll_and_update(artifact: str, notifier: Notifier, max_age: int = 60 * 60 * 24):
        # connect to the server and poll the number of nodes
        with SshConnectionHandler(host=os.environ['CLUSTER_HOST'], user=os.environ['CLUSTER_USER']) as handler:
            status = handler.poll_cluster_status()
        # get the artefacts from disk, append the polled statuses, and save
        entries = convert_and_drop_invalid_statuses(load_artifact(artifact))
        entries = append_status(entries, status, max_age=max_age)
        save_artifact(artifact, [status.to_dict() for status in entries])
        # dispatch the notifications
        notifier.dispatch(entries)

    def main():
        notifier = ConsoleNotifier()
        poll_and_update('history.json', notifier, max_age=60)

    main()
