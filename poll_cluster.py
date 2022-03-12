import json
import logging
import os
import random
import time
import warnings
from dataclasses import dataclass
from datetime import datetime
from pprint import pformat
from typing import List
from typing import NoReturn
from typing import Optional
from typing import Sequence
from typing import Tuple

import discord  # pip install py-cord
import discord.ext.commands
import discord.ext.tasks
import fabric
from paramiko.ssh_exception import SSHException


logger = logging.getLogger(__name__)


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

    @property
    def status(self) -> str:
        return 'online' if self.online else 'offline'

    @property
    def status_msg(self):
        return 'online' if self.online else f'offline ({self.error_msg})'

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

    def __init__(
        self,
        host: str,
        user: str,
        port: int = 22,
        password: str = None,
        connect_timeout: int = 10,
        num_retries: int = 5,
    ):
        self._host = host
        self._user = user
        self._port = port
        self._connect_timeout = connect_timeout
        self._num_retries = num_retries
        # ssh client
        self._connect_kwargs = {}
        self._client: Optional[fabric.Connection] = None
        # add password
        if password is not None:
            self._connect_kwargs['password'] = password

    def __enter__(self):
        # initialize the ssh client
        self._client = fabric.Connection(
            host=self._host,
            user=self._user,
            port=self._port,
            connect_timeout=self._connect_timeout,
            config=fabric.Config(overrides=dict(run=dict(hide=True))),
            connect_kwargs=self._connect_kwargs,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._client.close()
        self._client = None
        return self

    def poll_cluster_status(self) -> ClusterStatus:
        poll_time, error = Now(), 'unknown error'
        # try polling multiple times, if we fail each time then send a message!
        for i in range(self._num_retries):
            poll_time = Now()
            # try poll for the cluster status
            try:
                result: fabric.Result = self._client.run('sinfo --summarize')
                # check the result
                # TODO: authentication errors and the likes should be handled separately!
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
                logger.error(f'failed to poll cluster status, try {i+1} of {self._num_retries}: {str(e)}')
                error = e
        # we failed to poll the cluster status multiple times!
        return ClusterStatus(
            partitions=None,
            poll_time=poll_time.time,
            online=False,
            error_msg=str(error),
        )


# ========================================================================= #
# ARTEFACT HANDLING                                                         #
# ========================================================================= #


class ArtefactHandler(object):

    def __init__(self, path: str, max_age: int = 60*60*24):
        self._path = path
        self._max_age = max_age
        assert max_age > 0
        # inner storage
        self._entries: List[ClusterStatus] = None

    @property
    def max_age(self):
        return self._max_age

    @property
    def is_open(self):
        return (self._entries is not None)

    @property
    def entries(self):
        if not self.is_open:
            raise RuntimeError('The artifact handler is not open')
        return list(self._entries)

    def __enter__(self):
        if self.is_open:
            raise RuntimeError('The artifact handler is already open')
        # load the data from the file
        raw_entries = []
        if os.path.exists(self._path):
            try:
                with open(self._path, 'r') as fp:
                    raw_entries = json.load(fp)
            except Exception as e:
                print('Failed to load artifact:')
        # convert to lists of objects and append the new status
        entries = []
        for status in raw_entries:
            try:
                entries.append(ClusterStatus.from_dict(status))
            except Exception as e:
                warnings.warn(f'dropped invalid entry: {status}, reason: {e}')
        # store on this object
        self._entries = entries
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.is_open:
            raise RuntimeError('The artifact handler is already closed')
        # convert back to lists of dictionaries
        raw_entries = [status.to_dict() for status in self._entries]
        # save to disk
        with open(self._path, 'w') as fp:
            json.dump(raw_entries, fp)
        return self

    def push(self, status: ClusterStatus) -> NoReturn:
        if not self.is_open:
            raise RuntimeError('The artifact handler is not open')
        # make sure that the status we are appending is newer than everything else
        assert all(status.poll_time > entry.poll_time for entry in self._entries)
        # filter the old values
        entries = [entry for entry in self._entries if (status.poll_time - entry.poll_time < self.max_age)]
        # append the item
        entries.append(status)
        # sort entries in ascending order of poll_time, this means the oldest entries are first
        entries = sorted(entries, key=lambda entry: entry.poll_time)
        # done!
        self._entries = entries
        return self.entries


# ========================================================================= #
# UPDATE HANDLER                                                            #
# ========================================================================= #


class Notifier(object):

    def on_poll(self, curr: ClusterStatus):
        pass

    def on_first_status(self, curr: ClusterStatus):
        pass

    def on_changed_status(self, curr: ClusterStatus, prev: ClusterStatus):
        pass

    def on_unchanged_status(self, curr: ClusterStatus, prev: ClusterStatus):
        pass

    def on_after_dispatch(self, curr: ClusterStatus, prev: Optional[ClusterStatus]):
        pass


    def dispatch(self, entries: List[ClusterStatus]):
        # checks
        if len(entries) < 0:
            raise RuntimeError('This should never happen!')
        # always poll
        curr, prev = entries[-1], None
        self.on_poll(curr)
        # handle the correct case
        if len(entries) == 1:
            self.on_first_status(curr)
        else:
            prev = entries[-2]
            if curr.online != prev.online:
                self.on_changed_status(curr=curr, prev=prev)
            else:
                self.on_unchanged_status(curr=curr, prev=prev)
        # final things
        self.on_after_dispatch(curr, prev)


class ConsoleNotifier(Notifier):

    def on_poll(self, curr: ClusterStatus):
        pass

    def on_first_status(self, curr: ClusterStatus):
        print(f'started polling, cluster is: {curr.status_msg}')

    def on_changed_status(self, curr: ClusterStatus, prev: ClusterStatus):
        print(f'cluster is now: {curr.status_msg}')

    def on_unchanged_status(self, curr: ClusterStatus, prev: ClusterStatus):
        print(f'no change in cluster status: {curr.status_msg}')


class DiscordNotifier(Notifier):

    def __init__(
        self,
        webhook_url: str = None,
        username: str = None,
        avatar_url: str = None,
        num_emojies: int = 3,
        static_emojis: bool = False,
        append_qoute: bool = False,
        append_info: bool = False,
        update_on_unchanged: bool = False,
        offline: bool = False,
    ):
        self._webhook_url = os.environ['DISCORD_WEBHOOK'] if (webhook_url is None) else webhook_url
        self._username = username
        self._avatar_url = avatar_url
        self._num_emojies = num_emojies
        self._static_emojis = static_emojis
        self._append_qoute = append_qoute
        self._append_info = append_info
        self._update_on_unchanged = update_on_unchanged
        self._offline = offline
        # construct the webhook
        self._webhook = discord.Webhook.from_url(
            url=self._webhook_url,
            adapter=discord.RequestsWebhookAdapter(),
        )

    def _send(self, content: str):
        if self._offline:
            return
        self._webhook.send(
            content=content,
            wait=True,
            username=self._username,
            avatar_url=self._avatar_url,
            tts=False,
            file=None,
            files=None,
            embed=None,
            embeds=None,
            allowed_mentions=None,
        )

    def _make_msg(self, curr: ClusterStatus):
        # get emojis to use
        online = ['✨', '🌟', '🏆', '🥇', '✅', '🔋', '👌', '🤙', '👍', '🙌', '👏', '🤞', '🤩', '💃', '🕺', '🌞', '🧃', '🍦', '🍰', '🎉', '🎊', '🎈', '🥳', '💪', '🆗', '🆙', '💯', '🚀', '⏳', '💡', '❤️', '⤴️', '😇', '👼', '🍀', '🤑', '🎁']  # '🔛', '✔️'
        offline = ['🃏', '💤', '❗️', '❌', '🚫', '⚠️', '🧨', '🛠', '🪤', '🏚', '🏗', '🚧', '⛈', '🐋', '🐒', '🙈', '🙉', '🤒', '😴', '👎', '🤌', '👇', '🤦', '🙆', '😭', '🙃', '😵', '⛑', '💀', '⚰️', '🪦', '🙅', '⤵️', '🥔', '🚑', '🗿', '🧘', '🦤', '🤡', '💩', '🆘', '⛔️', '⁉️', '💔', '🏁']  # '💣'
        emoji = '🚀' if curr.online else '💀'
        # shuffle the emojies
        emojies = online if curr.online else offline
        random.shuffle(emojies)
        # get a random qoute
        qoute = ''
        if self._append_qoute:
            try:
                text = get_random_qoute(keywords=('love', 'success', 'happiness', 'life') if online else ('truth', 'pain', 'death'))
                text, author = text.split(' — ')
                qoute = f'\n> *{text}*' \
                        f'\n> - **{author}**'
            except:
                pass
        # generate the string!
        status = f'**{curr.status.upper()}**'
        emoji_l = (f'{"".join(emojies[:self._num_emojies])}  ' if self._num_emojies > 0 else '') if (not self._static_emojis) else f'{emoji}  '
        emoji_r = (f'  {"".join(emojies[-self._num_emojies:])}' if self._num_emojies > 0 else '') if (not self._static_emojis) else f''
        time_info = f'  |  [{datetime.fromtimestamp(curr.poll_time).strftime("%Y/%m/%d %H:%M:%S")}]'
        error_info = f'  |  *{curr.error_msg}*' if (not curr.online) else ''
        # get partition info sting
        partition_info = ''
        if self._append_info and curr.online and curr.partitions:
            # format everything
            table = [(f'{p.name}:', p.status_idle, p.status_alloc, p.status_down, p.status_total) for p in curr.partitions]
            lengths = [max(len(f'{v}') for v in col) for col in zip(*table)]
            table = [[f'{v:{l}}' for v, l in zip(row, lengths)] for row in table]
            # generate the string
            partitions_str = '\n'.join(f'{n} {i}|{a}|{d}|{t}  # [I|A|D|T]' for n, i, a, d, t in table)
            partition_info = f'\n```yaml\n{partitions_str}\n```'
        # combine into a single message
        msg = f'{emoji_l}{status}{emoji_r}{time_info}{error_info}' \
               f'{qoute}' \
               f'{partition_info}'
        # log the message
        logger.info('\n' + msg)
        return msg

    def on_poll(self, curr: ClusterStatus):
        pass

    def on_first_status(self, curr: ClusterStatus):
        logger.info(f'started polling, cluster is: {curr.status_msg}')
        self._send(self._make_msg(curr))

    def on_changed_status(self, curr: ClusterStatus, prev: ClusterStatus):
        self._send(self._make_msg(curr))
        logger.info(f'cluster is now: {curr.status_msg}')

    def on_unchanged_status(self, curr: ClusterStatus, prev: ClusterStatus):
        logger.info(f'no change in cluster status: {curr.status_msg}')
        msg = self._make_msg(curr)
        if self._update_on_unchanged:
            self._send(msg)

    def on_after_dispatch(self, curr: ClusterStatus, prev: Optional[ClusterStatus]):
        logger.info('Dispatched:')
        logger.info(f'- curr: {curr}')
        logger.info(f'- prev: {prev}')

# ========================================================================= #
# RANDOM QOUTES                                                             #
# ========================================================================= #


def get_random_qoute(keywords: Sequence[str] = ('failure',)):
    import requests
    import bs4
    # load all the qoutes for the different keywords
    keyword = random.choice(keywords)
    result = requests.get(f'https://zenquotes.io/keywords/{keyword}')
    page = bs4.BeautifulSoup(result.content, features="html.parser")
    qoutes = page.find_all('blockquote', {'class': 'blockquote'})
    qoutes = [qoute.text for qoute in qoutes]
    # sentiment analysis
    # | import nltk
    # | import nltk.sentiment
    # | nltk.download('vader_lexicon')
    # | sia = nltk.sentiment.SentimentIntensityAnalyzer()
    # | # get sentiment
    # | sentiment = [(text, sia.polarity_scores(text.split(' — ')[0])) for text in qoutes]
    # | sentiment = sorted(sentiment, key=lambda item: item[1]['compound'])
    # | for text, scores in sentiment:
    # |     print(scores['compound'], text)
    # done!
    return random.choice(qoutes)


# ========================================================================= #
# LOGIC                                                                     #
# ========================================================================= #


def poll_and_update(
    notifier: Notifier,
    artifact_path: str = 'history.json',
    connection_handler: SshConnectionHandler = None,
    max_age: int = 60 * 60 * 24,
):
    if connection_handler is None:
        connection_handler = SshConnectionHandler(host=os.environ['CLUSTER_HOST'], user=os.environ['CLUSTER_USER'])
    # connect to the server and poll the number of nodes
    with connection_handler as ssh_handler:
        status = ssh_handler.poll_cluster_status()
        logger.info(f'Polled Status:\n{pformat(status.to_dict())}')
    # get the artefacts from disk, append the polled statuses, and save
    with ArtefactHandler(artifact_path, max_age=max_age) as artifact_handler:
        entries = artifact_handler.push(status)
        logger.info(f'History Size: {len(entries)-1}')
    # dispatch the notifications
    notifier.dispatch(entries)


# ========================================================================= #
# MAIN                                                                      #
# ========================================================================= #


def to_boolean(var: str) -> bool:
    if isinstance(var, str):
        var = var.lower()
        if var in ('', 'false', 'f', 'no', 'n', '0'):
            return False
        elif var in ('true', 't', 'yes', 'y', '1'):
            return True
        else:
            raise ValueError(f'Cannot convert value to boolean, got invalid string: {repr(var)}')
    elif isinstance(var, int):
        return bool(var)
    elif isinstance(var, bool):
        return var
    else:
        raise TypeError(f'Cannot convert value to boolean: {repr(var)}, got invalid type: {type(var)}')


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)

    poll_and_update(
        notifier=DiscordNotifier(
            webhook_url = os.environ['DISCORD_WEBHOOK'],
            username    = os.environ.get('DISCORD_USER', 'Cluster Status'),
            avatar_url  = os.environ.get('DISCORD_IMG', 'https://raw.githubusercontent.com/nmichlo/uploads/main/cluster_avatar.jpg'),
            num_emojies = int(os.environ.get('DISCORD_MSG_EMOJIS', 0)),
            static_emojis       = to_boolean(os.environ.get('DISCORD_MSG_STATIC', True)),
            append_qoute        = to_boolean(os.environ.get('DISCORD_MSG_QUOTE',  False)),
            append_info         = to_boolean(os.environ.get('DISCORD_MSG_INFO',   False)),
            update_on_unchanged = to_boolean(os.environ.get('DISCORD_MSG_ALWAYS', False)),
            offline             = to_boolean(os.environ.get('DISCORD_DRY_RUN',    False)),
        ),
        connection_handler=SshConnectionHandler(
            host=os.environ['CLUSTER_HOST'],
            user=os.environ['CLUSTER_USER'],
            port=os.environ.get('CLUSTER_PORT', 22),
            password=os.environ.get('CLUSTER_PASSWORD', None),
            connect_timeout=int(os.environ.get('CLUSTER_CONNECT_TIMEOUT', 10)),
            num_retries=int(os.environ.get('CLUSTER_CONNECT_RETRIES', 5)),
        ),
        artifact_path='history.json',
        max_age=60 * 60 * 24,  # 1 day
    )


# ========================================================================= #
# END                                                                       #
# ========================================================================= #
