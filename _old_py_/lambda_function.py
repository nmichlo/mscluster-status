import asyncio
import logging
import os
import time

import datetime
from collections import namedtuple
from typing import Sequence
from typing import Tuple

import dateutil.tz
from typing import Optional

import fabric
import discord   # pip install py-cord
from paramiko.ssh_exception import SSHException


# ========================================================================= #
# VARS                                                                      #
# ========================================================================= #


LOG = logging.getLogger(__name__)


# ========================================================================= #
# LAMBDA FUNCTION                                                           #
# ========================================================================= #


def lambda_handler(event, context):

    # ssh: authentication details
    CLUSTER_HOST = os.environ['CLUSTER_HOST']
    CLUSTER_USER = os.environ['CLUSTER_USER']
    CLUSTER_PORT = os.environ.get('CLUSTER_PORT', 22)
    CLUSTER_PASSWORD = os.environ.get('CLUSTER_PASSWORD', None)

    # ssh: connection details
    CLUSTER_CONNECT_TIMEOUT = int(os.environ.get('CLUSTER_CONNECT_TIMEOUT', 10))
    CLUSTER_CONNECT_RETRIES = int(os.environ.get('CLUSTER_CONNECT_RETRIES', 5))

    # discord: get the bot token & channel to modify
    DISCORD_BOT_TOKEN = os.environ['DISCORD_BOT_TOKEN']
    DISCORD_BOT_CHANNEL_ID = int(os.environ['DISCORD_BOT_CHANNEL_ID'])
    DISCORD_BOT_WEBHOOK_NAME = os.environ.get('DISCORD_BOT_WEBHOOK_NAME', '[BOT] Cluster Status Hook [DO-NOT-EDIT]')
    DISCORD_BOT_TIMEOUT = int(os.environ.get('DISCORD_BOT_TIMEOUT', 30))
    DISCORD_BOT_RETRIES = int(os.environ.get('DISCORD_BOT_RETRIES', 1))

    # discord: get the information to display
    _DISCORD_USER_ON = os.environ.get('DISCORD_USER_ON', 'Cluster Status')
    _DISCORD_USER_OFF = os.environ.get('DISCORD_USER_OFF', 'Cluster Status')
    _DISCORD_IMG_ON = os.environ.get('DISCORD_IMG_ON',  'https://raw.githubusercontent.com/nmichlo/uploads/main/imgs/avatar/cat_happy.jpg')
    _DISCORD_IMG_OFF = os.environ.get('DISCORD_IMG_OFF', 'https://raw.githubusercontent.com/nmichlo/uploads/main/imgs/avatar/cat_glum.jpg')
    _DISCORD_EMOJI_ON = os.environ.get('DISCORD_EMOJI_ON',  '🌞')
    _DISCORD_EMOJI_OFF = os.environ.get('DISCORD_EMOJI_OFF', '⛈')
    _DISCORD_CHANNEL_NAME_ON = os.environ.get('DISCORD_CHANNEL_NAME_ON', 'cluster-status-🌞')
    _DISCORD_CHANNEL_NAME_OFF = os.environ.get('DISCORD_CHANNEL_NAME_OFF', 'cluster-status-⛈')

    # ~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~ #
    # SSH:
    # ~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~ #

    # 1. connect to cluster
    client = fabric.Connection(
        host=CLUSTER_HOST,
        user=CLUSTER_USER,
        port=CLUSTER_PORT,
        connect_timeout=CLUSTER_CONNECT_TIMEOUT,
        config=fabric.Config(overrides=dict(run=dict(hide=True))),
        connect_kwargs={} if (CLUSTER_PASSWORD is None) else {'password': CLUSTER_PASSWORD},
    )

    # 2. poll the cluster status
    LOG.info('polling cluster:')
    poll_success, poll_string, poll_time = _poll_cluster_status(client, num_retries=CLUSTER_CONNECT_RETRIES)
    LOG.info(f'polled cluster: success={repr(poll_success)}, msg={repr(poll_string)}, time={repr(time)}')

    # 3. update variables based on status
    bot_user         = _DISCORD_USER_ON         if poll_success else _DISCORD_USER_OFF
    bot_img          = _DISCORD_IMG_ON          if poll_success else _DISCORD_IMG_OFF
    bot_emoji        = _DISCORD_EMOJI_ON        if poll_success else _DISCORD_EMOJI_OFF
    bot_channel_name = _DISCORD_CHANNEL_NAME_ON if poll_success else _DISCORD_CHANNEL_NAME_OFF
    bot_status       = "ONLINE"                 if poll_success else "OFFLINE"

    # - prettify the sinfo string
    if poll_success:
        poll_string = fmt_sinfo_partitions(poll_string)

    # ~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~ #
    # DISCORD:
    # ~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~ #

    #   2. check for existing status message
    #      - if not (existing status message): post status
    #      - if (existing status message) and (status matches): update message
    #      - if (existing status message) and not (status matches): post new message

    bot_runner = _make_discord_bot_runner(bot_token=DISCORD_BOT_TOKEN)

    async def _run(client: discord.Client):
        # --- CHANNEL --- #

        print(list(client.get_all_channels()))
        exit(1)

        LOG.info('- getting channel')
        channel = client.get_channel(DISCORD_BOT_CHANNEL_ID)
        assert isinstance(channel, discord.channel.TextChannel)

        if channel.name != bot_channel_name:
            LOG.info(f'- editing channel: {repr(bot_channel_name)}')
            await channel.edit(name=bot_channel_name)

        # --- WEBHOOK --- #

        # get webhook to send message
        LOG.info('- getting webhooks')
        webhooks = await channel.webhooks()
        # - linear search for webhook, otherwise create it!
        webhook: Optional[discord.Webhook] = None
        for wh in webhooks:
            if wh.name == DISCORD_BOT_WEBHOOK_NAME:
                webhook = wh
                break
        # - create webhook if it does not exist
        if webhook is None:
            LOG.info(f'- creating webhook: {repr(DISCORD_BOT_WEBHOOK_NAME)}')
            webhook = await channel.create_webhook(name=DISCORD_BOT_WEBHOOK_NAME)

        # --- MSG --- #

        # check if we need to update the last message
        if not channel.last_message_id:
            LOG.info(f'- no last message found, will send a new message')
            last_msg = None
        else:
            last_msg: discord.Message = await channel.get_partial_message(channel.last_message_id).fetch()
            if last_msg.author.bot and (last_msg.author.id == webhook.id) and (bot_emoji in last_msg.content):
                LOG.info(f'- last message found, will update it')
            else:
                LOG.info(f'- last message found, but it is invalid, will send a new message: `{repr(last_msg.author.bot)} is False` or `{repr(last_msg.author.id)} != {repr(webhook.id)}` or {repr(bot_emoji)} not in {repr(last_msg.content)}')
                last_msg = None

        # get poll & msg creation times, then compute delta
        tz = dateutil.tz.gettz('GMT+2')
        time_poll = datetime.datetime.fromtimestamp(poll_time).astimezone(tz)
        time_msg = time_poll if (last_msg is None) else last_msg.created_at.astimezone(tz)

        # create the message
        msg_time = time_poll.strftime("**%d %b %H:%M** (_GMT+2_)")  # "**%Y-%m-%d %H:%M** (_GMT+2_)"
        msg_delta = _fmt_timedelta(delta=time_poll - time_msg)
        msg_content = f'{bot_emoji}  **{bot_status}**  |  Duration: **{msg_delta}**  |  Last Poll: {msg_time}\n```yaml\n{poll_string}\n```'

        # if the last message is not valid, or it is not the same, send a new one:
        if last_msg is not None:
            LOG.info(f'- editing last message: {repr(msg_content)}')
            await webhook.edit_message(
                message_id=last_msg.id,
                content=msg_content,
                username=bot_user,
                avatar_url=bot_img,
            )
        # update the old message
        else:
            LOG.info(f'- sending new message: {repr(msg_content)}')
            await webhook.send(
                content=msg_content,
                username=bot_user,
                avatar_url=bot_img,
            )

        # --- DONE --- #

    bot_runner(_run)

    # ~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~=~ #

    return {
        'StatusCode': 500,
        'body': 'success'
    }


# ========================================================================= #
# HELPER - SSH                                                              #
# ========================================================================= #


def _now():
    return int(time.time())


def _poll_cluster_status(client: fabric.Connection, num_retries: int = 5) -> (bool, Optional[str], int):
    poll_time, error = _now(), 'unknown error'
    # try polling multiple times, if we fail each time then send a message!
    for i in range(num_retries):
        poll_time = _now()
        # try poll for the cluster status
        try:
            result: fabric.Result = client.run('sinfo --summarize')
            # check the result
            if result.failed:
                raise SSHException(f'sinfo command returned non-zero status code: {repr(result.return_code)}')
            if result.stderr:
                raise SSHException(f'sinfo command returned error text: {repr(result.stderr)}')
            # parse the standard output
            return True, result.stdout, poll_time
        except Exception as e:
            # we failed to poll the cluster status
            LOG.error(f'failed to poll cluster status, try {i+1} of {num_retries}: {str(e)}')
            error = e
    # we failed to poll the cluster status multiple times!
    return False, str(error), poll_time


PartitionInfo = namedtuple('PartitionInfo', ['name', 'status', 'limit', 'nodelist', 'alloc', 'idle', 'down', 'total'])


def _parse_sinfo_partitions(sinfo_summary_string: str) -> Tuple[PartitionInfo]:
    """
    EXAMPLE:
        PARTITION AVAIL  TIMELIMIT   NODES(A/I/O/T)  NODELIST
        batch*       up 3-00:00:00       22/0/26/48  mscluster[11-58]
        biggpu       up 3-00:00:00          1/2/0/3  mscluster[10,59-60]
        stampede     up 3-00:00:00        35/1/4/40  mscluster[61-100]
    """
    # parse the lines
    (heading, *rows) = (row.strip() for row in sinfo_summary_string.splitlines() if row.strip())
    # return the status codes
    assert all(word in heading for word in ['PARTITION', 'AVAIL', 'TIMELIMIT', 'NODES(A/I/O/T)', 'NODELIST'])
    # parse the statuses
    partitions = []
    for row in rows:
        partition, avail, timelimit, _nodes, nodelist = [seg for seg in row.split(' ') if seg]
        # update the values
        partition = partition.rstrip('*')
        nodes_alloc, nodes_idle, nodes_down, nodes_total = _nodes.split('/')
        # store the partitions
        partitions.append(PartitionInfo(name=partition, status=avail, limit=timelimit, nodelist=nodelist, alloc=nodes_alloc, idle=nodes_idle, down=nodes_down, total=nodes_total))
    # generate the formatted table
    return tuple(partitions)


def fmt_sinfo_partitions(sinfo_summary_string: str) -> str:
    try:
        partitions = _parse_sinfo_partitions(sinfo_summary_string)
        # get the maximum lengths
        l = PartitionInfo(*[max(map(len, col)) for col in zip(*partitions)])
        # generate the output string
        return '\n'.join(
            f'{p.name+":":{l.name+1}} {p.idle:>{l.idle}}|{p.alloc:>{l.alloc}}|{p.down:>{l.down}}|{p.total:>{l.total}}  # [✓︎|✗|☠|⅀]  ({p.status})'
            for p in partitions
        )
    except:
        return f'>>> failed to parse sinfo string, got:\n{sinfo_summary_string}'


# ========================================================================= #
# HELPER - DISCORD                                                          #
# ========================================================================= #


def _fmt_timedelta(delta: datetime.timedelta, sep=' ') -> str:
    s = abs(int(delta.total_seconds()))
    y, s = divmod(s, 60*60*24*365)
    d, s = divmod(s, 60*60*24)
    h, s = divmod(s, 60*60)
    m, s = divmod(s, 60)
    i, segments = 0, []
    if (y > 0 or i > 5):             i, segments = max(i, 5), segments + [f'{y}y']
    if (d > 0 or i > 4):             i, segments = max(i, 4), segments + [f'{d}d']
    if (h > 0 or i > 3) and (i < 5): i, segments = max(i, 3), segments + [f'{h}h']
    if (m > 0 or i > 2) and (i < 4): i, segments = max(i, 2), segments + [f'{m}m']
    if (s > 0 or i > 1) and (i < 3): i, segments = max(i, 1), segments + [f'{s}s']
    return ('-' if delta.total_seconds() < 0 else '') + sep.join(segments)


def _make_discord_bot_runner(bot_token: str):
    # construct bot
    client = discord.Client()
    # make the run function
    def runner(fn):
        # entrypoint
        @client.event
        async def on_ready():
            try:
                await asyncio.wait_for(_call_fn(), timeout=120)
            except asyncio.TimeoutError as e:
                LOG.warning(f'discord update failed! {e}')
        # call function
        async def _call_fn():
            try:
                await fn(client)
            finally:
                await client.close()
        # run bot
        client.run(bot_token)
    # return runner!
    return runner


# ========================================================================= #
# END                                                                       #
# ========================================================================= #


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    lambda_handler(None, None)
