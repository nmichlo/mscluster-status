# Poll MsCluster Status

This utility polls the status of a ssh server, and tries to run
the `sinfo` command to obtain the status of the cluster as a whole.
- The utility logs this information to a discord channel if a state change is detected,
  ie. moving from online to offline, or vice-versa.
- Messages are updated with the online/offline duration if the state remains the same,
  otherwise if the state changes from online to offline or vice versa, then a new message is posted.

## Setup

The following environment variables are **required**:
- `DISCORD_BOT_TOKEN`:             (str) The discord bot token.
- `DISCORD_BOT_CHANNEL_ID`:        (str) The channel ID of the channel to edit if `DEBUG_SCRIPT=0`
- `DISCORD_BOT_CHANNEL_ID_DEBUG`:  (str) The channel ID of the channel to edit if `DEBUG_SCRIPT=1`
- `CLUSTER_HOST`:                  (url) The ip/hostname of the ssh server or cluster headnode
- `CLUSTER_USER`:                  (str) The ssh username to login
- `CLUSTER_PASSWORD`:              (str) The ssh password to login
- `DEBUG_SCRIPT`:                  (int) 1 if testing [default], 0 if production

The following environment variables are **optional**:
- `CLUSTER_PORT`:            (default: 22) The ssh port for the server
- `CLUSTER_CONNECT_RETRIES`: (default: 5) How many times to attempt connecting to the ssh server.
- `DISCORD_BOT_RETRIES`:     (default: 3) How many times to attempt connecting to discord to post updates.
- `DISCORD_TIME_LOCATION`:   (default: "Africa/Johannesburg") The timezone to use for formatting time in messages.

The following environment variables are **optional** and are only for formatting when online/offline:
- `DISCORD_USER_ON`: (default: "Cluster Status") The username to post under when the cluster is online.
- `DISCORD_USER_OFF`: (default: "Cluster Status") The username to post under when the cluster is offline.
- `DISCORD_IMG_ON`: (default: "https://raw.githubusercontent.com/nmichlo/uploads/main/imgs/avatar/cat_happy.jpg")
- `DISCORD_IMG_OFF`: (default: "https://raw.githubusercontent.com/nmichlo/uploads/main/imgs/avatar/cat_glum.jpg")
- `DISCORD_EMOJI_ON`: (default: "🌞") The emoji to search for in the update message to save the online state.
- `DISCORD_EMOJI_OFF`: (default: "⛈") The emoji to search for in the update message to save the offline state.
- `DISCORD_CHANNEL_NAME_ON`: (default: "cluster-status-🌞") The channel name to use when the cluster is online
- `DISCORD_CHANNEL_NAME_OFF`: (default: "cluster-status-⛈") The channel name to use when the cluster is offline

## Note On Persisting State

The script posts messages to discord and searches the last message for the emoji contained
in `DISCORD_EMOJI_ON` or `DISCORD_EMOJI_OFF` to save the state. If an emoji is found, then
that is the last online/offline state.
- If the current polled state does not match, then we post a new message instead of updating the old message.

# Cloud Setup

AWS Lambda is triggered by an AWS EventBridge Rule:
1. **events**: https://us-east-1.console.aws.amazon.com/events/home?region=us-east-1#/rules
2. **lambda**: https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions

We use the AWS free tier to run this script.

## Upload

1. Run `aws_build.sh` to generate `main.zip` and upload this to the AWS
   lambda function with the `Go 1.x` runtime setup for arch `x86_64`. The
   handler should also be changed to `main`
