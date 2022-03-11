# Poll MsCluster Status

This utility polls the status of a ssh server, and tries to run
the `sinfo` command to obtain the status of the cluster as a whole.
- The utility logs this information to a discord channel if a state change is detected,
  ie. moving from online to offline, or vice-versa.

## Setup

The following environment variables are **required**:
- `DISCORD_WEBHOOK`:  (url) The webhook to post messages to a discord channel.
- `CLUSTER_HOST`:     (url) The ip/hostname of the ssh server or cluster headnode
- `CLUSTER_USER`:     (str) The ssh username to login
- `CLUSTER_PASSWORD`: (str) The ssh password to login

The following environment variables are **optional**:
- `DISCORD_USER`:            (str) The username of the discord bot that will be displayed to users.
- `DISCORD_IMG`:             (url) A link to a profile picture that the discord bot will display.
- `DISCORD_MSG_QUOTE`:       (default: false) If we should include a random qoute after the status message
- `DISCORD_MSG_INFO`:        (default: true) If the information about each cluster partition should be appended to online status messages.
- `DISCORD_MSG_ALWAYS`:      (default: false) If a message should always be posted, otherwise a message is only posted on a state change.
- `CLUSTER_PORT`:            (default: 22) The port of the ssh server.
- `CLUSTER_CONNECT_TIMEOUT`: (default: 10) How long to an attempt is made to establish a connection with the ssh server.

## Note

The script produces the file `history.json` in the current working directory which is used to persist
state between invocations. This file needs to be cached by the github action as an artefact, that subsequent
runs search for and retrieve.
