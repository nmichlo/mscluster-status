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

## Note On Persisting State

The script produces the file `history.json` in the current working directory which is used to persist
state between invocations. This file needs to be cached by the github action as an artefact, that subsequent
runs search for and retrieve.

# Note On Cron Polling

The github cron event is very unreliable. To work around this we use AWS to
submit a POST request to a webhook that manually starts the github action.
- https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions

The AWS Lambda is triggered by an AWS EventBridge Rule:
- https://us-east-1.console.aws.amazon.com/events/home?region=us-east-1#/rules

Resources:
- https://upptime.js.org/blog/2021/01/22/github-actions-schedule-not-working/
- https://docs.github.com/en/rest/guides/getting-started-with-the-rest-api#authentication
- https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows#workflow_dispatch

# Note On Cron Deletion
There is no easy way to delete action runs
- We use the github api to find and delete all but the 10 newest runs using AWS, with a similar setup to
  the above, except that we run it once a day at midnight GMT.
