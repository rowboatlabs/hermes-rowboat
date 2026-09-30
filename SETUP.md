# Connect this Hermes to Rowboat

These are instructions for **you, the Hermes agent**, from the person you work with. They want you connected to their Rowboat org as a member of their team: people will @mention you in Rowboat spaces and you will answer in the thread. Follow the steps in order, using your terminal. Every command is safe to run twice.

The person's message gave you two values. Use them where the commands below say `<ROWBOAT_URL>` and `<HOME_CHANNEL>`:

- **Rowboat address**: the org's URL, such as `https://acme.rowboatlabs.com`.
- **Home channel**: an id such as `01M3RQS11GP2M8130HBK3R24FR`, which is their direct messages with you in Rowboat. If the message has none, skip that line.

## Rules

- **Never print, echo, or repeat the agent key**, and never ask for it in the chat. It is already in your environment as `ROWBOAT_AGENT_KEY`; the commands only refer to it as `${ROWBOAT_AGENT_KEY}`, which Hermes fills in itself.
- **Do not restart the gateway yourself.** You would stop your own run. Ask the person to do it at the end.
- If a step fails, stop and report the command and its error.

## 1. Check the key is there

```sh
hermes config get ROWBOAT_AGENT_KEY
```

It should show a masked value starting with `rbk_`. If it shows nothing, stop and ask the person to add it themselves, **outside this chat**: `hermes config set ROWBOAT_AGENT_KEY 'rbk_…'` in a terminal, or `ROWBOAT_AGENT_KEY` on the Hermes dashboard's Keys page. Then start again.

## 2. Save the settings

```sh
hermes config set ROWBOAT_URL '<ROWBOAT_URL>'
hermes config set ROWBOAT_HOME_CHANNEL '<HOME_CHANNEL>'
hermes config set ROWBOAT_ALLOW_ALL_USERS true
hermes config set ROWBOAT_OWNER_COMMANDS true
hermes config set mcp_servers.rowboat.url '${ROWBOAT_URL}/mcp'
hermes config set mcp_servers.rowboat.headers.Authorization 'Bearer ${ROWBOAT_AGENT_KEY}'
hermes config set display.platforms.rowboat.tool_progress off
hermes config set display.platforms.rowboat.show_reasoning false
hermes config set display.platforms.rowboat.long_running_notifications false
hermes config set display.platforms.rowboat.busy_ack_detail false
```

Keep the single quotes: they stop your shell from expanding `${…}`, so Hermes gets the placeholders.

What these do: point you at the org; send your unprompted messages (such as scheduled results) to the person's DM with you; let anyone Rowboat allows mention you; let only your owner run Hermes commands from Rowboat; give you the Rowboat tools over MCP; and keep your tool progress and reasoning out of shared threads.

## 3. Install the Rowboat plugin

Check whether it is already there:

```sh
hermes plugins list
```

If `rowboat-platform` is **not** listed:

```sh
hermes plugins install rowboatlabs/hermes-rowboat --enable
```

If it **is** listed, bring it up to date and make sure it is on:

```sh
hermes plugins update rowboat-platform
hermes plugins enable rowboat-platform
```

## 4. Check

```sh
hermes plugins list --enabled
hermes config get ROWBOAT_URL
hermes config get mcp_servers.rowboat.url
```

`rowboat-platform` should be enabled, `ROWBOAT_URL` should be the address above, and the MCP URL should be `${ROWBOAT_URL}/mcp`.

## 5. Tell the person

Say that you are set up, and ask them to restart your gateway so the Rowboat connection starts: send `/restart` in this chat, run `hermes gateway restart`, or press **Restart Gateway** on the Hermes dashboard. After that, they add you to a space in Rowboat (Add people) and mention you there.
