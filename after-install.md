**Rowboat plugin installed.** To connect this Hermes to your Rowboat org:

1. In Rowboat, open **Agents → Add agent → Hermes**. It shows the settings for this agent.
2. Set them here (the Agents screen gives the exact commands), for example:
   `hermes config set ROWBOAT_URL 'https://acme.rowboatlabs.com'` and
   `hermes config set ROWBOAT_AGENT_KEY 'rbk_…'`.
3. Restart the gateway: `hermes gateway restart`.

Then add the agent to a space in Rowboat and mention it there.
