# hermes-chatwoot-plugin

A [Hermes Agent](https://github.com/NousResearch/hermes-agent) gateway platform plugin that connects a Chatwoot Agent Bot as a native hermes messaging channel — right alongside Telegram, Discord, WhatsApp, etc.

Each hermes profile maps to one Chatwoot Agent Bot in one Chatwoot account. Incoming customer messages are handed to the hermes agent through the standard platform-adapter dispatch path; replies are posted back into the conversation via the Chatwoot API.

## Install

```bash
hermes plugins install Orbti-AI/hermes-chatwoot-plugin --enable
```

Per profile that should run this channel (via `-p <profile>`, or interactively through `hermes gateway setup`).

## Configure

Set in the profile's `.env` (or via `hermes gateway setup`):

| Variable | Required | Description |
|---|---|---|
| `CHATWOOT_BASE_URL` | yes | Base URL of the Chatwoot instance, no trailing slash |
| `CHATWOOT_BOT_API_KEY` | yes | The Agent Bot's own `access_token` (Chatwoot → Settings → Agent Bots → this bot) — not a personal user token |
| `CHATWOOT_ACCOUNT_ID` | yes | The Chatwoot `account_id` this bot belongs to |
| `CHATWOOT_WEBHOOK_HOST` | no | Bind host for the webhook server (default `0.0.0.0`) |
| `CHATWOOT_WEBHOOK_PORT` | no | Bind port for the webhook server (default `9000`) |

## Run

```bash
hermes -p <profile> gateway run
```

Then in Chatwoot, create (or edit) an Agent Bot with outgoing URL:

```
http://<your-host>:9000/webhook
```

Assign a conversation to the bot and send a message to confirm the round trip.

## How it works

- `connect()` starts a small `aiohttp` HTTP server (same lifecycle pattern as hermes's built-in generic webhook platform) listening for Chatwoot Agent Bot webhook events.
- Incoming customer messages (`message_type: "incoming"`, non-private) are wrapped into a `MessageEvent` and handed to `handle_message()` — the same dispatch path every built-in hermes channel (Telegram, Discord, IRC, ...) uses.
- `send()` posts the agent's reply back via `POST /api/v1/accounts/{account_id}/conversations/{conversation_id}/messages`.

Reference: [Chatwoot Agent Bots API](https://developers.chatwoot.com/api-reference/account-agentbots/create-an-agent-bot)

## License

MIT
