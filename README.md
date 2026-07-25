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
| `CHATWOOT_WEBHOOK_HOST` | no | Bind host (default `::`, dual-stack). A `0.0.0.0` bind is unreachable on IPv6-only private networks such as Railway's |
| `CHATWOOT_WEBHOOK_PORT` | no | Bind port for the webhook server (default `9000`) |

`aiohttp` is required and is **not** guaranteed to come with `hermes-agent` —
install it explicitly (`pip install aiohttp`). When it is missing the adapter
refuses to connect with `aiohttp is not available — cannot start webhook server`.

## Run

```bash
hermes gateway run
```

Then in Chatwoot, create (or edit) an Agent Bot with outgoing URL:

```
http://<your-host>:9000/webhook/<profile>
```

Connect the bot to an **inbox** (Settings → Inboxes → your inbox → Bot), not to
an individual conversation. Chatwoot only fires the webhook for conversations
in a bot-connected inbox, and only while they are `pending` — it pulls a
conversation out of the bot's control (status → `open`) as soon as a delivery
fails. Send a message in that inbox to confirm the round trip.

## Multi-tenant

One profile == one Agent Bot == one Chatwoot account. With
[`gateway.multiplex_profiles`](https://github.com/NousResearch/hermes-agent) on,
a single gateway process serves every profile and this plugin is the single
listener for all of them:

| profile | Agent Bot `outgoing_url` |
|---|---|
| imunizar | `http://host:9000/webhook/imunizar` |
| acme | `http://host:9000/webhook/acme` |

Onboarding a tenant is: create the profile, set its three `CHATWOOT_*` values,
restart. No code change, no port to allocate, and no redeploy that takes the
other tenants down with it.

The Chatwoot side needs to be able to reach the URL. Chatwoot routes Agent Bot
webhooks through `SafeFetch`, whose SSRF guard rejects private-IP hosts
outright (`Invalid webhook URL ... has no public ip addresses`) unless the
Chatwoot service sets `SAFE_FETCH_ALLOW_PRIVATE_NETWORK=true`.

## How it works

- `connect()` starts one `aiohttp` HTTP server (same lifecycle pattern as hermes's built-in generic webhook platform). Only the **active** profile binds; a secondary profile's adapter is a no-op, since the hub already serves it by path.
- Incoming customer messages (`message_type: "incoming"`, non-private) are wrapped into a `MessageEvent` and handed to `handle_message()` — the same dispatch path every built-in hermes channel (Telegram, Discord, IRC, ...) uses. The profile from the URL is stamped onto `source.profile`, so the agent turn resolves that tenant's config, skills, memory and model — mirroring the built-in webhook platform's `/p/<profile>/webhooks/<route>` prefix.
- `send()` posts the agent's reply back via `POST /api/v1/accounts/{account_id}/conversations/{conversation_id}/messages`. Because the hub receives every tenant's webhook, it is also the instance that sends every reply, so it resolves credentials per tenant. `chat_id` is namespaced `<profile>:<conversation_id>` to carry that routing — Chatwoot conversation ids are per-account and collide across tenants otherwise.
- A payload whose `account.id` disagrees with the profile's configured `CHATWOOT_ACCOUNT_ID` is dropped rather than guessed at: answering it would post one customer's conversation into another tenant's account.

Reference: [Chatwoot Agent Bots API](https://developers.chatwoot.com/api-reference/account-agentbots/create-an-agent-bot)

## License

MIT
