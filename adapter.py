"""Chatwoot Agent Bot gateway adapter for Hermes Agent.

Runs a small aiohttp HTTP server (same lifecycle pattern as the built-in
``gateway/platforms/webhook.py`` generic webhook adapter) that receives
Chatwoot Agent Bot webhook events, hands incoming customer messages to the
Hermes agent via the standard ``BasePlatformAdapter.handle_message()`` path
(same dispatch pattern as the built-in IRC adapter), and posts replies back
into the conversation via the Chatwoot API.

One hermes profile == one Chatwoot Agent Bot == one account_id. To onboard a
new client, create a new hermes profile, configure its own
CHATWOOT_BASE_URL/CHATWOOT_BOT_API_KEY/CHATWOOT_ACCOUNT_ID, install this
plugin on it, and run `hermes -p <profile> gateway run`.

Chatwoot Agent Bot API reference:
https://developers.chatwoot.com/api-reference/account-agentbots/create-an-agent-bot
"""

import json
import logging
import os
import socket as _socket
from typing import Any, Dict, Optional

try:
    from aiohttp import web
    import aiohttp

    AIOHTTP_AVAILABLE = True
except ImportError as _aiohttp_import_error:
    AIOHTTP_AVAILABLE = False
    web = None  # type: ignore[assignment]
    logging.getLogger(__name__).error(
        "[chatwoot] aiohttp import failed: %r", _aiohttp_import_error
    )

from gateway.config import Platform
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

logger = logging.getLogger(__name__)


class ChatwootAdapter(BasePlatformAdapter):
    """Async Chatwoot Agent Bot adapter implementing BasePlatformAdapter.

    Instantiated by the adapter_factory passed to register_platform().
    """

    def __init__(self, config, **kwargs):
        platform = Platform("chatwoot")
        super().__init__(config=config, platform=platform)

        self._base_url = os.environ.get("CHATWOOT_BASE_URL", "").rstrip("/")
        self._bot_api_key = os.environ.get("CHATWOOT_BOT_API_KEY", "")
        self._account_id = os.environ.get("CHATWOOT_ACCOUNT_ID", "")
        self._host = os.environ.get("CHATWOOT_WEBHOOK_HOST", "0.0.0.0")
        self._port = int(os.environ.get("CHATWOOT_WEBHOOK_PORT", "9000"))

        self._runner: Optional["web.AppRunner"] = None
        self._session: Optional["aiohttp.ClientSession"] = None

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not AIOHTTP_AVAILABLE:
            logger.error("[chatwoot] aiohttp is not available — cannot start webhook server")
            return False

        if not self._base_url or not self._bot_api_key or not self._account_id:
            logger.error(
                "[chatwoot] CHATWOOT_BASE_URL/CHATWOOT_BOT_API_KEY/CHATWOOT_ACCOUNT_ID "
                "must all be set"
            )
            return False

        # Port conflict detection — fail fast if port is already in use,
        # same check the built-in webhook adapter uses.
        try:
            with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as s:
                s.settimeout(1)
                s.connect(("127.0.0.1", self._port))
            logger.error(
                "[chatwoot] Port %d already in use. Set CHATWOOT_WEBHOOK_PORT to a free port.",
                self._port,
            )
            return False
        except (ConnectionRefusedError, OSError):
            pass  # port is free

        self._session = aiohttp.ClientSession()

        app = web.Application()
        app.router.add_get("/health", self._handle_health)
        app.router.add_post("/webhook", self._handle_webhook)

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()
        self._mark_connected()

        logger.info(
            "[chatwoot] Listening on %s:%d — account_id=%s",
            self._host,
            self._port,
            self._account_id,
        )
        return True

    async def disconnect(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        if self._session:
            await self._session.close()
            self._session = None
        self._mark_disconnected()
        logger.info("[chatwoot] Disconnected")

    # ── Inbound: Chatwoot webhook -> hermes agent ───────────────────────

    async def _handle_health(self, request: "web.Request") -> "web.Response":
        return web.json_response({"status": "ok", "platform": "chatwoot"})

    async def _handle_webhook(self, request: "web.Request") -> "web.Response":
        try:
            payload = await request.json()
        except Exception as e:
            logger.warning("[chatwoot] invalid JSON payload: %s", e)
            return web.json_response({"ok": False, "error": "invalid JSON"}, status=400)

        # Only react to the customer's own incoming messages — ignore the
        # bot's own outgoing messages and private notes to avoid a reply loop.
        if payload.get("message_type") != "incoming" or payload.get("private"):
            return web.json_response({"ok": True, "skipped": "not an incoming customer message"})

        account = payload.get("account") or {}
        conversation = payload.get("conversation") or {}
        account_id = str(account.get("id") or "")
        conversation_id = conversation.get("id") or payload.get("conversation_id")
        content = payload.get("content")
        sender = payload.get("sender") or {}

        if account_id and account_id != str(self._account_id):
            # Belongs to a different Chatwoot account than this bot instance
            # is configured for — ignore rather than cross-wire conversations.
            return web.json_response({"ok": True, "skipped": "account_id mismatch"})

        if not conversation_id or not content:
            return web.json_response(
                {"ok": False, "error": "missing conversation_id/content in payload"},
                status=200,
            )

        chat_id = str(conversation_id)
        source = self.build_source(
            chat_id=chat_id,
            chat_name=f"chatwoot-conversation-{chat_id}",
            chat_type="private",
            user_id=str(sender.get("id") or sender.get("email") or "customer"),
            user_name=sender.get("name") or "Customer",
        )

        event = MessageEvent(
            text=content,
            message_type=MessageType.TEXT,
            source=source,
            message_id=str(payload.get("id") or conversation_id),
            timestamp=__import__("datetime").datetime.now(),
        )

        # Dispatch and return immediately — Chatwoot only needs a fast 200 OK;
        # the agent turn + reply happen asynchronously via send().
        import asyncio

        asyncio.create_task(self.handle_message(event))
        return web.json_response({"ok": True})

    # ── Outbound: hermes reply -> Chatwoot API ──────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        conversation_id = chat_id
        url = (
            f"{self._base_url}/api/v1/accounts/{self._account_id}"
            f"/conversations/{conversation_id}/messages"
        )
        body = {"content": content, "message_type": "outgoing", "private": False}
        headers = {
            "Content-Type": "application/json",
            "api_access_token": self._bot_api_key,
        }

        try:
            assert self._session is not None
            async with self._session.post(url, json=body, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status >= 400:
                    text = await resp.text()
                    logger.warning("[chatwoot] reply POST failed: %s %s", resp.status, text[:2000])
                    return SendResult(success=False, error=f"HTTP {resp.status}: {text[:500]}")
                data = await resp.json()
                return SendResult(success=True, message_id=str(data.get("id", "")))
        except Exception as e:
            logger.warning("[chatwoot] reply POST error: %s", e)
            return SendResult(success=False, error=str(e))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        # Chatwoot supports a typing-indicator webhook event, not implemented
        # here — no-op for now.
        pass

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": f"chatwoot-conversation-{chat_id}", "type": "dm"}


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------


def check_requirements() -> bool:
    """Check if the Chatwoot bot credentials are configured."""
    return bool(
        os.environ.get("CHATWOOT_BASE_URL")
        and os.environ.get("CHATWOOT_BOT_API_KEY")
        and os.environ.get("CHATWOOT_ACCOUNT_ID")
    )


def register(ctx):
    """Plugin entry point: called by the Hermes plugin system."""
    ctx.register_platform(
        name="chatwoot",
        label="Chatwoot",
        adapter_factory=lambda cfg: ChatwootAdapter(cfg),
        check_fn=check_requirements,
        required_env=["CHATWOOT_BASE_URL", "CHATWOOT_BOT_API_KEY", "CHATWOOT_ACCOUNT_ID"],
        install_hint="Requires aiohttp (already a hermes-agent dependency)",
        emoji="💬",
        pii_safe=False,
        platform_hint=(
            "You are chatting via a Chatwoot Agent Bot — a customer support "
            "widget/inbox. Keep responses helpful, concise, and professional; "
            "this is a support conversation with a real customer."
        ),
    )
