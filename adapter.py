"""Chatwoot Agent Bot gateway adapter for Hermes Agent.

Runs a single aiohttp HTTP server (same lifecycle pattern as the built-in
``gateway/platforms/webhook.py`` generic webhook adapter) that receives
Chatwoot Agent Bot webhook events, hands incoming customer messages to the
Hermes agent via the standard ``BasePlatformAdapter.handle_message()`` path
(same dispatch pattern as the built-in IRC adapter), and posts replies back
into the conversation via the Chatwoot API.

Multi-tenant
------------
One Chatwoot Agent Bot per hermes profile. With ``gateway.multiplex_profiles``
on, a single gateway process serves every profile and this adapter is the
single listener for all of them:

    POST /webhook/<profile>   ->  that profile's agent, that tenant's account

The profile is captured from the URL and stamped onto the SessionSource, so
the agent turn resolves that profile's config, skills, memory and credentials
— the same mechanism the built-in webhook platform uses for its
``/p/<profile>/webhooks/<route>`` prefix.

Only the DEFAULT-scoped adapter binds the port; secondary profiles get a
no-op adapter, since a second bind on the same port could only collide and
the hub already serves them by path.

Every instance resolves the full tenant map, whether or not it binds, and
``chat_id`` is namespaced ``<profile>:<conversation_id>`` so ``send()`` can
recover the tenant from it. Both are needed because which instance sends a
reply is not fixed: hermes ≤ 0.18 replied through the instance that received
the webhook (the hub), and ≥ 0.19 replies through the adapter of the profile
the turn ran under. Resolving tenants everywhere works under both. The
namespace is required regardless — Chatwoot conversation ids are per-account
and collide across tenants, so a bare id could post one customer's reply into
another tenant's account.

Adding a tenant is: create the profile, set its three CHATWOOT_* values,
restart. No code change and no per-tenant port.

Chatwoot Agent Bot API reference:
https://developers.chatwoot.com/api-reference/account-agentbots/create-an-agent-bot
"""

import hashlib
import hmac
import json
import logging
import os
import socket as _socket
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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


# ---------------------------------------------------------------------------
# Profile / tenant resolution
#
# These reach into hermes internals (profiles_to_serve, multiplex_profiles).
# Each one is wrapped: on a hermes upgrade that moves or renames a seam, the
# adapter degrades to single-tenant on the flat /webhook path instead of
# failing to start and taking the channel down with it.
# ---------------------------------------------------------------------------


_TRUTHY = ("1", "true", "yes", "on")

# The profile that owns the shared listener under multiplexing. Matches
# hermes's own convention: "the default profile owns the single shared HTTP
# listener and serves every profile through the /p/<profile>/ URL prefix".
_HUB_PROFILE = "default"

# Chatwoot signs every Agent Bot delivery (Webhooks::Trigger#request_headers):
#   X-Chatwoot-Signature: sha256=HMAC_SHA256(secret, "<timestamp>.<raw body>")
# The secret is the Agent Bot's own `secret` column (has_secure_token via the
# WebhookSecretable concern) — a different value from its access_token.
_SIGNATURE_HEADER = "X-Chatwoot-Signature"
_TIMESTAMP_HEADER = "X-Chatwoot-Timestamp"

# How far the signed timestamp may drift before a delivery is refused. Bounds
# the window in which a captured request can be replayed; wide enough to
# absorb clock skew between containers.
_SIGNATURE_TOLERANCE_SECONDS = int(
    os.environ.get("CHATWOOT_SIGNATURE_TOLERANCE", "300")
)


def _multiplex_enabled() -> bool:
    """True when this gateway serves more than its own profile.

    Reads the operator env var, then falls back to scanning config.yaml as
    text. Deliberately NOT via ``load_gateway_config()``: that resolves plugin
    platforms, which calls this plugin's ``check_fn``, which calls back into
    here. The cycle surfaces as "Failed to process config.yaml — maximum
    recursion depth exceeded" and silently drops the profile to its .env
    defaults, which is a very long way from the actual cause.
    """
    raw = os.environ.get("GATEWAY_MULTIPLEX_PROFILES")
    if raw is not None and raw.strip():
        return raw.strip().lower() in _TRUTHY

    try:
        text = (_hermes_home() / "config.yaml").read_text(
            encoding="utf-8", errors="replace"
        )
    except Exception:
        return False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("multiplex_profiles:"):
            value = stripped.split(":", 1)[1].strip().strip("\"'").lower()
            return value in _TRUTHY
    return False


def _hermes_home() -> Path:
    """This instance's hermes home, honouring the multiplexer's scope.

    Must go through ``get_hermes_home()``: ``_profile_runtime_scope`` redirects
    a profile's home with ``set_hermes_home_override``, a contextvar, and does
    NOT touch ``os.environ["HERMES_HOME"]``. Reading the env var directly
    returns the process-wide root for every instance, so all of them look like
    the default profile and all of them try to bind.
    """
    try:
        from hermes_constants import get_hermes_home

        return Path(get_hermes_home())
    except Exception:
        return Path(os.environ.get("HERMES_HOME", "~/.hermes")).expanduser()


def _own_profile() -> str:
    """The profile THIS adapter instance belongs to.

    Derived from HERMES_HOME, not from ``get_active_profile_name()``. Under
    multiplexing each secondary adapter is constructed inside
    ``_profile_runtime_scope(profile_home)``, which repoints HERMES_HOME but
    leaves the active-profile marker alone — so asking for the active profile
    returns the hub for every instance, every instance concludes it owns the
    listener, and the second one to start dies on "port already in use".
    """
    try:
        home = _hermes_home().resolve()
    except Exception:
        return "default"
    return home.name if home.parent.name == "profiles" else "default"


def _active_profile() -> str:
    """The profile the gateway process itself was started as (the hub)."""
    try:
        from hermes_cli.profiles import get_active_profile_name

        return (get_active_profile_name() or "default").strip()
    except Exception:
        logger.debug("[chatwoot] could not resolve active profile", exc_info=True)
        return "default"


def _served_profiles() -> List[Tuple[str, Path]]:
    """(name, home) for every profile this gateway serves."""
    try:
        from hermes_cli.profiles import profiles_to_serve

        return [(n, Path(h)) for n, h in profiles_to_serve(multiplex=True)]
    except Exception:
        logger.debug("[chatwoot] could not enumerate profiles", exc_info=True)
        return []


def _read_env_file(path: Path) -> Dict[str, str]:
    """Parse KEY=VALUE pairs out of a profile's .env.

    Deliberately not a full dotenv parser: no interpolation, no `export`
    handling. It only needs to recover three flat values that hermes itself
    wrote, and a tenant whose .env is exotic enough to break this is better
    off surfacing as "missing credentials" than as a half-parsed token.
    """
    values: Dict[str, str] = {}
    try:
        for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    except Exception:
        logger.debug("[chatwoot] could not read %s", path, exc_info=True)
    return values


class _Tenant:
    """One profile's Chatwoot account: where to post, and as whom."""

    __slots__ = ("profile", "base_url", "account_id", "token", "secret")

    def __init__(
        self,
        profile: str,
        base_url: str,
        account_id: str,
        token: str,
        secret: str = "",
    ):
        self.profile = profile
        self.base_url = base_url.rstrip("/")
        self.account_id = account_id
        self.token = token
        # The Agent Bot's signing secret. Optional: leaving it unset is an
        # explicit opt-out of signature checking, logged loudly at startup.
        self.secret = secret

    @classmethod
    def from_values(
        cls, profile: str, values: Dict[str, str]
    ) -> "Optional[_Tenant]":
        base_url = values.get("CHATWOOT_BASE_URL", "").strip()
        account_id = values.get("CHATWOOT_ACCOUNT_ID", "").strip()
        token = values.get("CHATWOOT_BOT_API_KEY", "").strip()
        secret = values.get("CHATWOOT_WEBHOOK_SECRET", "").strip()
        if not (base_url and account_id and token):
            return None
        return cls(profile, base_url, account_id, token, secret)


def _discover_tenants() -> Dict[str, _Tenant]:
    """Map profile -> tenant for every served profile that is configured.

    The active profile is read from the live environment (hermes has already
    loaded its .env into os.environ); secondary profiles are read off disk,
    because only one profile's secrets can occupy os.environ at a time.
    """
    tenants: Dict[str, _Tenant] = {}
    mine = _own_profile()

    own = _Tenant.from_values(mine, dict(os.environ))
    if own:
        tenants[mine] = own

    if not _multiplex_enabled():
        return tenants

    for name, home in _served_profiles():
        if name in tenants:
            continue
        tenant = _Tenant.from_values(name, _read_env_file(home / ".env"))
        if tenant:
            tenants[name] = tenant
        else:
            logger.debug(
                "[chatwoot] profile '%s' has no complete CHATWOOT_* config — "
                "not served",
                name,
            )
    return tenants


class ChatwootAdapter(BasePlatformAdapter):
    """Async Chatwoot Agent Bot adapter implementing BasePlatformAdapter.

    Instantiated by the adapter_factory passed to register_platform().
    """

    def __init__(self, config, **kwargs):
        platform = Platform("chatwoot")
        super().__init__(config=config, platform=platform)

        # Default to '::' rather than '0.0.0.0': the common deployment is
        # container-to-container on an IPv6-only private network (Railway,
        # Fly), where a v4 bind is silently unreachable and shows up only as
        # ECONNREFUSED on the Chatwoot side.
        self._host = os.environ.get("CHATWOOT_WEBHOOK_HOST", "::")
        self._port = int(os.environ.get("CHATWOOT_WEBHOOK_PORT", "9000"))

        self._profile = _own_profile()
        self._tenants: Dict[str, _Tenant] = {}

        self._runner: Optional["web.AppRunner"] = None
        self._session: Optional["aiohttp.ClientSession"] = None

    # ── Lifecycle ────────────────────────────────────────────────────────

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not AIOHTTP_AVAILABLE:
            logger.error(
                "[chatwoot] aiohttp is not available — cannot start webhook "
                "server. Install it explicitly in the image; it is not "
                "guaranteed to arrive as a hermes-agent dependency."
            )
            return False

        # Resolve tenants on EVERY instance, before deciding who binds.
        #
        # A non-binding instance still sends: hermes ≥ 0.19 routes a reply
        # through the adapter of the profile the turn ran under, not through
        # the instance that received the webhook (0.18 did the latter). Both
        # are reasonable; an adapter that only knows its tenants when it owns
        # the listener works under one and fails under the other with
        # "unknown tenant" at send time — after the agent has already spent a
        # turn. Knowing them always costs one .env read and is version-proof.
        self._tenants = _discover_tenants()

        # Exactly one instance may bind. The hub is the DEFAULT-scoped one —
        # the profile whose HERMES_HOME is the hermes root rather than a
        # profiles/<name> directory.
        #
        # Deliberately not "am I the active profile": get_active_profile_name()
        # answers differently depending on the runtime scope an instance is
        # constructed and connected in, so with two tenants more than one
        # instance can conclude it is the active one and the second to start
        # dies on "port already in use". HERMES_HOME is unambiguous, and every
        # multiplexed gateway has exactly one default-scoped instance.
        if _multiplex_enabled() and self._profile != _HUB_PROFILE:
            logger.info(
                "[chatwoot] profile '%s' is served by the '%s' hub at "
                "/webhook/%s — not binding a second port",
                self._profile,
                _HUB_PROFILE,
                self._profile,
            )
            self._mark_connected()
            return True

        if not self._tenants:
            logger.error(
                "[chatwoot] no served profile has a complete "
                "CHATWOOT_BASE_URL/CHATWOOT_BOT_API_KEY/CHATWOOT_ACCOUNT_ID"
            )
            return False

        if self._port_in_use():
            logger.error(
                "[chatwoot] Port %d already in use. Set CHATWOOT_WEBHOOK_PORT "
                "to a free port.",
                self._port,
            )
            return False

        app = web.Application()
        app.router.add_get("/health", self._handle_health)
        # The per-profile route is what lets one listener serve every tenant.
        # The flat path stays registered for Agent Bots configured before the
        # hub existed; it resolves to the hub's own profile.
        app.router.add_post("/webhook/{profile}", self._handle_webhook)
        app.router.add_post("/webhook", self._handle_webhook)

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()
        self._mark_connected()

        for name, tenant in sorted(self._tenants.items()):
            logger.info(
                "[chatwoot] serving profile '%s' (account_id=%s) at "
                "%s:%d/webhook/%s",
                name,
                tenant.account_id,
                self._host,
                self._port,
                name,
            )
            if not tenant.secret:
                logger.warning(
                    "[chatwoot] profile '%s' has no CHATWOOT_WEBHOOK_SECRET — "
                    "webhook deliveries are NOT authenticated. Anything that "
                    "can reach %s:%d can forge a customer message for this "
                    "tenant. Set it to the Agent Bot's `secret` to enforce.",
                    name,
                    self._host,
                    self._port,
                )
        return True

    def _port_in_use(self) -> bool:
        """Probe the port on the family we are about to bind.

        Not hardcoded to 127.0.0.1: with a v6 bind host, a v4 probe reports
        "free" against an existing v6-only listener and lets the real bind
        fail later with a much less obvious error.
        """
        family = _socket.AF_INET6 if ":" in self._host else _socket.AF_INET
        probe_host = "::1" if family == _socket.AF_INET6 else "127.0.0.1"
        try:
            with _socket.socket(family, _socket.SOCK_STREAM) as s:
                s.settimeout(1)
                s.connect((probe_host, self._port))
            return True
        except (ConnectionRefusedError, OSError):
            return False

    async def _get_session(self) -> "aiohttp.ClientSession":
        """Return the outbound HTTP session, creating it on first use.

        Not created in ``connect()``: a non-binding instance returns from
        there early and still sends (hermes >= 0.19 replies through the
        profile's own adapter), so it would reach ``send()`` with no session.
        That failed as a bare ``assert``, whose AssertionError has an empty
        str() — the log read "reply POST error (profile=imunizar): " with
        nothing after the colon, which says almost nothing.
        """
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

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
        return web.json_response(
            {
                "status": "ok",
                "platform": "chatwoot",
                "profiles": sorted(self._tenants.keys()),
            }
        )

    def _resolve_profile(self, request: "web.Request") -> Optional[str]:
        """Map the request path to a served profile, or None to reject.

        A flat /webhook POST is the hub's own profile — that is what an Agent
        Bot configured before this version is still pointing at.
        """
        name = (request.match_info.get("profile") or "").strip()
        if not name:
            return self._profile if self._profile in self._tenants else None
        return name if name in self._tenants else None

    def _verify_signature(
        self, request: "web.Request", raw_body: bytes, tenant: _Tenant
    ) -> "Optional[web.Response]":
        """Return a 401 response when the delivery is not provably Chatwoot's.

        Without this, any host that can reach the port can forge a customer
        message: it cannot read the tenant's token, but it can drive the agent
        with attacker-chosen text inside that tenant's session and memory.
        """
        provided = request.headers.get(_SIGNATURE_HEADER, "")
        timestamp = request.headers.get(_TIMESTAMP_HEADER, "")
        if not provided or not timestamp:
            logger.warning(
                "[chatwoot] unsigned delivery for profile '%s' — rejected",
                tenant.profile,
            )
            return web.json_response(
                {"ok": False, "error": "missing signature"}, status=401
            )

        # Reject stale timestamps so a captured delivery cannot be replayed
        # indefinitely. The timestamp is inside the signed string, so it
        # cannot be adjusted without invalidating the signature.
        try:
            age = abs(time.time() - int(timestamp))
        except ValueError:
            logger.warning(
                "[chatwoot] non-numeric timestamp for profile '%s' — rejected",
                tenant.profile,
            )
            return web.json_response(
                {"ok": False, "error": "bad timestamp"}, status=401
            )
        if age > _SIGNATURE_TOLERANCE_SECONDS:
            logger.warning(
                "[chatwoot] delivery for profile '%s' is %.0fs out of date "
                "(tolerance %ds) — rejected",
                tenant.profile,
                age,
                _SIGNATURE_TOLERANCE_SECONDS,
            )
            return web.json_response({"ok": False, "error": "stale"}, status=401)

        expected = hmac.new(
            tenant.secret.encode("utf-8"),
            f"{timestamp}.".encode("utf-8") + raw_body,
            hashlib.sha256,
        ).hexdigest()
        # compare_digest, not ==: a short-circuiting comparison leaks how much
        # of the digest matched through timing, which is enough to forge one.
        if not hmac.compare_digest(f"sha256={expected}", provided):
            logger.warning(
                "[chatwoot] signature mismatch for profile '%s' — rejected",
                tenant.profile,
            )
            return web.json_response(
                {"ok": False, "error": "bad signature"}, status=401
            )
        return None

    async def _handle_webhook(self, request: "web.Request") -> "web.Response":
        profile = self._resolve_profile(request)
        if profile is None:
            requested = request.match_info.get("profile") or "(flat)"
            logger.warning(
                "[chatwoot] webhook for unknown/unconfigured profile %r", requested
            )
            return web.json_response(
                {"ok": False, "error": "Unknown or unconfigured profile"}, status=404
            )
        tenant = self._tenants[profile]

        # Read the body as bytes and verify BEFORE parsing. The signature
        # covers the exact bytes Chatwoot sent, so re-serializing parsed JSON
        # would produce a different string and never match.
        raw_body = await request.read()

        if tenant.secret:
            rejection = self._verify_signature(request, raw_body, tenant)
            if rejection is not None:
                return rejection

        try:
            payload = json.loads(raw_body)
        except Exception as e:
            logger.warning("[chatwoot] invalid JSON payload: %s", e)
            return web.json_response({"ok": False, "error": "invalid JSON"}, status=400)

        # Only react to the customer's own incoming messages — ignore the
        # bot's own outgoing messages and private notes to avoid a reply loop.
        if payload.get("message_type") != "incoming" or payload.get("private"):
            return web.json_response(
                {"ok": True, "skipped": "not an incoming customer message"}
            )

        account = payload.get("account") or {}
        conversation = payload.get("conversation") or {}
        account_id = str(account.get("id") or "")
        conversation_id = conversation.get("id") or payload.get("conversation_id")
        content = payload.get("content")
        sender = payload.get("sender") or {}

        if account_id and account_id != str(tenant.account_id):
            # The URL names one tenant and the payload another. Never guess:
            # replying would post one customer's conversation into a different
            # tenant's account, using that tenant's token.
            logger.warning(
                "[chatwoot] account mismatch on /webhook/%s: payload account=%s, "
                "profile configured for account=%s — dropped",
                profile,
                account_id,
                tenant.account_id,
            )
            return web.json_response({"ok": True, "skipped": "account_id mismatch"})

        if not conversation_id or not content:
            return web.json_response(
                {"ok": False, "error": "missing conversation_id/content in payload"},
                status=200,
            )

        # Namespaced so send() can recover the tenant: Chatwoot conversation
        # ids restart per account, so a bare id is ambiguous across tenants.
        chat_id = f"{profile}:{conversation_id}"
        source = self.build_source(
            chat_id=chat_id,
            chat_name=f"chatwoot-conversation-{conversation_id}",
            chat_type="private",
            user_id=str(sender.get("id") or sender.get("email") or "customer"),
            user_name=sender.get("name") or "Customer",
        )
        # Route the agent turn to this tenant's profile — its config, skills,
        # memory and model. Without this, every tenant would answer as the hub.
        try:
            source.profile = profile
        except Exception:
            logger.debug("[chatwoot] could not stamp source.profile", exc_info=True)

        event = MessageEvent(
            text=content,
            message_type=MessageType.TEXT,
            source=source,
            message_id=str(payload.get("id") or conversation_id),
            timestamp=__import__("datetime").datetime.now(),
        )

        # Dispatch and return immediately — Chatwoot only needs a fast 200 OK;
        # the agent turn + reply happen asynchronously via send(). Chatwoot
        # pulls a conversation out of the bot's control (status -> open) when
        # the webhook errors or stalls, so this must not await the turn.
        import asyncio

        asyncio.create_task(self.handle_message(event))
        return web.json_response({"ok": True})

    # ── Outbound: hermes reply -> Chatwoot API ──────────────────────────

    def _split_chat_id(self, chat_id: str) -> Tuple[Optional[_Tenant], str]:
        profile, sep, conversation_id = chat_id.partition(":")
        if not sep:
            # Pre-hub chat_id (bare conversation id) — belongs to this profile.
            return self._tenants.get(self._profile), chat_id
        return self._tenants.get(profile), conversation_id

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        tenant, conversation_id = self._split_chat_id(chat_id)
        if tenant is None:
            logger.warning(
                "[chatwoot] no tenant for chat_id %r — known: %s. The profile "
                "in the chat_id has no complete CHATWOOT_* config, or its .env "
                "was unreadable when this adapter connected.",
                chat_id,
                sorted(self._tenants) or "(nenhum)",
            )
            return SendResult(success=False, error=f"unknown tenant for {chat_id}")

        url = (
            f"{tenant.base_url}/api/v1/accounts/{tenant.account_id}"
            f"/conversations/{conversation_id}/messages"
        )
        body = {"content": content, "message_type": "outgoing", "private": False}
        headers = {
            "Content-Type": "application/json",
            "api_access_token": tenant.token,
        }

        try:
            session = await self._get_session()
            async with session.post(
                url,
                json=body,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status >= 400:
                    text = await resp.text()
                    logger.warning(
                        "[chatwoot] reply POST failed (profile=%s): %s %s",
                        tenant.profile,
                        resp.status,
                        text[:2000],
                    )
                    return SendResult(
                        success=False, error=f"HTTP {resp.status}: {text[:500]}"
                    )
                data = await resp.json()
                return SendResult(success=True, message_id=str(data.get("id", "")))
        except Exception as e:
            logger.warning(
                "[chatwoot] reply POST error (profile=%s): %s", tenant.profile, e
            )
            return SendResult(success=False, error=str(e))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        # Chatwoot supports a typing-indicator webhook event, not implemented
        # here — no-op for now.
        pass

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        _, conversation_id = self._split_chat_id(chat_id)
        return {"name": f"chatwoot-conversation-{conversation_id}", "type": "dm"}


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------


def check_requirements() -> bool:
    """True when at least one served profile can talk to Chatwoot.

    Not just this profile's own env: under multiplexing the hub runs on the
    active profile, which may itself have no Chatwoot account while every
    tenant it serves does. Gating on os.environ alone would leave the listener
    unstarted and all tenants unreachable.
    """
    return bool(_discover_tenants())


def register(ctx):
    """Plugin entry point: called by the Hermes plugin system."""
    ctx.register_platform(
        name="chatwoot",
        label="Chatwoot",
        adapter_factory=lambda cfg: ChatwootAdapter(cfg),
        check_fn=check_requirements,
        required_env=["CHATWOOT_BASE_URL", "CHATWOOT_BOT_API_KEY", "CHATWOOT_ACCOUNT_ID"],
        install_hint="Requires aiohttp (pip install aiohttp)",
        emoji="💬",
        pii_safe=False,
        platform_hint=(
            "You are chatting via a Chatwoot Agent Bot — a customer support "
            "widget/inbox. Keep responses helpful, concise, and professional; "
            "this is a support conversation with a real customer."
        ),
    )
