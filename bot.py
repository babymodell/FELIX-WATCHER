import os
import io
import asyncio
import sqlite3
import datetime
import random
from collections import defaultdict

import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

# ==================== ENV ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

dotenv_path = os.path.join(BASE_DIR, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
else:
    load_dotenv()

TOKEN = os.getenv("DISCORD_BOT_TOKEN")


def env_int(key: str):
    v = os.getenv(key)
    if not v:
        return None
    try:
        return int(v.strip())
    except ValueError:
        return None


def env_int_list(key: str) -> list[int]:
    v = os.getenv(key)
    if not v:
        return []
    out: list[int] = []
    for part in v.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            pass
    return out


WELCOME_CHANNEL_ID = env_int("WELCOME_CHANNEL_ID")
LOG_CHANNEL_ID = env_int("LOG_CHANNEL_ID")

TICKET_CATEGORY_ID = env_int("TICKET_CATEGORY_ID")
TICKET_PANEL_CHANNEL_ID = env_int("TICKET_PANEL_CHANNEL_ID")
TICKET_STAFF_ROLE_IDS = env_int_list("TICKET_STAFF_ROLE_ID")

UNMUTE_CHANNEL_ID = env_int("UNMUTE_CHANNEL_ID")

ROLE_PANEL_CHANNEL_ID = env_int("ROLE_PANEL_CHANNEL_ID")
ROLE_POLAND_ID = env_int("ROLE_POLAND_ID")
ROLE_GERMANY_ID = env_int("ROLE_GERMANY_ID")

TRANSCRIPT_LIMIT = env_int("TICKET_TRANSCRIPT_LIMIT") or 200

# ---- MARKET (keine Threads, keine extra Channels) ----
MARKET_BERLIN_PANEL_CHANNEL_ID = env_int("MARKET_BERLIN_PANEL_CHANNEL_ID")
MARKET_BERLIN_LISTINGS_CHANNEL_ID = env_int("MARKET_BERLIN_LISTINGS_CHANNEL_ID")
MARKET_BERLIN_STAFF_ROLE_IDS = env_int_list("MARKET_BERLIN_STAFF_ROLE_ID")

MARKET_POLAND_PANEL_CHANNEL_ID = env_int("MARKET_POLAND_PANEL_CHANNEL_ID")
MARKET_POLAND_LISTINGS_CHANNEL_ID = env_int("MARKET_POLAND_LISTINGS_CHANNEL_ID")
MARKET_POLAND_STAFF_ROLE_IDS = env_int_list("MARKET_POLAND_STAFF_ROLE_ID")

MARKET_ALLOWED_ROLE_BERLIN = ROLE_GERMANY_ID
MARKET_ALLOWED_ROLE_POLAND = ROLE_POLAND_ID

MARKET_CFG = {
    "berlin": {
        "label": "Berlin",
        "allowed_role_id": MARKET_ALLOWED_ROLE_BERLIN,
        "panel_channel_id": MARKET_BERLIN_PANEL_CHANNEL_ID,
        "listings_channel_id": MARKET_BERLIN_LISTINGS_CHANNEL_ID,
        "staff_role_ids": MARKET_BERLIN_STAFF_ROLE_IDS,
        "lang": "de",
    },
    "poland": {
        "label": "Polska",
        "allowed_role_id": MARKET_ALLOWED_ROLE_POLAND,
        "panel_channel_id": MARKET_POLAND_PANEL_CHANNEL_ID,
        "listings_channel_id": MARKET_POLAND_LISTINGS_CHANNEL_ID,
        "staff_role_ids": MARKET_POLAND_STAFF_ROLE_IDS,
        "lang": "pl",
    }
}

if not TOKEN:
    raise SystemExit("❌ DISCORD_BOT_TOKEN fehlt als Environment Variable (Railway Variables).")

# ==================== DB ====================
DB_PATH = os.path.join(BASE_DIR, "bot.sqlite3")


def db():
    # WAL + busy timeout: reduziert "database is locked"
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS mutes (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            unmute_at TEXT,
            PRIMARY KEY (guild_id, user_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS economy (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            balance INTEGER NOT NULL DEFAULT 0,
            last_daily TEXT,
            PRIMARY KEY (guild_id, user_id)
        )
    """)
    # NEU: Rollen-Backup für Mutes
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mute_role_backup (
            guild_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role_ids TEXT NOT NULL,
            PRIMARY KEY (guild_id, user_id)
        )
    """)
    return conn


# ==================== BOT ====================
intents = discord.Intents.default()
intents.members = True  # braucht "Server Members Intent" im Developer Portal

bot = commands.Bot(command_prefix="!", intents=intents)
discord.utils.setup_logging()

# ==================== Invite Tracking ====================
invite_cache = defaultdict(dict)  # guild_id -> {code: uses}
vanity_cache = {}                # guild_id -> uses
join_method_cache = {}           # (guild_id, user_id) -> dict(method=..., inviter=..., code=...)

# ==================== Helpers ====================
def now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


async def get_text_channel(guild: discord.Guild, channel_id: int | None) -> discord.TextChannel | None:
    if not channel_id:
        return None
    ch = guild.get_channel(channel_id)
    return ch if isinstance(ch, discord.TextChannel) else None


async def get_log_channel(guild: discord.Guild) -> discord.TextChannel | None:
    return await get_text_channel(guild, LOG_CHANNEL_ID)


async def send_log(
    guild: discord.Guild,
    *,
    title: str,
    description: str = "",
    color: discord.Color = discord.Color.blurple(),
    fields: list[tuple[str, str, bool]] | None = None,
    user: discord.abc.User | None = None,
    file: discord.File | None = None,
):
    log_ch = await get_log_channel(guild)
    if not log_ch:
        return

    emb = discord.Embed(title=title, description=description, color=color, timestamp=now_utc())
    if user is not None:
        try:
            emb.set_author(name=str(user), icon_url=user.display_avatar.url)
        except Exception:
            emb.set_author(name=str(user))

    if fields:
        for name, value, inline in fields:
            emb.add_field(name=name, value=value or "—", inline=inline)

    try:
        if file is not None:
            await log_ch.send(embed=emb, file=file)
        else:
            await log_ch.send(embed=emb)
    except discord.Forbidden:
        pass


def fmt_roles(member: discord.Member, limit: int = 18) -> str:
    roles = [r.mention for r in member.roles if r.name != "@everyone"]
    if not roles:
        return "—"
    if len(roles) > limit:
        return " ".join(roles[:limit]) + f" …(+{len(roles)-limit})"
    return " ".join(roles)


def discord_account_age(member: discord.Member) -> str:
    days = (now_utc() - member.created_at).days
    years = days // 365
    if years >= 1:
        return f"vor {years} Jahr(en)"
    months = days // 30
    if months >= 1:
        return f"vor {months} Monat(en)"
    return f"vor {days} Tag(en)"


def has_role(member: discord.Member, role_id: int | None) -> bool:
    if not role_id:
        return False
    r = member.guild.get_role(role_id)
    return bool(r and r in member.roles)


def is_staff(member: discord.Member) -> bool:
    if member.guild_permissions.administrator:
        return True
    if not TICKET_STAFF_ROLE_IDS:
        return False
    for rid in TICKET_STAFF_ROLE_IDS:
        role = member.guild.get_role(rid)
        if role and role in member.roles:
            return True
    return False


def is_market_staff(member: discord.Member, staff_role_ids: list[int]) -> bool:
    if member.guild_permissions.administrator:
        return True
    if not staff_role_ids:
        return False
    for rid in staff_role_ids:
        role = member.guild.get_role(rid)
        if role and role in member.roles:
            return True
    return False


def staff_check():
    async def predicate(interaction: discord.Interaction) -> bool:
        return (
            interaction.guild is not None and
            isinstance(interaction.user, discord.Member) and
            is_staff(interaction.user)
        )
    return app_commands.check(predicate)


async def refresh_invites_for_guild(guild: discord.Guild):
    try:
        invites = await guild.invites()
        invite_cache[guild.id] = {i.code: (i.uses or 0) for i in invites}
    except discord.Forbidden:
        invite_cache[guild.id] = {}

    try:
        v = await guild.vanity_invite()
        vanity_cache[guild.id] = (v.uses if v else 0)
    except discord.Forbidden:
        vanity_cache[guild.id] = vanity_cache.get(guild.id, 0)
    except discord.HTTPException:
        pass


async def detect_join_method(guild: discord.Guild) -> dict:
    used_code = None
    inviter = None

    try:
        new_invites = await guild.invites()
        old = invite_cache.get(guild.id, {})
        for inv in new_invites:
            before = old.get(inv.code, 0)
            uses = inv.uses or 0
            if uses > before:
                used_code = inv.code
                inviter = inv.inviter
                break
        invite_cache[guild.id] = {i.code: (i.uses or 0) for i in new_invites}
    except discord.Forbidden:
        pass

    if used_code:
        return {"method": "invite", "code": used_code, "inviter": inviter}

    try:
        v = await guild.vanity_invite()
        new_uses = (v.uses if v else 0)
        old_uses = vanity_cache.get(guild.id, 0)
        vanity_cache[guild.id] = new_uses
        if new_uses > old_uses:
            return {"method": "vanity", "code": None, "inviter": None}
    except (discord.Forbidden, discord.HTTPException):
        pass

    return {"method": "unknown", "code": None, "inviter": None}


def parse_topic(topic: str | None) -> dict:
    data = {}
    if not topic:
        return data
    parts = [p.strip() for p in topic.split("|")]
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            data[k.strip()] = v.strip()
    return data


async def build_text_channel_transcript(channel: discord.TextChannel, limit: int = 200) -> str:
    lines: list[str] = []
    lines.append(f"Transcript for #{channel.name} ({channel.id})")
    lines.append(f"Guild: {channel.guild.name} ({channel.guild.id})")
    lines.append(f"Exported at: {now_utc().isoformat()} UTC")
    if channel.topic:
        lines.append(f"Topic: {channel.topic}")
    lines.append("-" * 80)

    try:
        async for msg in channel.history(limit=limit, oldest_first=True):
            ts = msg.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")
            author = f"{msg.author} ({msg.author.id})"
            content = (msg.content or "").replace("\n", "\\n")
            lines.append(f"[{ts}] {author}: {content}")
            for a in msg.attachments:
                lines.append(f"  [Attachment] {a.url}")
    except Exception as e:
        lines.append(f"[Transcript error] {e}")

    lines.append("-" * 80)
    return "\n".join(lines)


# ==================== Mute Role Backup Helpers ====================
def _serialize_role_ids(role_ids: list[int]) -> str:
    return ",".join(str(r) for r in role_ids)


def _deserialize_role_ids(s: str) -> list[int]:
    out: list[int] = []
    for part in (s or "").split(","):
        part = part.strip()
        if part.isdigit():
            out.append(int(part))
    return out


def save_mute_roles_backup(guild_id: int, user_id: int, role_ids: list[int]):
    conn = db()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO mute_role_backup(guild_id, user_id, role_ids) VALUES (?, ?, ?)",
            (guild_id, user_id, _serialize_role_ids(role_ids))
        )
        conn.commit()
    finally:
        conn.close()


def pop_mute_roles_backup(guild_id: int, user_id: int) -> list[int]:
    conn = db()
    try:
        row = conn.execute(
            "SELECT role_ids FROM mute_role_backup WHERE guild_id=? AND user_id=?",
            (guild_id, user_id)
        ).fetchone()
        conn.execute(
            "DELETE FROM mute_role_backup WHERE guild_id=? AND user_id=?",
            (guild_id, user_id)
        )
        conn.commit()
        if not row:
            return []
        return _deserialize_role_ids(row[0])
    finally:
        conn.close()


def can_bot_manage_role(guild: discord.Guild, role: discord.Role) -> bool:
    if role.is_default() or role.managed:
        return False
    me = guild.me
    if not me:
        return False
    return me.top_role > role


# ==================== Ticket Helpers ====================
async def ensure_ticket_category(guild: discord.Guild) -> discord.CategoryChannel:
    if not TICKET_CATEGORY_ID:
        raise RuntimeError("TICKET_CATEGORY_ID ist nicht gesetzt.")
    cat = guild.get_channel(TICKET_CATEGORY_ID)
    if not isinstance(cat, discord.CategoryChannel):
        raise RuntimeError("Ticket-Kategorie nicht gefunden. Prüfe TICKET_CATEGORY_ID.")
    return cat


async def next_ticket_number(guild: discord.Guild) -> int:
    n = 1
    existing = {c.name for c in guild.text_channels}
    while f"ticket-{n}" in existing:
        n += 1
    return n


# ==================== Mute System ====================
MUTED_ROLE_NAME = "Muted"


async def get_or_create_muted_role(guild: discord.Guild) -> discord.Role:
    role = discord.utils.get(guild.roles, name=MUTED_ROLE_NAME)
    if role:
        return role
    return await guild.create_role(name=MUTED_ROLE_NAME, reason="Mute-System: Muted Rolle erstellt")


async def apply_mute_overwrites(guild: discord.Guild, muted_role: discord.Role):
    if not UNMUTE_CHANNEL_ID:
        raise RuntimeError("UNMUTE_CHANNEL_ID ist nicht gesetzt.")
    unmute_ch = guild.get_channel(UNMUTE_CHANNEL_ID)
    if not isinstance(unmute_ch, discord.TextChannel):
        raise RuntimeError("UNMUTE_CHANNEL_ID Channel nicht gefunden.")

    for ch in guild.text_channels:
        ow = ch.overwrites_for(muted_role)

        ow.view_channel = False
        ow.send_messages = False
        ow.add_reactions = False
        ow.send_messages_in_threads = False
        ow.create_public_threads = False
        ow.create_private_threads = False
        ow.read_message_history = True

        if ch.id == unmute_ch.id:
            ow.view_channel = True
            ow.send_messages = True
            ow.read_message_history = True

        try:
            await ch.set_permissions(muted_role, overwrite=ow, reason="Mute-System Overwrites aktualisiert")
        except discord.Forbidden:
            pass


# ==================== Role Panel ====================
class RolePanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def _toggle_role(self, interaction: discord.Interaction, role_id: int | None):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)
        if not role_id:
            return await interaction.response.send_message("Role-ID fehlt in Env-Variablen.", ephemeral=True)

        role = interaction.guild.get_role(role_id)
        if not role:
            return await interaction.response.send_message("Rolle nicht gefunden. Prüfe Role-ID.", ephemeral=True)

        member = interaction.user
        try:
            if role in member.roles:
                await member.remove_roles(role, reason="Role Panel toggle")
                await interaction.response.send_message(f"❌ Rolle entfernt: {role.mention}", ephemeral=True)
                await send_log(interaction.guild, title="➖ Rolle entfernt (Panel)", color=discord.Color.red(),
                               user=member, fields=[("Rolle", role.mention, True)])
            else:
                await member.add_roles(role, reason="Role Panel toggle")
                await interaction.response.send_message(f"✅ Rolle bekommen: {role.mention}", ephemeral=True)
                await send_log(interaction.guild, title="➕ Rolle hinzugefügt (Panel)", color=discord.Color.green(),
                               user=member, fields=[("Rolle", role.mention, True)])
        except discord.Forbidden:
            await interaction.response.send_message("❌ Ich habe keine Rechte Rollen zu vergeben.", ephemeral=True)

    @discord.ui.button(label="Poland", style=discord.ButtonStyle.danger, emoji="🇵🇱", custom_id="rolepanel:poland")
    async def poland(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._toggle_role(interaction, ROLE_POLAND_ID)

    @discord.ui.button(label="Germany", style=discord.ButtonStyle.secondary, emoji="🇩🇪", custom_id="rolepanel:germany")
    async def germany(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._toggle_role(interaction, ROLE_GERMANY_ID)


# ==================== Ticket Views ====================
class TicketManageView(discord.ui.View):
    def __init__(self, ticket_owner_id: int):
        super().__init__(timeout=None)
        self.ticket_owner_id = ticket_owner_id

    async def _update_status_embed(self, channel: discord.TextChannel, status_text: str):
        me = channel.guild.me
        async for msg in channel.history(limit=25):
            if me and msg.author.id == me.id and msg.embeds:
                emb = msg.embeds[0]
                if len(emb.fields) >= 2:
                    emb.set_field_at(1, name="Status", value=status_text, inline=False)
                else:
                    emb.add_field(name="Status", value=status_text, inline=False)
                try:
                    await msg.edit(embed=emb, view=self)
                except Exception:
                    pass
                break

    @discord.ui.button(label="Close Ticket", style=discord.ButtonStyle.danger, emoji="🔒", custom_id="ticket:close")
    async def close_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)
        ch = interaction.channel
        if not isinstance(ch, discord.TextChannel):
            return await interaction.response.send_message("Ungültiger Channel.", ephemeral=True)

        is_owner = interaction.user.id == self.ticket_owner_id
        if not (is_owner or is_staff(interaction.user)):
            return await interaction.response.send_message("❌ Du darfst dieses Ticket nicht schließen.", ephemeral=True)

        await interaction.response.send_message("🔒 Ticket wird in **5 Sekunden** geschlossen…", ephemeral=True)

        transcript = await build_text_channel_transcript(ch, limit=TRANSCRIPT_LIMIT)
        f = discord.File(fp=io.BytesIO(transcript.encode("utf-8")), filename=f"{ch.name}-transcript.txt")

        await send_log(
            interaction.guild,
            title="🔒 Ticket geschlossen",
            color=discord.Color.red(),
            user=interaction.user,
            fields=[("Channel", f"#{ch.name} (`{ch.id}`)", False),
                    ("Closed by", f"{interaction.user.mention} (`{interaction.user.id}`)", False)],
            file=f,
        )

        await asyncio.sleep(5)
        try:
            await ch.delete(reason=f"Ticket geschlossen von {interaction.user}")
        except discord.Forbidden:
            pass

    @discord.ui.button(label="Claim Ticket", style=discord.ButtonStyle.success, emoji="🧾", custom_id="ticket:claim")
    async def claim_ticket(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)
        if not is_staff(interaction.user):
            return await interaction.response.send_message("❌ Nur Staff kann claimen.", ephemeral=True)

        ch = interaction.channel
        if not isinstance(ch, discord.TextChannel):
            return await interaction.response.send_message("Ungültiger Channel.", ephemeral=True)

        topic_data = parse_topic(ch.topic)
        if topic_data.get("claimed_by") and topic_data.get("claimed_by") != "none":
            return await interaction.response.send_message("✅ Dieses Ticket ist bereits geclaimt.", ephemeral=True)

        new_topic = (ch.topic or "")
        if "claimed_by=" in new_topic:
            parts = [p.strip() for p in new_topic.split("|")]
            fixed = []
            for p in parts:
                if p.startswith("claimed_by="):
                    fixed.append(f"claimed_by={interaction.user.id}")
                else:
                    fixed.append(p)
            new_topic = " | ".join(fixed)
        else:
            new_topic = (new_topic + " | " if new_topic else "") + f"claimed_by={interaction.user.id}"

        await ch.edit(topic=new_topic, reason="Ticket geclaimt")
        await self._update_status_embed(ch, f"🟢 Geclaimt von {interaction.user.mention}")

        await interaction.response.send_message(f"🧾 Ticket geclaimt von {interaction.user.mention}", ephemeral=False)

        await send_log(
            interaction.guild,
            title="🧾 Ticket geclaimt",
            color=discord.Color.gold(),
            user=interaction.user,
            fields=[("Channel", f"{ch.mention} (`{ch.id}`)", False),
                    ("Claimed by", f"{interaction.user.mention} (`{interaction.user.id}`)", False)],
        )


class TicketOpenView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def _create_ticket(self, interaction: discord.Interaction, kind: str, emoji: str):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)

        guild = interaction.guild
        member = interaction.user

        try:
            category = await ensure_ticket_category(guild)
        except Exception as e:
            return await interaction.response.send_message(f"❌ {e}", ephemeral=True)

        # already has ticket (nur in der Ticket-Kategorie iterieren)
        for ch in category.text_channels:
            if ch.topic and f"user_id={member.id}" in ch.topic:
                return await interaction.response.send_message(f"Du hast bereits ein Ticket: {ch.mention}", ephemeral=True)

        ticket_no = await next_ticket_number(guild)
        channel_name = f"ticket-{ticket_no}"

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            member: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
            guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, manage_channels=True, read_message_history=True),
        }

        staff_roles = [guild.get_role(rid) for rid in TICKET_STAFF_ROLE_IDS]
        staff_roles = [r for r in staff_roles if r is not None]
        for r in staff_roles:
            overwrites[r] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True)

        topic = f"ticket_type={kind} | user_id={member.id} | claimed_by=none"
        ch = await guild.create_text_channel(
            name=channel_name,
            category=category,
            overwrites=overwrites,
            topic=topic,
            reason=f"Ticket erstellt von {member} ({kind})"
        )

        embed = discord.Embed(
            title="Tickets",
            description=f"{member.mention} created a new **{emoji} {kind}** ticket.",
            color=discord.Color.dark_grey()
        )
        embed.add_field(name="User", value=f"{member} (`{member.id}`)", inline=False)
        embed.add_field(name="Status", value="🟡 Open (not claimed)", inline=False)
        embed.set_thumbnail(url=member.display_avatar.url)

        staff_ping = " ".join(r.mention for r in staff_roles)

        await ch.send(content=staff_ping, embed=embed, view=TicketManageView(ticket_owner_id=member.id))
        await interaction.response.send_message(f"✅ Ticket erstellt: {ch.mention}", ephemeral=True)

        await send_log(
            guild,
            title="🎫 Ticket erstellt",
            color=discord.Color.green(),
            user=member,
            fields=[("Channel", f"{ch.mention} (`{ch.id}`)", False), ("Typ", kind, True)],
        )

    @discord.ui.button(label="Question", style=discord.ButtonStyle.secondary, emoji="❓", custom_id="ticket_open:question")
    async def question(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._create_ticket(interaction, "Question", "❓")

    @discord.ui.button(label="Recruitment", style=discord.ButtonStyle.primary, emoji="📌", custom_id="ticket_open:recruitment")
    async def recruitment(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._create_ticket(interaction, "Recruitment", "📌")

    @discord.ui.button(label="Partnership", style=discord.ButtonStyle.success, emoji="🤝", custom_id="ticket_open:partnership")
    async def partnership(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._create_ticket(interaction, "Partnership", "🤝")


# ==================== MARKET (kein Thread, kein extra Channel) ====================
def market_meta(seller_id: int, region_key: str, claimed_by: int | None) -> str:
    cb = claimed_by if claimed_by is not None else 0
    return f"seller_id={seller_id}|region={region_key}|claimed_by={cb}"


def parse_market_meta(emb: discord.Embed) -> dict:
    out = {"seller_id": 0, "region": None, "claimed_by": 0}
    if not emb.footer or not emb.footer.text:
        return out
    txt = emb.footer.text
    parts = [p.strip() for p in txt.split("|")]
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            k = k.strip()
            v = v.strip()
            if k == "seller_id":
                out["seller_id"] = int(v) if v.isdigit() else 0
            elif k == "region":
                out["region"] = v
            elif k == "claimed_by":
                out["claimed_by"] = int(v) if v.isdigit() else 0
    return out


class MarketListingView(discord.ui.View):
    def __init__(self, disabled: bool = False):
        super().__init__(timeout=None)
        self._disabled = disabled
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                item.disabled = disabled

    @discord.ui.button(label="Kontakt", style=discord.ButtonStyle.primary, emoji="📩", custom_id="market:contact")
    async def contact(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)

        msg = interaction.message
        if not msg or not msg.embeds:
            return await interaction.response.send_message("❌ Keine Anzeige gefunden.", ephemeral=True)

        emb = msg.embeds[0]
        meta = parse_market_meta(emb)
        region = meta.get("region")
        seller_id = int(meta.get("seller_id") or 0)

        if not region or seller_id == 0:
            return await interaction.response.send_message("❌ Anzeige-Metadaten fehlen.", ephemeral=True)

        buyer = interaction.user
        if buyer.id == seller_id:
            lang = MARKET_CFG[region]["lang"]
            return await interaction.response.send_message("❌ Du bist der Verkäufer." if lang != "pl" else "❌ Jesteś sprzedawcą.", ephemeral=True)

        seller = interaction.guild.get_member(seller_id)
        seller_mention = seller.mention if seller else f"<@{seller_id}>"

        lang = MARKET_CFG[region]["lang"]
        if lang == "pl":
            text = f"📩 {buyer.mention} chce skontaktować się ze sprzedawcą {seller_mention}. Napiszcie do siebie na DM i dogadajcie się."
            ok = "✅ Wysłano ping kontaktowy."
        else:
            text = f"📩 {buyer.mention} möchte den Verkäufer {seller_mention} kontaktieren. Bitte per DM klären."
            ok = "✅ Kontakt gepingt."

        await interaction.response.send_message(ok, ephemeral=True)
        await msg.channel.send(text)

        await send_log(
            interaction.guild,
            title="📩 Market Kontakt",
            color=discord.Color.blurple(),
            user=buyer,
            fields=[
                ("Region", MARKET_CFG[region]["label"], True),
                ("Seller", f"{seller_mention} (`{seller_id}`)", False),
                ("Listing Msg", f"`{msg.id}` in {msg.channel.mention}", False),
            ],
        )

    @discord.ui.button(label="Claim", style=discord.ButtonStyle.success, emoji="🧾", custom_id="market:claim")
    async def claim(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)

        msg = interaction.message
        if not msg or not msg.embeds:
            return await interaction.response.send_message("❌ Keine Anzeige gefunden.", ephemeral=True)

        emb = msg.embeds[0]
        meta = parse_market_meta(emb)
        region = meta.get("region")
        seller_id = int(meta.get("seller_id") or 0)
        claimed_by = int(meta.get("claimed_by") or 0)

        if not region or seller_id == 0:
            return await interaction.response.send_message("❌ Anzeige-Metadaten fehlen.", ephemeral=True)

        buyer = interaction.user
        lang = MARKET_CFG[region]["lang"]

        if buyer.id == seller_id:
            return await interaction.response.send_message("❌ Verkäufer kann nicht claimen." if lang != "pl" else "❌ Sprzedawca nie może zająć.", ephemeral=True)

        required_role = MARKET_CFG[region]["allowed_role_id"]
        if required_role and not has_role(buyer, required_role):
            return await interaction.response.send_message(
                "❌ Du hast nicht die passende Rolle für diesen Markt." if lang != "pl" else "❌ Nie masz odpowiedniej roli do tego rynku.",
                ephemeral=True
            )

        if claimed_by != 0:
            return await interaction.response.send_message("✅ Bereits geclaimt." if lang != "pl" else "✅ Ogłoszenie jest już zajęte.", ephemeral=True)

        new_emb = emb.copy()
        status_text = f"🟢 Geclaimt von {buyer.mention}" if lang != "pl" else f"🟢 Zajęte przez {buyer.mention}"

        found = False
        for i, f in enumerate(new_emb.fields):
            if f.name.lower() == "status":
                new_emb.set_field_at(i, name="Status", value=status_text, inline=False)
                found = True
                break
        if not found:
            new_emb.add_field(name="Status", value=status_text, inline=False)

        new_emb.set_footer(text=market_meta(seller_id=seller_id, region_key=region, claimed_by=buyer.id))
        await msg.edit(embed=new_emb)

        await interaction.response.send_message("✅ Geclaimt." if lang != "pl" else "✅ Zajęte.", ephemeral=True)

        await send_log(
            interaction.guild,
            title="🧾 Market Claim",
            color=discord.Color.gold(),
            user=buyer,
            fields=[
                ("Region", MARKET_CFG[region]["label"], True),
                ("Seller ID", str(seller_id), True),
                ("Claimed by", f"{buyer.mention} (`{buyer.id}`)", False),
                ("Listing Msg", f"`{msg.id}` in {msg.channel.mention}", False),
            ],
        )

    @discord.ui.button(label="Close", style=discord.ButtonStyle.danger, emoji="🔒", custom_id="market:close")
    async def close(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)

        msg = interaction.message
        if not msg or not msg.embeds:
            return await interaction.response.send_message("❌ Keine Anzeige gefunden.", ephemeral=True)

        emb = msg.embeds[0]
        meta = parse_market_meta(emb)
        region = meta.get("region")
        seller_id = int(meta.get("seller_id") or 0)
        claimed_by = int(meta.get("claimed_by") or 0)

        if not region or seller_id == 0:
            return await interaction.response.send_message("❌ Anzeige-Metadaten fehlen.", ephemeral=True)

        actor = interaction.user
        staff_ids = MARKET_CFG[region]["staff_role_ids"]
        lang = MARKET_CFG[region]["lang"]

        is_owner = actor.id == seller_id
        if not (is_owner or is_market_staff(actor, staff_ids)):
            return await interaction.response.send_message(
                "❌ Nur Verkäufer oder Staff kann schließen." if lang != "pl" else "❌ Tylko sprzedawca lub staff może zamknąć.",
                ephemeral=True
            )

        new_emb = emb.copy()
        closed_text = "🔴 Closed" if lang != "pl" else "🔴 Zamknięte"

        found = False
        for i, f in enumerate(new_emb.fields):
            if f.name.lower() == "status":
                new_emb.set_field_at(i, name="Status", value=closed_text, inline=False)
                found = True
                break
        if not found:
            new_emb.add_field(name="Status", value=closed_text, inline=False)

        new_emb.set_footer(text=market_meta(seller_id=seller_id, region_key=region, claimed_by=claimed_by))

        await msg.edit(embed=new_emb, view=MarketListingView(disabled=True))

        await interaction.response.send_message("✅ Anzeige geschlossen." if lang != "pl" else "✅ Ogłoszenie zamknięte.", ephemeral=True)

        await send_log(
            interaction.guild,
            title="🔒 Market Close",
            color=discord.Color.red(),
            user=actor,
            fields=[
                ("Region", MARKET_CFG[region]["label"], True),
                ("Seller ID", str(seller_id), True),
                ("Closed by", f"{actor.mention} (`{actor.id}`)", False),
                ("Listing Msg", f"`{msg.id}` in {msg.channel.mention}", False),
            ],
        )


class MarketSaleModal(discord.ui.Modal):
    def __init__(self, opener: discord.Member, region_key: str):
        self.opener = opener
        self.region_key = region_key
        lang = MARKET_CFG[region_key]["lang"]

        title = "Sprzedaż bezpośrednia" if lang == "pl" else "Direktverkauf"
        super().__init__(title=f"{title} ({MARKET_CFG[region_key]['label']})")

        if lang == "pl":
            self.item = discord.ui.TextInput(label="Co sprzedajesz?", max_length=200)
            self.price = discord.ui.TextInput(label="Cena", max_length=80)
            self.location = discord.ui.TextInput(label="Miejsce/Info", required=False, max_length=120)
            self.contact = discord.ui.TextInput(label="Kontakt/Godziny", required=False, max_length=150)
        else:
            self.item = discord.ui.TextInput(label="Was verkaufst du?", max_length=200)
            self.price = discord.ui.TextInput(label="Preis", max_length=80)
            self.location = discord.ui.TextInput(label="Ort/Info", required=False, max_length=120)
            self.contact = discord.ui.TextInput(label="Kontakt/Zeiten", required=False, max_length=150)

        self.add_item(self.item)
        self.add_item(self.price)
        self.add_item(self.location)
        self.add_item(self.contact)

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)

        guild = interaction.guild
        member = self.opener
        cfg = MARKET_CFG[self.region_key]
        lang = cfg["lang"]

        if not has_role(member, cfg["allowed_role_id"]):
            return await interaction.response.send_message(
                "❌ Du hast nicht die Rolle für diesen Markt." if lang != "pl" else "❌ Nie masz roli do tego rynku.",
                ephemeral=True
            )

        listings_ch = await get_text_channel(guild, cfg["listings_channel_id"])
        if not isinstance(listings_ch, discord.TextChannel):
            return await interaction.response.send_message("❌ Listings-Channel nicht gefunden (ID prüfen).", ephemeral=True)

        if lang == "pl":
            emb = discord.Embed(
                title=f"🛒 Sprzedaż bezpośrednia ({cfg['label']})",
                description=f"Sprzedawca: {member.mention}",
                color=discord.Color.green()
            )
            emb.add_field(name="Przedmiot", value=str(self.item), inline=False)
            emb.add_field(name="Cena", value=str(self.price), inline=True)
            emb.add_field(name="Miejsce/Info", value=(str(self.location).strip() or "—"), inline=True)
            emb.add_field(name="Kontakt/Godziny", value=(str(self.contact).strip() or "—"), inline=False)
            emb.add_field(name="Status", value="🟡 Otwarte", inline=False)
        else:
            emb = discord.Embed(
                title=f"🛒 Direktverkauf ({cfg['label']})",
                description=f"Seller: {member.mention}",
                color=discord.Color.green()
            )
            emb.add_field(name="Item", value=str(self.item), inline=False)
            emb.add_field(name="Preis", value=str(self.price), inline=True)
            emb.add_field(name="Ort/Info", value=(str(self.location).strip() or "—"), inline=True)
            emb.add_field(name="Kontakt/Zeiten", value=(str(self.contact).strip() or "—"), inline=False)
            emb.add_field(name="Status", value="🟡 Open", inline=False)

        emb.set_thumbnail(url=member.display_avatar.url)
        emb.set_footer(text=market_meta(seller_id=member.id, region_key=self.region_key, claimed_by=None))

        await listings_ch.send(embed=emb, view=MarketListingView(disabled=False))

        await interaction.response.send_message(
            f"✅ Anzeige gepostet in {listings_ch.mention}." if lang != "pl" else f"✅ Ogłoszenie dodane w {listings_ch.mention}.",
            ephemeral=True
        )

        await send_log(
            guild,
            title="💸 Market Listing erstellt",
            color=discord.Color.green(),
            user=member,
            fields=[("Region", cfg["label"], True), ("Channel", listings_ch.mention, True)],
        )


class MarketOpenViewBerlin(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Direktverkauf starten (Berlin)", style=discord.ButtonStyle.success, emoji="💸", custom_id="market_open:berlin")
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)
        if not has_role(interaction.user, MARKET_CFG["berlin"]["allowed_role_id"]):
            return await interaction.response.send_message("❌ Du brauchst die Berlin/Germany Rolle.", ephemeral=True)
        await interaction.response.send_modal(MarketSaleModal(opener=interaction.user, region_key="berlin"))


class MarketOpenViewPoland(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Rozpocznij sprzedaż (Polska)", style=discord.ButtonStyle.success, emoji="💸", custom_id="market_open:poland")
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Tylko na serwerze.", ephemeral=True)
        if not has_role(interaction.user, MARKET_CFG["poland"]["allowed_role_id"]):
            return await interaction.response.send_message("❌ Potrzebujesz roli Polska.", ephemeral=True)
        await interaction.response.send_modal(MarketSaleModal(opener=interaction.user, region_key="poland"))


# ==================== Economy Helpers ====================
def econ_get(guild_id: int, user_id: int) -> tuple[int, str | None]:
    conn = db()
    try:
        row = conn.execute(
            "SELECT balance, last_daily FROM economy WHERE guild_id=? AND user_id=?",
            (guild_id, user_id)
        ).fetchone()
        if not row:
            conn.execute(
                "INSERT OR IGNORE INTO economy(guild_id, user_id, balance, last_daily) VALUES (?, ?, 0, NULL)",
                (guild_id, user_id)
            )
            conn.commit()
            return 0, None
        return int(row[0]), row[1]
    finally:
        conn.close()


def econ_set_balance(guild_id: int, user_id: int, new_balance: int):
    conn = db()
    try:
        conn.execute(
            "INSERT INTO economy(guild_id, user_id, balance, last_daily) VALUES (?, ?, ?, NULL) "
            "ON CONFLICT(guild_id, user_id) DO UPDATE SET balance=excluded.balance",
            (guild_id, user_id, int(new_balance))
        )
        conn.commit()
    finally:
        conn.close()


def econ_set_daily(guild_id: int, user_id: int, last_daily_iso: str):
    conn = db()
    try:
        conn.execute(
            "INSERT INTO economy(guild_id, user_id, balance, last_daily) VALUES (?, ?, 0, ?) "
            "ON CONFLICT(guild_id, user_id) DO UPDATE SET last_daily=excluded.last_daily",
            (guild_id, user_id, last_daily_iso)
        )
        conn.commit()
    finally:
        conn.close()


# ==================== Commands: Setup Panels ====================
@bot.tree.command(name="ticket_setup", description="Postet das Ticket Panel (Staff/Admin)")
@staff_check()
async def ticket_setup(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)
    if not TICKET_PANEL_CHANNEL_ID:
        return await interaction.response.send_message("❌ TICKET_PANEL_CHANNEL_ID fehlt in Env-Variablen", ephemeral=True)

    panel_ch = interaction.guild.get_channel(TICKET_PANEL_CHANNEL_ID)
    if not isinstance(panel_ch, discord.TextChannel):
        return await interaction.response.send_message("❌ Panel-Channel nicht gefunden.", ephemeral=True)

    embed = discord.Embed(title="Tickets", description="Click below to create a new ticket", color=discord.Color.dark_grey())
    await panel_ch.send(embed=embed, view=TicketOpenView())
    await interaction.response.send_message(f"✅ Ticket-Panel gepostet in {panel_ch.mention}", ephemeral=True)


@bot.tree.command(name="role_setup", description="Postet das Rollen-Panel (Staff/Admin)")
@staff_check()
async def role_setup(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)
    if not ROLE_PANEL_CHANNEL_ID:
        return await interaction.response.send_message("❌ ROLE_PANEL_CHANNEL_ID fehlt in Env-Variablen", ephemeral=True)

    ch = interaction.guild.get_channel(ROLE_PANEL_CHANNEL_ID)
    if not isinstance(ch, discord.TextChannel):
        return await interaction.response.send_message("❌ Role-Panel Channel nicht gefunden.", ephemeral=True)

    embed = discord.Embed(
        title="Server Role",
        description=(
            "__________________________\n\n"
            "**Polski**\n"
            "Naciśnij przycisk. Wtedy dostaniesz swoją rolę.\n\n"
            "**Deutsch**\n"
            "Drück auf den Button. Dann bekommst du deine Rolle.\n\n"
            "__________________________"
        ),
        color=discord.Color.green()
    )
    await ch.send(embed=embed, view=RolePanelView())
    await interaction.response.send_message(f"✅ Rollen-Panel gepostet in {ch.mention}", ephemeral=True)


@bot.tree.command(name="market_setup_berlin", description="Postet Direktverkauf Panel (Berlin) (Staff/Admin)")
@staff_check()
async def market_setup_berlin(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)
    cfg = MARKET_CFG["berlin"]
    if not cfg["panel_channel_id"]:
        return await interaction.response.send_message("❌ MARKET_BERLIN_PANEL_CHANNEL_ID fehlt.", ephemeral=True)

    panel_ch = interaction.guild.get_channel(cfg["panel_channel_id"])
    if not isinstance(panel_ch, discord.TextChannel):
        return await interaction.response.send_message("❌ Berlin Panel-Channel nicht gefunden.", ephemeral=True)

    embed = discord.Embed(
        title="💸 Direktverkauf (Berlin)",
        description="Klicke unten, fülle das Formular aus und dein Verkauf wird als Anzeige im Markt gepostet.",
        color=discord.Color.green()
    )
    await panel_ch.send(embed=embed, view=MarketOpenViewBerlin())
    await interaction.response.send_message(f"✅ Berlin Direktverkauf-Panel gepostet in {panel_ch.mention}", ephemeral=True)


@bot.tree.command(name="market_setup_poland", description="Postuje panel sprzedaży (Polska) (Staff/Admin)")
@staff_check()
async def market_setup_poland(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message("Tylko na serwerze.", ephemeral=True)
    cfg = MARKET_CFG["poland"]
    if not cfg["panel_channel_id"]:
        return await interaction.response.send_message("❌ Brak MARKET_POLAND_PANEL_CHANNEL_ID.", ephemeral=True)

    panel_ch = interaction.guild.get_channel(cfg["panel_channel_id"])
    if not isinstance(panel_ch, discord.TextChannel):
        return await interaction.response.send_message("❌ Nie znaleziono kanału panelu (Polska).", ephemeral=True)

    embed = discord.Embed(
        title="💸 Sprzedaż bezpośrednia (Polska)",
        description="Kliknij poniżej, wypełnij formularz, a ogłoszenie pojawi się na rynku.",
        color=discord.Color.green()
    )
    await panel_ch.send(embed=embed, view=MarketOpenViewPoland())
    await interaction.response.send_message(f"✅ Panel Polska wysłany w {panel_ch.mention}", ephemeral=True)


# ==================== Commands: Ticket direct ====================
ticket_group = app_commands.Group(name="ticket", description="Ticket Commands")


@ticket_group.command(name="create", description="Erstellt ein Support-Ticket")
@app_commands.describe(typ="Typ: Question / Recruitment / Partnership")
async def ticket_create(interaction: discord.Interaction, typ: str):
    view = TicketOpenView()
    typ_l = typ.lower().strip()
    if typ_l in ("question", "frage"):
        await view._create_ticket(interaction, "Question", "❓")
    elif typ_l in ("recruitment", "bewerbung"):
        await view._create_ticket(interaction, "Recruitment", "📌")
    elif typ_l in ("partnership", "partner", "partnerschaft"):
        await view._create_ticket(interaction, "Partnership", "🤝")
    else:
        await interaction.response.send_message("❌ Ungültiger Typ. Nutze: Question / Recruitment / Partnership", ephemeral=True)


@ticket_group.command(name="close", description="Schließt dieses Ticket (löscht Channel)")
async def ticket_close(interaction: discord.Interaction):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)
    ch = interaction.channel
    if not isinstance(ch, discord.TextChannel):
        return await interaction.response.send_message("Nur im Ticket-Channel nutzbar.", ephemeral=True)

    topic_data = parse_topic(ch.topic)
    owner_id = int(topic_data.get("user_id", "0") or 0)
    is_owner = owner_id == interaction.user.id
    if not (is_owner or is_staff(interaction.user)):
        return await interaction.response.send_message("❌ Du darfst dieses Ticket nicht schließen.", ephemeral=True)

    await interaction.response.send_message("🔒 Ticket wird in **5 Sekunden** geschlossen…", ephemeral=True)

    transcript = await build_text_channel_transcript(ch, limit=TRANSCRIPT_LIMIT)
    f = discord.File(fp=io.BytesIO(transcript.encode("utf-8")), filename=f"{ch.name}-transcript.txt")

    await send_log(
        interaction.guild,
        title="🔒 Ticket geschlossen",
        color=discord.Color.red(),
        user=interaction.user,
        fields=[("Channel", f"#{ch.name} (`{ch.id}`)", False),
                ("Closed by", f"{interaction.user.mention} (`{interaction.user.id}`)", False)],
        file=f,
    )

    await asyncio.sleep(5)
    try:
        await ch.delete(reason=f"Ticket geschlossen von {interaction.user}")
    except discord.Forbidden:
        pass


bot.tree.add_command(ticket_group)

# ==================== Commands: Moderation ====================
@bot.tree.command(name="clear", description="Löscht Nachrichten (max 100)")
@app_commands.describe(anzahl="Anzahl (1-100)")
async def clear(interaction: discord.Interaction, anzahl: int):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)
    if not interaction.user.guild_permissions.manage_messages and not is_staff(interaction.user):
        return await interaction.response.send_message("❌ Keine Rechte (Manage Messages).", ephemeral=True)
    if not isinstance(interaction.channel, discord.TextChannel):
        return await interaction.response.send_message("Ungültiger Channel.", ephemeral=True)

    anzahl = max(1, min(100, anzahl))
    await interaction.response.defer(ephemeral=True)
    deleted = await interaction.channel.purge(limit=anzahl)
    await interaction.followup.send(f"✅ Gelöscht: {len(deleted)} Nachricht(en).", ephemeral=True)

    await send_log(
        interaction.guild,
        title="🧹 Messages gelöscht",
        color=discord.Color.blurple(),
        user=interaction.user,
        fields=[("Channel", interaction.channel.mention, True), ("Anzahl", str(len(deleted)), True)],
    )


@bot.tree.command(name="kick", description="Kickt einen User")
@app_commands.describe(user="User", grund="Grund (optional)")
async def kick(interaction: discord.Interaction, user: discord.Member, grund: str | None = None):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)
    if not interaction.user.guild_permissions.kick_members and not is_staff(interaction.user):
        return await interaction.response.send_message("❌ Keine Rechte (Kick Members).", ephemeral=True)

    grund = grund or "—"
    try:
        await user.kick(reason=f"{grund} | by {interaction.user}")
        await interaction.response.send_message(f"✅ {user} wurde gekickt. Grund: {grund}", ephemeral=True)

        await send_log(
            interaction.guild,
            title="👢 Kick",
            color=discord.Color.orange(),
            user=user,
            fields=[("User", f"{user.mention} (`{user.id}`)", False),
                    ("Moderator", f"{interaction.user.mention} (`{interaction.user.id}`)", False),
                    ("Grund", grund, False)],
        )
    except discord.Forbidden:
        await interaction.response.send_message("❌ Bot hat keine Rechte zum Kicken.", ephemeral=True)


@bot.tree.command(name="ban", description="Bannt einen User")
@app_commands.describe(user="User", grund="Grund (optional)")
async def ban(interaction: discord.Interaction, user: discord.Member, grund: str | None = None):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)
    if not interaction.user.guild_permissions.ban_members and not is_staff(interaction.user):
        return await interaction.response.send_message("❌ Keine Rechte (Ban Members).", ephemeral=True)

    grund = grund or "—"
    try:
        await user.ban(reason=f"{grund} | by {interaction.user}")
        await interaction.response.send_message(f"✅ {user} wurde gebannt. Grund: {grund}", ephemeral=True)

        await send_log(
            interaction.guild,
            title="⛔ Ban",
            color=discord.Color.red(),
            user=user,
            fields=[("User", f"{user.mention} (`{user.id}`)", False),
                    ("Moderator", f"{interaction.user.mention} (`{interaction.user.id}`)", False),
                    ("Grund", grund, False)],
        )
    except discord.Forbidden:
        await interaction.response.send_message("❌ Bot hat keine Rechte zum Bannen.", ephemeral=True)


@bot.tree.command(name="timeout", description="Timeout für einen User (Minuten)")
@app_commands.describe(user="User", minuten="Dauer in Minuten", grund="Grund (optional)")
async def timeout(interaction: discord.Interaction, user: discord.Member, minuten: int, grund: str | None = None):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)
    if not interaction.user.guild_permissions.moderate_members and not is_staff(interaction.user):
        return await interaction.response.send_message("❌ Keine Rechte (Moderate Members).", ephemeral=True)

    minuten = max(1, min(10080, minuten))
    until = now_utc() + datetime.timedelta(minutes=minuten)
    grund = grund or "—"

    try:
        await user.timeout(until, reason=f"{grund} | by {interaction.user}")
        await interaction.response.send_message(f"✅ Timeout gesetzt für {user.mention}: {minuten} Minuten.", ephemeral=True)

        await send_log(
            interaction.guild,
            title="⏳ Timeout",
            color=discord.Color.orange(),
            user=user,
            fields=[("User", f"{user.mention} (`{user.id}`)", False),
                    ("Moderator", f"{interaction.user.mention} (`{interaction.user.id}`)", False),
                    ("Dauer", f"{minuten} Minuten", True),
                    ("Bis", until.strftime("%d.%m.%Y %H:%M UTC"), True),
                    ("Grund", grund, False)],
        )
    except discord.Forbidden:
        await interaction.response.send_message("❌ Bot hat keine Rechte für Timeout.", ephemeral=True)


# ==================== Commands: Mute ====================
@bot.tree.command(name="mute_setup", description="Einmaliges Setup: Muted Rolle + Overwrites (Staff/Admin)")
@staff_check()
async def mute_setup(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)
    muted_role = await get_or_create_muted_role(interaction.guild)
    try:
        await apply_mute_overwrites(interaction.guild, muted_role)
    except Exception as e:
        return await interaction.followup.send(f"❌ Fehler: {e}", ephemeral=True)

    await interaction.followup.send("✅ Mute-Setup abgeschlossen (Muted-Rolle & Overwrites).", ephemeral=True)


@bot.tree.command(name="mute", description="Mutet einen User (nur Unmute-Channel + eigene Tickets schreibbar)")
@staff_check()
@app_commands.describe(user="User", minuten="Dauer in Minuten (optional)", grund="Grund (optional)")
async def mute(interaction: discord.Interaction, user: discord.Member, minuten: int | None = None, grund: str | None = None):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)

    await interaction.response.defer(ephemeral=True)

    if user.guild_permissions.administrator:
        return await interaction.followup.send("❌ Admins kann ich nicht muten.", ephemeral=True)

    if not UNMUTE_CHANNEL_ID:
        return await interaction.followup.send("❌ UNMUTE_CHANNEL_ID fehlt in Env-Variablen", ephemeral=True)

    grund = grund or "Kein Grund angegeben"

    muted_role = await get_or_create_muted_role(interaction.guild)
    try:
        await apply_mute_overwrites(interaction.guild, muted_role)
    except Exception as e:
        return await interaction.followup.send(f"❌ Mute-Setup Fehler: {e}", ephemeral=True)

    if muted_role in user.roles:
        return await interaction.followup.send("✅ User ist bereits gemutet.", ephemeral=True)

    # 1) Rollen sichern + entfernen (User soll nur noch Muted haben)
    backup_role_ids: list[int] = []
    roles_to_remove: list[discord.Role] = []
    cannot_remove: list[discord.Role] = []

    for r in user.roles:
        if r.is_default():
            continue
        if r.id == muted_role.id:
            continue

        backup_role_ids.append(r.id)

        if can_bot_manage_role(interaction.guild, r):
            roles_to_remove.append(r)
        else:
            cannot_remove.append(r)

    save_mute_roles_backup(interaction.guild.id, user.id, backup_role_ids)

    removed_count = 0
    try:
        if roles_to_remove:
            await user.remove_roles(*roles_to_remove, reason=f"Mute: Rollen entfernt | by {interaction.user} | {grund}")
            removed_count = len(roles_to_remove)
    except discord.Forbidden:
        pass

    # 2) Muted geben
    try:
        await user.add_roles(muted_role, reason=f"Muted von {interaction.user} | {grund}")
    except discord.Forbidden:
        return await interaction.followup.send("❌ Ich habe keine Rechte, Rollen zu vergeben.", ephemeral=True)

    unmute_at = None
    if minuten is not None and minuten > 0:
        unmute_at_dt = now_utc() + datetime.timedelta(minutes=minuten)
        unmute_at = unmute_at_dt.isoformat()

    conn = db()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO mutes(guild_id, user_id, unmute_at) VALUES (?, ?, ?)",
            (interaction.guild.id, user.id, unmute_at)
        )
        conn.commit()
    finally:
        conn.close()

    unmute_ch = interaction.guild.get_channel(UNMUTE_CHANNEL_ID)
    unmute_hint = f"#{unmute_ch.name}" if isinstance(unmute_ch, discord.TextChannel) else "den Unmute-Channel"
    dauer_txt = f"{minuten} Minuten" if (minuten and minuten > 0) else "unbestimmt"

    try:
        await user.send(
            f"🔇 Du wurdest auf **{interaction.guild.name}** gemutet.\n"
            f"👮 Von: {interaction.user}\n"
            f"📝 Grund: {grund}\n"
            f"⏳ Dauer: {dauer_txt}\n\n"
            f"✅ Du kannst **nur** im **{unmute_hint}** schreiben (für Unmute) "
            f"und **in deinen eigenen Ticket-Channels**."
        )
    except Exception:
        pass

    await interaction.followup.send(f"🔇 {user.mention} wurde gemutet. Dauer: {dauer_txt}", ephemeral=True)

    # Fail-safe Log: welche Rollen konnten nicht entfernt werden?
    cannot_txt = "—"
    if cannot_remove:
        cannot_txt = " ".join(r.mention for r in cannot_remove[:20])
        if len(cannot_remove) > 20:
            cannot_txt += f" …(+{len(cannot_remove)-20})"

    await send_log(
        interaction.guild,
        title="🔇 User gemutet",
        color=discord.Color.orange(),
        user=user,
        fields=[
            ("User", f"{user.mention} (`{user.id}`)", False),
            ("Moderator", f"{interaction.user.mention} (`{interaction.user.id}`)", False),
            ("Dauer", dauer_txt, True),
            ("Grund", grund, False),
            ("Rollen gesichert", str(len(backup_role_ids)), True),
            ("Rollen entfernt", str(removed_count), True),
            ("Nicht entfernbar", cannot_txt, False),
        ],
    )


@bot.tree.command(name="unmute", description="Entmutet einen User")
@staff_check()
async def unmute(interaction: discord.Interaction, user: discord.Member):
    if not interaction.guild:
        return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)

    muted_role = discord.utils.get(interaction.guild.roles, name=MUTED_ROLE_NAME)
    if not muted_role or muted_role not in user.roles:
        return await interaction.response.send_message("User ist nicht gemutet.", ephemeral=True)

    # Muted entfernen
    try:
        await user.remove_roles(muted_role, reason=f"Unmuted von {interaction.user}")
    except discord.Forbidden:
        return await interaction.response.send_message("❌ Ich habe keine Rechte, Rollen zu entfernen.", ephemeral=True)

    # Rollen wiederherstellen
    role_ids = pop_mute_roles_backup(interaction.guild.id, user.id)
    to_add: list[discord.Role] = []
    skipped = 0

    for rid in role_ids:
        role = interaction.guild.get_role(rid)
        if not role:
            skipped += 1
            continue
        if role.managed or role.is_default():
            skipped += 1
            continue
        if not can_bot_manage_role(interaction.guild, role):
            skipped += 1
            continue
        to_add.append(role)

    restored = 0
    add_failed = False
    if to_add:
        try:
            await user.add_roles(*to_add, reason=f"Restore roles after unmute | by {interaction.user}")
            restored = len(to_add)
        except discord.Forbidden:
            add_failed = True
            skipped += len(to_add)

    conn = db()
    try:
        conn.execute("DELETE FROM mutes WHERE guild_id=? AND user_id=?", (interaction.guild.id, user.id))
        conn.commit()
    finally:
        conn.close()

    try:
        await user.send(f"✅ Du wurdest auf **{interaction.guild.name}** entmutet.")
    except Exception:
        pass

    await interaction.response.send_message(
        f"✅ {user.mention} wurde entmutet. Rollen restored: **{restored}**, skipped: **{skipped}**.",
        ephemeral=True
    )

    await send_log(
        interaction.guild,
        title="🔊 User entmutet",
        color=discord.Color.green(),
        user=user,
        fields=[
            ("User", f"{user.mention} (`{user.id}`)", False),
            ("Moderator", f"{interaction.user.mention} (`{interaction.user.id}`)", False),
            ("Rollen restored", str(restored), True),
            ("Rollen skipped", str(skipped), True),
            ("Add Roles failed", "Ja" if add_failed else "Nein", True),
        ],
    )


# ==================== Economy Commands ====================
@bot.tree.command(name="balance", description="Zeigt den Kontostand")
@app_commands.describe(user="User (optional)")
async def balance(interaction: discord.Interaction, user: discord.Member | None = None):
    if not interaction.guild:
        return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)
    user = user or interaction.user
    bal, _ = econ_get(interaction.guild.id, user.id)
    await interaction.response.send_message(f"💰 {user.mention} hat **{bal}** Coins.", ephemeral=True)


@bot.tree.command(name="daily", description="Tägliche Coins abholen (alle 24h)")
async def daily(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)

    guild_id = interaction.guild.id
    user_id = interaction.user.id
    bal, last = econ_get(guild_id, user_id)

    now = now_utc()
    if last:
        try:
            last_dt = datetime.datetime.fromisoformat(last)
            if now - last_dt < datetime.timedelta(hours=24):
                remaining = datetime.timedelta(hours=24) - (now - last_dt)
                hours = int(remaining.total_seconds() // 3600)
                mins = int((remaining.total_seconds() % 3600) // 60)
                return await interaction.response.send_message(
                    f"⏳ Daily schon benutzt. Warte noch **{hours}h {mins}m**.",
                    ephemeral=True
                )
        except Exception:
            pass

    reward = random.randint(50, 150)
    econ_set_balance(guild_id, user_id, bal + reward)
    econ_set_daily(guild_id, user_id, now.isoformat())
    await interaction.response.send_message(f"✅ Daily erhalten: **+{reward}** Coins. (Neu: {bal + reward})", ephemeral=True)


@bot.tree.command(name="pay", description="Zahle Coins an einen User")
@app_commands.describe(user="Empfänger", amount="Betrag")
async def pay(interaction: discord.Interaction, user: discord.Member, amount: int):
    if not interaction.guild:
        return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)
    if user.id == interaction.user.id:
        return await interaction.response.send_message("❌ Du kannst dir nicht selbst Coins zahlen.", ephemeral=True)
    if amount <= 0:
        return await interaction.response.send_message("❌ Betrag muss > 0 sein.", ephemeral=True)

    guild_id = interaction.guild.id
    sender_id = interaction.user.id
    recv_id = user.id

    sender_bal, _ = econ_get(guild_id, sender_id)
    recv_bal, _ = econ_get(guild_id, recv_id)

    if sender_bal < amount:
        return await interaction.response.send_message("❌ Nicht genug Coins.", ephemeral=True)

    econ_set_balance(guild_id, sender_id, sender_bal - amount)
    econ_set_balance(guild_id, recv_id, recv_bal + amount)

    await interaction.response.send_message(
        f"✅ {interaction.user.mention} hat **{amount}** Coins an {user.mention} gezahlt.",
        ephemeral=False
    )


# ==================== Fun/Info Commands ====================
@bot.tree.command(name="ping", description="Zeigt die Bot-Latenz")
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message(f"🏓 Pong: **{round(bot.latency*1000)}ms**", ephemeral=True)


@bot.tree.command(name="info", description="Kurze Info über den Bot")
async def info(interaction: discord.Interaction):
    await interaction.response.send_message(
        f"🤖 **{bot.user}**\n"
        f"Servers: **{len(bot.guilds)}**\n"
        f"Latency: **{round(bot.latency*1000)}ms**",
        ephemeral=True
    )


@bot.tree.command(name="helpme", description="Zeigt eine Befehlsübersicht")
async def helpme(interaction: discord.Interaction):
    await interaction.response.send_message(
        "✅ **Commands**\n"
        "Moderation: /clear /kick /ban /timeout /mute /unmute /mute_setup\n"
        "Tickets: /ticket_setup /ticket create /ticket close\n"
        "Roles: /role_setup\n"
        "Market: /market_setup_berlin /market_setup_poland\n"
        "Economy: /balance /daily /pay\n"
        "Fun: /roll /coinflip /8ball\n"
        "Info: /ping /info /avatar /userinfo /serverinfo",
        ephemeral=True
    )


@bot.tree.command(name="avatar", description="Zeigt den Avatar eines Users")
@app_commands.describe(user="User (optional)")
async def avatar(interaction: discord.Interaction, user: discord.Member | None = None):
    user = user or interaction.user
    await interaction.response.send_message(user.display_avatar.url, ephemeral=True)


@bot.tree.command(name="userinfo", description="Infos über einen User")
@app_commands.describe(user="User (optional)")
async def userinfo(interaction: discord.Interaction, user: discord.Member | None = None):
    if not interaction.guild:
        return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)
    user = user or interaction.user
    emb = discord.Embed(title="Userinfo", color=discord.Color.blurple())
    emb.set_thumbnail(url=user.display_avatar.url)
    emb.add_field(name="User", value=f"{user} ({user.id})", inline=False)
    emb.add_field(name="Joined Server", value=user.joined_at.strftime("%d.%m.%Y %H:%M") if user.joined_at else "—", inline=False)
    emb.add_field(name="Created", value=user.created_at.strftime("%d.%m.%Y %H:%M"), inline=False)
    emb.add_field(name="Roles", value=fmt_roles(user), inline=False)
    await interaction.response.send_message(embed=emb, ephemeral=True)


@bot.tree.command(name="serverinfo", description="Infos über den Server")
async def serverinfo(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)
    g = interaction.guild
    emb = discord.Embed(title="Serverinfo", color=discord.Color.green())
    if g.icon:
        emb.set_thumbnail(url=g.icon.url)
    emb.add_field(name="Name", value=g.name, inline=False)
    emb.add_field(name="ID", value=str(g.id), inline=False)
    emb.add_field(name="Members", value=str(g.member_count), inline=False)
    emb.add_field(name="Owner", value=str(g.owner), inline=False)
    await interaction.response.send_message(embed=emb, ephemeral=True)


@bot.tree.command(name="roll", description="Würfeln (Standard 1-100)")
@app_commands.describe(maximum="Max (optional)")
async def roll(interaction: discord.Interaction, maximum: int | None = None):
    maximum = maximum or 100
    maximum = max(1, min(100000, maximum))
    value = random.randint(1, maximum)
    await interaction.response.send_message(f"🎲 {interaction.user.mention} rolled **{value}** (1-{maximum})")


@bot.tree.command(name="coinflip", description="Kopf oder Zahl")
async def coinflip(interaction: discord.Interaction):
    await interaction.response.send_message(f"🪙 Ergebnis: **{random.choice(['Kopf', 'Zahl'])}**")


@bot.tree.command(name="8ball", description="Magic 8 Ball")
@app_commands.describe(frage="Deine Frage")
async def eightball(interaction: discord.Interaction, frage: str):
    answers = [
        "Ja.", "Nein.", "Vielleicht.", "Sehr wahrscheinlich.", "Unwahrscheinlich.",
        "Frag später nochmal.", "Ich glaube schon.", "Auf keinen Fall.", "Sieht gut aus.", "Keine Ahnung."
    ]
    await interaction.response.send_message(f"🎱 Frage: **{frage}**\nAntwort: **{random.choice(answers)}**")


# ==================== Muted Message Enforcement ====================
# Strikter als Permissions: wenn gemutet, dann darf user nur im UNMUTE channel
# und in seinem eigenen Ticket schreiben (Topic user_id=...).
@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    if not message.guild or not isinstance(message.author, discord.Member):
        return await bot.process_commands(message)

    member = message.author
    muted_role = discord.utils.get(message.guild.roles, name=MUTED_ROLE_NAME)
    if muted_role and muted_role in member.roles:
        # Allow: UNMUTE channel
        if UNMUTE_CHANNEL_ID and isinstance(message.channel, discord.TextChannel) and message.channel.id == UNMUTE_CHANNEL_ID:
            return await bot.process_commands(message)

        # Allow: own ticket channel (topic user_id=member.id AND in ticket category)
        allowed = False
        if isinstance(message.channel, discord.TextChannel):
            try:
                topic_data = parse_topic(message.channel.topic)
                owner_id = int(topic_data.get("user_id", "0") or 0)
                if owner_id == member.id and (TICKET_CATEGORY_ID is None or message.channel.category_id == TICKET_CATEGORY_ID):
                    allowed = True
            except Exception:
                allowed = False

        if not allowed:
            try:
                await message.delete()
            except Exception:
                pass

            # kurzer Hinweis (auto-delete)
            try:
                warn = await message.channel.send(
                    f"🔇 {member.mention} du bist gemutet. Du darfst nur im <#{UNMUTE_CHANNEL_ID}> "
                    f"und in **deinen** Ticket-Channels schreiben."
                )
                await asyncio.sleep(6)
                await warn.delete()
            except Exception:
                pass

            return  # block processing

    await bot.process_commands(message)


# ==================== Auto Unmute Loop ====================
@tasks.loop(seconds=30)
async def auto_unmute_loop():
    conn = db()
    try:
        rows = conn.execute("SELECT guild_id, user_id, unmute_at FROM mutes WHERE unmute_at IS NOT NULL").fetchall()
    finally:
        conn.close()

    now = now_utc()
    for guild_id, user_id, unmute_at in rows:
        try:
            unmute_time = datetime.datetime.fromisoformat(unmute_at)
        except Exception:
            continue

        if now >= unmute_time:
            guild = bot.get_guild(guild_id)
            if not guild:
                continue

            member = guild.get_member(user_id)
            muted_role = discord.utils.get(guild.roles, name=MUTED_ROLE_NAME)

            did_unmute = False
            restored = 0
            skipped = 0

            if member and muted_role and muted_role in member.roles:
                try:
                    await member.remove_roles(muted_role, reason="Auto-Unmute (Timer)")
                    did_unmute = True
                except Exception:
                    did_unmute = False

                if did_unmute:
                    role_ids = pop_mute_roles_backup(guild_id, user_id)
                    to_add: list[discord.Role] = []
                    for rid in role_ids:
                        role = guild.get_role(rid)
                        if not role:
                            skipped += 1
                            continue
                        if role.managed or role.is_default():
                            skipped += 1
                            continue
                        if not can_bot_manage_role(guild, role):
                            skipped += 1
                            continue
                        to_add.append(role)

                    if to_add:
                        try:
                            await member.add_roles(*to_add, reason="Restore roles after auto-unmute")
                            restored = len(to_add)
                        except Exception:
                            skipped += len(to_add)

            # mutes entry cleanup
            conn2 = db()
            try:
                conn2.execute("DELETE FROM mutes WHERE guild_id=? AND user_id=?", (guild_id, user_id))
                conn2.commit()
            finally:
                conn2.close()

            if did_unmute and member:
                await send_log(
                    guild,
                    title="⏱️ Auto-Unmute",
                    color=discord.Color.green(),
                    user=member,
                    fields=[
                        ("User", f"{member.mention} (`{member.id}`)", False),
                        ("Grund", "Timer abgelaufen", True),
                        ("Rollen restored", str(restored), True),
                        ("Rollen skipped", str(skipped), True),
                    ],
                )


@auto_unmute_loop.before_loop
async def before_auto_unmute():
    await bot.wait_until_ready()


# ==================== Events ====================
@bot.event
async def on_member_join(member: discord.Member):
    welcome_ch = await get_text_channel(member.guild, WELCOME_CHANNEL_ID)
    if welcome_ch:
        banner_url = member.guild.banner.url if member.guild.banner else None
        emb = discord.Embed(
            title="New user! :D",
            description=f"Welcome {member.mention}",
            color=discord.Color.green()
        )
        emb.set_thumbnail(url=member.display_avatar.url)
        emb.set_image(url=banner_url or member.display_avatar.url)
        emb.add_field(name="Number of users", value=str(member.guild.member_count), inline=False)
        try:
            await welcome_ch.send(embed=emb)
        except discord.Forbidden:
            pass

    jm = await detect_join_method(member.guild)
    join_method_cache[(member.guild.id, member.id)] = jm

    log_ch = await get_log_channel(member.guild)
    if log_ch:
        emb = discord.Embed(title="Member joined", color=discord.Color.green())
        emb.set_author(name=str(member), icon_url=member.display_avatar.url)
        emb.set_thumbnail(url=member.display_avatar.url)
        emb.add_field(
            name="Joined Discord",
            value=f"{member.created_at.strftime('%d.%m.%Y %H:%M')} • {discord_account_age(member)}",
            inline=False
        )
        emb.add_field(name="User", value=f"{member.mention} ({member.id})", inline=False)

        if jm["method"] == "vanity":
            emb.add_field(name="Join method", value="Vanity Invite", inline=False)
        elif jm["method"] == "invite":
            inviter_txt = jm["inviter"].mention if jm["inviter"] else "Unbekannt"
            emb.add_field(name="Join method", value=f"Invite `{jm['code']}` • invited by {inviter_txt}", inline=False)
        else:
            emb.add_field(name="Join method", value="Unknown (fehlende Invite-Rechte?)", inline=False)

        await log_ch.send(embed=emb)


@bot.event
async def on_member_remove(member: discord.Member):
    log_ch = await get_log_channel(member.guild)
    if not log_ch:
        return

    jm = join_method_cache.get((member.guild.id, member.id), {"method": "unknown", "code": None, "inviter": None})
    if jm["method"] == "vanity":
        join_txt = "Vanity Invite"
    elif jm["method"] == "invite":
        inviter_txt = jm["inviter"].mention if jm["inviter"] else "Unbekannt"
        join_txt = f"Invite `{jm['code']}` • invited by {inviter_txt}"
    else:
        join_txt = "Unknown"

    emb = discord.Embed(title="Member left", color=discord.Color.red())
    emb.set_author(name=str(member), icon_url=member.display_avatar.url)
    emb.set_thumbnail(url=member.display_avatar.url)
    emb.add_field(name="Roles", value=fmt_roles(member), inline=False)
    emb.add_field(name="User", value=f"<@{member.id}> ({member.id})", inline=False)
    emb.add_field(name="Joined via", value=join_txt, inline=False)
    await log_ch.send(embed=emb)


@bot.event
async def on_guild_join(guild: discord.Guild):
    await refresh_invites_for_guild(guild)


@bot.event
async def on_ready():
    bot.add_view(TicketOpenView())
    bot.add_view(RolePanelView())
    bot.add_view(MarketOpenViewBerlin())
    bot.add_view(MarketOpenViewPoland())
    bot.add_view(MarketListingView(disabled=False))

    if not auto_unmute_loop.is_running():
        auto_unmute_loop.start()

    print(f"✅ Online als {bot.user} ({bot.user.id})")

    for g in bot.guilds:
        await refresh_invites_for_guild(g)

    try:
        await bot.tree.sync()
    except Exception as e:
        print("Sync error:", e)


bot.run(TOKEN)
