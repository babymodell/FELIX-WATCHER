import os
import asyncio
import sqlite3
import datetime
import random
from collections import defaultdict

import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

# -------------------- ENV --------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Lokal: .env laden, auf Railway egal (da kommen Vars aus Environment)
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

if not TOKEN:
    raise SystemExit("❌ DISCORD_BOT_TOKEN fehlt als Environment Variable (Railway Variables).")

# -------------------- DB --------------------
DB_PATH = os.path.join(BASE_DIR, "bot.sqlite3")


def db():
    conn = sqlite3.connect(DB_PATH)
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
    return conn


# -------------------- BOT --------------------
intents = discord.Intents.default()
intents.members = True  # wichtig für join/leave, roles, moderation
# message_content intent brauchst du nur für Prefix-Commands. Wir nutzen Slash.
# intents.message_content = True  # optional

bot = commands.Bot(command_prefix="!", intents=intents)
discord.utils.setup_logging()

# -------------------- Invite Tracking (optional) --------------------
invite_cache = defaultdict(dict)  # guild_id -> {code: uses}
vanity_cache = {}                # guild_id -> uses
join_method_cache = {}           # (guild_id, user_id) -> dict(method=..., inviter=..., code=...)


# -------------------- Helpers --------------------
def now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


async def get_text_channel(guild: discord.Guild, channel_id: int | None) -> discord.TextChannel | None:
    if not channel_id:
        return None
    ch = guild.get_channel(channel_id)
    return ch if isinstance(ch, discord.TextChannel) else None


async def get_log_channel(guild: discord.Guild) -> discord.TextChannel | None:
    return await get_text_channel(guild, LOG_CHANNEL_ID)


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


def is_staff(member: discord.Member) -> bool:
    # Admin immer staff
    if member.guild_permissions.administrator:
        return True
    # Wenn keine Staff Rollen gesetzt sind, gilt: kein Staff
    if not TICKET_STAFF_ROLE_IDS:
        return False
    for rid in TICKET_STAFF_ROLE_IDS:
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


# -------------------- Ticket Helpers --------------------
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


# -------------------- Mute System --------------------
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


# -------------------- Role Panel --------------------
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
            else:
                await member.add_roles(role, reason="Role Panel toggle")
                await interaction.response.send_message(f"✅ Rolle bekommen: {role.mention}", ephemeral=True)
        except discord.Forbidden:
            await interaction.response.send_message("❌ Ich habe keine Rechte Rollen zu vergeben.", ephemeral=True)

    @discord.ui.button(label="Poland", style=discord.ButtonStyle.danger, emoji="🇵🇱", custom_id="rolepanel:poland")
    async def poland(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._toggle_role(interaction, ROLE_POLAND_ID)

    @discord.ui.button(label="Germany", style=discord.ButtonStyle.secondary, emoji="🇩🇪", custom_id="rolepanel:germany")
    async def germany(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._toggle_role(interaction, ROLE_GERMANY_ID)


# -------------------- Ticket Views --------------------
class TicketManageView(discord.ui.View):
    def __init__(self, ticket_owner_id: int):
        super().__init__(timeout=None)
        self.ticket_owner_id = ticket_owner_id

    async def _update_status_embed(self, channel: discord.TextChannel, status_text: str):
        async for msg in channel.history(limit=25):
            if msg.author == channel.guild.me and msg.embeds:
                emb = msg.embeds[0]
                if len(emb.fields) >= 2:
                    emb.set_field_at(1, name="Status", value=status_text, inline=False)
                else:
                    emb.add_field(name="Status", value=status_text, inline=False)
                try:
                    await msg.edit(embed=emb, view=self)
                except:
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
        log_ch = await get_log_channel(interaction.guild)
        if log_ch:
            await log_ch.send(f"🔒 Ticket close: #{ch.name} von {interaction.user} ({interaction.user.id})")

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
        log_ch = await get_log_channel(interaction.guild)
        if log_ch:
            await log_ch.send(f"🧾 Ticket claim: #{ch.name} von {interaction.user} ({interaction.user.id})")


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

        # check: already has ticket
        for ch in guild.text_channels:
            if ch.topic and f"user_id={member.id}" in ch.topic and ch.category_id == category.id:
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

        await ch.send(
            content=staff_ping,
            embed=embed,
            view=TicketManageView(ticket_owner_id=member.id)
        )

        await interaction.response.send_message(f"✅ Ticket erstellt: {ch.mention}", ephemeral=True)

    @discord.ui.button(label="Question", style=discord.ButtonStyle.secondary, emoji="❓", custom_id="ticket_open:question")
    async def question(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._create_ticket(interaction, "Question", "❓")

    @discord.ui.button(label="Recruitment", style=discord.ButtonStyle.primary, emoji="📌", custom_id="ticket_open:recruitment")
    async def recruitment(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._create_ticket(interaction, "Recruitment", "📌")

    @discord.ui.button(label="Partnership", style=discord.ButtonStyle.success, emoji="🤝", custom_id="ticket_open:partnership")
    async def partnership(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._create_ticket(interaction, "Partnership", "🤝")


# -------------------- Economy Helpers --------------------
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


# -------------------- Commands: Setup Panels --------------------
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


# -------------------- Commands: Ticket direct --------------------
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
    await asyncio.sleep(5)
    try:
        await ch.delete(reason=f"Ticket geschlossen von {interaction.user}")
    except discord.Forbidden:
        pass

bot.tree.add_command(ticket_group)


# -------------------- Commands: Moderation --------------------
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
    except discord.Forbidden:
        await interaction.response.send_message("❌ Bot hat keine Rechte zum Bannen.", ephemeral=True)


@bot.tree.command(name="timeout", description="Timeout für einen User (Minuten)")
@app_commands.describe(user="User", minuten="Dauer in Minuten", grund="Grund (optional)")
async def timeout(interaction: discord.Interaction, user: discord.Member, minuten: int, grund: str | None = None):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)
    if not interaction.user.guild_permissions.moderate_members and not is_staff(interaction.user):
        return await interaction.response.send_message("❌ Keine Rechte (Moderate Members).", ephemeral=True)

    minuten = max(1, min(10080, minuten))  # max 7 Tage
    until = now_utc() + datetime.timedelta(minutes=minuten)
    grund = grund or "—"

    try:
        await user.timeout(until, reason=f"{grund} | by {interaction.user}")
        await interaction.response.send_message(f"✅ Timeout gesetzt für {user.mention}: {minuten} Minuten.", ephemeral=True)
    except discord.Forbidden:
        await interaction.response.send_message("❌ Bot hat keine Rechte für Timeout.", ephemeral=True)


# -------------------- Commands: Mute --------------------
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
    except:
        pass

    await interaction.followup.send(f"🔇 {user.mention} wurde gemutet. Dauer: {dauer_txt}", ephemeral=True)


@bot.tree.command(name="unmute", description="Entmutet einen User")
@staff_check()
async def unmute(interaction: discord.Interaction, user: discord.Member):
    if not interaction.guild:
        return await interaction.response.send_message("Nur im Server nutzbar.", ephemeral=True)

    muted_role = discord.utils.get(interaction.guild.roles, name=MUTED_ROLE_NAME)
    if not muted_role or muted_role not in user.roles:
        return await interaction.response.send_message("User ist nicht gemutet.", ephemeral=True)

    try:
        await user.remove_roles(muted_role, reason=f"Unmuted von {interaction.user}")
    except discord.Forbidden:
        return await interaction.response.send_message("❌ Ich habe keine Rechte, Rollen zu entfernen.", ephemeral=True)

    conn = db()
    try:
        conn.execute("DELETE FROM mutes WHERE guild_id=? AND user_id=?", (interaction.guild.id, user.id))
        conn.commit()
    finally:
        conn.close()

    try:
        await user.send(f"✅ Du wurdest auf **{interaction.guild.name}** entmutet.")
    except:
        pass

    await interaction.response.send_message(f"✅ {user.mention} wurde entmutet.", ephemeral=True)


# -------------------- Economy Commands --------------------
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
        except:
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


# -------------------- Fun/Info Commands --------------------
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


# -------------------- Auto Unmute Loop --------------------
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
        except:
            continue

        if now >= unmute_time:
            guild = bot.get_guild(guild_id)
            if not guild:
                continue

            member = guild.get_member(user_id)
            muted_role = discord.utils.get(guild.roles, name=MUTED_ROLE_NAME)

            if member and muted_role and muted_role in member.roles:
                try:
                    await member.remove_roles(muted_role, reason="Auto-Unmute (Timer)")
                except:
                    pass

            conn2 = db()
            try:
                conn2.execute("DELETE FROM mutes WHERE guild_id=? AND user_id=?", (guild_id, user_id))
                conn2.commit()
            finally:
                conn2.close()


@auto_unmute_loop.before_loop
async def before_auto_unmute():
    await bot.wait_until_ready()


# -------------------- Events --------------------
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
    # Persistente Views (Buttons funktionieren nach Restart)
    bot.add_view(TicketOpenView())
    bot.add_view(RolePanelView())

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
