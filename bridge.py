"""Local Telegram <-> MAX bridge. Python 3.10+. See README.md."""
import argparse
import asyncio
import contextlib
import getpass
import hashlib
import json
import logging
import os
from pathlib import Path
import sqlite3
import sys
import time
import webbrowser

import aiohttp
from pymax import WebClient, Message, Photo, File, Video
from pymax.config import ExtraConfig
from pymax.api.messages.payloads import SendMessagePayload, SendMessagePayloadMessage, ReplyLink, EditMessagePayload
from pymax.api.response import require_payload_model
from pymax.protocol import Opcode
from pymax.exceptions import ApiError

ROOT = Path(os.environ.get('TGMAX_DATA_HOME') or (
    str(Path(os.environ.get('LOCALAPPDATA', str(Path.home()))) / 'TelegramMaxBridge')
    if getattr(sys, 'frozen', False) else str(Path(__file__).resolve().parent)))
DATA = ROOT / "data"
LOG = logging.getLogger("bridge")
MAX_BYTES = 20 * 1024 * 1024


def chunks(text, limit=3000):
    # Telegram limits are in UTF-16 units. Never split a Unicode code point.
    part, size = [], 0
    for char in text:
        units = 2 if ord(char) > 0xFFFF else 1
        if size + units > limit:
            yield "".join(part)
            part, size = [], 0
        part.append(char)
        size += units
    if part:
        yield "".join(part)


def fingerprint(text):
    return hashlib.sha256(text.encode()).hexdigest()


class Store:
    def __init__(self, path, recover=True):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript('''
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT);
            CREATE TABLE IF NOT EXISTS jobs(
                id INTEGER PRIMARY KEY, side TEXT, chat TEXT, mid TEXT,
                payload TEXT, state TEXT DEFAULT 'pending', error TEXT,
                UNIQUE(side,chat,mid));
            CREATE TABLE IF NOT EXISTS parts(
                job INTEGER, part INTEGER, state TEXT, PRIMARY KEY(job,part));
            CREATE TABLE IF NOT EXISTS plans(
                job INTEGER, part INTEGER, text TEXT, kind TEXT, name TEXT, data BLOB,
                PRIMARY KEY(job,part));
            CREATE TABLE IF NOT EXISTS echoes(
                chat TEXT, cid TEXT, mid TEXT, hash TEXT);
            CREATE INDEX IF NOT EXISTS echo_lookup ON echoes(chat,cid,mid);
        ''')
        if 'route' not in {r[1] for r in self.db.execute('PRAGMA table_info(jobs)')}:
            self.db.execute('ALTER TABLE jobs ADD COLUMN route TEXT')
        self.db.execute('CREATE INDEX IF NOT EXISTS job_route ON jobs(route,state,id)')
        # A crash after a send may mean delivery succeeded. Never blindly resend.
        if recover:
            self.db.execute("UPDATE jobs SET state='uncertain', error='Interrupted during send' WHERE state='sending'")
        self.db.commit()

    def get(self, key, default=None):
        row = self.db.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
        return row[0] if row else default

    def set(self, key, value):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO kv VALUES(?,?)", (key, str(value)))

    def add(self, side, chat, mid, payload, route=None):
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO jobs(side,chat,mid,payload,route) VALUES(?,?,?,?,?)",
                            (side, str(chat), str(mid), json.dumps(payload, ensure_ascii=False), route))

    def state(self, job, state, error=None):
        with self.db:
            self.db.execute("UPDATE jobs SET state=?,error=? WHERE id=?", (state, error, job))
            if state == 'done':
                self.db.execute("UPDATE jobs SET payload='{}' WHERE id=?", (job,))
                self.db.execute("DELETE FROM plans WHERE job=?", (job,))

    def part_done(self, job, index):
        return self.db.execute("SELECT 1 FROM parts WHERE job=? AND part=? AND state='done'", (job, index)).fetchone() is not None

    def save_part(self, job, index):
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO parts VALUES(?,?,'done')", (job, index))

    def expect_echo(self, chat, cid, text):
        with self.db:
            self.db.execute("INSERT INTO echoes VALUES(?,?,NULL,?)", (str(chat), str(cid), fingerprint(text)))

    def sent_echo(self, chat, cid, mid):
        with self.db:
            self.db.execute("UPDATE echoes SET mid=? WHERE chat=? AND cid=?", (str(mid), str(chat), str(cid)))

    def is_echo(self, message, own_id):
        if message.sender != own_id:
            return False
        return self.db.execute(
            "SELECT 1 FROM echoes WHERE chat=? AND (cid=? OR mid=? OR hash=?) LIMIT 1",
            (str(message.chat_id), str(message.cid), str(message.id), fingerprint(message.text))).fetchone() is not None


class TelegramError(Exception):
    def __init__(self, code, retry_after=0, description=''):
        self.code, self.retry_after = code, retry_after
        self.not_modified = 'message is not modified' in description.lower()
        self.photo_rejected = code == 400 and any(reason in description.lower() for reason in
            ('photo_invalid_dimensions', 'photo_content_type_invalid', 'image_process_failed', 'photo_ext_invalid', 'photo_invalid', 'file is too big'))
        super().__init__(f"Telegram API error {code}")


class Telegram:
    def __init__(self, token, session):
        self.token, self.http = token, session

    async def call(self, method, payload=None, form=None):
        # Do not log request URLs: Telegram embeds the token in them.
        async with self.http.post(f"https://api.telegram.org/bot{self.token}/{method}",
                                  json=payload if form is None else None, data=form) as response:
            result = await response.json()
        if not result.get("ok"):
            raise TelegramError(result.get("error_code", 0), result.get("parameters", {}).get("retry_after", 0), result.get('description', ''))
        return result["result"]

    async def updates(self, offset, timeout=25):
        return await self.call("getUpdates", {"offset": offset, "timeout": timeout,
                                               "allowed_updates": ["message", "edited_message", "my_chat_member", "message_reaction", "message_reaction_count", "poll", "poll_answer"]})

    async def download(self, file_id):
        info = await self.call("getFile", {"file_id": file_id})
        return await download(self.http, f"https://api.telegram.org/file/bot{self.token}/{info['file_path']}")

    async def send(self, chat, text, media=None, reply_to=None):
        reply = {'reply_parameters': {'message_id': int(reply_to), 'allow_sending_without_reply': True}} if reply_to else {}
        if not media:
            return await self.call("sendMessage", {"chat_id": chat, "text": text,
                                                    "link_preview_options": {"is_disabled": True}, **reply})
        kind, name, data = media
        if kind == 'photo' and len(data) > 10 * 1024 * 1024:
            kind = 'file'
        field, method = {"photo": ("photo", "sendPhoto"), "video": ("video", "sendVideo"),
                         "voice": ("voice", "sendVoice")}.get(kind, ("document", "sendDocument"))
        form = aiohttp.FormData()
        form.add_field("chat_id", str(chat))
        form.add_field("caption", text)
        if reply:
            form.add_field('reply_parameters', json.dumps(reply['reply_parameters']))
        form.add_field(field, data, filename=name, content_type="application/octet-stream")
        try:
            return await self.call(method, form=form)
        except TelegramError as exc:
            if kind == 'photo' and exc.photo_rejected:
                # A definite rejection means no photo was sent. Preserve original bytes.
                return await self.send(chat, text, ('file', name, data), reply_to=reply_to)
            raise

    async def edit(self, chat, mid, text):
        try:
            return await self.call('editMessageText', {'chat_id': chat, 'message_id': int(mid), 'text': text,
                                                       'link_preview_options': {'is_disabled': True}})
        except TelegramError as exc:
            if not exc.not_modified:
                raise

    async def delete(self, chat, mid):
        return await self.call('deleteMessage', {'chat_id': chat, 'message_id': int(mid)})

    async def pin(self, chat, mid):
        return await self.call('pinChatMessage', {'chat_id': chat, 'message_id': int(mid), 'disable_notification': True})

    async def send_poll(self, chat, poll, reply_to=None):
        payload = {'chat_id': chat, 'question': poll['question'], 'options': [{'text': t} for t in poll['options']],
                   'is_anonymous': True, 'allows_multiple_answers': poll.get('multiple', False),
                   'is_closed': poll.get('closed', False)}
        if reply_to:
            payload['reply_parameters'] = {'message_id': int(reply_to), 'allow_sending_without_reply': True}
        return await self.call('sendPoll', payload)


async def download(session, url):
    if not url or not url.startswith("https://"):
        raise ValueError("Missing HTTPS media URL")
    async with session.get(url) as response:
        response.raise_for_status()
        if response.content_length and response.content_length > MAX_BYTES:
            raise ValueError("Media exceeds 20 MiB")
        data = bytearray()
        async for block in response.content.iter_chunked(65536):
            data.extend(block)
            if len(data) > MAX_BYTES:
                raise ValueError("Media exceeds 20 MiB")
        return bytes(data)


class MaxAdapter:
    def __init__(self, client, session, store):
        self.client, self.http, self.store = client, session, store

    async def send(self, chat, text, media=None, reply_to=None, poll=None):
        attachments = []
        if media:
            kind, name, data = media
            cls = {"photo": Photo, "video": Video}.get(kind, File)
            attachments = [cls(raw=data, name=name)]
        if poll:
            from pymax.types import Poll, PollAnswer, PollFlags
            flags = PollFlags.FLAG_SETTINGS_ANONYMOUS | PollFlags.FLAG_SETTINGS_REVOTE
            if poll.get('multiple'):
                flags |= PollFlags.FLAG_SETTINGS_MULTISELECT
            if poll.get('closed'):
                flags |= PollFlags.FLAG_SETTINGS_CLOSED
            attachments.append(Poll(title=poll['question'], answers=[PollAnswer(text=t) for t in poll['options']], settings=flags))
        # Pinned PyMax adapter: preserve literal text (public send_message parses markdown)
        # and register CID before sending, including the event-before-response race.
        service = self.client._app.api.messages
        uploaded = await service._upload_attachments(attachments)
        cid = service._next_cid()
        self.store.expect_echo(chat, cid, text)
        frame = SendMessagePayload(chat_id=chat, notify=True, message=SendMessagePayloadMessage(
            text=text, cid=cid, elements=[], attaches=uploaded,
            link=ReplyLink(message_id=int(reply_to)) if reply_to else None))
        try:
            response = await self.client._app.invoke(Opcode.MSG_SEND, frame.to_payload())
        except ApiError as exc:
            if exc.error != 'attachment.not.ready':
                raise
            await service._process_attachment_error(uploaded)
            response = await self.client._app.invoke(Opcode.MSG_SEND, frame.to_payload())
        message = require_payload_model(response, Message)
        self.store.sent_echo(chat, cid, message.id)
        return message

    async def edit(self, chat, mid, text):
        frame = EditMessagePayload(chat_id=chat, message_id=int(mid), text=text, elements=[], attachments=[])
        return await self.client._app.invoke(Opcode.MSG_EDIT, frame.to_payload())

    async def delete(self, chat, mid):
        return await self.client.delete_message(chat_id=chat, message_ids=[int(mid)], for_me=False)

    async def pin(self, chat, mid):
        return await self.client.pin_message(chat_id=chat, message_id=int(mid), notify_pin=False)

    async def send_poll(self, chat, poll, reply_to=None):
        return await self.send(chat, 'Опрос из Telegram', reply_to=reply_to, poll=poll)

    async def media(self, message, attach):
        kind = attach.get("type", attach.get("_type"))
        if kind == "PHOTO":
            return "photo", "photo.jpg", await download(self.http, attach.get("base_url"))
        if kind == "AUDIO":
            return "voice", "voice.ogg", await download(self.http, attach.get("url"))
        if kind == "STICKER":
            return "file", "sticker.webp", await download(self.http, attach.get("url"))
        if kind == "FILE":
            if attach.get("size", 0) > MAX_BYTES:
                raise ValueError("Media exceeds 20 MiB")
            info = await self.client.get_file_by_id(message.chat_id, message.id, attach["file_id"])
            return "file", Path(attach.get("name") or "file.bin").name, await download(self.http, info.url if info else None)
        if kind == "VIDEO":
            info = await self.client.get_video_by_id(message.chat_id, message.id, attach["video_id"])
            return "video", "video.mp4", await download(self.http, info.url if info else None)
        return None


def tg_content(message):
    text = message.get("text") or message.get("caption") or ""
    if message.get("poll"):
        poll = message["poll"]
        text += "\nОпрос: " + poll["question"] + "\n" + "\n".join("• " + o["text"] for o in poll["options"])
    if message.get("location"):
        p = message["location"]
        text += f"\nМестоположение: {p['latitude']}, {p['longitude']}"
    if message.get("contact"):
        p = message["contact"]
        text += f"\nКонтакт: {p.get('first_name', '')} {p.get('last_name', '')}: {p.get('phone_number', '')}"
    media = None
    for kind, default in [("photo", "photo.jpg"), ("video", "video.mp4"), ("voice", "voice.ogg"),
                          ("document", "file.bin"), ("audio", "audio.mp3"), ("animation", "animation.mp4"),
                          ("video_note", "video.mp4"), ("sticker", "sticker.webp")]:
        if kind in message:
            item = message[kind][-1] if kind == "photo" else message[kind]
            if kind == "sticker":
                default = "sticker.tgs" if item.get("is_animated") else "sticker.webm" if item.get("is_video") else default
            target = kind if kind in {"photo", "video", "voice"} else "file"
            media = (target, item.get("file_name") or default, item)
            break
    if not text and not media and not any(k in message for k in ("new_chat_members", "left_chat_member", "new_chat_title", "new_chat_photo", "delete_chat_photo", "group_chat_created", "supergroup_chat_created", "pinned_message")):
        text = "[Сообщение неподдерживаемого типа]"
    return text, media


class Bridge:
    def __init__(self, config, store, tg, mx, bot_id, own_id):
        self.cfg, self.store, self.tg, self.mx = config, store, tg, mx
        self.bot_id, self.own_id = bot_id, own_id
        self.route = route_key(config)
        from sync_engine import SyncEngine
        self.sync = SyncEngine(self)

    def ingest_tg(self, message):
        if message.get("chat", {}).get("id") != self.cfg["telegram_chat_id"]:
            return
        if self.sync.command(message):
            return
        if message.get('pinned_message'):
            self.sync.queue('pin', 'tg', message['pinned_message']['message_id'], {})
            return
        if message.get("from", {}).get("id") == self.bot_id or message.get("date", 0) < self.cfg["linked_at"]:
            return
        if message.get("text", "").split(" ")[0].split("@")[0] == "/bridge":
            return
        text, media = tg_content(message)
        if text or media:
            self.sync.remember('tg', message['message_id'], message)
            self.store.add("tg", message["chat"]["id"], message["message_id"], message, self.route)

    def ingest_max(self, message):
        if message.chat_id != self.cfg["max_chat_id"] or message.time < self.cfg["linked_at"] * 1000:
            return
        if message.status == 'REMOVED':
            self.sync.deleted('max', message.id)
            return
        if message.status == 'EDITED':
            self.sync.edited('max', message.id, message.model_dump(mode='json'))
            return
        if self.store.is_echo(message, self.own_id):
            return
        if not message.text and all(a.type in {"CONTROL", "CALL"} for a in message.attaches):
            return
        data = message.model_dump(mode='json')
        self.sync.remember('max', message.id, data)
        self.store.add("max", message.chat_id, message.id, data, self.route)

    def ingest_update(self, update):
        if 'message' in update:
            self.ingest_tg(update['message'])
        self.sync.telegram_update(update)

    async def poll(self):
        delay = 1
        while True:
            try:
                updates = await self.tg.updates(int(self.store.get("offset", 0)))
                for update in updates:
                    self.ingest_update(update)
                    self.store.set("offset", update["update_id"] + 1)
                delay = 1
            except TelegramError as exc:
                if exc.code in (401, 409):
                    raise
                LOG.warning("Telegram polling: code %s", exc.code)
                await asyncio.sleep(max(delay, exc.retry_after))
                delay = min(delay * 2, 30)
            except (aiohttp.ClientError, TimeoutError):
                LOG.warning("Telegram connection unavailable; retrying")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def catch_up(self):
        # Read only the configured MAX group. An overlapping time window plus
        # persistent message IDs catches events lost while disconnected.
        while True:
            try:
                cutoff = int(self.store.get("max_scan_time:" + self.route, self.cfg["linked_at"] * 1000))
                cutoff = max(self.cfg["linked_at"] * 1000, cutoff - 60000)
                scan_start = int(time.time() * 1000)
                cursor, collected = scan_start, {}
                for _ in range(500):
                    page = await self.mx.client.fetch_history(self.cfg["max_chat_id"], backward=100, from_time=cursor)
                    if not page:
                        break
                    for message in page:
                        if message.chat_id is None:
                            message.chat_id = self.cfg["max_chat_id"]
                        if message.time >= cutoff:
                            collected[message.id] = message
                    oldest = min(m.time for m in page)
                    if oldest <= cutoff:
                        break
                    if oldest >= cursor:
                        raise RuntimeError("MAX history pagination made no progress")
                    cursor = oldest - 1
                else:
                    raise RuntimeError("MAX history scan limit reached")
                for message in sorted(collected.values(), key=lambda m: (m.time, m.id)):
                    self.ingest_max(message)
                self.store.set("max_scan_time:" + self.route, scan_start)
            except Exception as exc:
                LOG.warning("MAX history sync delayed (%s)", type(exc).__name__)
            await asyncio.sleep(30)

    async def relay(self, row):
        if row['route'] != self.route:
            raise RuntimeError('Задание относится к другой связке групп.')
        if self.sync.is_deleted(row['side'], row['mid']):
            return True
        source = json.loads(row['payload'])
        if self.sync.extract_poll(row['side'], source):
            return await self.sync.relay_poll(row, source)
        plan = self.store.db.execute("SELECT * FROM plans WHERE job=? ORDER BY part", (row['id'],)).fetchall()
        if plan:
            operations = [(p['text'], (p['kind'], p['name'], p['data']) if p['kind'] else None) for p in plan]
            return await self.deliver(row, operations)
        source = json.loads(row["payload"])
        media_list, notices = [], []
        if row["side"] == "tg":
            user = source.get("sender_chat") or source.get("from", {})
            name = user.get("title") or " ".join(filter(None, [user.get("first_name"), user.get("last_name")])) or "Участник"
            text, media = tg_content(source)
            if media:
                kind, filename, item = media
                try:
                    if item.get("file_size", 0) > MAX_BYTES:
                        raise ValueError("Media exceeds 20 MiB")
                    blob = await self.tg.download(item["file_id"])
                    # Send voice as OGG file to avoid inventing duration metadata.
                    media_list.append(("file" if kind == "voice" else kind, filename, blob))
                except (aiohttp.ClientError, TimeoutError):
                    raise
                except Exception as exc:
                    notices.append(f"[Вложение {filename} не перенесено: {type(exc).__name__}; лимит 20 МиБ]")
            label = "Telegram"
        else:
            message = Message.model_validate(source)
            name = f"Участник {message.sender}"
            try:
                user = await self.mx.client.get_user(message.sender) if message.sender else None
                if user and user.names:
                    n = user.names[0]
                    name = n.name or " ".join(filter(None, [n.first_name, n.last_name])) or name
            except Exception:
                pass
            text = message.text
            for attach in source.get("attaches", []):
                kind = attach.get("type")
                if kind in {"CONTROL", "CALL", "SHARE"}:
                    continue
                try:
                    media = await self.mx.media(message, attach)
                    if media:
                        media_list.append(media)
                    elif kind == "POLL":
                        notices.append("[Опрос] " + str(attach.get("title", "")))
                    else:
                        notices.append(f"[Вложение {kind} не поддерживается]")
                except (aiohttp.ClientError, TimeoutError):
                    raise
                except Exception as exc:
                    notices.append(f"[Вложение {kind} не перенесено: {type(exc).__name__}; лимит 20 МиБ]")
            label = "MAX"
        # One text part followed by separate attachments; part checkpoints survive restarts.
        heading = f"{label} · {name[:150]}\n"
        operations = [(part, None) for part in chunks(heading + text + ("\n" + "\n".join(notices) if notices else ""))]
        operations.extend((heading.rstrip(), media) for media in media_list)
        with self.store.db:
            for index, (part, media) in enumerate(operations):
                self.store.db.execute("INSERT INTO plans VALUES(?,?,?,?,?,?)",
                                      (row['id'], index, part, *(media or (None, None, None))))
        return await self.deliver(row, operations)

    async def deliver(self, row, operations):
        target, send = (self.cfg['max_chat_id'], self.mx.send) if row['side'] == 'tg' else (self.cfg['telegram_chat_id'], self.tg.send)
        reply_to = self.sync.reply_for(row)
        for index, (part, media) in enumerate(operations):
            if self.store.part_done(row["id"], index):
                continue
            self.store.state(row["id"], "sending")
            try:
                result = await send(target, part, media, **({'reply_to': reply_to} if reply_to else {}))
                self.sync.map_result(row, index, result, 'media' if media else 'text', part)
            except TelegramError as exc:
                if exc.code == 429:
                    self.store.state(row["id"], "pending", "Rate limited")
                    await asyncio.sleep(max(1, exc.retry_after))
                    return False
                raise
            self.store.save_part(row["id"], index)
            self.store.state(row["id"], "pending")
            await asyncio.sleep(1.1 if row['side'] == 'tg' else 3.1)
        return True

    async def worker(self):
        while True:
            row = self.store.db.execute("SELECT * FROM jobs WHERE state='pending' AND route=? ORDER BY id LIMIT 1", (self.route,)).fetchone()
            if not row:
                await asyncio.sleep(.3)
                continue
            try:
                async with self.sync.lock:
                    if await self.relay(row):
                        self.store.state(row["id"], "done")
                        LOG.info("Delivered job %s (%s)", row["id"], row["side"])
            except Exception as exc:
                # Unknown delivery outcomes are NOT automatically retried.
                current = self.store.db.execute('SELECT state FROM jobs WHERE id=?', (row['id'],)).fetchone()
                if current['state'] == 'pending' and isinstance(exc, (aiohttp.ClientError, TimeoutError)):
                    LOG.warning('Загрузка вложения задания %s прервана; повтор через 5 секунд', row['id'])
                    await asyncio.sleep(5)
                    continue
                self.store.state(row["id"], "uncertain", type(exc).__name__)
                LOG.error("Job %s needs inspection (%s). Run: python bridge.py status", row["id"], type(exc).__name__)


def route_key(config):
    return config.get('route_id') or f"{config['telegram_chat_id']}:{config['max_chat_id']}:{config['linked_at']}"


def route_configs(config):
    routes = config.get('routes', [config] if 'telegram_chat_id' in config else [])
    telegram_ids, max_ids, route_ids = set(), set(), set()
    for route in routes:
        if route['telegram_chat_id'] in telegram_ids or route['max_chat_id'] in max_ids:
            raise RuntimeError('Каждая группа может входить только в одну связку.')
        if route_key(route) in route_ids:
            raise RuntimeError('Повторяющийся идентификатор связки.')
        telegram_ids.add(route['telegram_chat_id'])
        max_ids.add(route['max_chat_id'])
        route_ids.add(route_key(route))
    return routes


class MultiBridge:
    """One Telegram update consumer, isolated workers/checkpoints for each pair."""
    def __init__(self, config, store, tg, mx, bot_id, own_id):
        self.store, self.tg = store, tg
        self.routes = [Bridge(r, store, tg, mx, bot_id, own_id) for r in route_configs(config)]
        if not self.routes:
            raise RuntimeError('Добавьте хотя бы одну связку групп.')
        self.by_tg = {r.cfg['telegram_chat_id']: r for r in self.routes}
        self.by_max = {r.cfg['max_chat_id']: r for r in self.routes}
        if 'telegram_chat_id' in config:
            with store.db:
                store.db.execute("UPDATE jobs SET route=? WHERE route IS NULL AND ((side='tg' AND chat=?) OR (side='max' AND chat=?))",
                                 (self.routes[0].route, str(config['telegram_chat_id']), str(config['max_chat_id'])))

    def ingest_tg(self, message):
        route = self.by_tg.get(message.get('chat', {}).get('id'))
        if route:
            route.ingest_tg(message)

    def ingest_max(self, message):
        route = self.by_max.get(message.chat_id)
        if route:
            route.ingest_max(message)

    def ingest_update(self, update):
        if 'message' in update:
            self.ingest_tg(update['message'])
        for route in self.routes:
            route.sync.telegram_update(update)

    def max_edit(self, message):
        route = self.by_max.get(message.chat_id)
        if route:
            route.sync.edited('max', message.id, message.model_dump(mode='json'))

    def max_delete(self, event):
        route = self.by_max.get(event.chat_id)
        if route:
            for mid in event.message_ids:
                route.sync.deleted('max', mid)

    def max_reactions(self, event):
        route = self.by_max.get(event.chat_id)
        if route:
            route.sync.max_reactions(event)

    poll = Bridge.poll

    async def worker(self):
        await asyncio.gather(*(task for route in self.routes for task in (route.worker(), route.sync.worker())))

    async def catch_up(self):
        await asyncio.gather(*(task for route in self.routes for task in (route.catch_up(), route.sync.monitor())))


class QR:
    async def show_qr(self, url):
        import qrcode
        path = DATA / "max-login.png"
        qrcode.make(url).save(path)
        print("\nОткройте MAX на телефоне → Устройства → Подключить устройство.", flush=True)
        print("QR-код сохранён локально:", path, flush=True)
        webbrowser.open(path.as_uri())


def token():
    value = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    path = ROOT / "token.txt"
    if not value and path.exists():
        value = path.read_text(encoding="utf-8-sig").strip()
    if not value:
        value = getpass.getpass("Токен Telegram-бота (ввод скрыт): ").strip()
        path.write_text(value, encoding="utf-8")
    return value


async def choose(prompt, entries):
    for i, (ident, title) in enumerate(entries, 1):
        print(f"{i}. {title} ({ident})")
    while True:
        value = await asyncio.to_thread(input, prompt)
        try:
            return entries[int(value) - 1][0] if 0 < int(value) <= len(entries) else int("invalid")
        except ValueError:
            print("Введите номер из списка.")


async def setup(tg, client, bot, store):
    print(f"\nTelegram-бот: https://t.me/{bot['username']}")
    print("Добавьте его в нужную группу и назначьте администратором.\nОтправьте /bridge в группе Telegram, затем нажмите Enter здесь.")
    await asyncio.to_thread(input)
    groups = {}
    for _ in range(100):
        updates = await tg.updates(int(store.get("offset", 0)), timeout=0)
        for u in updates:
            chat = u.get("message", u.get("my_chat_member", {})).get("chat", {})
            if chat.get("type") in {"group", "supergroup"}:
                groups[chat["id"]] = chat.get("title", "Группа")
            store.set("offset", u["update_id"] + 1)
        if len(updates) < 100:
            break
    if not groups:
        raise RuntimeError("Группа Telegram не найдена. Отправьте /bridge в группе и запустите setup ещё раз.")
    tg_chat = await choose("Номер группы Telegram: ", list(groups.items()))
    membership = await tg.call("getChatMember", {"chat_id": tg_chat, "user_id": bot["id"]})
    if membership["status"] != "administrator":
        raise RuntimeError("Сделайте Telegram-бота администратором для чтения всех сообщений.")
    print("\nВход MAX через QR. Подтвердите вход на своём телефоне.")
    await client.connect()
    groups = [(c.id, c.title or "Группа") for c in client.chats or [] if c.type == "CHAT"]
    if not groups:
        raise RuntimeError("MAX не вернул группы. Добавьте аккаунт в группу и повторите setup.")
    max_chat = await choose("Номер группы MAX: ", groups)
    cfg = {"telegram_chat_id": tg_chat, "max_chat_id": max_chat,
           "max_user_id": client.me.contact.id, "linked_at": int(time.time())}
    if (ROOT / "config.json").exists():
        old = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
        if old["telegram_chat_id"] == tg_chat and old["max_chat_id"] == max_chat:
            cfg["linked_at"] = old["linked_at"]
        else:
            # Queued content must never be delivered into a newly selected group.
            with store.db:
                store.db.execute("UPDATE jobs SET state='cancelled' WHERE state!='done'")
                store.db.execute("DELETE FROM plans")
                store.db.execute("DELETE FROM kv WHERE k='max_scan_time'")
    (ROOT / "config.json").write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    print("\nГруппы связаны. Запустите START.cmd (или python bridge.py run).")


async def main(mode, job_id=None):
    DATA.mkdir(exist_ok=True)
    store = Store(DATA / "bridge.sqlite3", recover=mode == "run")
    if mode == 'retry':
        row = store.db.execute("SELECT * FROM jobs WHERE id=? AND state='uncertain'", (job_id,)).fetchone()
        if not row:
            raise RuntimeError("Задание с таким ID в состоянии uncertain не найдено.")
        print("Проверьте целевую группу: сообщение могло дойти до сбоя. Повтор может создать дубликат.")
        answer = await asyncio.to_thread(input, "Для повтора введите RETRY: ")
        if answer == 'RETRY':
            store.state(job_id, 'pending')
            print("Задание возвращено в очередь. Запустите мост.")
        return
    if mode == "status":
        for row in store.db.execute("SELECT state,COUNT(*) n FROM jobs GROUP BY state"):
            print(row["state"], row["n"])
        for row in store.db.execute("SELECT id,side,mid,state,error FROM jobs WHERE state='uncertain'"):
            print(dict(row))
        return
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120)) as session:
        tg = Telegram(token(), session)
        bot = await tg.call("getMe")
        webhook = await tg.call("getWebhookInfo")
        if mode == "check":
            print(json.dumps({"username": bot.get("username"), "id": bot["id"],
                              "webhook_configured": bool(webhook.get("url")),
                              "can_join_groups": bot.get("can_join_groups")}, ensure_ascii=False))
            return
        if webhook.get("url"):
            raise RuntimeError("У бота уже настроен webhook. Отключите существующий сервис перед запуском моста.")
        client = WebClient(work_dir=str(DATA), session_name="max-session.db", qr_provider=QR(),
                           extra_config=ExtraConfig(log_level="CRITICAL", telemetry=False))
        try:
            if mode == "setup":
                await setup(tg, client, bot, store)
                return
            cfg = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
            mx = MaxAdapter(client, session, store)
            bridge = MultiBridge(cfg, store, tg, mx, bot["id"], cfg["max_user_id"])
            ready = asyncio.Event()

            @client.on_message()
            async def on_message(message, _client):
                bridge.ingest_max(message)

            @client.on_message_edit()
            async def on_edit(message, _client):
                bridge.max_edit(message)

            @client.on_message_delete()
            async def on_delete(event, _client):
                bridge.max_delete(event)

            @client.on_reaction_update()
            async def on_reactions(event, _client):
                bridge.max_reactions(event)

            @client.on_start()
            async def on_start(_client):
                if client.me.contact.id != cfg["max_user_id"]:
                    raise RuntimeError("В MAX выполнен вход в другой аккаунт. Повторите setup.")
                ready.set()
                LOG.info("MAX connected")

            max_task = asyncio.create_task(client.start())
            ready_task = asyncio.create_task(ready.wait())
            tasks = [max_task, ready_task]
            try:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                if max_task in done:
                    await max_task
                    raise RuntimeError("MAX disconnected before login")
                print("Мост запущен. Для остановки нажмите Ctrl+C. Компьютер должен оставаться включённым.", flush=True)
                tasks = [max_task, asyncio.create_task(bridge.poll()), asyncio.create_task(bridge.worker()),
                         asyncio.create_task(bridge.catch_up())]
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    await task
                raise RuntimeError("Bridge component stopped")
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await client.close()
            (DATA / "max-login.png").unlink(missing_ok=True)


@contextlib.contextmanager
def single_instance():
    DATA.mkdir(parents=True, exist_ok=True)
    handle = (DATA / "bridge.lock").open("a+b")
    handle.seek(0)
    if handle.read(1) == b"":
        handle.write(b"0")
        handle.flush()
    handle.seek(0)
    try:
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        raise RuntimeError("Мост уже запущен. Остановите другой экземпляр.") from None
    try:
        yield
    finally:
        handle.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["setup", "run", "check", "status", "retry"], nargs="?", default="run")
    parser.add_argument("job_id", nargs="?", type=int)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        with single_instance() if args.mode != "status" else contextlib.nullcontext():
            asyncio.run(main(args.mode, args.job_id))
    except KeyboardInterrupt:
        print("\nМост остановлен.")
    except Exception as exc:
        # Library/network exception strings may include tokens or signed URLs.
        print("Ошибка:", str(exc) if isinstance(exc, (RuntimeError, TelegramError, FileNotFoundError)) else type(exc).__name__)
        sys.exit(1)
