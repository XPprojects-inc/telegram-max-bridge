"""Desktop backend; all methods run on one asyncio thread."""
import asyncio
import base64
import ctypes
from ctypes import wintypes
import json
import logging
import os
from pathlib import Path
import time
import uuid

import aiohttp
from pymax import WebClient
from pymax.auth.qr import QrAuthFlow
from pymax.config import ExtraConfig

import bridge as core


def protect_token(text, decrypt=False):
    """Windows DPAPI, bound to the current Windows user; never bundle secrets."""
    if os.name != 'nt':
        raise RuntimeError('Эта версия приложения предназначена для Windows.')

    class Blob(ctypes.Structure):
        _fields_ = [('size', wintypes.DWORD), ('data', ctypes.POINTER(ctypes.c_ubyte))]

    raw = base64.b64decode(text) if decrypt else text.encode('utf-8')
    buffer = ctypes.create_string_buffer(raw)
    source = Blob(len(raw), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ubyte)))
    result = Blob()
    crypt = ctypes.WinDLL('crypt32', use_last_error=True)
    function = crypt.CryptUnprotectData if decrypt else crypt.CryptProtectData
    function.argtypes = [ctypes.POINTER(Blob), ctypes.c_void_p, ctypes.c_void_p,
                         ctypes.c_void_p, ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(Blob)]
    function.restype = wintypes.BOOL
    if not function(ctypes.byref(source), None, None, None, None, 1, ctypes.byref(result)):
        raise RuntimeError('Не удалось открыть сохранённый токен. Введите его заново.')
    try:
        output = ctypes.string_at(result.data, result.size)
        return output.decode('utf-8') if decrypt else base64.b64encode(output).decode('ascii')
    finally:
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.LocalFree.argtypes = [ctypes.c_void_p]
        kernel.LocalFree(ctypes.cast(result.data, ctypes.c_void_p))


def read_settings():
    path = core.ROOT / 'desktop-settings.json'
    return json.loads(path.read_text(encoding='utf-8')) if path.exists() else {'routes': [], 'telegram_catalog': {}}


def write_settings(config):
    core.ROOT.mkdir(parents=True, exist_ok=True)
    core.route_configs(config)
    path = core.ROOT / 'desktop-settings.json'
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding='utf-8')
    temporary.replace(path)


class AuthUI:
    def __init__(self, emit):
        self.emit = emit

    async def show_qr(self, url):
        self.emit('qr', url)

    async def get_password(self, hint=None):
        future = asyncio.get_running_loop().create_future()
        self.emit('password', (future, hint))
        return await future


class Controller:
    def __init__(self, emit):
        self.emit = emit
        self.session = self.store = self.tg = self.bot = None
        self.cfg = {'routes': [], 'telegram_catalog': {}}
        self.max_groups = []
        self.running = False
        self.router = None

    async def initialize(self):
        core.DATA.mkdir(parents=True, exist_ok=True)
        self.store = core.Store(core.DATA / 'bridge.sqlite3')
        from sync_engine import schema
        schema(self.store.db)
        with self.store.db:
            self.store.db.execute("UPDATE sync_tasks SET state='failed',error='Программа остановилась во время отправки' WHERE state='sending'")
        self.cfg = read_settings()
        core.route_configs(self.cfg)
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))
        self.publish()

    def publish(self):
        self.emit('settings', {'routes': list(self.cfg.get('routes', [])),
                             'telegram_catalog': dict(self.cfg.get('telegram_catalog', {})),
                             'max_groups': list(self.max_groups),
                             'token_saved': bool(self.cfg.get('token_dpapi')),
                             'bot_name': self.cfg.get('bot_name', ''),
                             'max_user_id': self.cfg.get('max_user_id')})

    def stopped_only(self):
        if self.running:
            raise RuntimeError('Сначала остановите мост.')

    async def connect_telegram(self, entered_token=''):
        self.stopped_only()
        value = entered_token.strip()
        if not value:
            encrypted = self.cfg.get('token_dpapi')
            if not encrypted:
                raise RuntimeError('Введите токен Telegram-бота.')
            value = protect_token(encrypted, decrypt=True)
        candidate = core.Telegram(value, self.session)
        bot = await candidate.call('getMe')
        webhook = await candidate.call('getWebhookInfo')
        if webhook.get('url'):
            raise RuntimeError('У бота настроен webhook другого сервиса. Отключите его перед подключением.')
        if self.cfg.get('bot_id') and self.cfg['bot_id'] != bot['id'] and self.cfg.get('routes'):
            raise RuntimeError('Это другой бот. Сначала удалите существующие связки.')
        if self.cfg.get('bot_id') != bot['id']:
            self.cfg['telegram_catalog'] = {}
            self.store.set('offset', 0)
        self.tg, self.bot = candidate, bot
        self.cfg.update(token_dpapi=protect_token(value), bot_id=bot['id'], bot_name=bot.get('username', ''))
        write_settings(self.cfg)
        self.publish()
        self.emit('notice', 'Telegram подключён: @' + bot.get('username', ''))

    async def ensure_telegram(self):
        if not self.tg:
            await self.connect_telegram()

    async def scan_telegram(self):
        self.stopped_only()
        await self.ensure_telegram()
        catalog = self.cfg.setdefault('telegram_catalog', {})
        router = core.MultiBridge(self.cfg, self.store, self.tg, None, self.bot['id'], self.cfg.get('max_user_id')) if self.cfg.get('routes') else None
        for _ in range(100):
            updates = await self.tg.updates(int(self.store.get('offset', 0)), timeout=0)
            for update in updates:
                chat = update.get('message', update.get('my_chat_member', {})).get('chat', {})
                if chat.get('type') in {'group', 'supergroup'}:
                    catalog[str(chat['id'])] = chat.get('title', 'Группа')
                if router:
                    router.ingest_update(update)
                self.store.set('offset', update['update_id'] + 1)
            if len(updates) < 100:
                break
        write_settings(self.cfg)
        self.publish()
        self.emit('notice', f'Найдено групп Telegram: {len(catalog)}. Если нужной нет, отправьте в ней /bridge и обновите список.')

    def make_max(self):
        ui = AuthUI(self.emit)
        return WebClient(work_dir=str(core.DATA), session_name='max-session.db',
                         auth_flow=QrAuthFlow(ui, ui),
                         extra_config=ExtraConfig(log_level='CRITICAL', telemetry=False))

    async def scan_max(self):
        self.stopped_only()
        client = self.make_max()
        try:
            await client.connect()
            if not client.me:
                raise RuntimeError('MAX не подтвердил вход. Повторите подключение.')
            own_id = client.me.contact.id
            if self.cfg.get('max_user_id') not in (None, own_id) and self.cfg.get('routes'):
                raise RuntimeError('Выполнен вход в другой MAX-аккаунт. Удалите старые связки перед сменой аккаунта.')
            self.cfg['max_user_id'] = own_id
            self.max_groups = [(c.id, c.title or 'Группа') for c in client.chats or [] if c.type == 'CHAT']
            write_settings(self.cfg)
            self.publish()
            self.emit('notice', f'MAX подключён. Найдено групп: {len(self.max_groups)}.')
        finally:
            self.emit('qr_close', None)
            await client.close()

    async def add_pair(self, telegram_id, max_id):
        self.stopped_only()
        await self.ensure_telegram()
        max_names = dict(self.max_groups)
        if not self.cfg.get('max_user_id') or max_id not in max_names:
            raise RuntimeError('Сначала подключите MAX и выберите группу из списка.')
        tg_names = self.cfg.get('telegram_catalog', {})
        if str(telegram_id) not in tg_names:
            raise RuntimeError('Обновите группы Telegram и выберите группу из списка.')
        member = await self.tg.call('getChatMember', {'chat_id': telegram_id, 'user_id': self.bot['id']})
        if member['status'] != 'administrator':
            raise RuntimeError('Назначьте Telegram-бота администратором выбранной группы.')
        route = dict(telegram_chat_id=telegram_id, max_chat_id=max_id, linked_at=int(time.time()),
                     route_id=uuid.uuid4().hex, telegram_title=tg_names[str(telegram_id)], max_title=max_names[max_id])
        updated = dict(self.cfg, routes=[*self.cfg.get('routes', []), route])
        core.route_configs(updated)
        write_settings(updated)
        self.cfg = updated
        self.publish()
        self.emit('notice', 'Связка добавлена. Можно добавить следующую или запустить мост.')

    async def remove_pair(self, route_id):
        self.stopped_only()
        self.cfg['routes'] = [r for r in self.cfg['routes'] if core.route_key(r) != route_id]
        write_settings(self.cfg)
        with self.store.db:
            self.store.db.execute("DELETE FROM plans WHERE job IN (SELECT id FROM jobs WHERE route=?)", (route_id,))
            self.store.db.execute("UPDATE jobs SET state='cancelled',payload='{}' WHERE route=? AND state!='done'", (route_id,))
            for table in ('sync_tasks', 'sync_sources', 'sync_links', 'sync_polls', 'sync_reactors', 'sync_effects', 'sync_poll_replacements'):
                self.store.db.execute(f'DELETE FROM {table} WHERE route=?', (route_id,))
        self.publish()
        self.emit('notice', 'Связка удалена; её неотправленные сообщения отменены.')

    async def statistics(self):
        counts = {r['state']: r['n'] for r in self.store.db.execute('SELECT state, COUNT(*) n FROM jobs GROUP BY state')}
        errors = [dict(r) for r in self.store.db.execute("SELECT id,side,chat,error FROM jobs WHERE state='uncertain' ORDER BY id DESC LIMIT 100")]
        sync_errors = [dict(r) for r in self.store.db.execute("SELECT id,side,error FROM sync_tasks WHERE state='failed' ORDER BY id DESC LIMIT 100")]
        for row in sync_errors:
            row['id'] = 'S' + str(row['id'])
        errors.extend(sync_errors)
        counts['uncertain'] = counts.get('uncertain', 0) + len(sync_errors)
        counts['pending'] = counts.get('pending', 0) + self.store.db.execute("SELECT COUNT(*) FROM sync_tasks WHERE state IN ('pending','sending')").fetchone()[0]
        self.emit('stats', (counts, errors))

    async def retry(self, job_id):
        self.stopped_only()
        if str(job_id).startswith('S'):
            number = int(str(job_id)[1:])
            row = self.store.db.execute("SELECT * FROM sync_tasks WHERE id=? AND state='failed'", (number,)).fetchone()
            if not row or row['route'] not in {core.route_key(r) for r in self.cfg['routes']}:
                raise RuntimeError('Задание синхронизации или его связка не найдены.')
            with self.store.db:
                self.store.db.execute("UPDATE sync_tasks SET state='pending',due=0,error=NULL WHERE id=?", (number,))
                self.store.db.execute("UPDATE sync_effects SET peer_mid=NULL WHERE route=? AND side=? AND mid=? AND kind=? AND peer_mid='?'", (row['route'], row['side'], row['mid'], 'edit_extra' if row['kind'] == 'edit' else row['kind']))
            await self.statistics()
            return
        row = self.store.db.execute("SELECT route FROM jobs WHERE id=? AND state='uncertain'", (job_id,)).fetchone()
        if not row or row['route'] not in {core.route_key(r) for r in self.cfg['routes']}:
            raise RuntimeError('Задание отсутствует либо его связка удалена.')
        self.store.state(job_id, 'pending')
        await self.statistics()
        self.emit('notice', 'Задание возвращено в очередь. Запустите мост.')

    async def run(self):
        await self.ensure_telegram()
        core.route_configs(self.cfg)
        if not self.cfg.get('routes'):
            raise RuntimeError('Добавьте хотя бы одну связку групп.')
        for route in self.cfg['routes']:
            member = await self.tg.call('getChatMember', {'chat_id': route['telegram_chat_id'], 'user_id': self.bot['id']})
            if member['status'] != 'administrator':
                raise RuntimeError('Бот должен быть администратором: ' + route.get('telegram_title', str(route['telegram_chat_id'])))
        self.running = True
        client = self.make_max()
        mx = core.MaxAdapter(client, self.session, self.store)
        self.router = core.MultiBridge(self.cfg, self.store, self.tg, mx, self.bot['id'], self.cfg['max_user_id'])
        ready = asyncio.Event()
        invalid_account = asyncio.Event()
        auth_error = []
        tasks = []

        @client.on_message()
        async def on_message(message, _client):
            if ready.is_set() and not auth_error:
                self.router.ingest_max(message)

        @client.on_message_edit()
        async def on_edit(message, _client):
            if ready.is_set() and not auth_error:
                self.router.max_edit(message)

        @client.on_message_delete()
        async def on_delete(event, _client):
            if ready.is_set() and not auth_error:
                self.router.max_delete(event)

        @client.on_reaction_update()
        async def on_reactions(event, _client):
            if ready.is_set() and not auth_error:
                self.router.max_reactions(event)

        @client.on_start()
        async def on_start(_client):
            if not client.me or client.me.contact.id != self.cfg['max_user_id']:
                auth_error.append('Аккаунт MAX изменился. Остановите мост и проверьте вход.')
                invalid_account.set()
            ready.set()
            self.emit('qr_close', None)

        try:
            maximum = asyncio.create_task(client.start())
            waiting = asyncio.create_task(ready.wait())
            tasks = [maximum, waiting]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if maximum in done:
                await maximum
                raise RuntimeError('Соединение MAX закрыто до завершения входа.')
            if auth_error:
                raise RuntimeError(auth_error[0])
            self.emit('running', len(self.cfg['routes']))
            tasks = [maximum, asyncio.create_task(self.router.poll()), asyncio.create_task(self.router.worker()),
                     asyncio.create_task(self.router.catch_up()), asyncio.create_task(invalid_account.wait())]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                await task
            if auth_error:
                raise RuntimeError(auth_error[0])
            raise RuntimeError('Один из компонентов моста остановился. Проверьте соединение и запустите снова.')
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await client.close()
            # Cancellation can interrupt an in-flight send; retain uncertainty.
            with self.store.db:
                self.store.db.execute("UPDATE jobs SET state='uncertain',error='Остановлено во время отправки' WHERE state='sending'")
                self.store.db.execute("UPDATE sync_tasks SET state='failed',error='Остановлено во время отправки' WHERE state='sending'")
            self.running = False
            self.router = None
            self.emit('qr_close', None)
            self.emit('stopped', None)

    async def close(self):
        if self.session:
            await self.session.close()
        if self.store:
            self.store.db.close()


def friendly_error(exc):
    if isinstance(exc, core.TelegramError):
        return {401: 'Telegram отклонил токен. Проверьте его в BotFather.',
                403: 'Telegram запретил доступ. Проверьте участие и права бота в группе.',
                409: 'Этот токен уже используется другим процессом. Остановите другой экземпляр бота.',
                429: 'Telegram ограничил частоту запросов. Подождите и повторите.'}.get(exc.code, f'Ошибка Telegram API: {exc.code}.')
    if isinstance(exc, (aiohttp.ClientError, TimeoutError, ConnectionError, OSError)):
        return 'Не удалось подключиться. Проверьте интернет и доступность Telegram/MAX, затем повторите.'
    if isinstance(exc, RuntimeError):
        return str(exc)
    return f'Операция не выполнена ({type(exc).__name__}). Попробуйте ещё раз.'
