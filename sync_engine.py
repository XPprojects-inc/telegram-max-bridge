"""Durable message relationships and observable cross-platform metadata.

Only edits/deletes of ORIGINALS can mutate copies. Reactions and polls are
summaries, never impersonated votes/reactions. No inference of Telegram deletions.
"""
import asyncio
from collections import Counter
import hashlib
import json
import logging
import time

LOG = logging.getLogger('bridge')


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def message_id(result):
    value = result.get('message_id') if isinstance(result, dict) else getattr(result, 'id', None)
    return str(value) if isinstance(value, (int, str)) and str(value).lstrip('-').isdigit() else None


def reaction_name(reaction):
    return reaction.get('emoji') or ('эмодзи ' + reaction['custom_emoji_id'] if reaction.get('custom_emoji_id') else '⭐')


def schema(db):
    db.executescript('''
      CREATE TABLE IF NOT EXISTS sync_sources(
        route TEXT,side TEXT,mid TEXT,body TEXT,deleted INTEGER DEFAULT 0,
        PRIMARY KEY(route,side,mid));
      CREATE TABLE IF NOT EXISTS sync_links(
        route TEXT,side TEXT,mid TEXT,part INTEGER,peer_mid TEXT,kind TEXT,text TEXT,
        PRIMARY KEY(route,side,mid,part));
      CREATE INDEX IF NOT EXISTS sync_peer ON sync_links(route,side,peer_mid);
      CREATE TABLE IF NOT EXISTS sync_tasks(
        id INTEGER PRIMARY KEY,route TEXT,kind TEXT,side TEXT,mid TEXT,body TEXT,
        version INTEGER DEFAULT 1,state TEXT DEFAULT 'pending',error TEXT,
        due REAL DEFAULT 0,UNIQUE(route,kind,side,mid));
      CREATE TABLE IF NOT EXISTS sync_effects(
        route TEXT,kind TEXT,side TEXT,mid TEXT,part INTEGER,peer_mid TEXT,text TEXT,
        PRIMARY KEY(route,kind,side,mid,part));
      CREATE TABLE IF NOT EXISTS sync_reactors(
        route TEXT,mid TEXT,actor TEXT,reactions TEXT,stamp INTEGER,
        PRIMARY KEY(route,mid,actor));
      CREATE TABLE IF NOT EXISTS sync_polls(
        route TEXT,side TEXT,mid TEXT,poll_id TEXT,body TEXT,live INTEGER,updated REAL,
        PRIMARY KEY(route,side,mid));
      CREATE TABLE IF NOT EXISTS sync_poll_replacements(
        route TEXT,mid TEXT,bot_mid TEXT,body TEXT,PRIMARY KEY(route,mid));
    ''')


class SyncEngine:
    def __init__(self, bridge):
        self.b = bridge
        self.store = bridge.store
        self.db = bridge.store.db
        self.route = bridge.route
        self.lock = asyncio.Lock()
        schema(self.db)

    def chat(self, side):
        return self.b.cfg['telegram_chat_id' if side == 'tg' else 'max_chat_id']

    def adapter(self, side):
        return self.b.tg if side == 'tg' else self.b.mx

    @staticmethod
    def opposite(side):
        return 'max' if side == 'tg' else 'tg'

    def queue(self, kind, side, mid, body):
        mid, body = str(mid), encode(body)
        with self.db:
            self.db.execute('''INSERT INTO sync_tasks(route,kind,side,mid,body) VALUES(?,?,?,?,?)
                ON CONFLICT(route,kind,side,mid) DO UPDATE SET body=excluded.body,version=version+1,
                state='pending',error=NULL,due=0 WHERE body!=excluded.body''', (self.route, kind, side, mid, body))

    def source(self, side, mid):
        return self.db.execute('SELECT * FROM sync_sources WHERE route=? AND side=? AND mid=?',
                               (self.route, side, str(mid))).fetchone()

    def is_deleted(self, side, mid):
        row = self.source(side, mid)
        return bool(row and row['deleted'])

    def counterpart(self, side, mid):
        """Resolve either an original or any of its mirrored parts, scoped to route."""
        if side == 'tg':
            replacement = self.db.execute('SELECT bot_mid FROM sync_poll_replacements WHERE route=? AND mid=?',
                                          (self.route, str(mid))).fetchone()
            if replacement:
                mid = replacement['bot_mid']
        row = self.db.execute('SELECT peer_mid FROM sync_links WHERE route=? AND side=? AND mid=? ORDER BY part LIMIT 1',
                              (self.route, side, str(mid))).fetchone()
        if row:
            return row['peer_mid']
        row = self.db.execute('SELECT mid FROM sync_links WHERE route=? AND side=? AND peer_mid=? LIMIT 1',
                              (self.route, self.opposite(side), str(mid))).fetchone()
        return row['mid'] if row else None

    def remember(self, side, mid, body):
        if self.is_copy(side, mid):
            return
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO sync_sources(route,side,mid,body) VALUES(?,?,?,?)',
                            (self.route, side, str(mid), encode(body)))
        poll = self.extract_poll(side, body)
        if poll:
            self.register_poll(side, mid, poll, side == 'max' or body.get('from', {}).get('id') == self.b.bot_id)

    def is_copy(self, side, mid):
        return self.db.execute('SELECT 1 FROM sync_links WHERE route=? AND side=? AND peer_mid=?',
                               (self.route, self.opposite(side), str(mid))).fetchone() is not None

    def map_result(self, row, part, result, kind, text):
        mid = message_id(result)
        if not mid:
            return
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO sync_links VALUES(?,?,?,?,?,?,?)',
                            (self.route, row['side'], str(row['mid']), part, mid, kind, text))

    def reply_for(self, row):
        body = json.loads(row['payload'])
        if row['side'] == 'tg':
            reply = body.get('reply_to_message', {}).get('message_id')
        else:
            link = body.get('link') or {}
            reply = link.get('message', {}).get('id') if link.get('type') == 'REPLY' else None
        return self.counterpart(row['side'], reply) if reply is not None else None

    def command(self, message):
        if message.get('chat', {}).get('id') != self.chat('tg'):
            return False
        if message.get('date', 0) < self.b.cfg['linked_at']:
            return False
        command = message.get('text', '').split(' ', 1)[0].split('@')[0]
        if command != '/bridge_poll':
            return False
        self.queue('create_poll', 'tg', message['message_id'], message)
        return True

    @staticmethod
    def extract_poll(side, body):
        if side == 'tg':
            p = body.get('poll')
            if not p:
                return None
            return dict(id=str(p['id']), question=p['question'], options=[o['text'] for o in p['options']],
                        counts=[o.get('voter_count', 0) for o in p['options']], total=p.get('total_voter_count', 0),
                        multiple=p.get('allows_multiple_answers', False), closed=p.get('is_closed', False))
        for p in body.get('attaches', []):
            if p.get('type', p.get('_type')) == 'POLL':
                answers = p.get('answers', [])
                state = p.get('state') or {}
                results = {str(r['answer_id']): r['vote_count'] for r in state.get('result') or []}
                available = state.get('result') is not None or not state.get('total', 0)
                return dict(id=str(p.get('poll_id', '')), question=p['title'], options=[o['text'] for o in answers],
                            counts=[results.get(str(o.get('answer_id')), 0) for o in answers] if available else None,
                            total=state.get('total', 0), multiple=bool(p.get('settings', 0) & 2),
                            closed=bool(p.get('settings', 0) & 8))
        return None

    def register_poll(self, side, mid, poll, live):
        if side == 'tg' and self.db.execute('SELECT 1 FROM sync_poll_replacements WHERE route=? AND mid=?',
                                           (self.route, str(mid))).fetchone():
            return
        prior = self.db.execute('SELECT live FROM sync_polls WHERE route=? AND side=? AND mid=?',
                                (self.route, side, str(mid))).fetchone()
        live = live or bool(prior and prior['live'])
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO sync_polls VALUES(?,?,?,?,?,?,?)',
                            (self.route, side, str(mid), poll['id'], encode(poll), int(live), time.time()))
        self.queue('poll', side, mid, {**poll, 'live': live})

    async def relay_poll(self, row, body):
        poll = self.extract_poll(row['side'], body)
        if self.store.part_done(row['id'], 0):
            return True
        mapped_row = row
        if row['side'] == 'tg' and body.get('from', {}).get('id') != self.b.bot_id:
            replacement = self.db.execute('SELECT * FROM sync_poll_replacements WHERE route=? AND mid=?',
                                          (self.route, str(row['mid']))).fetchone()
            if replacement:
                bot_body = json.loads(replacement['body'])
            else:
                # Persist the local bot poll before sending to MAX; a retry resumes here.
                self.store.state(row['id'], 'sending')
                bot_body = await self.b.tg.send_poll(self.chat('tg'), poll, reply_to=row['mid'])
                bot_body['from'] = {'id': self.b.bot_id, 'is_bot': True}
                bot_mid = message_id(bot_body)
                if not bot_mid:
                    raise RuntimeError('Telegram не вернул ID нового опроса.')
                with self.db:
                    self.db.execute('INSERT INTO sync_poll_replacements VALUES(?,?,?,?)',
                                    (self.route, str(row['mid']), bot_mid, encode(bot_body)))
                    self.db.execute("DELETE FROM sync_polls WHERE route=? AND side='tg' AND mid=?", (self.route, str(row['mid'])))
                    self.db.execute("UPDATE sync_tasks SET state='done' WHERE route=? AND kind='poll' AND side='tg' AND mid=?", (self.route, str(row['mid'])))
                await asyncio.sleep(3.1)
            self.remember('tg', bot_body['message_id'], bot_body)
            mapped_row = dict(row)
            mapped_row['mid'] = str(bot_body['message_id'])
            poll = self.extract_poll('tg', bot_body)
        target_side = self.opposite(row['side'])
        self.store.state(row['id'], 'sending')
        result = await self.adapter(target_side).send_poll(self.chat(target_side), poll, reply_to=self.reply_for(row))
        self.map_result(mapped_row, 0, result, 'poll', poll['question'])
        target_mid = message_id(result)
        if target_mid:
            if mapped_row is not row:
                self.queue('poll_notice', target_side, target_mid, {})
            result_body = result if isinstance(result, dict) else result.model_dump(mode='json')
            peer_poll = self.extract_poll(target_side, result_body)
            if peer_poll:
                self.register_poll(target_side, target_mid, peer_poll, True)
        self.store.save_part(row['id'], 0)
        self.store.state(row['id'], 'pending')
        return True

    def edited(self, side, mid, body):
        # Never edit a person's original in response to changes to a bridge copy.
        source = self.source(side, mid)
        if not source or source['deleted'] or self.is_copy(side, mid):
            return
        poll = self.extract_poll(side, body)
        if poll:
            self.register_poll(side, mid, poll, side == 'max')
            return
        old = json.loads(source['body'])
        keys = ('text', 'caption') if side == 'tg' else ('text',)
        if any(body.get(key, '') != old.get(key, '') for key in keys):
            with self.db:
                self.db.execute('UPDATE sync_sources SET body=? WHERE route=? AND side=? AND mid=?',
                                (encode(body), self.route, side, str(mid)))
            self.queue('edit', side, mid, body)

    def deleted(self, side, mid):
        if not self.source(side, mid) or self.is_copy(side, mid):
            return
        with self.db:
            self.db.execute('UPDATE sync_sources SET deleted=1 WHERE route=? AND side=? AND mid=?',
                            (self.route, side, str(mid)))
            self.db.execute("UPDATE jobs SET state='cancelled' WHERE route=? AND side=? AND mid=? AND state='pending'",
                            (self.route, side, str(mid)))
        self.queue('delete', side, mid, {})

    def telegram_update(self, update):
        edited = update.get('edited_message')
        if edited and edited.get('chat', {}).get('id') == self.chat('tg'):
            self.edited('tg', edited['message_id'], edited)
        reaction = update.get('message_reaction')
        if reaction and reaction['chat']['id'] == self.chat('tg'):
            actor = 'user:' + str(reaction['user']['id']) if reaction.get('user') else 'chat:' + str(reaction.get('actor_chat', {}).get('id'))
            mid = str(reaction['message_id'])
            with self.db:
                self.db.execute('''INSERT INTO sync_reactors VALUES(?,?,?,?,?)
                    ON CONFLICT(route,mid,actor) DO UPDATE SET reactions=excluded.reactions,stamp=excluded.stamp
                    WHERE excluded.stamp>=stamp''', (self.route, mid, actor,
                    encode([reaction_name(r) for r in reaction.get('new_reaction', [])]), reaction.get('date', 0)))
            counts = Counter()
            for row in self.db.execute('SELECT reactions FROM sync_reactors WHERE route=? AND mid=?', (self.route, mid)):
                counts.update(json.loads(row['reactions']))
            self.queue('reactions', 'tg', mid, dict(counts))
        reaction = update.get('message_reaction_count')
        if reaction and reaction['chat']['id'] == self.chat('tg'):
            self.queue('reactions', 'tg', reaction['message_id'], {reaction_name(r['type']): r['total_count'] for r in reaction.get('reactions', [])})
        poll = update.get('poll')
        if poll:
            rows = self.db.execute("SELECT mid,live FROM sync_polls WHERE route=? AND side='tg' AND poll_id=?", (self.route, str(poll['id']))).fetchall()
            for row in rows:
                # A stopped foreign poll gives a final snapshot, not ongoing live access.
                self.register_poll('tg', row['mid'], self.extract_poll('tg', {'poll': poll}), bool(row['live']))

    def max_reactions(self, event):
        if event.counters is not None:
            self.queue('reactions', 'max', event.message_id, {c.reaction: c.count for c in event.counters})
        else:
            self.queue('fetch_reactions', 'max', event.message_id, {'version': time.time()})

    async def summary(self, kind, side, mid, text):
        from bridge import chunks
        peer = self.counterpart(side, mid)
        if peer is None:
            return False
        target = self.opposite(side)
        adapter = self.adapter(target)
        text_parts = list(chunks(text))
        for index, part in enumerate(text_parts):
            effect = self.db.execute('SELECT * FROM sync_effects WHERE route=? AND kind=? AND side=? AND mid=? AND part=?',
                                     (self.route, kind, side, str(mid), index)).fetchone()
            if effect and effect['peer_mid'] == '?':
                raise RuntimeError('Отправка сводки не подтверждена; проверьте группу перед повтором.')
            if effect and effect['peer_mid']:
                if effect['text'] != part:
                    await adapter.edit(self.chat(target), effect['peer_mid'], part)
            else:
                with self.db:
                    self.db.execute('INSERT OR REPLACE INTO sync_effects VALUES(?,?,?,?,?,?,?)',
                                    (self.route, kind, side, str(mid), index, '?', part))
                result = await adapter.send(self.chat(target), part, reply_to=peer)
                peer_mid = message_id(result)
                if not peer_mid:
                    raise RuntimeError('Платформа не вернула ID сводки.')
                with self.db:
                    self.db.execute('UPDATE sync_effects SET peer_mid=? WHERE route=? AND kind=? AND side=? AND mid=? AND part=?',
                                    (peer_mid, self.route, kind, side, str(mid), index))
            with self.db:
                self.db.execute('UPDATE sync_effects SET text=? WHERE route=? AND kind=? AND side=? AND mid=? AND part=?',
                                (part, self.route, kind, side, str(mid), index))
            await asyncio.sleep(1.1 if target == 'max' else 3.1)
        await self.clear_extra(kind, side, mid, len(text_parts))
        return True

    async def clear_extra(self, kind, side, mid, start=0):
        target = self.opposite(side)
        for old in self.db.execute('SELECT * FROM sync_effects WHERE route=? AND kind=? AND side=? AND mid=? AND part>=?',
                                   (self.route, kind, side, str(mid), start)).fetchall():
            if old['peer_mid'] not in (None, '?') and old['text'] != '[Продолжение удалено]':
                await self.adapter(target).edit(self.chat(target), old['peer_mid'], '[Продолжение удалено]')
                with self.db:
                    self.db.execute('UPDATE sync_effects SET text=? WHERE route=? AND kind=? AND side=? AND mid=? AND part=?',
                                    ('[Продолжение удалено]', self.route, kind, side, str(mid), old['part']))

    async def apply(self, task):
        from bridge import chunks
        kind, side, mid = task['kind'], task['side'], task['mid']
        body = json.loads(task['body'])
        target = self.opposite(side)
        adapter = self.adapter(target)
        peer = self.counterpart(side, mid)
        if kind == 'create_poll':
            user = body.get('from', {}).get('id')
            member = await self.b.tg.call('getChatMember', {'chat_id': self.chat('tg'), 'user_id': user})
            if member.get('status') not in ('creator', 'administrator'):
                raise RuntimeError('/bridge_poll доступна администраторам группы.')
            parts = [p.strip() for p in body.get('text', '').partition(' ')[2].split('|')]
            if len(parts) < 3 or len(parts) > 11 or not all(parts) or len(parts[0]) > 300 or any(len(x) > 100 for x in parts[1:]):
                raise RuntimeError('Формат: /bridge_poll Вопрос | Ответ 1 | Ответ 2 (2–10 вариантов).')
            # Sending is not idempotent: crash or timeout must be inspected manually.
            with self.db:
                self.db.execute("UPDATE sync_tasks SET state='sending' WHERE id=?", (task['id'],))
            result = await self.b.tg.send_poll(self.chat('tg'), {'question': parts[0], 'options': parts[1:]})
            result.setdefault('chat', {'id': self.chat('tg')})
            result['from'] = {'id': self.b.bot_id, 'is_bot': True}
            self.remember('tg', result['message_id'], result)
            poll = self.extract_poll('tg', result)
            self.register_poll('tg', result['message_id'], poll, True)
            self.store.add('tg', self.chat('tg'), result['message_id'], result, self.route)
            return True
        if self.is_deleted(side, mid) and kind != 'delete':
            return True
        if peer is None:
            if kind == 'delete':
                return True
            # Wait while the source is queued; ignore unrelated pre-bridge content.
            queued = self.db.execute("SELECT 1 FROM jobs WHERE route=? AND side=? AND mid=? AND state IN ('pending','sending','uncertain')", (self.route, side, mid)).fetchone()
            return False if queued else True
        if kind == 'reactions':
            label = 'Telegram' if side == 'tg' else 'MAX'
            values = ', '.join(f'{emoji} {count}' for emoji, count in sorted(body.items()) if count > 0) or 'нет'
            return await self.summary(kind, side, mid, f'Реакции {label}: {values}')
        if kind == 'poll_notice':
            return await self.summary(kind, side, mid,
                'Опрос связан с MAX. Голосуйте в этом опросе от бота: его результаты передаются в MAX. '
                'Голоса из исходного опроса не переносятся. На двух платформах голосование отдельное.')
        if kind == 'poll':
            if side == 'tg' and self.db.execute('SELECT 1 FROM sync_poll_replacements WHERE route=? AND mid=?',
                                               (self.route, mid)).fetchone():
                return True
            label = 'Telegram' if side == 'tg' else 'MAX'
            lines = [f'Голоса {label}:']
            counts = body.get('counts')
            for i, option in enumerate(body['options']):
                lines.append(f'{i + 1}. {option} — {counts[i] if counts is not None and i < len(counts) else "скрыто"}')
            lines.append(f'Проголосовало: {body.get("total", 0)}')
            if body.get('closed'):
                lines.append('Опрос закрыт на исходной платформе.')
            if side == 'tg' and not body.get('live'):
                lines.append('Снимок результатов: Telegram не предоставляет боту живые результаты этого опроса.')
            return await self.summary(kind, side, mid, '\n'.join(lines))
        if kind == 'fetch_reactions':
            info = await self.b.mx.client.get_reactions(self.chat('max'), [int(mid)])
            reaction = (info or {}).get(str(mid))
            if reaction:
                self.queue('reactions', 'max', mid, {c.reaction: c.count for c in reaction.counters})
            return True
        if kind == 'edit':
            links = self.db.execute("SELECT * FROM sync_links WHERE route=? AND side=? AND mid=? AND kind='text' ORDER BY part", (self.route, side, mid)).fetchall()
            if not links:
                return True
            heading = links[0]['text'].split('\n', 1)[0] + '\n'
            text = body.get('text') or body.get('caption') or '[Текст удалён]'
            new_parts = list(chunks(heading + text))
            for i, part in enumerate(new_parts):
                if i < len(links):
                    link = links[i]
                    if link['text'] != part:
                        await adapter.edit(self.chat(target), link['peer_mid'], part)
                        with self.db:
                            self.db.execute('UPDATE sync_links SET text=? WHERE route=? AND side=? AND mid=? AND part=?',
                                            (part, self.route, side, mid, link['part']))
                else:
                    # Additional text is a durable summary so retry cannot blindly duplicate it.
                    await self.summary('edit_extra', side, mid, '\n'.join(new_parts[len(links):]))
                    break
            for link in links[len(new_parts):]:
                if link['text'] != '[Продолжение удалено]':
                    await adapter.edit(self.chat(target), link['peer_mid'], '[Продолжение удалено]')
                    with self.db:
                        self.db.execute('UPDATE sync_links SET text=? WHERE route=? AND side=? AND mid=? AND part=?',
                                        ('[Продолжение удалено]', self.route, side, mid, link['part']))
            if len(new_parts) <= len(links):
                await self.clear_extra('edit_extra', side, mid)
            return True
        if kind == 'delete':
            links = self.db.execute('SELECT * FROM sync_links WHERE route=? AND side=? AND mid=?', (self.route, side, mid)).fetchall()
            endpoints = [(side, mid)] + [(target, link['peer_mid']) for link in links]
            for endpoint_side, endpoint_mid in endpoints:
                effects = self.db.execute('SELECT * FROM sync_effects WHERE route=? AND side=? AND mid=?',
                                          (self.route, endpoint_side, endpoint_mid)).fetchall()
                for effect in effects:
                    if effect['peer_mid'] not in (None, '?'):
                        await self.adapter(self.opposite(endpoint_side)).delete(self.chat(self.opposite(endpoint_side)), effect['peer_mid'])
                    with self.db:
                        self.db.execute('DELETE FROM sync_effects WHERE route=? AND side=? AND mid=? AND kind=? AND part=?',
                                        (self.route, endpoint_side, endpoint_mid, effect['kind'], effect['part']))
                with self.db:
                    self.db.execute('DELETE FROM sync_polls WHERE route=? AND side=? AND mid=?', (self.route, endpoint_side, endpoint_mid))
            for link in links:
                await adapter.delete(self.chat(target), link['peer_mid'])
                with self.db:
                    self.db.execute('DELETE FROM sync_links WHERE route=? AND side=? AND mid=? AND part=?', (self.route, side, mid, link['part']))
            return True
        if kind == 'pin':
            expected = self.store.get('pin:' + self.route + ':' + target)
            if expected != str(peer):
                await adapter.pin(self.chat(target), peer)
                self.store.set('pin:' + self.route + ':' + target, peer)
            return True
        return True

    async def worker(self):
        while True:
            task = self.db.execute("SELECT * FROM sync_tasks WHERE route=? AND state='pending' AND due<=? ORDER BY id LIMIT 1", (self.route, time.time())).fetchone()
            if not task:
                await asyncio.sleep(.5)
                continue
            try:
                async with self.lock:
                    complete = await self.apply(task)
                with self.db:
                    self.db.execute('UPDATE sync_tasks SET state=?,due=? WHERE id=? AND version=?',
                                    ('done' if complete else 'pending', time.time() + 2, task['id'], task['version']))
            except Exception as exc:
                error = str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__
                with self.db:
                    self.db.execute("UPDATE sync_tasks SET state='failed',error=? WHERE id=? AND version=?", (error, task['id'], task['version']))
                LOG.error('Синхронизация S%s требует проверки: %s', task['id'], error)
            await asyncio.sleep(1)

    def observe_pin(self, side, mid):
        key = 'pin:' + self.route + ':' + side
        before = self.store.get(key)
        current = str(mid) if mid is not None else ''
        if before != current:
            self.store.set(key, current)
            if mid is not None and self.counterpart(side, mid):
                self.queue('pin', side, mid, {'change': time.time()})

    async def monitor(self):
        while True:
            # Pin snapshots expose the current pin, not Telegram's entire pin stack.
            for side in ('tg', 'max'):
                try:
                    if side == 'tg':
                        chat = await self.b.tg.call('getChat', {'chat_id': self.chat(side)})
                        mid = (chat.get('pinned_message') or {}).get('message_id')
                    else:
                        chat = await self.b.mx.client.get_chat(self.chat(side))
                        mid = chat.pinned_message.id if chat and chat.pinned_message else None
                    self.observe_pin(side, mid)
                except Exception as exc:
                    LOG.warning('Проверка закрепления %s: %s', side, type(exc).__name__)
            polls = self.db.execute("SELECT mid FROM sync_polls WHERE route=? AND side='max' ORDER BY updated", (self.route,)).fetchall()
            for row in polls:
                try:
                    msg = await self.b.mx.client.get_message(self.chat('max'), int(row['mid']))
                    if msg:
                        poll = self.extract_poll('max', msg.model_dump(mode='json'))
                        if poll:
                            self.register_poll('max', row['mid'], poll, True)
                except Exception as exc:
                    LOG.warning('Проверка опроса MAX: %s', type(exc).__name__)
                await asyncio.sleep(.5)
            await asyncio.sleep(30)
