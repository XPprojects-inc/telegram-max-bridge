import asyncio
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from pymax import Message
from bridge import Bridge, Store, MaxAdapter, chunks, tg_content, MAX_BYTES


def message(**kwargs):
    return Message(id=kwargs.pop('id', 1), chat_id=kwargs.pop('chat_id', -2),
                   sender=kwargs.pop('sender', 77), time=2000000, type='USER', **kwargs)


class CoreTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'state.db'
        self.store = Store(self.path)
        self.tg = SimpleNamespace(send=AsyncMock(), download=AsyncMock(return_value=b'blob'))
        self.mx = SimpleNamespace(send=AsyncMock(), media=AsyncMock(return_value=None),
                                  client=SimpleNamespace(get_user=AsyncMock(return_value=None)))
        self.cfg = dict(telegram_chat_id=-1, max_chat_id=-2, linked_at=1000, max_user_id=77)
        self.bridge = Bridge(self.cfg, self.store, self.tg, self.mx, 88, 77)

    def tearDown(self):
        self.store.db.close()
        self.temp.cleanup()

    def tg_message(self, **extra):
        return dict(chat={'id': -1}, message_id=1, date=2000, **{'from': {'id': 5, 'first_name': 'Анна'}}, **extra)

    def rows(self):
        return self.store.db.execute('SELECT * FROM jobs ORDER BY id').fetchall()

    def test_deduplication(self):
        m = self.tg_message(text='hi')
        self.bridge.ingest_tg(m)
        self.bridge.ingest_tg(m)
        self.assertEqual(len(self.rows()), 1)

    def test_other_groups_ignored(self):
        m = self.tg_message(text='private')
        m['chat']['id'] = -99
        self.bridge.ingest_tg(m)
        self.bridge.ingest_max(message(chat_id=-99, text='private'))
        self.assertFalse(self.rows())

    def test_own_manual_message_is_copied(self):
        self.bridge.ingest_max(message(text='my ordinary message'))
        self.assertEqual(len(self.rows()), 1)

    def test_echo_before_send_response(self):
        self.store.expect_echo(-2, 123, 'Telegram · Анна\nhello')
        self.bridge.ingest_max(message(cid=123, text='Telegram · Анна\nhello'))
        self.assertFalse(self.rows())

    def test_echo_without_cid(self):
        self.store.expect_echo(-2, 123, 'Telegram · Анна\nhello')
        self.bridge.ingest_max(message(text='Telegram · Анна\nhello'))
        self.assertFalse(self.rows())

    def test_other_person_same_text_is_not_suppressed(self):
        self.store.expect_echo(-2, 123, 'hello')
        self.bridge.ingest_max(message(sender=99, text='hello'))
        self.assertEqual(len(self.rows()), 1)

    def test_telegram_bot_ignored(self):
        m = self.tg_message(text='echo')
        m['from']['id'] = 88
        self.bridge.ingest_tg(m)
        self.assertFalse(self.rows())

    def test_old_messages_and_commands_ignored(self):
        m = self.tg_message(text='old')
        m['date'] = 999
        self.bridge.ingest_tg(m)
        self.bridge.ingest_tg(self.tg_message(text='/bridge@mybot'))
        self.assertFalse(self.rows())

    def test_service_messages_ignored(self):
        self.bridge.ingest_tg(self.tg_message(new_chat_members=[{'id': 55}]))
        self.bridge.ingest_max(message())
        self.assertFalse(self.rows())

    def test_unicode_chunks_preserve_text(self):
        text = '😀А' * 5000
        parts = list(chunks(text))
        self.assertEqual(''.join(parts), text)
        self.assertTrue(all(len(p.encode('utf-16-le')) // 2 <= 3000 for p in parts))

    def test_crash_does_not_blindly_resend(self):
        self.bridge.ingest_tg(self.tg_message(text='hello'))
        self.store.state(1, 'sending')
        self.store.db.close()
        self.store = Store(self.path)
        self.assertEqual(self.rows()[0]['state'], 'uncertain')

    async def test_tg_to_max_text(self):
        self.bridge.ingest_tg(self.tg_message(text='a_b **literal**'))
        with patch('bridge.asyncio.sleep', new=AsyncMock()):
            self.assertTrue(await self.bridge.relay(self.rows()[0]))
        self.mx.send.assert_awaited_once_with(-2, 'Telegram · Анна\na_b **literal**', None)

    async def test_max_to_tg(self):
        self.bridge.ingest_max(message(sender=22, text='Привет'))
        with patch('bridge.asyncio.sleep', new=AsyncMock()):
            await self.bridge.relay(self.rows()[0])
        self.tg.send.assert_awaited_once_with(-1, 'MAX · Участник 22\nПривет', None)

    async def test_media_and_part_resume(self):
        self.bridge.ingest_tg(self.tg_message(caption='photo', photo=[{'file_id': 'photo1'}]))
        with patch('bridge.asyncio.sleep', new=AsyncMock()):
            await self.bridge.relay(self.rows()[0])
            await self.bridge.relay(self.rows()[0])
        self.assertEqual(self.mx.send.await_count, 2)
        self.tg.download.assert_awaited_once()
        self.assertEqual(self.mx.send.await_args.args[2], ('photo', 'photo.jpg', b'blob'))

    async def test_oversize_is_reported(self):
        self.bridge.ingest_tg(self.tg_message(document={'file_id': 'x', 'file_size': MAX_BYTES + 1}))
        with patch('bridge.asyncio.sleep', new=AsyncMock()):
            await self.bridge.relay(self.rows()[0])
        self.tg.download.assert_not_awaited()
        self.assertIn('не перенесено', self.mx.send.await_args.args[1])

    async def test_max_adapter_plaintext_and_early_echo(self):
        async def invoke(opcode, payload):
            body = payload['message']
            incoming = message(cid=body['cid'], text=body['text'])
            self.assertTrue(self.store.is_echo(incoming, 77))
            self.assertEqual(body['elements'], [])
            return SimpleNamespace(payload=incoming.model_dump(by_alias=True))
        client = SimpleNamespace(_app=SimpleNamespace(
            api=SimpleNamespace(messages=SimpleNamespace(_upload_attachments=AsyncMock(return_value=[]), _next_cid=lambda: 123)),
            invoke=invoke))
        adapter = MaxAdapter(client, None, self.store)
        await adapter.send(-2, '**raw** _a_ [x](url)')
        self.assertTrue(self.store.is_echo(message(text='changed', id=1), 77))

    def test_status_does_not_mutate_inflight_job(self):
        self.bridge.ingest_tg(self.tg_message(text='hello'))
        self.store.state(1, 'sending')
        observer = Store(self.path, recover=False)
        observer.db.close()
        self.assertEqual(self.rows()[0]['state'], 'sending')

    def test_echo_survives_restart(self):
        self.store.expect_echo(-2, 123, 'hello')
        self.store.sent_echo(-2, 123, 1)
        self.store.db.close()
        self.store = Store(self.path)
        self.assertTrue(self.store.is_echo(message(text='hello', cid=123), 77))

    async def test_max_photo_download_and_send(self):
        m = message(text='caption', attaches=[dict(_type='PHOTO', baseUrl='https://example.org/photo.jpg',
                    width=10, height=10, photoId=1, photoToken='test')])
        self.mx.media.return_value = ('photo', 'photo.jpg', b'jpg')
        self.bridge.ingest_max(m)
        with patch('bridge.asyncio.sleep', new=AsyncMock()):
            await self.bridge.relay(self.rows()[0])
        self.assertEqual(self.tg.send.await_count, 2)
        self.assertEqual(self.tg.send.await_args.args[2], ('photo', 'photo.jpg', b'jpg'))

    async def test_history_catchup_scope_and_dedup(self):
        self.mx.client.fetch_history = AsyncMock(side_effect=[[message(text='offline')], []])
        async def stop(_):
            raise asyncio.CancelledError
        with patch('bridge.asyncio.sleep', side_effect=stop):
            with self.assertRaises(asyncio.CancelledError):
                await self.bridge.catch_up()
        self.bridge.ingest_max(message(text='offline'))
        self.assertEqual(len(self.rows()), 1)
        self.assertEqual(self.mx.client.fetch_history.call_args.args[0], -2)


if __name__ == '__main__':
    unittest.main()
