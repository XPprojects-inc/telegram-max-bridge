import asyncio
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import bridge as core
import controller
from pymax import Message


class MultiTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = core.Store(Path(self.temp.name) / 'test.db')
        self.cfg = {'max_user_id': 77, 'routes': [
            dict(telegram_chat_id=-1, max_chat_id=-2, linked_at=1000, route_id='pair-a'),
            dict(telegram_chat_id=-3, max_chat_id=-4, linked_at=1000, route_id='pair-b')]}
        self.tg = SimpleNamespace(send=AsyncMock(), updates=AsyncMock())
        self.mx = SimpleNamespace(send=AsyncMock(), client=SimpleNamespace(get_user=AsyncMock(return_value=None)))
        self.router = core.MultiBridge(self.cfg, self.store, self.tg, self.mx, 88, 77)

    def tearDown(self):
        self.store.db.close()
        self.temp.cleanup()

    def tg_message(self, chat):
        return {'chat': {'id': chat, 'type': 'supergroup', 'title': str(chat)}, 'message_id': 1,
                'date': 2000, 'text': 'hello', 'from': {'id': 5, 'first_name': 'Author'}}

    def max_message(self, chat):
        return Message(id=2, chat_id=chat, sender=55, time=2000000, type='USER', text='hello')

    def rows(self):
        return self.store.db.execute('SELECT * FROM jobs ORDER BY id').fetchall()

    async def test_two_pairs_both_directions_never_cross(self):
        for chat in (-1, -3):
            self.router.ingest_tg(self.tg_message(chat))
        for chat in (-2, -4):
            self.router.ingest_max(self.max_message(chat))
        with patch('bridge.asyncio.sleep', new=AsyncMock()):
            for row in self.rows():
                route = next(r for r in self.router.routes if r.route == row['route'])
                await route.relay(row)
        self.assertEqual([c.args[0] for c in self.mx.send.await_args_list], [-2, -4])
        self.assertEqual([c.args[0] for c in self.tg.send.await_args_list], [-1, -3])

    def test_same_message_ids_are_scoped_to_chat(self):
        self.router.ingest_tg(self.tg_message(-1))
        self.router.ingest_tg(self.tg_message(-3))
        self.router.ingest_tg(self.tg_message(-1))
        self.assertEqual(len(self.rows()), 2)

    async def test_wrong_worker_cannot_relay_foreign_job(self):
        self.router.ingest_tg(self.tg_message(-3))
        with self.assertRaises(RuntimeError):
            await self.router.routes[0].relay(self.rows()[0])
        self.mx.send.assert_not_awaited()

    def test_reject_shared_telegram_group(self):
        self.cfg['routes'][1]['telegram_chat_id'] = -1
        with self.assertRaises(RuntimeError):
            core.route_configs(self.cfg)

    def test_reject_shared_max_group(self):
        self.cfg['routes'][1]['max_chat_id'] = -2
        with self.assertRaises(RuntimeError):
            core.route_configs(self.cfg)

    def test_unpaired_groups_ignored(self):
        self.router.ingest_tg(self.tg_message(-99))
        self.router.ingest_max(self.max_message(-98))
        self.assertFalse(self.rows())

    async def test_one_update_consumer_dispatches_all_pairs(self):
        self.tg.updates.side_effect = [[{'update_id': 6, 'message': self.tg_message(-1)},
                                       {'update_id': 7, 'message': self.tg_message(-3)}], asyncio.CancelledError()]
        with self.assertRaises(asyncio.CancelledError):
            await self.router.poll()
        self.assertEqual([r['route'] for r in self.rows()], ['pair-a', 'pair-b'])
        self.assertEqual(self.store.get('offset'), '8')

    def test_migrate_original_single_pair_queue(self):
        self.store.add('tg', -1, 9, self.tg_message(-1))
        legacy = dict(telegram_chat_id=-1, max_chat_id=-2, linked_at=1000)
        router = core.MultiBridge(legacy, self.store, self.tg, self.mx, 88, 77)
        self.assertEqual(self.rows()[0]['route'], router.routes[0].route)

    def test_new_pair_does_not_inherit_old_queue(self):
        self.router.ingest_tg(self.tg_message(-1))
        self.cfg['routes'][0]['route_id'] = 'new-pair'
        new = core.MultiBridge(self.cfg, self.store, self.tg, self.mx, 88, 77)
        self.assertNotEqual(new.routes[0].route, self.rows()[0]['route'])

    async def test_remove_pair_cancels_only_its_queue(self):
        for chat in (-1, -3):
            self.router.ingest_tg(self.tg_message(chat))
        backend = controller.Controller(lambda *_: None)
        backend.store, backend.cfg = self.store, self.cfg
        with patch('controller.write_settings'):
            await backend.remove_pair('pair-a')
        self.assertEqual([r['state'] for r in self.rows()], ['cancelled', 'pending'])
        self.assertEqual(len(backend.cfg['routes']), 1)

    async def test_discovery_does_not_discard_linked_messages(self):
        backend = controller.Controller(lambda *_: None)
        backend.store, backend.cfg, backend.tg, backend.bot = self.store, self.cfg, self.tg, {'id': 88}
        self.tg.updates.return_value = [{'update_id': 42, 'message': self.tg_message(-3)}]
        with patch('controller.write_settings'):
            await backend.scan_telegram()
        self.assertEqual(self.rows()[0]['route'], 'pair-b')
        self.assertEqual(self.store.get('offset'), '43')

    async def test_running_configuration_is_locked(self):
        backend = controller.Controller(lambda *_: None)
        backend.running = True
        with self.assertRaises(RuntimeError):
            await backend.remove_pair('pair-a')


if __name__ == '__main__':
    unittest.main()
