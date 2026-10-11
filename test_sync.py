import asyncio
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import bridge as core
from pymax import Message
from sync_engine import SyncEngine, reaction_name


def tg(mid=1, text='hello', **kwargs):
    return {'message_id': mid, 'date': 2000, 'chat': {'id': -1}, 'from': {'id': 5, 'first_name': 'Анна'}, 'text': text, **kwargs}


def mx(mid=1, text='hello', **kwargs):
    return Message(id=mid, chat_id=-2, time=2000000, type='USER', sender=9, text=text, **kwargs)


def tg_poll(mid=1, counts=(0, 0)):
    message = tg(mid, '')
    message.pop('text')
    message['poll'] = {'id': 'p1', 'question': 'Выбираем?', 'options': [
        {'text': 'Первый', 'voter_count': counts[0]}, {'text': 'Второй', 'voter_count': counts[1]}],
        'total_voter_count': sum(counts), 'is_closed': False, 'allows_multiple_answers': False}
    return message


def max_poll(mid=40, counts=(0, 0), results=True):
    return mx(mid, '', attaches=[{'_type': 'POLL', 'title': 'Выбираем?', 'answers': [
        {'text': 'Первый', 'answer_id': 17}, {'text': 'Второй', 'answer_id': 23}],
        'settings': 5, 'poll_id': 100, 'version': 1, 'state': {'total': sum(counts), 'voter_preview_ids': [],
        'result': [{'answer_id': i, 'vote_count': c, 'votes': [], 'rate': 0, 'options': 0} for i, c in zip((17, 23), counts)] if results else None}}])


class SyncTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'state.db'
        self.store = core.Store(self.path)
        self.tg = SimpleNamespace(send=AsyncMock(return_value={'message_id': 50}), edit=AsyncMock(),
                                  delete=AsyncMock(), pin=AsyncMock(), call=AsyncMock(), send_poll=AsyncMock(return_value=tg_poll(50)))
        self.mx = SimpleNamespace(send=AsyncMock(return_value=mx(40)), edit=AsyncMock(), delete=AsyncMock(),
                                  pin=AsyncMock(), send_poll=AsyncMock(return_value=max_poll()),
                                  client=SimpleNamespace(get_user=AsyncMock(return_value=None), get_reactions=AsyncMock(return_value={})))
        self.b = core.Bridge(dict(telegram_chat_id=-1, max_chat_id=-2, linked_at=1000, route_id='route'), self.store, self.tg, self.mx, 88, 77)
        self.s = self.b.sync
        self.sleeps = patch('asyncio.sleep', new=AsyncMock())
        self.sleeps.start()

    def tearDown(self):
        self.sleeps.stop()
        self.store.db.close()
        self.temp.cleanup()

    def task(self, kind, side='tg', mid=1):
        return self.store.db.execute('SELECT * FROM sync_tasks WHERE kind=? AND side=? AND mid=?', (kind, side, str(mid))).fetchone()

    def job(self, side='tg', mid=1):
        return self.store.db.execute('SELECT * FROM jobs WHERE side=? AND mid=?', (side, str(mid))).fetchone()

    async def mirror(self, side='tg'):
        self.b.ingest_tg(tg()) if side == 'tg' else self.b.ingest_max(mx())
        row = self.job(side)
        await self.b.relay(row)
        self.store.state(row['id'], 'done')

    async def test_reply_to_original_uses_peer_id(self):
        await self.mirror()
        self.b.ingest_tg(tg(2, 'ответ', reply_to_message={'message_id': 1}))
        await self.b.relay(self.job(mid=2))
        self.assertEqual(self.mx.send.await_args.kwargs['reply_to'], '40')

    async def test_reply_to_copy_returns_to_original(self):
        await self.mirror()
        self.b.ingest_max(mx(3, 'ответ', link={'type': 'REPLY', 'chat_id': -2, 'message': mx(40).model_dump()}))
        await self.b.relay(self.job('max', 3))
        self.assertEqual(self.tg.send.await_args.kwargs['reply_to'], '1')

    async def test_text_edit_updates_existing_copy(self):
        await self.mirror()
        self.s.edited('tg', 1, tg(text='новый текст'))
        await self.s.apply(self.task('edit'))
        self.mx.edit.assert_awaited_once_with(-2, '40', 'Telegram · Анна\nновый текст')
        self.assertEqual(self.mx.send.await_count, 1)

    async def test_max_edit_updates_tg(self):
        await self.mirror('max')
        self.s.edited('max', 1, mx(text='изменено').model_dump(mode='json'))
        await self.s.apply(self.task('edit', 'max'))
        self.tg.edit.assert_awaited_once_with(-1, '50', 'MAX · Участник 9\nизменено')

    async def test_editing_copy_does_not_edit_persons_original(self):
        await self.mirror()
        self.s.edited('max', 40, mx(40, 'edited copy').model_dump(mode='json'))
        self.assertIsNone(self.task('edit', 'max', 40))

    async def test_queued_edit_waits_for_original(self):
        self.b.ingest_tg(tg())
        self.s.edited('tg', 1, tg(text='changed'))
        self.assertFalse(await self.s.apply(self.task('edit')))
        await self.b.relay(self.job())
        self.assertTrue(await self.s.apply(self.task('edit')))

    async def test_delete_max_original_deletes_tg_copy(self):
        await self.mirror('max')
        self.s.deleted('max', 1)
        await self.s.apply(self.task('delete', 'max'))
        self.tg.delete.assert_awaited_once_with(-1, '50')

    async def test_delete_copy_does_not_delete_original(self):
        await self.mirror()
        self.s.deleted('max', 40)
        self.assertIsNone(self.task('delete', 'max', 40))

    async def test_delete_before_send_cancels_queue(self):
        self.b.ingest_max(mx())
        self.s.deleted('max', 1)
        self.assertEqual(self.job('max')['state'], 'cancelled')
        await self.b.relay(self.job('max'))
        self.tg.send.assert_not_awaited()

    def reaction(self, user, emojis, stamp=2000):
        return {'message_reaction': {'chat': {'id': -1}, 'message_id': 1, 'date': stamp,
                 'user': {'id': user}, 'new_reaction': [{'type': 'emoji', 'emoji': e} for e in emojis]}}

    async def test_reactions_add_remove_and_no_duplicate_summary(self):
        await self.mirror()
        self.s.telegram_update(self.reaction(10, ['👍']))
        self.s.telegram_update(self.reaction(11, ['👍']))
        self.s.telegram_update(self.reaction(11, ['👍']))
        await self.s.apply(self.task('reactions'))
        self.assertIn('Реакции Telegram: 👍 2', self.mx.send.await_args.args)
        self.s.telegram_update(self.reaction(10, [], 2001))
        await self.s.apply(self.task('reactions'))
        self.mx.edit.assert_awaited_with(-2, '40', 'Реакции Telegram: 👍 1')
        self.assertEqual(self.mx.send.await_count, 2)  # original + one persistent summary

    async def test_reaction_removal_shows_none(self):
        await self.mirror()
        self.s.telegram_update(self.reaction(10, []))
        await self.s.apply(self.task('reactions'))
        self.assertEqual(self.mx.send.await_args.args[1], 'Реакции Telegram: нет')

    async def test_anonymous_reaction_counts(self):
        await self.mirror()
        self.s.telegram_update({'message_reaction_count': {'chat': {'id': -1}, 'message_id': 1,
             'reactions': [{'type': {'type': 'emoji', 'emoji': '❤️'}, 'total_count': 8}]}})
        await self.s.apply(self.task('reactions'))
        self.assertEqual(self.mx.send.await_args.args[1], 'Реакции Telegram: ❤️ 8')

    async def test_max_reactions_on_copy_go_to_original(self):
        await self.mirror()
        self.s.max_reactions(SimpleNamespace(message_id='40', counters=[SimpleNamespace(reaction='🔥', count=3)]))
        await self.s.apply(self.task('reactions', 'max', 40))
        self.tg.send.assert_awaited_once_with(-1, 'Реакции MAX: 🔥 3', reply_to='1')

    async def test_summary_timeout_is_not_blindly_retried(self):
        await self.mirror()
        self.mx.send.side_effect = TimeoutError()
        self.s.queue('reactions', 'tg', 1, {'👍': 1})
        with self.assertRaises(TimeoutError):
            await self.s.apply(self.task('reactions'))
        with self.assertRaises(RuntimeError):
            await self.s.apply(self.task('reactions'))
        self.assertEqual(self.mx.send.await_count, 2)

    async def test_foreign_poll_cloned_with_same_question_and_options(self):
        self.b.ingest_tg(tg_poll(counts=(3, 2)))
        await self.b.relay(self.job())
        spec = self.mx.send_poll.await_args.args[1]
        self.assertEqual(spec['options'], ['Первый', 'Второй'])
        self.assertEqual(spec['question'], 'Выбираем?')
        await self.s.apply(self.task('poll', 'tg', 50))
        summary = self.mx.send.await_args.args[1]
        self.assertIn('Голоса Telegram:', summary)
        self.assertIn('Первый — 0', summary)
        self.assertNotIn('Снимок результатов', summary)
        self.tg.send_poll.assert_awaited_once()
        self.assertEqual(self.s.counterpart('tg', 1), '40')
        self.assertEqual(self.s.counterpart('max', 40), '50')

    async def test_auto_poll_retry_reuses_bot_copy(self):
        self.b.ingest_tg(tg_poll())
        self.mx.send_poll.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError):
            await self.b.relay(self.job())
        self.mx.send_poll.side_effect = None
        await self.b.relay(self.job())
        self.tg.send_poll.assert_awaited_once()
        await self.s.apply(self.task('poll_notice', 'max', 40))
        self.assertIn('Голосуйте', self.tg.send.await_args.args[1])
        self.assertEqual(self.tg.send.await_args.kwargs['reply_to'], '50')

    async def test_auto_poll_ignores_old_poll_updates_and_tracks_new_votes(self):
        original = tg_poll()
        original['poll']['id'] = 'original'
        self.b.ingest_tg(original)
        await self.b.relay(self.job())
        self.s.telegram_update({'poll': tg_poll(50, (7, 1))['poll']})
        await self.s.apply(self.task('poll', 'tg', 50))
        self.assertIn('Первый — 7', self.mx.send.await_args.args[1])
        self.assertIsNone(self.store.db.execute("SELECT 1 FROM sync_polls WHERE poll_id='original'").fetchone())
        self.mx.send.reset_mock()
        await self.s.apply(self.task('poll', 'tg', 1))
        self.mx.send.assert_not_awaited()

    async def test_command_poll_is_not_recreated(self):
        self.tg.call.return_value = {'status': 'administrator'}
        self.b.ingest_tg(tg(text='/bridge_poll Вопрос | Да | Нет'))
        await self.s.apply(self.task('create_poll'))
        await self.b.relay(self.job('tg', 50))
        self.assertEqual(self.tg.send_poll.await_count, 1)

    async def test_max_poll_cloned_to_telegram(self):
        self.b.ingest_max(max_poll(1, (2, 1)))
        await self.b.relay(self.job('max'))
        self.assertEqual(self.tg.send_poll.await_args.args[1]['options'], ['Первый', 'Второй'])
        await self.s.apply(self.task('poll', 'max'))
        self.assertIn('Голоса MAX:', self.tg.send.await_args.args[1])

    async def test_bot_poll_update_refreshes_reverse_summary(self):
        self.b.ingest_max(max_poll(1))
        await self.b.relay(self.job('max'))
        self.s.telegram_update({'poll': tg_poll(50, (4, 1))['poll']})
        await self.s.apply(self.task('poll', 'tg', 50))
        text = self.mx.send.await_args.args[1]
        self.assertIn('Первый — 4', text)
        self.assertNotIn('Снимок', text)

    def test_hidden_max_results_are_not_invented(self):
        p = self.s.extract_poll('max', max_poll(1, (1, 0), results=False).model_dump(mode='json'))
        self.assertIsNone(p['counts'])

    async def test_create_poll_requires_admin(self):
        self.b.ingest_tg(tg(text='/bridge_poll Вопрос | Да | Нет'))
        self.tg.call.return_value = {'status': 'member'}
        with self.assertRaises(RuntimeError):
            await self.s.apply(self.task('create_poll'))
        self.tg.send_poll.assert_not_awaited()

    async def test_create_poll_queues_own_bot_poll_and_marks_live(self):
        self.b.ingest_tg(tg(text='/bridge_poll Вопрос | Да | Нет'))
        self.tg.call.return_value = {'status': 'administrator'}
        self.assertTrue(await self.s.apply(self.task('create_poll')))
        self.assertIsNotNone(self.job('tg', 50))
        p = self.store.db.execute("SELECT live FROM sync_polls WHERE side='tg' AND mid='50'").fetchone()
        self.assertEqual(p['live'], 1)

    async def test_pin_translation_and_echo_suppression(self):
        await self.mirror()
        self.s.observe_pin('tg', 1)
        await self.s.apply(self.task('pin'))
        self.mx.pin.assert_awaited_once_with(-2, '40')
        self.s.observe_pin('max', 40)
        self.assertIsNone(self.task('pin', 'max', 40))

    async def test_reactions_and_links_survive_restart(self):
        await self.mirror()
        self.s.telegram_update(self.reaction(1, ['👍']))
        self.store.db.close()
        self.store = core.Store(self.path)
        b = core.Bridge(self.b.cfg, self.store, self.tg, self.mx, 88, 77)
        self.assertEqual(b.sync.counterpart('tg', 1), '40')
        b.sync.telegram_update(self.reaction(2, ['👍']))
        self.assertEqual(json.loads(self.task('reactions')['body']), {'👍': 2})

    def test_reactions_other_group_ignored(self):
        update = self.reaction(1, ['👍'])
        update['message_reaction']['chat']['id'] = -999
        self.s.telegram_update(update)
        self.assertIsNone(self.task('reactions'))

    def test_custom_emoji_label_is_explicit(self):
        self.assertEqual(reaction_name({'type': 'custom_emoji', 'custom_emoji_id': '123'}), 'эмодзи 123')

    def test_old_reaction_event_does_not_overwrite_new(self):
        self.s.telegram_update(self.reaction(1, ['👍'], 2010))
        self.s.telegram_update(self.reaction(1, ['❤️'], 2000))
        self.assertEqual(json.loads(self.task('reactions')['body']), {'👍': 1})


if __name__ == '__main__':
    unittest.main()
