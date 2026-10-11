import unittest
from unittest.mock import AsyncMock, patch
import bridge as core
import test_sync as fixtures
from test_sync import tg, mx


class ImageTests(unittest.IsolatedAsyncioTestCase):
    setUp = fixtures.SyncTests.setUp
    tearDown = fixtures.SyncTests.tearDown
    job = fixtures.SyncTests.job

    async def test_photo_rejection_falls_back_to_original_file(self):
        adapter = core.Telegram('unused', None)
        adapter.call = AsyncMock(side_effect=[core.TelegramError(400, description='PHOTO_INVALID_DIMENSIONS'), {'message_id': 2}])
        await adapter.send(-1, 'caption', ('photo', 'original.png', b'original bytes'), reply_to=7)
        self.assertEqual([c.args[0] for c in adapter.call.await_args_list], ['sendPhoto', 'sendDocument'])
        fields = adapter.call.await_args.kwargs['form']._fields
        self.assertTrue(any(f[0].get('name') == 'document' and f[2] == b'original bytes' for f in fields))
        self.assertTrue(any(f[0].get('name') == 'reply_parameters' for f in fields))

    async def test_timeout_does_not_trigger_duplicate_file_send(self):
        adapter = core.Telegram('unused', None)
        adapter.call = AsyncMock(side_effect=TimeoutError())
        with self.assertRaises(TimeoutError):
            await adapter.send(-1, '', ('photo', 'a.jpg', b'image'))
        adapter.call.assert_awaited_once()

    async def test_large_photo_sent_as_file(self):
        adapter = core.Telegram('unused', None)
        adapter.call = AsyncMock(return_value={'message_id': 2})
        await adapter.send(-1, '', ('photo', 'large.jpg', b'x' * (10 * 1024 * 1024 + 1)))
        self.assertEqual(adapter.call.await_args.args[0], 'sendDocument')

    async def test_max_photo_url_download(self):
        adapter = core.MaxAdapter(None, object(), self.store)
        with patch('bridge.download', new=AsyncMock(return_value=b'image')) as download:
            result = await adapter.media(mx(), {'type': 'PHOTO', 'base_url': 'https://example.org/p.jpg'})
        self.assertEqual(result, ('photo', 'photo.jpg', b'image'))
        download.assert_awaited_once_with(adapter.http, 'https://example.org/p.jpg')

    async def test_multiple_max_images_preserve_caption_order_and_checkpoints(self):
        photos = [dict(_type='PHOTO', baseUrl=f'https://example.org/{i}.jpg', width=10,
                       height=10, photoId=i, photoToken='test') for i in (1, 2)]
        self.mx.media = AsyncMock(side_effect=[('photo', '1.jpg', b'one'), ('photo', '2.jpg', b'two')])
        self.b.ingest_max(mx(text='Подпись', attaches=photos))
        await self.b.relay(self.job('max'))
        await self.b.relay(self.job('max'))
        self.assertEqual(self.tg.send.await_count, 3)
        self.assertIn('Подпись', self.tg.send.await_args_list[0].args[1])
        self.assertEqual([c.args[2][2] for c in self.tg.send.await_args_list[1:]], [b'one', b'two'])

    async def test_download_timeout_keeps_image_queued(self):
        self.tg.download = AsyncMock(side_effect=TimeoutError())
        self.b.ingest_tg(tg(photo=[{'file_id': 'photo'}]))
        with self.assertRaises(TimeoutError):
            await self.b.relay(self.job())
        self.mx.send.assert_not_awaited()
        self.assertEqual(self.job()['state'], 'pending')
        self.tg.download.side_effect = None
        self.tg.download.return_value = b'image'
        await self.b.relay(self.job())
        self.assertEqual(self.mx.send.await_args.args[2][2], b'image')


if __name__ == '__main__':
    unittest.main()
