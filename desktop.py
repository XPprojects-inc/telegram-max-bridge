"""Standalone Windows GUI. Network activity stays off the Tk main thread."""
import asyncio
import io
import json
import logging
import os
from pathlib import Path
import queue
import sys
import threading
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

# PyMax's logging setup requires a stream even in a windowed executable.
if sys.stderr is None:
    sys.stderr = open(os.devnull, 'w', encoding='utf-8')
if sys.stdout is None:
    sys.stdout = open(os.devnull, 'w', encoding='utf-8')

from PIL import ImageTk
import qrcode

import bridge as core
from controller import Controller, friendly_error, protect_token

VERSION = '3.1.0'


class EventLog(logging.Handler):
    def __init__(self, events):
        super().__init__()
        self.events = events

    def emit(self, record):
        self.events.put(('log', self.format(record)))


class App:
    def __init__(self, root, start_backend=True):
        self.root = root
        self.events = queue.Queue()
        self.loop = asyncio.new_event_loop()
        self.controller = Controller(lambda event, value: self.events.put((event, value)))
        self.active_task = None
        self.busy = False
        self.closing = False
        self.qr_window = None
        self.password_window = None
        self.qr_image = None
        self.settings = {'routes': [], 'telegram_catalog': {}, 'max_groups': []}
        self.tg_ids = []
        self.max_ids = []
        self.controls = []
        self._build()
        self.log_handler = EventLog(self.events)
        self.log_handler.setFormatter(logging.Formatter('%(asctime)s  %(message)s', '%H:%M:%S'))
        core.LOG.addHandler(self.log_handler)
        core.LOG.setLevel(logging.INFO)
        if start_backend:
            self.thread = threading.Thread(target=self._loop_thread, daemon=True)
            self.thread.start()
            self.submit(self.controller.initialize, title='Загрузка настроек…')
        self.root.after(100, self.drain)
        self.root.after(3000, self.stats_tick)
        self.root.protocol('WM_DELETE_WINDOW', self.close)

    def _loop_thread(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def _build(self):
        self.root.title('Telegram ↔ MAX · Мост групп')
        self.root.geometry(f"980x{min(950, self.root.winfo_screenheight() - 100)}")
        self.root.minsize(880, 620)
        self.root.configure(bg='#F1F5F9')
        self.root.option_add('*Font', ('Segoe UI', 10))
        style = ttk.Style(self.root)
        style.theme_use('clam')
        style.configure('.', font=('Segoe UI', 10), background='#F1F5F9', foreground='#0F172A')
        style.configure('TButton', padding=(12, 9))
        style.configure('Accent.TButton', background='#2563EB', foreground='white', font=('Segoe UI', 10, 'bold'))
        style.map('Accent.TButton', background=[('disabled', '#64748B'), ('active', '#1D4ED8')])
        style.configure('Card.TFrame', background='white')
        style.configure('Card.TLabel', background='white')
        style.configure('Title.TLabel', font=('Segoe UI', 23, 'bold'))
        style.configure('Section.TLabel', font=('Segoe UI', 12, 'bold'), background='white')
        style.configure('Hint.TLabel', foreground='#475569', background='white')
        style.configure('Treeview', rowheight=32, background='white', fieldbackground='white')
        style.configure('Treeview.Heading', font=('Segoe UI', 10, 'bold'), padding=7)
        shell = ttk.Frame(self.root)
        shell.pack(fill='both', expand=True)
        canvas = tk.Canvas(shell, bg='#F1F5F9', highlightthickness=0)
        scrollbar = ttk.Scrollbar(shell, orient='vertical', command=canvas.yview)
        scrollbar.pack(side='right', fill='y')
        canvas.pack(side='left', fill='both', expand=True)
        canvas.configure(yscrollcommand=scrollbar.set)
        outer = ttk.Frame(canvas, padding=24)
        item = canvas.create_window(0, 0, anchor='nw', window=outer)
        def layout(_=None):
            canvas.itemconfigure(item, width=canvas.winfo_width())
            canvas.configure(scrollregion=canvas.bbox('all'))
        outer.bind('<Configure>', layout)
        canvas.bind('<Configure>', layout)
        def wheel(event):
            if event.widget.winfo_toplevel() == self.root and not isinstance(event.widget, (ttk.Combobox, ttk.Treeview, tk.Text)):
                canvas.yview_scroll(int(-event.delta / 120), 'units')
        self.root.bind_all('<MouseWheel>', wheel, add='+')
        ttk.Label(outer, text='Telegram ↔ MAX', style='Title.TLabel').pack(anchor='w')
        ttk.Label(outer, text='Независимые связки групп · сообщения в обе стороны', foreground='#475569').pack(anchor='w', pady=(4, 16))

        account = ttk.Frame(outer, style='Card.TFrame', padding=16)
        account.pack(fill='x', pady=(0, 12))
        account.columnconfigure(1, weight=1)
        ttk.Label(account, text='1. Подключите аккаунты', style='Section.TLabel').grid(row=0, column=0, columnspan=3, sticky='w', pady=(0, 10))
        ttk.Label(account, text='Токен Telegram-бота', style='Card.TLabel').grid(row=1, column=0, sticky='w', padx=(0, 12))
        self.token = tk.StringVar()
        self.token_entry = ttk.Entry(account, textvariable=self.token, show='•')
        self.token_entry.grid(row=1, column=1, sticky='ew', padx=(0, 12))
        self.controls.append(self.token_entry)
        self.button(account, 'Подключить Telegram', self.connect_tg).grid(row=1, column=2, sticky='ew')
        self.account_text = tk.StringVar(value='Токен вводится один раз. Python и приложение MAX на ПК не нужны.')
        ttk.Label(account, textvariable=self.account_text, style='Hint.TLabel').grid(row=2, column=0, columnspan=3, sticky='w', pady=(8, 12))
        self.button(account, 'Обновить группы Telegram', lambda: self.submit(self.controller.scan_telegram, title='Поиск групп Telegram…')).grid(row=3, column=0, columnspan=2, sticky='w')
        self.button(account, 'Войти / обновить MAX', lambda: self.submit(self.controller.scan_max, title='Вход в MAX…')).grid(row=3, column=2, sticky='ew')

        pairing = ttk.Frame(outer, style='Card.TFrame', padding=16)
        pairing.pack(fill='both', expand=True, pady=(0, 12))
        pairing.columnconfigure(0, weight=1)
        pairing.columnconfigure(1, weight=1)
        pairing.rowconfigure(5, weight=1)
        ttk.Label(pairing, text='2. Соедините группы попарно', style='Section.TLabel').grid(row=0, column=0, columnspan=3, sticky='w')
        ttk.Label(pairing, text='Добавьте бота администратором в Telegram и отправьте /bridge в каждой нужной группе.',
                  style='Hint.TLabel', wraplength=810).grid(row=1, column=0, columnspan=3, sticky='w', pady=(6, 12))
        ttk.Label(pairing, text='Группа Telegram', style='Card.TLabel').grid(row=2, column=0, sticky='w')
        ttk.Label(pairing, text='Группа MAX', style='Card.TLabel').grid(row=2, column=1, sticky='w')
        self.tg_choice = ttk.Combobox(pairing, state='readonly')
        self.max_choice = ttk.Combobox(pairing, state='readonly')
        self.tg_choice.grid(row=3, column=0, sticky='ew', padx=(0, 12), pady=(5, 12), ipady=5)
        self.max_choice.grid(row=3, column=1, sticky='ew', padx=(0, 12), pady=(5, 12), ipady=5)
        self.controls.extend([self.tg_choice, self.max_choice])
        self.button(pairing, 'Добавить связку', self.add_pair).grid(row=3, column=2, pady=(5, 12))
        ttk.Label(pairing, text='Одна группа — одна связка. Между разными связками сообщения не передаются.',
                  style='Hint.TLabel').grid(row=4, column=0, columnspan=3, sticky='w', pady=(0, 10))
        self.routes = ttk.Treeview(pairing, columns=('tg', 'max'), show='headings', selectmode='browse', height=4)
        self.routes.heading('tg', text='Telegram')
        self.routes.heading('max', text='MAX')
        self.routes.column('tg', width=320)
        self.routes.column('max', width=320)
        self.routes.grid(row=5, column=0, columnspan=3, sticky='nsew')
        scroll = ttk.Scrollbar(pairing, command=self.routes.yview)
        scroll.grid(row=5, column=3, sticky='ns')
        self.routes.configure(yscrollcommand=scroll.set)
        self.button(pairing, 'Удалить выбранную связку', self.remove_pair).grid(row=6, column=0, columnspan=2, sticky='w', pady=(10, 0))

        actions = ttk.Frame(outer)
        actions.pack(fill='x')
        self.start_button = self.button(actions, 'Запустить мост', lambda: self.submit(self.controller.run, title='Подключение моста…'), style='Accent.TButton')
        self.start_button.pack(side='left')
        self.stop_button = ttk.Button(actions, text='Остановить', command=self.cancel, state='disabled')
        self.stop_button.pack(side='left', padx=10)
        ttk.Button(actions, text='Очередь и ошибки', command=self.show_queue).pack(side='right')
        ttk.Button(actions, text='Инструкция', command=self.show_help).pack(side='right', padx=8)
        self.status = tk.StringVar(value='Готов к настройке')
        ttk.Label(outer, textvariable=self.status, foreground='#1E40AF', wraplength=880).pack(anchor='w', pady=(10, 6))
        self.progress = ttk.Progressbar(outer, mode='indeterminate')
        self.progress.pack(fill='x', pady=(0, 8))
        self.stats = tk.StringVar(value='Отправлено: 0  ·  В очереди: 0  ·  Требуют проверки: 0')
        ttk.Label(outer, textvariable=self.stats, foreground='#475569').pack(anchor='w')
        ttk.Label(outer, text='MAX работает через неофициальный клиент. Окно можно свернуть; закрытие остановит мост.',
                  foreground='#475569', wraplength=880).pack(anchor='w', pady=(8, 0))
        self.log_lines = []
        self.error_rows = []
        self.queue_window = None

    def button(self, parent, text, command, **kwargs):
        button = ttk.Button(parent, text=text, command=command, **kwargs)
        self.controls.append(button)
        return button

    def set_busy(self, value):
        self.busy = value
        for control in self.controls:
            control.configure(state='disabled' if value else 'readonly' if isinstance(control, ttk.Combobox) else 'normal')
        self.stop_button.configure(state='normal' if value else 'disabled')
        if value:
            self.progress.start(15)
        else:
            self.progress.stop()

    def submit(self, function, *args, title='Выполняется…'):
        if self.busy or self.closing:
            return
        self.set_busy(True)
        self.status.set(title)

        async def execute():
            self.active_task = asyncio.current_task()
            try:
                await function(*args)
            except asyncio.CancelledError:
                self.events.put(('notice', 'Операция остановлена.'))
            except Exception as exc:
                self.events.put(('error', friendly_error(exc)))
            finally:
                self.active_task = None
                self.events.put(('qr_close', None))
                self.events.put(('done', None))

        asyncio.run_coroutine_threadsafe(execute(), self.loop)

    def cancel(self):
        self.status.set('Остановка…')
        self.stop_button.configure(state='disabled')
        def stop():
            if self.active_task:
                self.active_task.cancel()
        self.loop.call_soon_threadsafe(stop)

    def connect_tg(self):
        value = self.token.get()
        self.token.set('')
        self.submit(self.controller.connect_telegram, value, title='Проверка Telegram-бота…')

    def add_pair(self):
        a, b = self.tg_choice.current(), self.max_choice.current()
        if a < 0 or b < 0:
            messagebox.showinfo('Выберите две группы', 'Выберите группу Telegram и группу MAX из списков.', parent=self.root)
            return
        self.submit(self.controller.add_pair, self.tg_ids[a], self.max_ids[b], title='Проверка прав и добавление связки…')

    def remove_pair(self):
        selected = self.routes.selection()
        if not selected:
            return
        if messagebox.askyesno('Удалить связку?', 'Неотправленные сообщения этой связки будут отменены.', parent=self.root):
            self.submit(self.controller.remove_pair, selected[0], title='Удаление связки…')

    def show_settings(self, value):
        self.settings = value
        self.routes.delete(*self.routes.get_children())
        for pair in value['routes']:
            self.routes.insert('', 'end', iid=core.route_key(pair), values=(
                pair.get('telegram_title', str(pair['telegram_chat_id'])), pair.get('max_title', str(pair['max_chat_id']))))
        self.tg_ids = [int(k) for k in value['telegram_catalog']]
        self.tg_choice['values'] = [f'{title} ({ident})' for ident, title in value['telegram_catalog'].items()]
        self.max_ids = [int(ident) for ident, _ in value['max_groups']]
        self.max_choice['values'] = [f'{title} ({ident})' for ident, title in value['max_groups']]
        for choice in (self.tg_choice, self.max_choice):
            if choice['values']:
                choice.current(0)
            else:
                choice.set('')
        tg = '@' + value['bot_name'] if value.get('bot_name') else 'не подключён'
        mx = 'вход сохранён' if value.get('max_user_id') else 'нужен вход по QR'
        self.account_text.set(f'Telegram: {tg}  ·  MAX: {mx}' + ('  ·  Токен сохранён' if value.get('token_saved') else ''))

    def show_qr(self, url):
        self.close_qr()
        window = tk.Toplevel(self.root)
        self.qr_window = window
        window.title('Вход в MAX')
        window.resizable(False, False)
        window.transient(self.root)
        frame = ttk.Frame(window, padding=24)
        frame.pack()
        ttk.Label(frame, text='Подтвердите вход в MAX', font=('Segoe UI', 16, 'bold')).pack(pady=(0, 12))
        code = qrcode.QRCode(box_size=7, border=4)
        code.add_data(url)
        code.make(fit=True)
        self.qr_image = ImageTk.PhotoImage(code.make_image().convert('RGB'))
        ttk.Label(frame, image=self.qr_image).pack()
        ttk.Label(frame, text='MAX на телефоне → Устройства → Подключить устройство.\nОтсканируйте этот QR-код и подтвердите вход.',
                  justify='center').pack(pady=12)
        ttk.Button(frame, text='Отменить вход', command=self.cancel).pack()
        window.protocol('WM_DELETE_WINDOW', self.cancel)

    def close_qr(self):
        if self.qr_window and self.qr_window.winfo_exists():
            self.qr_window.destroy()
        self.qr_window = None
        self.qr_image = None

    def password(self, value):
        future, hint = value
        window = tk.Toplevel(self.root)
        self.password_window = window
        window.title('Пароль MAX')
        window.transient(self.root)
        window.grab_set()
        frame = ttk.Frame(window, padding=24)
        frame.pack()
        ttk.Label(frame, text='MAX запросил пароль двухэтапной проверки.').pack(anchor='w')
        if hint:
            ttk.Label(frame, text='Подсказка: ' + str(hint), wraplength=400).pack(anchor='w', pady=8)
        entry = ttk.Entry(frame, show='•', width=36)
        entry.pack(fill='x', pady=12)
        entry.focus_set()
        def finish(cancel=False):
            text = entry.get()
            entry.delete(0, 'end')
            window.destroy()
            self.password_window = None
            def resolve():
                if not future.done():
                    future.cancel() if cancel else future.set_result(text)
            self.loop.call_soon_threadsafe(resolve)
        ttk.Button(frame, text='Подтвердить', command=finish).pack(side='left')
        ttk.Button(frame, text='Отмена', command=lambda: finish(True)).pack(side='right')
        entry.bind('<Return>', lambda _: finish())
        window.protocol('WM_DELETE_WINDOW', lambda: finish(True))

    def drain(self):
        try:
            while True:
                kind, value = self.events.get_nowait()
                if kind == 'settings':
                    self.show_settings(value)
                elif kind == 'done':
                    self.set_busy(False)
                    if self.password_window and self.password_window.winfo_exists():
                        self.password_window.destroy()
                        self.password_window = None
                    if self.closing:
                        self.finish_close()
                        return
                elif kind in {'notice', 'error'}:
                    self.status.set(value)
                    self.log_lines.append(value)
                    if kind == 'error' and not self.closing:
                        messagebox.showerror('Не удалось выполнить действие', value, parent=self.root)
                elif kind == 'qr':
                    self.show_qr(value)
                elif kind == 'qr_close':
                    self.close_qr()
                elif kind == 'password':
                    self.password(value)
                elif kind == 'running':
                    self.progress.stop()
                    self.status.set(f'Мост работает · Связок: {value}. Окно можно свернуть.')
                elif kind == 'stopped':
                    self.status.set('Мост остановлен.')
                elif kind == 'stats':
                    counts, self.error_rows = value
                    self.stats.set(f"Отправлено: {counts.get('done', 0)}  ·  В очереди: {counts.get('pending', 0) + counts.get('sending', 0)}  ·  Требуют проверки: {counts.get('uncertain', 0)}")
                elif kind == 'log':
                    self.log_lines.append(value)
                elif kind == 'closed':
                    core.LOG.removeHandler(self.log_handler)
                    self.root.destroy()
                    return
                self.log_lines = self.log_lines[-200:]
        except queue.Empty:
            pass
        self.root.after(100, self.drain)

    def stats_tick(self):
        if self.controller.store and self.loop.is_running() and not self.closing:
            asyncio.run_coroutine_threadsafe(self.controller.statistics(), self.loop)
        if not self.closing:
            self.root.after(3000, self.stats_tick)

    def show_queue(self):
        if self.queue_window and self.queue_window.winfo_exists():
            self.queue_window.destroy()
        window = tk.Toplevel(self.root)
        self.queue_window = window
        window.title('Очередь и ошибки')
        window.geometry('780x500')
        frame = ttk.Frame(window, padding=18)
        frame.pack(fill='both', expand=True)
        ttk.Label(frame, text='Ошибки и задания, требующие проверки', font=('Segoe UI', 12, 'bold')).pack(anchor='w')
        listing = ttk.Treeview(frame, columns=('id', 'side', 'error'), show='headings', height=5)
        for key, title in [('id', 'ID'), ('side', 'Источник'), ('error', 'Причина')]:
            listing.heading(key, text=title)
        listing.column('id', width=60)
        listing.column('side', width=100)
        listing.column('error', width=400)
        listing.pack(fill='x', pady=12)
        for row in self.error_rows:
            listing.insert('', 'end', iid=str(row['id']), values=(row['id'], row['side'], row['error']))
        def retry():
            if self.busy:
                messagebox.showinfo('Остановите мост', 'Для повтора задания сначала остановите мост.', parent=window)
                return
            selected = listing.selection()
            if selected and messagebox.askyesno('Повторить отправку?', 'Сначала проверьте целевую группу. Сообщение могло дойти; повтор может создать дубликат.', parent=window):
                self.submit(self.controller.retry, selected[0], title='Возврат задания в очередь…')
                window.destroy()
        ttk.Button(frame, text='Повторить выбранное задание', command=retry).pack(anchor='w')
        log = tk.Text(frame, height=10, wrap='word', font=('Consolas', 10))
        log.pack(fill='both', expand=True, pady=(12, 0))
        log.insert('1.0', '\n'.join(self.log_lines) or 'Событий пока нет.')
        log.configure(state='disabled')

    def show_help(self):
        messagebox.showinfo('Как подключить группы',
            '1. Введите токен и нажмите «Подключить Telegram».\n\n'
            '2. Добавьте бота администратором в каждую нужную группу Telegram, отправьте /bridge и обновите группы.\n\n'
            '3. Нажмите «Войти / обновить MAX» и подтвердите QR-код с телефона. Аккаунт должен состоять в нужных группах MAX.\n\n'
            '4. Выберите по одной группе с каждой стороны и нажмите «Добавить связку». Повторите для остальных пар.\n\n'
            '5. Нажмите «Запустить мост». Для изменения связок сначала остановите его.\n\n'
            'Компьютер и интернет должны оставаться включёнными. MAX на ПК и Python устанавливать не нужно. '
            'Файлы до 20 МиБ. Передаются ответы, правки текста и новые закрепления. Реакции и голоса — отдельными обновляемыми сводками.\n\n'
            'Создавайте опрос обычным способом: мост ответит копией от бота и свяжет её с MAX. '
            'Голосовать в Telegram нужно в копии от бота. Исходный опрос остаётся, старые голоса не переносятся. '
            'Голоса двух платформ независимы. Команда /bridge_poll также доступна.\n\n'
            'Удаления передаются только MAX → Telegram. Снятие закрепления, замена вложений и старые копии версии 2 не синхронизируются.\n\n'
            'Неофициальный клиент MAX может перестать работать при изменении API; возможны ограничения аккаунта.\n\n'
            'Настройки и сессия сохраняются в:\n' + str(core.ROOT), parent=self.root)

    def close(self):
        if self.busy and not messagebox.askyesno('Остановить и закрыть?', 'Копирование сообщений остановится. Закрыть приложение?', parent=self.root):
            return
        self.closing = True
        if self.busy:
            self.cancel()
        else:
            self.finish_close()

    def finish_close(self):
        async def finish():
            try:
                await self.controller.close()
            finally:
                self.events.put(('closed', None))
                self.loop.stop()
        if self.loop.is_running():
            asyncio.run_coroutine_threadsafe(finish(), self.loop)
            self.root.after(100, self.drain)
        else:
            self.root.destroy()


def self_test(destination):
    """No network/credentials: test the actual frozen runtime and Tk widgets."""
    import tempfile
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    import ssl
    import sqlite3
    from pymax import Message
    checks = []
    root = tk.Tk()
    root.withdraw()
    app = App(root, start_backend=False)
    try:
        app.show_settings({'routes': [dict(telegram_chat_id=-1, max_chat_id=-2, linked_at=1,
                                           telegram_title='Тестовая группа Telegram', max_title='Тестовая группа MAX')],
                           'telegram_catalog': {'-1': 'Тестовая группа Telegram'},
                           'max_groups': [(-2, 'Тестовая группа MAX')], 'token_saved': False, 'bot_name': '', 'max_user_id': None})
        root.update_idletasks()
        assert len(app.routes.get_children()) == 1
        assert app.tg_ids == [-1] and app.max_ids == [-2]
        app.set_busy(True)
        assert str(app.start_button['state']) == 'disabled'
        app.set_busy(False)
        assert str(app.max_choice['state']) == 'readonly'
        checks.append('Tk/Tcl: окно, списки связок, блокировка управления')
        code = qrcode.make('local-runtime-test').convert('RGB')
        photo = ImageTk.PhotoImage(code)
        assert photo.width() > 0
        checks.append('QR / Pillow / ImageTk')
        encrypted = protect_token('local-test-secret')
        assert protect_token(encrypted, decrypt=True) == 'local-test-secret'
        checks.append('Windows DPAPI: шифрование и расшифровка')
        assert ssl.create_default_context().cert_store_stats()['x509'] > 0
        checks.append('TLS: системное хранилище сертификатов')
        client = app.controller.make_max()
        client.extra_config.persist_session = False
        async def check_runtime():
            await client._ensure_runtime()
            assert client._app is not None
            await client.close()
        asyncio.run(check_runtime())
        checks.append('MAX: создание WebClient, конфигурации и транспорта без консоли')
        with tempfile.TemporaryDirectory() as tmp:
            store = core.Store(Path(tmp) / 'test.db')
            cfg = {'routes': [dict(telegram_chat_id=-1, max_chat_id=-2, linked_at=1),
                              dict(telegram_chat_id=-3, max_chat_id=-4, linked_at=1)]}
            router = core.MultiBridge(cfg, store, None, None, 99, 10)
            router.ingest_tg({'chat': {'id': -3}, 'message_id': 1, 'date': 2, 'text': 'test', 'from': {'id': 5}})
            router.ingest_max(Message(id=2, chat_id=-2, sender=6, time=2000, type='USER', text='test'))
            rows = store.db.execute('SELECT side,route FROM jobs ORDER BY id').fetchall()
            assert rows[0]['route'] == router.routes[1].route
            assert rows[1]['route'] == router.routes[0].route
            store.db.close()
        checks.append('SQLite / PyMax / маршрутизация двух связок')
        report = {'ok': True, 'version': VERSION, 'frozen': bool(getattr(sys, 'frozen', False)),
                  'checks': checks, 'live_max_tested': False}
    finally:
        core.LOG.removeHandler(app.log_handler)
        root.destroy()
    Path(destination).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')


def main():
    if '--self-test' in sys.argv:
        destination = sys.argv[sys.argv.index('--self-test') + 1]
        try:
            self_test(destination)
        except Exception as exc:
            Path(destination).write_text(json.dumps({'ok': False, 'error': type(exc).__name__, 'detail': str(exc)}), encoding='utf-8')
            raise
        return
    root = tk.Tk()
    try:
        with core.single_instance():
            App(root)
            root.mainloop()
    except Exception as exc:
        messagebox.showerror('Telegram ↔ MAX', friendly_error(exc), parent=root)
        root.destroy()


if __name__ == '__main__':
    main()
