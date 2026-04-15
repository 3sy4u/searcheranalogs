# ==================== ИМПОРТЫ ====================
import io
import os
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Final

import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer, QPropertyAnimation
from PyQt6.QtGui import QColor, QFontDatabase, QFont, QGuiApplication, QIcon
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLineEdit, QPushButton, QLabel, QTableWidget, QTableWidgetItem,
    QHeaderView, QAbstractItemView, QDialog, QTextEdit, QFormLayout,
    QMessageBox
)

# ==================== КОНФИГУРАЦИЯ ====================
CURRENT_VERSION: Final = "1.0.0"
UPDATE_CHECK_URL: Final = "https://api.github.com/repos/3sy4u/searcheranalogs/releases/latest"
SHEET_URL: Final = "https://docs.google.com/spreadsheets/d/1qviJPyDXzN_DKPD1tVMdsPW_IVl-3Fn2yQtEzuK0XFc/edit?usp=sharing"


def _extract_spreadsheet_id(url: str) -> str:
    try:
        return url.split("/d/")[1].split("/")[0]
    except IndexError:
        raise ValueError(f"Некорректный URL Google Sheets: {url}")


SPREADSHEET_ID: Final = _extract_spreadsheet_id(SHEET_URL)
BRAND_COLUMNS: Final = ("Lincoln", "CisoLube", "Tribo", "KOCU", "Bijur Delimon", "MecLube")
COLUMN_SPECS: Final = "Характеристики"
FONT_FILE: Final = "Sansation-Bold.ttf"
FONT_FALLBACK: Final = "Arial"
ICON_FILE: Final = "app_icon.ico"
SERVICE_ACCOUNT_FILENAME: Final = "service_account.json"

# ==================== УТИЛИТЫ ====================
_RESOURCE_ROOT: str | None = None


def get_resource_path(filename: str) -> str:
    global _RESOURCE_ROOT
    if _RESOURCE_ROOT is None:
        _RESOURCE_ROOT = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(_RESOURCE_ROOT, filename)


_LOADED_FONT_NAME: str | None = None


def load_font() -> str:
    global _LOADED_FONT_NAME
    if _LOADED_FONT_NAME is not None:
        return _LOADED_FONT_NAME

    app = QApplication.instance()
    if app is None:
        _LOADED_FONT_NAME = FONT_FALLBACK
        return _LOADED_FONT_NAME

    font_path = get_resource_path(FONT_FILE)
    if not os.path.exists(font_path):
        _LOADED_FONT_NAME = FONT_FALLBACK
        return _LOADED_FONT_NAME

    font_id = QFontDatabase.addApplicationFont(font_path)
    if font_id == -1:
        _LOADED_FONT_NAME = FONT_FALLBACK
        return _LOADED_FONT_NAME

    families = QFontDatabase.applicationFontFamilies(font_id)
    _LOADED_FONT_NAME = families[0] if families else FONT_FALLBACK
    return _LOADED_FONT_NAME


# ==================== WORKERS ====================
class LoadWorker(QThread):
    finished = pyqtSignal(object)
    error = pyqtSignal(str)
    _EXPORT_URL: Final = SHEET_URL.split("/edit")[0] + "/export?format=xlsx"

    def run(self) -> None:
        try:
            with requests.Session() as session:
                session.headers.update({'Accept-Encoding': 'gzip'})
                resp = session.get(self._EXPORT_URL, timeout=20)
                resp.raise_for_status()
                df = pd.read_excel(
                    io.BytesIO(resp.content), header=0, dtype=str, engine='openpyxl',
                    keep_default_na=False, na_values=[]
                )
                df.columns = df.columns.astype(str).str.strip()
                df = df.dropna(how='all').reset_index(drop=True)
            self.finished.emit(df)
        except requests.exceptions.Timeout:
            self.error.emit("Тайм-аут загрузки. Проверьте соединение.")
        except requests.exceptions.ConnectionError:
            self.error.emit("Нет соединения с интернетом.")
        except Exception as e:
            self.error.emit(f"Ошибка: {type(e).__name__}: {e}")


_CREDENTIALS_CACHE: dict[tuple, Credentials] = {}


def _get_cached_credentials(scopes: tuple[str, ...]) -> Credentials:
    cache_key = (get_resource_path(SERVICE_ACCOUNT_FILENAME), tuple(scopes))
    if cache_key not in _CREDENTIALS_CACHE:
        _CREDENTIALS_CACHE[cache_key] = Credentials.from_service_account_file(
            _CREDENTIALS_CACHE[cache_key] if False else get_resource_path(SERVICE_ACCOUNT_FILENAME),
            scopes=list(scopes)
        )
    return _CREDENTIALS_CACHE[cache_key]


class AddArticleWorker(QThread):
    finished = pyqtSignal(bool, str, object)

    def __init__(self, articles_dict: dict, extra_data: dict,
                 existing_cache: set | None = None,
                 check_duplicates: bool = True,
                 force_add: bool = False):
        super().__init__()
        self.articles_dict = articles_dict
        self.extra_data = extra_data
        self.existing_cache = existing_cache
        self.check_duplicates = check_duplicates
        self.force_add = force_add

    def run(self) -> None:
        try:
            scopes = (
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file"
            )
            creds = _get_cached_credentials(scopes)
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(SPREADSHEET_ID)
            worksheet = sh.get_worksheet(0)
            headers = [h.strip() for h in worksheet.row_values(1)]

            if self.check_duplicates and not self.force_add:
                existing_articles = (
                    self.existing_cache.copy() if self.existing_cache
                    else self._build_articles_cache(worksheet)
                )
                duplicates = {
                    brand: article.strip()
                    for brand, article in self.articles_dict.items()
                    if (art_clean := article.strip().lower()) and art_clean in existing_articles
                }
                if duplicates:
                    self.finished.emit(False, "duplicates_warning", {
                        "duplicates": duplicates, "all_articles": self.articles_dict, "extra_data": self.extra_data
                    })
                    return

            full_data = {**self.articles_dict, **self.extra_data}
            row_to_append = [full_data.get(h, "") for h in headers]
            worksheet.append_row(row_to_append, value_input_option='RAW', insert_data_option='INSERT_ROWS')

            filled_count = sum(1 for x in row_to_append if str(x).strip())
            self.finished.emit(True, f"✅ Добавлено! (Заполнено ячеек: {filled_count})", None)
        except gspread.exceptions.APIError as e:
            self.finished.emit(False, f"Google API ошибка: {e}", None)
        except Exception as e:
            self.finished.emit(False, f"Ошибка: {type(e).__name__}: {e}", None)

    @staticmethod
    def _build_articles_cache(worksheet) -> set[str]:
        cache = set()
        for row in worksheet.get_all_values()[1:]:
            for cell in row:
                val = str(cell).strip().lower()
                if val and val not in {"nan", "none", ""}:
                    cache.add(val)
        return cache


class UpdaterWorker(QThread):
    progress = pyqtSignal(int, str)
    finished_update = pyqtSignal(str)
    error = pyqtSignal(str)
    no_update = pyqtSignal()
    _HEADERS: dict = {'Accept': 'application/vnd.github.v3+json', 'User-Agent': 'SearcherApp/1.0'}
    _CHUNK_SIZE: int = 8192
    _PROGRESS_THROTTLE_SEC: float = 0.2

    def __init__(self, current_version: str, repo_url: str):
        super().__init__()
        self.current_version = current_version
        self.repo_url = repo_url

    def run(self) -> None:
        self.progress.emit(10, "Проверка обновлений...")
        try:
            with requests.Session() as session:
                session.headers.update(self._HEADERS)
                resp = session.get(self.repo_url, timeout=15)
                if resp.status_code == 403:
                    remaining = resp.headers.get('X-RateLimit-Remaining', '0')
                    if int(remaining) <= 0:
                        raise PermissionError("Превышен лимит запросов к GitHub API.")
                resp.raise_for_status()
                data = resp.json()
                remote_version = data.get('tag_name', 'v0.0.0').lstrip('v').strip()
                exe_asset = next((a for a in data.get('assets', []) if a.get('name', '').lower().endswith('.exe')),
                                 None)
                if not exe_asset:
                    raise FileNotFoundError("Не найден .exe файл в релизе.")
                if self._compare_versions(remote_version, self.current_version) <= 0:
                    self.no_update.emit()
                    return
                self.progress.emit(30, f"Найдена версия {remote_version}. Скачивание...")
                new_exe_path = self._download_file(session, exe_asset['browser_download_url'])
                self.progress.emit(100, "Готово!")
                self.finished_update.emit(new_exe_path)
        except requests.exceptions.Timeout:
            self.error.emit("Тайм-аут соединения с GitHub.")
        except requests.exceptions.ConnectionError:
            self.error.emit("Нет подключения к интернету.")
        except Exception as e:
            self.error.emit(f"Ошибка: {type(e).__name__}: {e}")

    def _download_file(self, session: requests.Session, url: str) -> str:
        temp_dir = tempfile.gettempdir()
        new_exe_path = os.path.join(temp_dir, f"searcher_update_{os.getpid()}_{int(time.time())}.exe")
        with session.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            total_length = int(r.headers.get('content-length', 0))
            downloaded = 0
            last_percent = 0
            last_emit_time = time.time()
            with open(new_exe_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=self._CHUNK_SIZE):
                    if not chunk: continue
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_length > 0:
                        percent = min(30 + int((downloaded / total_length) * 70), 99)
                        now = time.time()
                        if percent != last_percent or (now - last_emit_time) >= self._PROGRESS_THROTTLE_SEC:
                            self.progress.emit(percent, f"Скачано {percent - 30}%")
                            last_percent = percent
                            last_emit_time = now
        return new_exe_path

    @staticmethod
    def _compare_versions(v1: str, v2: str) -> int:
        def to_tuple(v: str) -> tuple[int, ...]: return tuple(int(x) for x in v.split('.'))

        t1, t2 = to_tuple(v1), to_tuple(v2)
        max_len = max(len(t1), len(t2))
        t1 += (0,) * (max_len - len(t1))
        t2 += (0,) * (max_len - len(t2))
        return 1 if t1 > t2 else (-1 if t1 < t2 else 0)


# ==================== UI КОМПОНЕНТЫ ====================
class CustomTableWidget(QTableWidget):
    _DASH_PLACEHOLDER: Final = "—"
    _EMPTY_STRING: Final = ""
    _TAB: Final = "\t"
    _NEWLINE: Final = "\n"

    def keyPressEvent(self, event) -> None:
        if event.key() != Qt.Key.Key_C or event.modifiers() != Qt.KeyboardModifier.ControlModifier:
            super().keyPressEvent(event)
            return

        selected = self.selectedItems()
        if not selected:
            super().keyPressEvent(event)
            return

        item_method = self.item
        clipboard = QGuiApplication.clipboard()

        if len(selected) == 1:
            val = selected[0].text()
            if val != self._DASH_PLACEHOLDER:
                clipboard.setText(val)
            super().keyPressEvent(event)
            return

        rows = sorted({item.row() for item in selected})
        cols = sorted({item.column() for item in selected})
        result = []

        for r in rows:
            row_vals = []
            for c in cols:
                widget_item = item_method(r, c)
                val = widget_item.text() if widget_item else self._EMPTY_STRING
                row_vals.append(self._EMPTY_STRING if val == self._DASH_PLACEHOLDER else val)
            result.append(self._TAB.join(row_vals))

        clipboard.setText(self._NEWLINE.join(result))
        super().keyPressEvent(event)


class AddArticleDialog(QDialog):
    _STYLE_INPUT_DUPLICATE: Final = "border: 2px solid #e74c3c; background-color: #3d1f1f; color: #ff9999; border-radius: 6px; padding: 8px 16px;"
    _STYLE_INPUT_NORMAL: Final = ""
    _STYLE_DIALOG_BG: Final = "QDialog { background-color: #2b2b2b; } QLabel { color: #ffffff; }"
    _STYLE_BTN_PRIMARY: Final = "background-color: #e74c3c; color: #ffffff; border: none; border-radius: 6px; padding: 10px 20px; font-weight: bold;"
    _STYLE_BTN_SECONDARY: Final = "background-color: #555555; color: #ffffff; border: none; border-radius: 6px; padding: 10px 20px;"
    _EMPTY_SET: Final = frozenset()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Добавить артикулы и данные")
        self.setMinimumSize(650, 600)
        self._parent_ref = parent
        self._worker: AddArticleWorker | None = None
        self._pending_data: dict | None = None
        self._article_edits: dict[str, QLineEdit] = {}
        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)

        common_group = QWidget()
        common_layout = QVBoxLayout(common_group)
        common_layout.setContentsMargins(0, 0, 0, 10)
        common_layout.addWidget(self._styled_label("<b>Общая информация:</b>", "#aaa", padding_bottom=10))

        self.edit_name = self._text_edit("Название (опционально)...", max_height=50)
        self.edit_specs = self._text_edit("Характеристики (опционально)...", min_height=80)
        common_layout.addWidget(self.edit_name)
        common_layout.addWidget(self.edit_specs)
        layout.addWidget(common_group)

        form = QFormLayout()
        form.setSpacing(12)
        form.addRow(self._styled_label("<b>Артикулы брендов:</b>", "#aaa", padding_top=10))
        for brand in BRAND_COLUMNS:
            edit = QLineEdit()
            edit.setPlaceholderText(f"Артикул для {brand}")
            form.addRow(f"{brand}:", edit)
            self._article_edits[brand] = edit
            edit.textChanged.connect(lambda text, b=brand: self._check_duplicate_live(b, text))
        layout.addLayout(form)

        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        self.btn_save = QPushButton("💾 Сохранить")
        self.btn_save.clicked.connect(self.start_saving)
        self.btn_close = QPushButton("Отмена")
        self.btn_close.clicked.connect(self.close)
        btn_layout.addWidget(self.btn_save)
        btn_layout.addWidget(self.btn_close)
        layout.addLayout(btn_layout)

    def _styled_label(self, text: str, color: str, padding_top: int = 0, padding_bottom: int = 0) -> QLabel:
        label = QLabel(text)
        padding = f"padding: {padding_top}px 0 {padding_bottom}px;" if padding_top or padding_bottom else ""
        label.setStyleSheet(f"color: {color}; font-size: 14px; {padding}")
        return label

    def _text_edit(self, placeholder: str, min_height: int = 0, max_height: int = 16777215) -> QTextEdit:
        edit = QTextEdit()
        edit.setPlaceholderText(placeholder)
        if min_height: edit.setMinimumHeight(min_height)
        if max_height < 16777215: edit.setMaximumHeight(max_height)
        return edit

    def _get_cache(self) -> set:
        parent = self._parent_ref
        return parent.existing_articles_cache if parent and hasattr(parent,
                                                                    'existing_articles_cache') else self._EMPTY_SET

    def _check_duplicate_live(self, brand: str, text: str = "") -> None:
        text_clean = text.strip().lower() if text else self._article_edits[brand].text().strip().lower()
        edit = self._article_edits[brand]
        is_duplicate = text_clean and text_clean in self._get_cache()
        edit.setStyleSheet(self._STYLE_INPUT_DUPLICATE if is_duplicate else self._STYLE_INPUT_NORMAL)
        edit.setToolTip("⚠️ Уже есть в базе" if is_duplicate else "")

    def start_saving(self, force_add: bool = False) -> None:
        if force_add and self._pending_data:
            articles_dict = self._pending_data["all_articles"]
            extra_data = self._pending_data["extra_data"]
        else:
            extra_data = {
                "Название": self.edit_name.toPlainText().strip(),
                COLUMN_SPECS: self.edit_specs.toPlainText().strip()
            }
            articles_dict = {brand: edit.text().strip() for brand, edit in self._article_edits.items()}
            if not any(articles_dict.values()) and not any(extra_data.values()):
                QMessageBox.warning(self, "Ошибка", "Заполните хотя бы одно поле!")
                return

        self.btn_save.setEnabled(False)
        self.btn_save.setText("Сохранение...")
        self._worker = AddArticleWorker(articles_dict, extra_data, existing_cache=self._get_cache(),
                                        check_duplicates=not force_add, force_add=force_add)
        self._worker.finished.connect(self.on_save_finished)
        self._worker.start()

    def _show_duplicate_confirmation(self, duplicates: dict) -> bool:
        dup_lines = [f"• {brand}: <b>{art}</b>" for brand, art in duplicates.items()]
        html = f"<div style='font-family: sans-serif; font-size: 14px; line-height: 1.4;'>" \
               f"<h3 style='color: #f39c12; margin-top: 0;'>⚠️ Внимание: найдены дубликаты</h3>" \
               f"<p>Следующие артикулы уже есть в базе:</p>" \
               f"<div style='background-color: #3d1f1f; padding: 10px; border-radius: 6px; margin: 10px 0; border-left: 3px solid #e74c3c;'>{'<br>'.join(dup_lines)}</div>" \
               f"<p><b>Вы хотите добавить их всё равно?</b><br><span style='color: #aaa;'>(Это создаст дублирующиеся строки)</span></p></div>"
        return self._exec_confirm_dialog("Подтверждение", html)

    def _exec_confirm_dialog(self, title: str, html: str) -> bool:
        dialog = QDialog(self)
        dialog.setWindowTitle(title)
        dialog.setMinimumWidth(450)
        dialog.setStyleSheet(self._STYLE_DIALOG_BG)
        dlg_layout = QVBoxLayout(dialog)
        label = QLabel(html)
        label.setTextFormat(Qt.TextFormat.RichText)
        dlg_layout.addWidget(label)
        btn_box = QHBoxLayout()
        btn_force = QPushButton("🔥 Да, добавить")
        btn_force.setStyleSheet(self._STYLE_BTN_PRIMARY)
        btn_force.clicked.connect(dialog.accept)
        btn_cancel = QPushButton("Нет, отмена")
        btn_cancel.setStyleSheet(self._STYLE_BTN_SECONDARY)
        btn_cancel.clicked.connect(dialog.reject)
        btn_box.addWidget(btn_force)
        btn_box.addWidget(btn_cancel)
        dlg_layout.addLayout(btn_box)
        return dialog.exec() == QDialog.DialogCode.Accepted

    def on_save_finished(self, success: bool, message: str, payload: dict | None) -> None:
        self.btn_save.setEnabled(True)
        self.btn_save.setText("💾 Сохранить")
        if not success and message == "duplicates_warning" and payload:
            self._pending_data = payload
            if self._show_duplicate_confirmation(payload.get("duplicates", {})):
                self.start_saving(force_add=True)
            else:
                self._pending_data = None
            return
        if success:
            QMessageBox.information(self, "Успешно", message)
            if self._parent_ref and hasattr(self._parent_ref, '_load_data'):
                self._parent_ref._load_data()
            self.close()
        else:
            QMessageBox.critical(self, "Ошибка", f"Не удалось сохранить:\n\n{message}")
            self._pending_data = None


# ==================== ГЛАВНОЕ ОКНО ====================
class SearcherApp(QMainWindow):
    _PLACEHOLDER: Final = "—"
    _INVALID_VALUES: Final = {"nan", "none", ""}
    _STATUS_COLORS: Final = {"loading": "#f39c12", "success": "#27ae60", "error": "#e74c3c", "idle": "#aaaaaa",
                             "info": "#2980b9"}

    def __init__(self, font_name: str):
        super().__init__()
        self.df: pd.DataFrame | None = None
        self.font_name: str = font_name
        self.existing_articles_cache: set[str] = set()
        self.search_timer = QTimer()
        self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self.search)
        self.setWindowTitle("YNIC & DLS Searcher")
        self.setMinimumSize(980, 700)
        self._set_window_icon()
        self._apply_styles()
        self._build_ui()
        self._load_data()
        QTimer.singleShot(2000, self.check_for_updates)

    def _set_window_icon(self) -> None:
        base_path = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
        icon_path = os.path.join(base_path, ICON_FILE)
        if os.path.exists(icon_path):
            icon = QIcon(icon_path)
            self.setWindowIcon(icon)
            QApplication.setWindowIcon(icon)

    def _apply_styles(self) -> None:
        f = self.font_name
        self.setStyleSheet(f"""
            QMainWindow, QWidget#central {{ background-color: #2b2b2b; }}
            QLabel#title {{ color: #ffffff; font-size: 22px; font-family: "{f}"; padding: 16px; background-color: #3a3a3a; }}
            QLabel#status {{ color: #aaaaaa; font-size: 13px; font-family: "{f}"; padding: 4px 0; }}
            QLabel#beta {{ color: #800000; font-size: 24px; font-family: "{f}"; font-weight: bold; opacity: 0.9; }}
            QLineEdit {{ background-color: #3a3a3a; color: #ffffff; border: 1px solid #555555; border-radius: 6px; font-size: 18px; font-family: "{f}"; padding: 8px 16px; }}
            QPushButton#btn_search, QPushButton#btn_add {{ background-color: #800000; color: #ffffff; border: none; border-radius: 6px; font-size: 15px; font-family: "{f}"; padding: 10px 20px; }}
            QPushButton#btn_search:hover, QPushButton#btn_add:hover {{ background-color: #a00000; }}
            QPushButton#btn_params {{ background-color: #555555; color: #888888; border: none; border-radius: 6px; font-size: 15px; font-family: "{f}"; padding: 10px 20px; }}
            QPushButton#btn_refresh {{ background-color: #3a3a3a; color: #aaaaaa; border: 1px solid #555555; border-radius: 6px; font-size: 13px; font-family: "{f}"; padding: 8px 20px; }}
            QTableWidget {{ background-color: #2b2b2b; color: #ffffff; gridline-color: #444444; border: none; font-size: 14px; font-family: "{f}"; }}
            QTableWidget::item:selected {{ background-color: #800000; color: #ffffff; }}
        """)

    def _build_ui(self) -> None:
        central = QWidget()
        central.setObjectName("central")
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        self.lbl_title = QLabel("Поиск аналогов")
        self.lbl_title.setObjectName("title")
        self.lbl_title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.lbl_title)
        self.lbl_status = QLabel("Загрузка...")
        self.lbl_status.setObjectName("status")
        self.lbl_status.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.lbl_status)
        search_frame = QWidget()
        search_layout = QHBoxLayout(search_frame)
        search_layout.setContentsMargins(30, 12, 30, 12)
        search_layout.setSpacing(10)
        self.entry = QLineEdit()
        self.entry.setPlaceholderText("Введи часть артикула...")
        self.entry.textChanged.connect(self.on_text_changed)
        self.entry.returnPressed.connect(self.perform_search)
        search_layout.addWidget(self.entry)
        self.btn_search = QPushButton("ПОИСК")
        self.btn_search.setObjectName("btn_search")
        self.btn_search.setEnabled(False)
        self.btn_search.clicked.connect(self.perform_search)
        search_layout.addWidget(self.btn_search)
        self.btn_add = QPushButton("➕ Добавить артикулы")
        self.btn_add.clicked.connect(self.open_add_dialog)
        search_layout.addWidget(self.btn_add)
        self.btn_params = QPushButton("Поиск по параметрам")
        self.btn_params.setObjectName("btn_params")
        self.btn_params.setEnabled(False)
        search_layout.addWidget(self.btn_params)
        layout.addWidget(search_frame)
        self.table = CustomTableWidget()
        self.table.setColumnCount(len(BRAND_COLUMNS))
        self.table.setHorizontalHeaderLabels([b.upper() for b in BRAND_COLUMNS])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.verticalHeader().setVisible(False)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.table.doubleClicked.connect(self._open_specs)
        table_frame = QWidget()
        tl = QVBoxLayout(table_frame)
        tl.setContentsMargins(20, 0, 20, 10)
        tl.addWidget(self.table)
        layout.addWidget(table_frame)
        bottom = QWidget()
        bl = QHBoxLayout(bottom)
        bl.setContentsMargins(20, 8, 20, 16)
        hint = QLabel("Двойной клик — характеристики  |  Ctrl+C — скопировать")
        hint.setObjectName("status")
        bl.addWidget(hint)
        bl.addStretch()
        self.btn_refresh = QPushButton("↻  ОБНОВИТЬ БАЗУ")
        self.btn_refresh.setObjectName("btn_refresh")
        self.btn_refresh.clicked.connect(self._load_data)
        bl.addWidget(self.btn_refresh)
        layout.addWidget(bottom)
        self.beta_label = QLabel("Release 1.0")
        self.beta_label.setObjectName("beta")
        self.beta_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignBottom)
        layout.addWidget(self.beta_label)

    def open_add_dialog(self) -> None:
        AddArticleDialog(self).exec()

    def perform_search(self) -> None:
        if self.btn_search.isEnabled():
            geo = self.btn_search.geometry()
            anim = QPropertyAnimation(self.btn_search, b"geometry")
            anim.setDuration(120)
            anim.setStartValue(geo)
            anim.setKeyValueAt(0.5, geo.adjusted(0, 2, 0, 2))
            anim.setEndValue(geo)
            anim.start()
        self.search()

    def _load_data(self) -> None:
        self.btn_search.setEnabled(False)
        self.btn_refresh.setEnabled(False)
        self._set_status("● Обновление...", self._STATUS_COLORS["loading"])
        self.worker = LoadWorker()
        self.worker.finished.connect(self._on_load_success)
        self.worker.error.connect(self._on_load_error)
        self.worker.start()

    def _on_load_success(self, df: pd.DataFrame) -> None:
        self.df = df
        self._build_cache_from_df(df)
        self._set_status(f"● База загружена: {len(self.df)} поз.", self._STATUS_COLORS["success"])
        self.btn_search.setEnabled(True)
        self.btn_refresh.setEnabled(True)

    def _on_load_error(self, msg: str) -> None:
        self._set_status(f"● Ошибка: {msg}", self._STATUS_COLORS["error"])
        self.btn_refresh.setEnabled(True)

    def _set_status(self, text: str, color: str) -> None:
        self.lbl_status.setText(text)
        self.lbl_status.setStyleSheet(
            f"color: {color}; font-size: 13px; font-family: '{self.font_name}'; padding: 4px 0;")

    def on_text_changed(self) -> None:
        self.search_timer.start(300)

    def search(self) -> None:
        if self.df is None: return
        query = self.entry.text().strip().lower()
        if not query:
            self._set_status("● Введите часть артикула...", self._STATUS_COLORS["idle"])
            self._populate_table_batch(self.df.iloc[:0])
            return

        mask = pd.Series(False, index=self.df.index)
        for col in BRAND_COLUMNS:
            if col in self.df.columns:
                mask |= self.df[col].astype(str).str.lower().str.contains(query, regex=False, na=False)
        matched_df = self.df[mask]
        self._populate_table_batch(matched_df)
        count = len(matched_df)
        if count > 0:
            self._set_status(f"✅ Найдено: {count}", self._STATUS_COLORS["info"])
            if self.table.rowCount() > 0: self.table.selectRow(0)
        else:
            self._set_status(f"❌ Ничего не найдено по «{query}»", self._STATUS_COLORS["error"])

    def _populate_table_batch(self, df_slice: pd.DataFrame) -> None:
        self.table.setUpdatesEnabled(False)
        try:
            self.table.setRowCount(0)
            if df_slice.empty: return
            self.table.setRowCount(len(df_slice))
            for row_idx, row in enumerate(df_slice.itertuples(index=True)):
                df_idx = row.Index
                for col_idx, brand in enumerate(BRAND_COLUMNS):
                    val = str(getattr(row, brand, "")).strip()
                    display = self._PLACEHOLDER if val.lower() in self._INVALID_VALUES else val
                    item = QTableWidgetItem(display)
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                    if display == self._PLACEHOLDER: item.setForeground(QColor("#555555"))
                    item.setData(Qt.ItemDataRole.UserRole, df_idx)
                    self.table.setItem(row_idx, col_idx, item)
        finally:
            self.table.setUpdatesEnabled(True)

    def _open_specs(self, index) -> None:
        if self.df is None: return
        item = self.table.item(index.row(), index.column())
        if not item: return
        df_index = item.data(Qt.ItemDataRole.UserRole)
        if df_index is None or df_index not in self.df.index: return
        row = self.df.loc[df_index]
        specs_text = str(row.get(COLUMN_SPECS, "")).strip() or "Характеристики не заполнены"
        title_parts = [f"{brand}: {str(row.get(brand, '')).strip()}" for brand in BRAND_COLUMNS if
                       str(row.get(brand, '')).strip().lower() not in self._INVALID_VALUES]
        title = "  |  ".join(title_parts) if title_parts else "Характеристики"
        dialog = QDialog(self)
        dialog.setWindowTitle("Характеристики")
        dialog.setMinimumSize(520, 420)
        dialog.setStyleSheet(f"""
            QDialog {{ background-color: #2b2b2b; }}
            QLabel {{ color: #cccccc; font-size: 12px; font-family: "{self.font_name}"; padding: 12px 16px 4px; }}
            QTextEdit {{ background-color: #3a3a3a; color: #ffffff; border: 1px solid #555555; border-radius: 6px; font-size: 14px; font-family: "{self.font_name}"; padding: 12px; margin: 0 16px; }}
            QPushButton {{ background-color: #800000; color: #ffffff; border: none; border-radius: 6px; font-size: 13px; font-family: "{self.font_name}"; padding: 8px 24px; margin: 8px; }}
            QPushButton:hover {{ background-color: #a00000; }}
        """)
        dlg_layout = QVBoxLayout(dialog)
        dlg_layout.setContentsMargins(0, 0, 0, 8)
        dlg_layout.addWidget(QLabel(title))
        text_area = QTextEdit()
        text_area.setPlainText(specs_text)
        text_area.setReadOnly(True)
        dlg_layout.addWidget(text_area)
        btn_box = QHBoxLayout()
        btn_box.addStretch()
        btn_copy = QPushButton("КОПИРОВАТЬ")
        btn_copy.clicked.connect(lambda _, txt=specs_text: self._copy_to_clipboard(txt))
        btn_box.addWidget(btn_copy)
        btn_close = QPushButton("ЗАКРЫТЬ")
        btn_close.clicked.connect(dialog.close)
        btn_box.addWidget(btn_close)
        dlg_layout.addLayout(btn_box)
        dialog.exec()

    def _copy_to_clipboard(self, text: str) -> None:
        QGuiApplication.clipboard().setText(text)
        self._set_status("📋 Скопировано", self._STATUS_COLORS["info"])

    def _build_cache_from_df(self, df: pd.DataFrame) -> None:
        cache = set()
        for col in BRAND_COLUMNS:
            if col in df.columns:
                vals = df[col].astype(str).str.strip().str.lower()
                cache.update(vals[~vals.isin(self._INVALID_VALUES)])
        self.existing_articles_cache = cache

    def check_for_updates(self) -> None:
        self.updater = UpdaterWorker(CURRENT_VERSION, UPDATE_CHECK_URL)
        self.updater.progress.connect(self._on_update_progress)
        self.updater.finished_update.connect(self._on_update_ready)
        self.updater.error.connect(self._on_update_error)
        self.updater.no_update.connect(self._on_no_update)
        self.updater.start()

    def _on_update_progress(self, percent: int, msg: str) -> None:
        self._set_status(f"🔄 {msg}", self._STATUS_COLORS["loading"])

    def _on_no_update(self) -> None:
        self._set_status("● База загружена" if self.df is not None else "● Готово", self._STATUS_COLORS["success"])

    def _on_update_error(self, msg: str) -> None:
        self._set_status("● Ошибка проверки обновлений", self._STATUS_COLORS["error"])
        print(f"[Update] Error: {msg}")

    def _on_update_ready(self, new_exe_path: str) -> None:
        reply = QMessageBox.question(self, "Доступно обновление", "Найдена новая версия!\nУстановить сейчас?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                     QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            self._install_update(new_exe_path)

    _UPDATE_BATCH_TEMPLATE: Final = r"""@echo off
setlocal EnableDelayedExpansion
chcp 65001 >nul
set "RETRY_COUNT=0"
set "MAX_RETRIES=10"
set "LOG_FILE={log_path}"
set "NEW_EXE={new_exe}"
set "CURRENT_EXE={current_exe}"
set "BACKUP_EXE={backup_exe}"
set "BATCH_FILE=%~f0"
echo [%TIME%] Start update... >> "%LOG_FILE%"
timeout /t 2 /nobreak >nul
:RETRY
timeout /t 1 >nul
set /a RETRY_COUNT+=1
echo [%TIME%] Attempt !RETRY_COUNT!/!MAX_RETRIES!... >> "%LOG_FILE%"
move /Y "%NEW_EXE%" "%CURRENT_EXE%" >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
    if !RETRY_COUNT! GEQ !MAX_RETRIES! (
        echo [%TIME%] FAILED >> "%LOG_FILE%"
        if exist "%BACKUP_EXE%" move /Y "%BACKUP_EXE%" "%CURRENT_EXE%" >> "%LOG_FILE%" 2>&1
        exit /b 1
    )
    goto RETRY
)
echo [%TIME%] SUCCESS >> "%LOG_FILE%"
if exist "%BACKUP_EXE%" del /Q "%BACKUP_EXE%" >> "%LOG_FILE%" 2>&1
echo [%TIME%] Launching... >> "%LOG_FILE%"
start "" "%CURRENT_EXE%"
timeout /t 2 /nobreak >nul
del /Q "%BATCH_FILE%" >> "%LOG_FILE%" 2>&1
exit
"""

    def _install_update(self, new_exe_path: str) -> None:
        try:
            current_exe = sys.executable
            app_dir = os.path.dirname(current_exe)
            bat_path = os.path.join(app_dir, "_update_temp.bat")
            backup_exe = f"{current_exe}.backup"
            log_path = os.path.join(app_dir, "update.log")
            if os.path.exists(current_exe):
                shutil.copy2(current_exe, backup_exe)
            bat_content = self._UPDATE_BATCH_TEMPLATE.format(new_exe=f'"{new_exe_path}"',
                                                             current_exe=f'"{current_exe}"',
                                                             backup_exe=f'"{backup_exe}"', log_path=f'"{log_path}"')
            with open(bat_path, 'w', encoding='utf-8') as f:
                f.write(bat_content)
            subprocess.Popen([bat_path], creationflags=subprocess.CREATE_NO_WINDOW | subprocess.DETACHED_PROCESS)
            QTimer.singleShot(300, QApplication.quit)
        except Exception as e:
            QMessageBox.critical(self, "Ошибка установки", f"Не удалось применить обновление:\n{str(e)}")


# ==================== ЗАПУСК ====================
if __name__ == "__main__":
    def _global_excepthook(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        import traceback
        err_msg = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        print(f"🔥 CRITICAL ERROR:\n{err_msg}")


    sys.excepthook = _global_excepthook

    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setApplicationName("YNIC & DLS Searcher")
    app.setApplicationVersion(CURRENT_VERSION)
    font_name = load_font()
    app.setFont(QFont(font_name, 12))
    window = SearcherApp(font_name=font_name)
    window.show()
    sys.exit(app.exec())