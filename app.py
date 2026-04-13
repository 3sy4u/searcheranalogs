import io
import os
import sys

import gspread
import pandas as pd
import requests
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer, QPropertyAnimation
from PyQt6.QtGui import QColor, QFontDatabase, QFont, QGuiApplication, QIcon
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLineEdit, QPushButton, QLabel, QTableWidget, QTableWidgetItem,
    QHeaderView, QAbstractItemView, QDialog, QTextEdit, QFormLayout,
    QMessageBox
)
from google.oauth2.service_account import Credentials

# ==================== КОНФИГУРАЦИЯ ОБНОВЛЕНИЯ ====================
CURRENT_VERSION = "1.0.0"  # 🔹 Меняйте это число перед сборкой новой версии!
UPDATE_CHECK_URL = "https://raw.githubusercontent.com/3sy4u/searcheranalogs/refs/heads/main/version.json"
# Или прямая ссылка на файл на вашем хостинге

# ==================== КОНФИГУРАЦИЯ ====================
URL = "https://docs.google.com/spreadsheets/d/1qviJPyDXzN_DKPD1tVMdsPW_IVl-3Fn2yQtEzuK0XFc/edit?usp=sharing"
SPREADSHEET_ID = URL.split("/d/")[1].split("/")[0]

# Порядок столбцов строго как в твоей таблице
BRAND_COLUMNS = ["Lincoln", "CisoLube", "Tribo", "KOCU", "Bijur Delimon", "MecLube"]

COLUMN_SPECS = "Характеристики"

FONT_FILE = "Sansation-Bold.ttf"
FONT_FALLBACK = "Arial"
ICON_FILE = "app_icon.ico"
def get_resource_path(filename):
    if hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, filename)
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)

# И использовать:
SERVICE_ACCOUNT_FILE = get_resource_path("service_account.json")


# ==================== ЗАГРУЗКА ШРИФТА ====================
def load_font():
    if not QApplication.instance():
        return FONT_FALLBACK

    if hasattr(sys, "_MEIPASS"):
        base_path = sys._MEIPASS
    else:
        base_path = os.path.dirname(os.path.abspath(__file__))

    font_path = os.path.join(base_path, FONT_FILE)
    if not os.path.exists(font_path):
        return FONT_FALLBACK

    font_id = QFontDatabase.addApplicationFont(font_path)
    families = QFontDatabase.applicationFontFamilies(font_id)
    return families[0] if families else FONT_FALLBACK


# ==================== ПОТОК ЗАГРУЗКИ ДАННЫХ ====================
class LoadWorker(QThread):
    finished = pyqtSignal(object)
    error = pyqtSignal(str)

    def run(self):
        try:
            d_url = URL.split("/edit")[0] + "/export?format=xlsx"
            resp = requests.get(d_url, timeout=20)
            resp.raise_for_status()
            df = pd.read_excel(io.BytesIO(resp.content), header=0, dtype=str, engine='openpyxl', keep_default_na=False)
            df.columns = [str(col).strip() for col in df.columns]
            df = df.dropna(how='all').reset_index(drop=True)
            self.finished.emit(df)
        except Exception as e:
            self.error.emit(str(e))


# ==================== ПОТОК ДОБАВЛЕНИЯ АРТИКУЛА ====================
class AddArticleWorker(QThread):
    finished = pyqtSignal(bool, str, object)

    def __init__(self, articles_dict, extra_data, existing_cache=None, check_duplicates=True, force_add=False):
        super().__init__()
        self.articles_dict = articles_dict
        self.extra_data = extra_data
        self.existing_cache = existing_cache
        self.check_duplicates = check_duplicates
        self.force_add = force_add  # 🔹 Новый флаг

    def run(self):
        try:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file"
            ]
            creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(SPREADSHEET_ID)
            worksheet = sh.get_worksheet(0)

            headers = worksheet.row_values(1)

            # 🔹 Проверка дубликатов ТОЛЬКО если не принудительная запись
            existing_articles = set()
            duplicates = {}

            if self.check_duplicates and not self.force_add:
                if self.existing_cache is None:
                    all_values = worksheet.get_all_values()
                    for row in all_values[1:]:
                        for cell in row:
                            val = str(cell).strip().lower()
                            if val and val not in ["nan", "none", ""]:
                                existing_articles.add(val)
                else:
                    existing_articles = self.existing_cache.copy()

                for brand, article in self.articles_dict.items():
                    art_clean = article.strip().lower()
                    if art_clean and art_clean in existing_articles:
                        duplicates[brand] = article.strip()

                # Если есть дубликаты — останавливаемся и предупреждаем
                if duplicates:
                    self.finished.emit(False, "duplicates_warning", {
                        "duplicates": duplicates,
                        "all_articles": self.articles_dict,
                        "extra_data": self.extra_data
                    })
                    return

            # Если проверка пройдена (или принудительно игнорируем) — добавляем
            # Собираем всё в один словарь для удобной подстановки
            full_data = {**self.articles_dict, **self.extra_data}

            row_to_append = []
            for header in headers:
                val = full_data.get(header.strip(), "")
                row_to_append.append(val)

            worksheet.append_row(row_to_append, value_input_option='RAW')

            # Считаем, сколько полей реально заполнено
            filled_count = sum(1 for x in row_to_append if x)
            self.finished.emit(True, f"✅ Добавлено! (Заполнено ячеек: {filled_count})", None)

        except Exception as e:
            self.finished.emit(False, f"Ошибка: {str(e)}", None)


# ==================== ТАБЛИЦА ====================
class CustomTableWidget(QTableWidget):
    def keyPressEvent(self, event):
        if event.key() == Qt.Key.Key_C and event.modifiers() == Qt.KeyboardModifier.ControlModifier:
            selected = self.selectedItems()
            if not selected:
                super().keyPressEvent(event)
                return
            if len(selected) == 1:
                val = selected[0].text()
                if val != "—":
                    QGuiApplication.clipboard().setText(val)
                super().keyPressEvent(event)
                return

            rows = sorted(set(item.row() for item in selected))
            cols = sorted(set(item.column() for item in selected))
            result = []
            for r in rows:
                row_vals = [self.item(r, c).text() if self.item(r, c) else "" for c in cols]
                row_vals = [v if v != "—" else "" for v in row_vals]
                result.append("\t".join(row_vals))
            QGuiApplication.clipboard().setText("\n".join(result))
            super().keyPressEvent(event)
            return
        super().keyPressEvent(event)


# ==================== ОКНО ДОБАВЛЕНИЯ ====================
class AddArticleDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Добавить артикулы и данные")
        self.setMinimumSize(650, 600)
        self.parent = parent
        self.worker = None
        self.pending_data = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)

        # --- Блок ОБЩИХ ДАННЫХ ---
        common_group = QWidget()
        common_layout = QVBoxLayout(common_group)
        common_layout.setContentsMargins(0, 0, 0, 10)

        lbl_common = QLabel("<b>Общая информация:</b>")
        lbl_common.setStyleSheet("color: #aaa; font-size: 14px;")
        common_layout.addWidget(lbl_common)

        self.edit_name = QTextEdit()
        self.edit_name.setPlaceholderText("Название (опционально)...")
        self.edit_name.setMaximumHeight(50)
        common_layout.addWidget(self.edit_name)

        self.edit_specs = QTextEdit()
        self.edit_specs.setPlaceholderText("Характеристики (опционально)...")
        self.edit_specs.setMinimumHeight(80)
        common_layout.addWidget(self.edit_specs)

        layout.addWidget(common_group)

        # --- Блок АРТИКУЛОВ БРЕНДОВ ---
        form = QFormLayout()
        form.setSpacing(12)
        self.article_edits = {}

        lbl_brands = QLabel("<b>Артикулы брендов:</b>")
        lbl_brands.setStyleSheet("color: #aaa; font-size: 14px; padding-top: 10px;")
        form.addRow(lbl_brands)

        for brand in BRAND_COLUMNS:
            edit = QLineEdit()
            edit.setPlaceholderText(f"Артикул для {brand}")
            form.addRow(f"{brand}:", edit)
            self.article_edits[brand] = edit
            edit.textChanged.connect(lambda text, b=brand: self._check_duplicate_live(b, text))

        layout.addLayout(form)

        # --- КНОПКИ ---
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        self.btn_save = QPushButton("💾 Сохранить")
        self.btn_save.clicked.connect(self.start_saving)
        btn_layout.addWidget(self.btn_save)

        self.btn_close = QPushButton("Отмена")
        self.btn_close.clicked.connect(self.close)
        btn_layout.addWidget(self.btn_close)

        layout.addLayout(btn_layout)

    def _check_duplicate_live(self, brand, text):
        text_clean = text.strip().lower()
        cache = self.parent.existing_articles_cache if self.parent else set()
        edit = self.article_edits[brand]

        if text_clean and text_clean in cache:
            edit.setStyleSheet(
                "border: 2px solid #e74c3c; background-color: #3d1f1f; color: #ff9999; border-radius: 6px; padding: 8px 16px;")
            edit.setToolTip(f"⚠️ Уже есть в базе")
        else:
            edit.setStyleSheet("")
            edit.setToolTip("")

    def start_saving(self, force_add=False):
        extra_data = {
            "Название": self.edit_name.toPlainText().strip(),
            COLUMN_SPECS: self.edit_specs.toPlainText().strip()
        }

        articles_dict = {}
        has_any = False
        for brand, edit in self.article_edits.items():
            text = edit.text().strip()
            articles_dict[brand] = text
            if text:
                has_any = True

        if not has_any and not any(extra_data.values()):
            QMessageBox.warning(self, "Ошибка", "Заполните хотя бы одно поле!")
            return

        # 🔹 ПОЛНОСТЬЮ ИСПРАВЛЕННАЯ СТРОКА (с двоеточием и полным названием переменной)
        if force_add and self.pending_data:
            articles_dict = self.pending_data["all_articles"]
            extra_data = self.pending_data["extra_data"]

        self.btn_save.setEnabled(False)
        self.btn_save.setText("Сохранение...")

        cache = self.parent.existing_articles_cache if self.parent else None
        check_dup = not force_add

        self.worker = AddArticleWorker(
            articles_dict,
            extra_data,
            existing_cache=cache,
            check_duplicates=check_dup,
            force_add=force_add
        )
        self.worker.finished.connect(self.on_save_finished)
        self.worker.start()

    def on_save_finished(self, success, message, extra_data_payload):
        self.btn_save.setEnabled(True)
        self.btn_save.setText("💾 Сохранить")

        if not success and message == "duplicates_warning" and extra_data_payload:
            duplicates = extra_data_payload.get("duplicates", {})
            self.pending_data = extra_data_payload

            dup_lines = [f"• {brand}: <b>{art}</b>" for brand, art in duplicates.items()]

            html_content = f"""
            <div style='font-family: sans-serif; font-size: 14px; line-height: 1.4;'>
                <h3 style='color: #f39c12; margin-top: 0;'>⚠️ Внимание: найдены дубликаты</h3>
                <p>Следующие артикулы уже есть в базе:</p>
                <div style='background-color: #3d1f1f; padding: 10px; border-radius: 6px; margin: 10px 0; border-left: 3px solid #e74c3c;'>
                    {'<br>'.join(dup_lines)}
                </div>
                <p><b>Вы хотите добавить их всё равно?</b><br>
                <span style='color: #aaa;'>(Это создаст дублирующиеся строки в таблице)</span></p>
            </div>
            """

            confirm_dialog = QDialog(self)
            confirm_dialog.setWindowTitle("Подтверждение")
            confirm_dialog.setMinimumWidth(450)
            confirm_dialog.setStyleSheet("QDialog { background-color: #2b2b2b; } QLabel { color: #ffffff; }")

            dlg_layout = QVBoxLayout(confirm_dialog)
            label = QLabel(html_content)
            label.setTextFormat(Qt.TextFormat.RichText)
            dlg_layout.addWidget(label)

            btn_box = QHBoxLayout()
            btn_force = QPushButton("🔥 Да, добавить дубликаты")
            btn_force.setStyleSheet(
                "background-color: #e74c3c; color: #ffffff; border: none; border-radius: 6px; padding: 10px 20px; font-weight: bold;")
            btn_force.clicked.connect(lambda: confirm_dialog.accept())

            btn_cancel = QPushButton("Нет, отмена")
            btn_cancel.setStyleSheet(
                "background-color: #555555; color: #ffffff; border: none; border-radius: 6px; padding: 10px 20px;")
            btn_cancel.clicked.connect(lambda: confirm_dialog.reject())

            btn_box.addWidget(btn_force)
            btn_box.addWidget(btn_cancel)
            dlg_layout.addLayout(btn_box)

            if confirm_dialog.exec() == QDialog.DialogCode.Accepted:
                self.start_saving(force_add=True)
            else:
                self.pending_data = None
            return

        if success:
            QMessageBox.information(self, "Успешно", message)
            if self.parent:
                self.parent._load_data()
            self.close()
        else:
            QMessageBox.critical(self, "Ошибка", f"Не удалось сохранить:\n\n{message}")
            self.pending_data = None


class UpdaterWorker(QThread):
    progress = pyqtSignal(int, str)  # процент, сообщение
    finished_update = pyqtSignal(str)  # путь к новому файлу
    error = pyqtSignal(str)
    no_update = pyqtSignal()

    def __init__(self, current_version, check_url):
        super().__init__()
        self.current_version = current_version
        self.check_url = check_url

    def run(self):
        try:
            # 1. Проверка версии
            self.progress.emit(10, "Проверка обновлений...")
            resp = requests.get(self.check_url, timeout=5)
            resp.raise_for_status()
            data = resp.json()

            remote_version = data.get("version", "0.0.0")
            download_url = data.get("download_url")
            changelog = data.get("changelog", "")

            # Сравнение верностей (простое строковое, если формат X.Y.Z)
            if self._compare_versions(remote_version, self.current_version) <= 0:
                self.no_update.emit()
                return

            self.progress.emit(30, f"Найдена версия {remote_version}. Скачивание...")

            # 2. Скачивание во временную папку
            if not download_url:
                raise Exception("Нет ссылки на скачивание в version.json")

            temp_dir = tempfile.gettempdir()
            new_exe_path = os.path.join(temp_dir, "analog_searcher_new.exe")

            with requests.get(download_url, stream=True) as r:
                r.raise_for_status()
                total_length = int(r.headers.get('content-length', 0))
                downloaded = 0

                with open(new_exe_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_length > 0:
                                percent = 30 + int((downloaded / total_length) * 70)
                                self.progress.emit(percent, f"Скачано {percent}%")

            self.progress.emit(100, "Готово!")
            self.finished_update.emit(new_exe_path)

        except Exception as e:
            self.error.emit(str(e))

    def _compare_versions(self, v1, v2):
        """Сравнивает версии вида '1.0.1' и '1.0.2'. Возвращает 1, 0 или -1"""
        v1_parts = list(map(int, v1.split('.')))
        v2_parts = list(map(int, v2.split('.')))

        for i in range(max(len(v1_parts), len(v2_parts))):
            val1 = v1_parts[i] if i < len(v1_parts) else 0
            val2 = v2_parts[i] if i < len(v2_parts) else 0
            if val1 > val2: return 1
            if val1 < val2: return -1
        return 0
# ==================== ГЛАВНОЕ ОКНО ====================
# ==================== ГЛАВНОЕ ОКНО ====================
class SearcherApp(QMainWindow):
    def __init__(self, font_name: str):
        super().__init__()
        self.df = None
        self.font_name = font_name
        self.existing_articles_cache = set()

        self.search_timer = QTimer()
        self.search_timer.setSingleShot(True)
        self.search_timer.timeout.connect(self.search)

        self.setWindowTitle("YNIC & DLS Searcher")
        self.setMinimumSize(980, 700)

        self._set_window_icon()
        self._apply_styles()
        self._build_ui()
        self._load_data()

        # 🔹 Автопроверка обновлений через 2 сек после старта
        QTimer.singleShot(2000, self.check_for_updates)

    def _set_window_icon(self):
        if hasattr(sys, "_MEIPASS"):
            base_path = sys._MEIPASS
        else:
            base_path = os.path.dirname(os.path.abspath(__file__))
        icon_path = os.path.join(base_path, ICON_FILE)
        if os.path.exists(icon_path):
            icon = QIcon(icon_path)
            self.setWindowIcon(icon)
            QApplication.setWindowIcon(icon)

    def _apply_styles(self):
        f = self.font_name
        self.setStyleSheet(f"""
            QMainWindow, QWidget#central {{ background-color: #2b2b2b; }}
            QLabel#title {{ color: #ffffff; font-size: 22px; font-family: "{f}"; padding: 16px; background-color: #3a3a3a; }}
            QLabel#status {{ color: #aaaaaa; font-size: 13px; font-family: "{f}"; padding: 4px 0; }}
            QLabel#beta {{ color: #800000; font-size: 92px; font-family: "{f}"; font-weight: bold; opacity: 0.5; }}
            QLineEdit {{ background-color: #3a3a3a; color: #ffffff; border: 1px solid #555555; 
                border-radius: 6px; font-size: 18px; font-family: "{f}"; padding: 8px 16px; }}
            QPushButton#btn_search, QPushButton#btn_add {{ 
                background-color: #800000; color: #ffffff; border: none; border-radius: 6px; 
                font-size: 15px; font-family: "{f}"; padding: 10px 20px; }}
            QPushButton#btn_search:hover, QPushButton#btn_add:hover {{ background-color: #a00000; }}
            QPushButton#btn_params {{ 
                background-color: #555555; color: #888888; border: none; border-radius: 6px; 
                font-size: 15px; font-family: "{f}"; padding: 10px 20px; }}
            QPushButton#btn_refresh {{ 
                background-color: #3a3a3a; color: #aaaaaa; border: 1px solid #555555; 
                border-radius: 6px; font-size: 13px; font-family: "{f}"; padding: 8px 20px; 
            }}
            QTableWidget {{ background-color: #2b2b2b; color: #ffffff; gridline-color: #444444; border: none; 
                font-size: 14px; font-family: "{f}"; }}
            QTableWidget::item:selected {{ background-color: #800000; color: #ffffff; }}
        """)

    def _build_ui(self):
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

        self.beta_label = QLabel("BETA")
        self.beta_label.setObjectName("beta")
        self.beta_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignBottom)
        layout.addWidget(self.beta_label)

    def open_add_dialog(self):
        dialog = AddArticleDialog(self)
        dialog.exec()

    def perform_search(self):
        if self.btn_search.isEnabled():
            original = self.btn_search.geometry()
            anim = QPropertyAnimation(self.btn_search, b"geometry")
            anim.setDuration(120)
            anim.setKeyValueAt(0, original)
            anim.setKeyValueAt(0.5, original.adjusted(0, 2, 0, 2))
            anim.setKeyValueAt(1, original)
            anim.start()
        self.search()

    def _load_data(self):
        self.btn_search.setEnabled(False)
        self.btn_refresh.setEnabled(False)
        self._set_status("● Обновление...", "#f39c12")

        self.worker = LoadWorker()
        self.worker.finished.connect(self._on_load_success)
        self.worker.error.connect(self._on_load_error)
        self.worker.start()

    def _on_load_success(self, df):
        self.df = df
        self._build_cache_from_df(df)
        self._set_status(f"● База загружена: {len(self.df)} поз.", "#27ae60")
        self.btn_search.setEnabled(True)
        self.btn_refresh.setEnabled(True)

    def _on_load_error(self, msg):
        self._set_status(f"● Ошибка: {msg}", "#e74c3c")
        self.btn_refresh.setEnabled(True)

    def _set_status(self, text, color):
        self.lbl_status.setText(text)
        self.lbl_status.setStyleSheet(
            f"color: {color}; font-size: 13px; font-family: '{self.font_name}'; padding: 4px 0;")

    def on_text_changed(self):
        self.search_timer.start(300)

    def search(self):
        if self.df is None:
            return
        query = self.entry.text().strip().lower()
        self.table.setRowCount(0)

        if not query:
            self._set_status("● Введите часть артикула...", "#aaaaaa")
            return

        found_count = 0
        for idx, row in self.df.iterrows():
            for col in self.df.columns:
                if query in str(row[col]).strip().lower():
                    row_idx = self.table.rowCount()
                    self.table.insertRow(row_idx)

                    for col_idx, brand in enumerate(BRAND_COLUMNS):
                        val = str(row.get(brand, "")).strip()
                        display = "—" if val.lower() in ["nan", "none", ""] else val
                        item = QTableWidgetItem(display)
                        item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                        if display == "—":
                            item.setForeground(QColor("#555555"))
                        item.setData(Qt.ItemDataRole.UserRole, idx)
                        self.table.setItem(row_idx, col_idx, item)

                    found_count += 1
                    break

        if found_count > 0:
            self._set_status(f"✅ Найдено: {found_count}", "#2980b9")
            if self.table.rowCount() > 0:
                self.table.selectRow(0)
        else:
            self._set_status(f"❌ Ничего не найдено по «{query}»", "#e74c3c")

    def _open_specs(self, index):
        if self.df is None:
            return
        item = self.table.item(index.row(), index.column())
        if not item:
            return
        df_index = item.data(Qt.ItemDataRole.UserRole)
        if df_index is None or df_index not in self.df.index:
            return

        row = self.df.loc[df_index]
        specs_text = str(row.get(COLUMN_SPECS, "")).strip() or "Характеристики не заполнены"

        title_parts = [f"{brand}: {str(row.get(brand, '')).strip()}"
                       for brand in BRAND_COLUMNS
                       if str(row.get(brand, '')).strip().lower() not in ["nan", "none", ""]]

        title = "  |  ".join(title_parts) if title_parts else "Характеристики"

        dialog = QDialog(self)
        dialog.setWindowTitle("Характеристики")
        dialog.setMinimumSize(520, 420)
        dialog.setStyleSheet(f"""
            QDialog {{ background-color: #2b2b2b; }}
            QLabel {{ color: #cccccc; font-size: 12px; font-family: "{self.font_name}"; padding: 12px 16px 4px; }}
            QTextEdit {{ background-color: #3a3a3a; color: #ffffff; border: 1px solid #555555; 
                        border-radius: 6px; font-size: 14px; font-family: "{self.font_name}"; padding: 12px; margin: 0 16px; }}
            QPushButton {{ background-color: #800000; color: #ffffff; border: none; border-radius: 6px; 
                          font-size: 13px; font-family: "{self.font_name}"; padding: 8px 24px; margin: 8px; }}
            QPushButton:hover {{ background-color: #a00000; }}
        """)

        dlg_layout = QVBoxLayout(dialog)
        dlg_layout.setContentsMargins(0, 0, 0, 8)

        lbl = QLabel(title)
        lbl.setWordWrap(True)
        dlg_layout.addWidget(lbl)

        text_area = QTextEdit()
        text_area.setPlainText(specs_text)
        text_area.setReadOnly(True)
        dlg_layout.addWidget(text_area)

        btn_box = QHBoxLayout()
        btn_box.addStretch()
        btn_copy = QPushButton("КОПИРОВАТЬ")
        btn_copy.clicked.connect(lambda: (
            QGuiApplication.clipboard().setText(specs_text),
            self._set_status("📋 Характеристики скопированы", "#2980b9")
        ))
        btn_box.addWidget(btn_copy)

        btn_close = QPushButton("ЗАКРЫТЬ")
        btn_close.clicked.connect(dialog.close)
        btn_box.addWidget(btn_close)

        dlg_layout.addLayout(btn_box)
        dialog.exec()

    def _build_cache_from_df(self, df):
        self.existing_articles_cache.clear()
        for col in BRAND_COLUMNS:
            if col in df.columns:
                for val in df[col].dropna().astype(str):
                    val_clean = val.strip().lower()
                    if val_clean and val_clean not in ["nan", "none", ""]:
                        self.existing_articles_cache.add(val_clean)

    # ==================== АВТООБНОВЛЕНИЕ ====================
    def check_for_updates(self):
        """Запускает фоновую проверку обновлений"""
        # Убедитесь, что UpdaterWorker импортирован или объявлен выше этого класса
        self.updater = UpdaterWorker(CURRENT_VERSION, UPDATE_CHECK_URL)
        self.updater.progress.connect(self._on_update_progress)
        self.updater.finished_update.connect(self._on_update_ready)
        self.updater.error.connect(self._on_update_error)
        self.updater.no_update.connect(self._on_no_update)
        self.updater.start()

    def _on_update_progress(self, percent, msg):
        self.lbl_status.setText(f"🔄 {msg}")
        self.lbl_status.setStyleSheet("color: #f39c12; font-size: 13px;")

    def _on_no_update(self):
        # Тихо обновляем статус, не мешая пользователю
        self.lbl_status.setText("● База загружена" if self.df is not None else "● Готово")
        self.lbl_status.setStyleSheet("color: #27ae60; font-size: 13px;")

    def _on_update_error(self, msg):
        self.lbl_status.setText("● Ошибка проверки обновлений")
        self.lbl_status.setStyleSheet("color: #e74c3c; font-size: 13px;")
        print(f"[Update] Error: {msg}")

    def _on_update_ready(self, new_exe_path):
        """Показывает диалог подтверждения установки"""
        reply = QMessageBox.question(
            self,
            "Доступно обновление",
            "Найдена новая версия программы!\n\nУстановить сейчас?\nПрограмма перезапустится автоматически.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if reply == QMessageBox.StandardButton.Yes:
            self._install_update(new_exe_path)

    def _install_update(self, new_exe_path):
        """Заменяет старый .exe на новый и перезапускает приложение"""
        try:
            current_exe = sys.executable
            bat_path = os.path.join(os.path.dirname(current_exe), "update_self.bat")

            with open(bat_path, 'w', encoding='utf-8') as f:
                f.write(f"""@echo off
echo Installing update...
timeout /t 2 /nobreak >nul
del "{current_exe}"
move "{new_exe_path}" "{current_exe}"
echo Launching...
start "" "{current_exe}"
del "%~f0"
exit
                """)

            # Запускаем батник скрыто и закрываем GUI
            subprocess.Popen(bat_path, creationflags=subprocess.CREATE_NO_WINDOW)
            QApplication.quit()

        except Exception as e:
            QMessageBox.critical(self, "Ошибка установки", f"Не удалось применить обновление:\n{str(e)}")
            self.lbl_status.setText("● Ошибка обновления")
            self.lbl_status.setStyleSheet("color: #e74c3c;")


# ==================== ЗАПУСК ====================
if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    font_name = load_font()
    app.setFont(QFont(font_name, 12))

    window = SearcherApp(font_name=font_name)
    window.show()
    sys.exit(app.exec())