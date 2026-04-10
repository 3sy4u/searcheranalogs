import gspread
from google.oauth2.service_account import Credentials
from PyQt6.QtWidgets import QApplication, QMainWindow, QPushButton, QMessageBox
import sys


class TestWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Тест подключения к Google Sheets")
        self.setGeometry(300, 300, 400, 200)

        btn = QPushButton("Проверить добавление артикула", self)
        btn.clicked.connect(self.test_add)
        btn.setGeometry(50, 50, 300, 50)

    def test_add(self):
        try:
            # Проверка 1: файл существует?
            creds = Credentials.from_service_account_file("service_account.json")

            # Проверка 2: scopes
            gc = gspread.authorize(creds)

            # Проверка 3: открытие таблицы
            sh = gc.open_by_key("1qviJPyDXzN_DKPD1tVMdsPW_IVl-3Fn2yQtEzuK0XFc")
            worksheet = sh.sheet1

            # Проверка 4: добавление строки
            test_row = ["TEST-LINCOLN", "", "", "", "", "", "Тестовая запись от диагностики"]
            worksheet.append_row(test_row, value_input_option='RAW')

            QMessageBox.information(self, "Успех!",
                                    "Тестовая строка успешно добавлена в таблицу!\n\n"
                                    "Проверьте последнюю строку в Google Таблице.")

        except FileNotFoundError:
            QMessageBox.critical(self, "Ошибка",
                                 "Файл 'service_account.json' не найден!\n\n"
                                 "Убедитесь, что он лежит в одной папке с программой.")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка",
                                 f"Ошибка:\n\n{type(e).__name__}\n{str(e)}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = TestWindow()
    window.show()
    sys.exit(app.exec())