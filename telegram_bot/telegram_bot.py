import json
import logging
import requests
from pathlib import Path


class TelegramBot:
    def __init__(self, config_path=None):
        self.setup_logging()
        # Загрузка конфигурации
        config_path = config_path or Path(__file__).parent / "telegram_bot_config.json"
        self.config = self.load_config(config_path)
        if self.config:
            self.api_token = self.config.get("api_token")
            self.chat_id = self.config.get("chat_id")
        else:
            logging.error("Конфигурация не загружена. Бот не может быть инициализирован.")
            self.api_token = None
            self.chat_id = None

    @staticmethod
    def load_config(file_path):
        """
            Загружает конфигурацию из JSON-файла по заданному пути.
        """
        try:
            with open(file_path) as config_file:
                return json.load(config_file)

        except FileNotFoundError:
            logging.error(f"Файл конфигурации не найден: {file_path}")

            return None
        except json.JSONDecodeError as e:
            logging.error(f"Ошибка в синтаксисе конфигурации JSON: {e}")

            return None
        except Exception as e:
            logging.error("Ошибка при загрузке конфигурационного файла Telegram: %s", e)

            return None

    def setup_logging(self):
        """
            Настраивает логирование: создает лог-файл для записи всех действий скрипта с указанным форматом.
        """
        logging.basicConfig(
            filename="telegram_bot.log",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

    def send_message(self, message):
        """
            Отправляет сообщение в Telegram
        """
        url = f"https://api.telegram.org/bot{self.api_token}/sendMessage"
        data = {"chat_id": self.chat_id, "text": message}
        try:
            response = requests.post(url, data=data)
            if response.status_code == 200:
                logging.info("Сообщение отправлено: %s", message)
            else:
                logging.error("Ошибка при отправке сообщения: %s", response.text)
        except Exception as e:
            logging.error("Ошибка подключения к Telegram: %s", e)


bot = TelegramBot()
bot.send_message("Это тестовое сообщение от Telegram-бота.")

