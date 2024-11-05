import json
import logging
from pathlib import Path

import requests


class TelegramBot:
    def __init__(self, config_path=None):
        self.logger = self.setup_logging()
        # Загрузка конфигурации
        config_path = config_path or Path(__file__).parent / "telegram_bot_config.json"
        self.config = self.load_config(config_path)
        if self.config:
            self.api_token = self.config.get("api_token")
            self.chat_id = self.config.get("chat_id")
        else:
            self.logger.error("Конфигурация не загружена. Бот не может быть инициализирован.")
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
            logging.getLogger("TelegramBotLogger").error(f"Файл конфигурации не найден: {file_path}")

            return None
        except json.JSONDecodeError as e:
            logging.getLogger("TelegramBotLogger").error(f"Ошибка в синтаксисе конфигурации JSON: {e}")

            return None
        except Exception as e:
            logging.getLogger("TelegramBotLogger").error("Ошибка при загрузке конфигурационного файла Telegram: %s", e)

            return None

    def setup_logging(self):
        """
            Настраивает логирование: создает лог-файл для записи всех действий скрипта с указанным форматом.
        """
        logger = logging.getLogger("TelegramBotLogger")
        logger.setLevel(logging.INFO)
        log_file_path = Path(__file__).parent / "telegram_bot.log"
        file_handler = logging.FileHandler(log_file_path)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger

    def send_message(self, message):
        """
            Отправляет сообщение в Telegram
        """
        if not self.api_token or not self.chat_id:
            self.logger.error("Не удалось отправить сообщение: некорректные данные конфигурации.")
            return
        url = f"https://api.telegram.org/bot{self.api_token}/sendMessage"
        data = {"chat_id": self.chat_id, "text": message}
        try:
            response = requests.post(url, data=data)
            if response.status_code == 200:
                self.logger.info("Сообщение отправлено: %s", message)
            else:
                self.logger.error("Ошибка при отправке сообщения: %s", response.text)
        except Exception as e:
            self.logger.error("Ошибка подключения к Telegram: %s", e)


bot = TelegramBot()
# bot.send_startup_message()
