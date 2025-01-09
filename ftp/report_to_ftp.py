import datetime
import ftplib
import json
import logging
import time
from pathlib import Path
from telegram_bot.telegram_bot import TelegramBot


class FTPUploader:
    """
        Класс для загрузки файлов на FTP-серверы. Выполняет загрузку конфигурации,
        настройку логирования и загрузку файлов в зависимости от типа загрузки (ежедневной или ежемесячной).
    """

    def __init__(self, config_path=None, max_retries=3, retry_delay=5):
        """
            Инициализирует объект FTPUploader. Загружает конфигурацию, настраивает логи, задает конфигурацию для
            FTP-серверов и количество повторов и задержек для повторных подключений.
        """
        config_path = config_path or Path(__file__).parent / "common_ftp_config.json"
        self.config = self.load_json_config(config_path)

        self.log_path = Path(self.config["folders_path"]["log_path"])
        self.setup_logging()

        if not self.config:
            logging.error(f"Основная конфигурация не найдена или пуста: {config_path}")

            return
        else:
            logging.info("Конфигурация загружена.")

        self.telegram_bot = TelegramBot()

        logging.info("Запуск отправки статистики NTPD.")

        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.local_ftp = self.config["local_ftp"]
        self.public_ftp = self.config["public_ftp"]
        self.final_data_path = Path(self.config["folders_path"]["final_data_path"])
        self.final_day_data_path = Path(self.config["folders_path"]["final_day_data_path"])

    @staticmethod
    def load_json_config(file_path):
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

    def setup_logging(self):
        """
            Настраивает логирование: создает лог-файл для записи всех действий скрипта.
        """
        log_path = Path(self.config["folders_path"]["log_path"]) / "report_to_ftp.log"
        logging.basicConfig(
            filename=log_path,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

    def connect_to_ftp(self, ftp_config):
        """
            Подключение к FTP-серверу с повторными попытками.
        """
        attempt = 0
        while attempt < self.max_retries:
            try:
                ftp = ftplib.FTP(ftp_config["ftp_host"])
                ftp.login(ftp_config["ftp_user"], ftp_config["ftp_pass"])
                logging.info(f"Подключение к {ftp_config['ftp_host']} установлено.")
                return ftp

            except ftplib.all_errors as e:
                attempt += 1
                logging.error(f"Ошибка подключения к {ftp_config['ftp_host']}: {e}")

                if attempt < self.max_retries:
                    logging.info(f"Повторная попытка подключения через {self.retry_delay} секунд...")
                    time.sleep(self.retry_delay)
        error_message = f"Не удалось подключиться к {ftp_config['ftp_host']} после {self.max_retries} попыток."
        logging.error(f"Не удалось подключиться к {ftp_config['ftp_host']} после {self.max_retries} попыток.")
        self.telegram_bot.send_message(error_message)

        return None

    def upload_files(self, ftp, source_folder, ftp_path):
        """
            Загружает файлы на уже подключенный FTP-сервер.
        """
        try:
            ftp.cwd(ftp_path)
            logging.info(f"Текущий FTP-путь установлен на {ftp_path}.")

            for file_path in source_folder.iterdir():
                if file_path.is_file():
                    with open(file_path, "rb") as file:
                        ftp.storbinary(f"STOR {file_path.name}", file)
                        logging.info(f"Файл {file_path.name} загружен в {ftp_path}.")
            info_message = f"Загрузка файлов в {ftp_path} успешно завершена."
            logging.info(f"Загрузка файлов в {ftp_path} успешно завершена.")
            self.telegram_bot.send_message(info_message)

        except ftplib.all_errors as e:
            error_message = f"Ошибка при загрузке файлов в {ftp_path}: {e}"
            logging.error(f"Ошибка при загрузке файлов в {ftp_path}: {e}")
            self.telegram_bot.send_message(error_message)

    def execute_transfer(self):
        """
            Определяет, какой тип загрузки выполнять (ежедневный или ежемесячный), и выполняет отправку файлов.
        """
        now = datetime.datetime.now()

        # Ежемесячная отправка
        if now.day == 1 and now.hour == 10:
            logging.info("Ежемесячная загрузка началась.")
            # Если это январь, используем предыдущий год для файлов за декабрь
            year_str = str(now.year - 1) if now.month == 1 else str(now.year)
            dynamic_ftp_path = self.public_ftp["ftp_path_template"].format(year=year_str)

            ftp = self.connect_to_ftp(self.public_ftp)
            if ftp:
                self.create_ftp_directory_if_not_exists(ftp, dynamic_ftp_path)  # Проверяем и создаём папку для года
                self.upload_files(ftp, self.final_data_path, dynamic_ftp_path)
                ftp.quit()

        # Ежедневная отправка
        if now.hour == 10:
            logging.info("Ежедневная загрузка началась.")
            ftp = self.connect_to_ftp(self.local_ftp)
            if ftp:
                self.upload_files(ftp, self.final_day_data_path, self.local_ftp["ftp_path"])
                ftp.quit()

        logging.info("Процесс передачи файлов завершён.\n" + "=" * 124)

    def create_ftp_directory_if_not_exists(self, ftp, directory):
        """
            Проверяет, существует ли указанная директория на FTP-сервере, и создаёт её при необходимости.
        """
        try:
            ftp.cwd(directory)
            # logging.info(f"Директория {directory} уже существует на FTP-сервере.")
        except ftplib.error_perm as e:
            if str(e).startswith("550"):  # Ошибка "550: Directory not found"
                logging.warning(f"Директория {directory} отсутствует. Создаём...")
                try:
                    ftp.mkd(directory)
                    logging.info(f"Директория {directory} успешно создана.")
                except ftplib.all_errors as mkdir_error:
                    logging.error(f"Не удалось создать директорию {directory}: {mkdir_error}")
                    self.telegram_bot.send_message(f"Ошибка создания директории {directory}: {mkdir_error}")
            else:
                logging.error(f"Ошибка при проверке директории {directory}: {e}")
                self.telegram_bot.send_message(f"Ошибка при проверке директории {directory}: {e}")


uploader = FTPUploader()
uploader.execute_transfer()
