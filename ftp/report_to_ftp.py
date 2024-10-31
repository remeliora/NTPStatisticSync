import datetime
import ftplib
import json
import logging
import os


class FTPUploader:
    """
        Класс для загрузки файлов на FTP-серверы. Выполняет загрузку конфигурации,
        настройку логирования и загрузку файлов в зависимости от типа загрузки (ежедневной или ежемесячной).
    """

    def __init__(self, config_path=None):
        if config_path is None:
            config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "common_ftp_config.json")

        self.config = self.load_json_config(config_path)

        if not self.config:
            logging.error(f"Основная конфигурация не найдена или пуста: {config_path}")

        self.log_path = self.config["folders_path"]["log_path"]
        self.setup_logging()
        logging.info("Конфигурация загружена.")
        logging.info("Запуск отправки статистики NTPD.")

        self.local_ftp = self.config["local_ftp"]
        self.public_ftp = self.config["public_ftp"]
        self.final_data_path = self.config["folders_path"]["final_data_path"]
        self.final_day_data_path = self.config["folders_path"]["final_day_data_path"]

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
        logging.basicConfig(
            filename=os.path.join(self.log_path, "report_to_ftp.log"),
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

    def upload_files(self, ftp_config, source_folder, dynamic_path=None):
        """
            Загружает файлы на указанный FTP-сервер из указанной директории.
            Если передан dynamic_path, используется он, иначе используется стандартный путь из конфигурации.
        """
        ftp_path = dynamic_path if dynamic_path else ftp_config["ftp_path"]

        try:
            with ftplib.FTP(ftp_config["ftp_host"]) as ftp:
                ftp.login(ftp_config["ftp_user"], ftp_config["ftp_pass"])
                ftp.cwd(ftp_path)
                logging.info(f"Подключение к {ftp_config['ftp_host']} на пути {ftp_path} установлено.")

                for file_name in os.listdir(source_folder):
                    file_path = os.path.join(source_folder, file_name)
                    if os.path.isfile(file_path):
                        with open(file_path, "rb") as file:
                            ftp.storbinary(f"STOR {file_name}", file)
                            logging.info(f"Файл {file_name} загружен на {ftp_config['ftp_host']} в {ftp_path}.")
        except ftplib.all_errors as e:
            logging.error(f"Ошибка FTP: {e}")

    def execute_transfer(self):
        """
            Определяет, какой тип загрузки выполнять (ежедневный или ежемесячный), и выполняет отправку файлов.
        """
        now = datetime.datetime.now()

        # Ежедневная отправка
        if now.hour == 10:
            logging.info("Ежедневная загрузка началась.")
            self.upload_files(self.local_ftp, self.final_day_data_path)
            self.upload_files(self.public_ftp, self.final_day_data_path)

        # Ежемесячная отправка
        if now.day == 1 and now.hour == 10:
            logging.info("Ежемесячная загрузка началась.")
            year_str = now.strftime("%Y")
            dynamic_ftp_path = self.public_ftp["ftp_path_template"].format(year=year_str)

            self.upload_files(self.local_ftp, self.final_data_path)
            self.upload_files(self.public_ftp, self.final_data_path, dynamic_path=dynamic_ftp_path)


uploader = FTPUploader()
uploader.execute_transfer()
