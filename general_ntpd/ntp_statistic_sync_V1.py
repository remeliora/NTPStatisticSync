import datetime
import ftplib
import json
import logging
import os
import shutil
import subprocess
import time


class NTPDataSync:
    """
        Класс для синхронизации данных NTP-статистики. Выполняет загрузку конфигурации, управление логами,
        создание нужных директорий, выполнение команды NTPQ, запись данных в файлы и их перемещение в
        финальные директории.
    """

    def __init__(self, config_path=None, local_ftp_config_path=None):
        """
            Инициализирует объект NTPDataSync. Загружает конфигурацию, настраивает логи, задает пути для файлов
            и проверяет существование финальных директорий.
        """
        if config_path is None:
            config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
        if local_ftp_config_path is None:
            local_ftp_config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "local_ftp_config.json")

        self.config = self.load_json_config(config_path)
        self.local_ftp_config = self.load_json_config(local_ftp_config_path)

        self.general_path = self.config["general_path"]
        self.setup_logging()

        if not self.config:
            logging.error(f"Основная конфигурация не найдена или пуста: {config_path}")
        if not self.local_ftp_config:
            logging.error(f"Конфигурация локального FTP не найдена или пуста: {local_ftp_config_path}")

        logging.info("Конфигурация загружена.")
        logging.info("Запуск синхронизации статистики NTPD.")

        self.report_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        self.file_paths = self.define_file_paths()
        self.ensure_final_directories()

        self.ntpd_drift = self.config["ntpd_drift_path"]
        self.drift_statistic_path = os.path.join(self.general_path, "NTP_DRIFT_STAT.txt")

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

    def upload_to_ftp(self, file_path):
        """
            Загрузка файла на FTP-сервер.
        """
        try:
            with ftplib.FTP(self.local_ftp_config["ftp_host"]) as ftp:
                ftp.login(
                    user=self.local_ftp_config["ftp_user"],
                    passwd=self.local_ftp_config["ftp_pass"]
                )
                ftp.cwd(self.local_ftp_config["ftp_path"])
                with open(file_path, "rb") as file:
                    ftp.storbinary(f"STOR {os.path.basename(file_path)}", file)
                logging.info(f"Файл {file_path} успешно отправлен на FTP-сервер.")
        except ftplib.all_errors as e:
            logging.error(f"Ошибка при отправке файла на FTP: {e}")

    def setup_logging(self):
        """
            Настраивает логирование: создает лог-файл для записи всех действий скрипта с указанным форматом.
        """
        self.ensure_directory_exists(self.general_path)

        logging.basicConfig(
            filename=os.path.join(self.general_path, "ntp_statistic_sync.log"),
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

    def define_file_paths(self):
        """
            Определяет пути для различных файлов на основе текущей даты и конфигурации.
        """
        now = datetime.datetime.now()
        date_id = now.strftime("%Y%m%d")
        month_id = now.strftime("%m")
        year_id = now.strftime("%Y")

        return {
            "daily_path": os.path.join(self.general_path, year_id, month_id,
                                       f"{self.config['file_prefix']}{date_id}.log"),
            "month_path": os.path.join(self.general_path, year_id, month_id,
                                       f"{self.config['file_prefix']}{date_id[:6]}.log"),
            "year_path": os.path.join(self.general_path, year_id, f"{self.config['file_prefix']}{date_id[:4]}.log"),
            "month_to_report_path": os.path.join(self.config["actual_data_path"],
                                                 f"{self.config['report_file_prefix']}{date_id[:6]}.log"),
            "day_to_report_path": os.path.join(self.config["actual_day_data_path"],
                                               f"{self.config['report_file_prefix']}{date_id}.log"),
            "short_ntpd_path": os.path.join(self.general_path, "ShortNtpd.log"),
            "final_day_path": os.path.join(self.config["final_day_data_path"],
                                           f"{self.config['report_file_prefix']}{date_id}.log"),
            "final_month_path": os.path.join(self.config["final_data_path"],
                                             f"{self.config['report_file_prefix']}{date_id[:6]}.log")
        }

    def ensure_final_directories(self):
        """
            Создает финальные директории, если они еще не существуют, для хранения данных за день и месяц.
        """
        final_directories = [
            self.config["final_day_data_path"],
            self.config["final_data_path"]
        ]
        for path in final_directories:
            self.ensure_directory_exists(path)

    @staticmethod
    def ensure_directory_exists(path):
        """
            Общий метод для проверки, существует ли указанный каталог. Если не существует, то создает его.
        """
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
            logging.info(f"Каталог {path} создан.")

    def check_and_restart_ntp_service(self):
        """
            Проверяет выполнение команды ntpq -pn. Если команда неуспешна, перезапускает сервис NTP
            и повторяет проверку.
        """
        logging.info("Проверка состояния сервиса NTP.")
        ntp_data = self.run_ntpq()

        if not ntp_data or not ntp_data.strip():
            logging.warning("Команда ntpq -pn завершилась с ошибкой. Перезапуск службы NTP...")
            self.restart_ntp_service()

            # Ждем 10 секунд перед повторной проверкой
            time.sleep(10)
            ntp_data = self.run_ntpq()

            if ntp_data:
                logging.info("Сервис NTP успешно перезапущен.")
            else:
                logging.error("Ошибка: Сервис NTP не удалось перезапустить.")

            if ntp_data.strip():
                logging.info("Целостность данных не нарушена.")
            else:
                logging.error("Ошибка: Сервис NTP возвращает пустые данные.")
        else:
            logging.info("Служба NTP работает корректно. Команда ntpq -pn выполнена успешно.")


    def restart_ntp_service(self):
        """
            Перезапускает службу NTP с помощью командной строки Windows.
        """
        try:
            subprocess.run(["net", "stop", "ntp"], check=True)
            time.sleep(10)  # Ждем перед перезапуском
            subprocess.run(["net", "start", "ntp"], check=True)
            logging.info("Команда ntpq -pn выполнена успешно. Служба NTP перезапущена.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Не удалось перезапустить службу NTP: {e}")

    def run_ntpq(self):
        """
            Выполняет команду 'ntpq -pn' для получения данных NTP-сервера и возвращает результат.
            Если произошла ошибка, возвращает пустую строку.
        """
        try:
            result = subprocess.run(["ntpq", "-pn"], capture_output=True, text=True)
            # logging.info("Команда ntpq -pn выполнена успешно. Служба NTP работает корректно.")
            return result.stdout
        except Exception as e:
            logging.error(f"Ошибка при выполнении ntpq -pn: {e}")
            return ""

    def update_drift_stat(self):
        """
            Обновляет файл NTP_DRIFT_STAT.txt, добавляя текущие дату и время, а затем содержимое ntp.drift.
        """
        drift_file = os.path.join(self.ntpd_drift, "ntp.drift")
        date_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            with open(self.drift_statistic_path, "a") as drift_stat, open(drift_file) as drift:
                drift_stat.write(f"{date_time} xyz\n")
                drift_stat.write(drift.read())
                logging.info(f"Файл {self.drift_statistic_path} успешно обновлен.")
        except FileNotFoundError:
            logging.error(f"Файл ntp.drift не найден в {self.ntpd_drift}")
        except Exception as e:
            logging.error(f"Ошибка при обновлении {self.drift_statistic_path}: {e}")

    def write_to_file(self, file_path, data, date_time, append=True):
        self.ensure_directory_exists(os.path.dirname(file_path))
        try:
            with open(file_path, "a" if append else "w") as f:
                f.write(f"{date_time}\n")
                f.write(data)
            logging.info(f"Записаны данные в файл {file_path}.")
        except Exception as e:
            logging.error(f"Ошибка при записи в файл {file_path}: {e}")

    def transfer_to_final(self, source_path, destination_path, is_monthly=False):
        """
            Перемещает файл из источника в финальную директорию.
            Также удаляет старые файлы в папке назначения, кроме текущего.
        """
        try:
            if os.path.exists(source_path):
                self.ensure_directory_exists(os.path.dirname(destination_path))
                shutil.move(source_path, destination_path)
                logging.info(f"Перенос файла {source_path} в {destination_path} завершен.")
                # Удаление старых файлов
                if is_monthly:
                    self.clean_final_directory(self.config["final_data_path"], exclude=[destination_path])
                else:
                    self.clean_final_directory(self.config["final_day_data_path"], exclude=[destination_path])
            else:
                logging.warning(f"Файл {source_path} не найден для переноса.")
        except Exception as e:
            logging.error(f"Ошибка при переносе файла {source_path} в {destination_path}: {e}")

    def clean_final_directory(self, directory, exclude=None):
        """
            Удаляет все файлы в указанной директории, кроме файлов, указанных в списке exclude.
        """
        if exclude is None:
            exclude = set()
        for file in os.listdir(directory):
            full_path = os.path.join(directory, file)
            if full_path not in exclude and os.path.isfile(full_path):
                os.remove(full_path)
                logging.info(f"Удален файл {full_path} из директории {directory}")

    def execute_sync(self):
        """
            Выполняет процесс синхронизации: запускает команду NTPQ, записывает данные в файлы,
            переносит данные в финальные директории по условиям (ежедневно и ежемесячно).
        """
        # Проверка состояния и возможный перезапуск службы NTP
        self.check_and_restart_ntp_service()

        # Получение данных из команды NTPQ
        ntp_data = self.run_ntpq()
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Запись данных в файлы
        for path_key in ["daily_path", "month_path", "year_path", "month_to_report_path", "day_to_report_path",
                         "short_ntpd_path"]:
            self.write_to_file(self.file_paths[path_key], ntp_data, current_time,
                               append=(path_key != "short_ntpd_path"))

        # Перенос данных в финальные директории по условиям
        now = datetime.datetime.now()

        # Ежемесячная проверка
        if now.day == 1 and now.hour == 0:  # Первый день нового месяца
            self.rotate_monthly_file()

        # Ежедневная проверка
        if now.hour == 0:  # Каждый день в полночь
            self.rotate_daily_file()

        # Обновление NTP_DRIFT_STAT.txt
        self.update_drift_stat()

        # Отправка файла ShortNtpd.log на FTP
        self.upload_to_ftp(self.file_paths["short_ntpd_path"])

        logging.info("Скрипт синхронизации статистики NTPD завершён.")
        logging.info("=" * 82)

    def rotate_monthly_file(self):
        previous_month = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime("%Y%m")
        current_month = datetime.datetime.now().strftime("%Y%m")

        previous_month_path = os.path.join(self.config["actual_data_path"],
                                           f"{self.config['report_file_prefix']}{previous_month}.log")
        current_month_path = os.path.join(self.config["actual_data_path"],
                                          f"{self.config['report_file_prefix']}{current_month}.log")
        final_month_path = os.path.join(self.config["final_data_path"],
                                        f"{self.config['report_file_prefix']}{previous_month}.log")

        self.transfer_to_final(previous_month_path, final_month_path, is_monthly=True)
        self.clean_final_directory(self.config["actual_data_path"], exclude=[current_month_path])
        logging.info(f"Создание нового файла за месяц: {current_month_path}")

    def rotate_daily_file(self):
        previous_day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
        current_day = datetime.datetime.now().strftime("%Y%m%d")

        previous_day_path = os.path.join(self.config["actual_day_data_path"],
                                         f"{self.config['report_file_prefix']}{previous_day}.log")
        current_day_path = os.path.join(self.config["actual_day_data_path"],
                                        f"{self.config['report_file_prefix']}{current_day}.log")
        final_day_path = os.path.join(self.config["final_day_data_path"],
                                      f"{self.config['report_file_prefix']}{previous_day}.log")

        self.transfer_to_final(previous_day_path, final_day_path)
        self.clean_final_directory(self.config["actual_day_data_path"], exclude=[current_day_path])
        logging.info(f"Создание нового файла за день: {current_day_path}")


# Запуск
sync = NTPDataSync()
sync.execute_sync()