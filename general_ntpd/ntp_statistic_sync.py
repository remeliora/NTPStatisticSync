import datetime
import ftplib
import json
import logging
import shutil
import subprocess
import time
from pathlib import Path
from telegram_bot.telegram_bot import TelegramBot


class NTPDataSync:
    """
        Класс для синхронизации данных NTP-статистики. Выполняет загрузку конфигурации, управление логами,
        создание нужных директорий, выполнение команды NTPQ, запись данных в файлы и их перемещение в
        финальные директории.
    """

    def __init__(self, config_path=None, max_retries=3, retry_delay=5):
        """
            Инициализирует объект NTPDataSync. Загружает конфигурацию, настраивает логи, задает пути для файлов
            и проверяет существование финальных директорий.
        """
        config_path = config_path or Path(__file__).parent / "common_config.json"
        self.config = self.load_json_config(config_path)

        self.general_path = Path(self.config["folders_path"]["general_path"])
        self.setup_logging()

        if not self.config:
            logging.error(f"Основная конфигурация не найдена или пуста: {config_path}")

            return
        else:
            logging.info("Конфигурация загружена.")

        self.telegram_bot = TelegramBot()

        logging.info("Запуск синхронизации статистики NTPD.")

        self.report_date = datetime.datetime.now().strftime("%d.%m.%Y %H:%M")
        self.file_paths = self.define_file_paths()
        self.ensure_final_directories()

        self.ntp_servers = self.config.get("ntp_servers", [])

        self.ntpd_drift = Path(self.config["folders_path"]["ntpd_drift_path"])
        self.drift_statistic_path = Path(self.general_path / "NTP_DRIFT_STAT.txt")

        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.local_ftp = self.config["local_ftp"]

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

    def upload_file_to_ftp(self, ftp, file_path, ftp_path):
        """
            Загрузка файла на FTP-сервер.
        """
        try:
            ftp.cwd(ftp_path)
            logging.info(f"Текущий FTP-путь установлен на {ftp_path}.")

            with open(file_path, "rb") as file:
                ftp.storbinary(f"STOR {file_path.name}", file)
                logging.info(f"Файл {file_path.name} загружен в {ftp_path}.")

        except ftplib.all_errors as e:
            error_message = f"Ошибка при загрузке файла {file_path} в {ftp_path}: {e}"
            logging.error(f"Ошибка при загрузке файла {file_path} в {ftp_path}: {e}")
            self.telegram_bot.send_message(error_message)

    def setup_logging(self):
        """
            Настраивает логирование: создает лог-файл для записи всех действий скрипта с указанным форматом.
        """
        self.general_path.mkdir(parents=True, exist_ok=True)
        log_path = Path(self.general_path / "ntp_statistic_sync.log")
        logging.basicConfig(
            filename=log_path,
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

        general_path = Path(self.general_path)
        return {
            "daily_path": general_path / year_id / month_id / f"{self.config['folders_path']['file_prefix']}{date_id}.log",
            "month_path": general_path / year_id / month_id / f"{self.config['folders_path']['file_prefix']}{date_id[:6]}.log",
            "year_path": general_path / year_id / f"{self.config['folders_path']['file_prefix']}{date_id[:4]}.log",
            "month_to_report_path": Path(self.config["folders_path"][
                                             "actual_data_path"]) / f"{self.config['folders_path']['report_file_prefix']}{date_id[:6]}.log",
            "day_to_report_path": Path(self.config["folders_path"][
                                           "actual_day_data_path"]) / f"{self.config['folders_path']['report_file_prefix']}{date_id}.log",
            "short_ntpd_path": general_path / "ShortNtpd.log",
            "final_day_path": Path(self.config["folders_path"][
                                       "final_day_data_path"]) / f"{self.config['folders_path']['report_file_prefix']}{date_id}.log",
            "final_month_path": Path(self.config["folders_path"][
                                         "final_data_path"]) / f"{self.config['folders_path']['report_file_prefix']}{date_id[:6]}.log"
        }

    def ensure_final_directories(self):
        """
            Создает финальные директории, если они еще не существуют, для хранения данных за день и месяц.
        """
        for path in ["final_day_data_path", "final_data_path"]:
            Path(self.config["folders_path"][path]).mkdir(parents=True, exist_ok=True)

    def check_and_restart_ntp_service(self):
        """
            Проверяет и при необходимости перезапускает службу NTP.
        """
        logging.info("Проверка состояния сервиса NTP.")

        if not self.is_ntp_service_running():
            warning_message = f"Служба NTP не работает. Перезапуск..."
            logging.warning("Служба NTP не работает. Перезапуск...")
            self.telegram_bot.send_message(warning_message)
            self.restart_ntp_service()
            time.sleep(10)

            if not self.is_ntp_service_running():
                error_message = f"Ошибка: не удалось перезапустить службу NTP."
                logging.error("Ошибка: не удалось перезапустить службу NTP.")
                self.telegram_bot.send_message(error_message)
            else:
                info_message = f"Служба NTP успешно перезапущена."
                logging.info("Служба NTP успешно перезапущена.")
                self.telegram_bot.send_message(info_message)
        else:
            logging.info("Служба NTP работает корректно. Команда ntpq -pn выполнена успешно.")

    def is_ntp_service_running(self):
        """
            Проверяет, возвращает ли команда ntpq -pn корректные данные.
        """
        ntp_data = self.run_ntpq()
        if not ntp_data:
            error_message = f"Ошибка: Сбой в работе службы NTP."
            logging.error("Ошибка: Сбой в работе службы NTP.")
            self.telegram_bot.send_message(error_message)

            return False
        elif not ntp_data.strip():
            error_message = f"Ошибка: NTP возвращает пустые данные."
            logging.error("Ошибка: NTP возвращает пустые данные.")
            self.telegram_bot.send_message(error_message)

            return False
        return True

    def restart_ntp_service(self):
        """
            Перезапускает службу NTP с помощью командной строки Windows.
        """
        try:
            stop_result = subprocess.run(["net", "stop", "ntp"], check=True)
            logging.info(f"Служба NTP остановлена, код возврата: {stop_result.returncode}")

            time.sleep(10)  # Ждем перед перезапуском

            start_result = subprocess.run(["net", "start", "ntp"], check=True)
            logging.info(f"Служба NTP перезапущена, код возврата: {start_result.returncode}")

        except subprocess.CalledProcessError as e:
            logging.error(f"Ошибка при перезапуске службы NTP. Код возврата: {e.returncode}, сообщение: {e}")
        except Exception as e:
            logging.error(f"Неожиданная ошибка при перезапуске службы NTP: {e}")

    def run_ntpq(self):
        """
            Выполняет команду 'ntpq -pn' для получения данных NTP-сервера и возвращает результат.
            Если произошла ошибка, возвращает пустую строку.
        """
        try:
            result = subprocess.run(["ntpq", "-pn"], capture_output=True, text=True, check=True)
            # logging.info("Команда ntpq -pn выполнена успешно. Служба NTP работает корректно.")
            return result.stdout

        except subprocess.CalledProcessError as e:
            logging.error(f"Ошибка при выполнении ntpq -pn: {e}")

            return ""
        except Exception as e:
            logging.error(f"Неожиданная ошибка при выполнении ntpq -pn: {e}")

            return ""

    def verify_ntp_servers(self):
        """
            Проверяет, что все сервера из конфигурации присутствуют в списке опрашиваемых серверов.
            Логирует предупреждения для каждого отсутствующего сервера.
        """
        ntp_data = self.run_ntpq()
        if not ntp_data:
            error_message = f"Невозможно выполнить проверку серверов: отсутствуют данные от ntpq."
            logging.error("Невозможно выполнить проверку серверов: отсутствуют данные от ntpq.")
            self.telegram_bot.send_message(error_message)
            return False

        # Список опрашиваемых серверов из вывода ntpq
        polled_servers = set()

        for line in ntp_data.splitlines():
            # Проверяем, является ли строка информацией о сервере
            if line.startswith(("*", "o", "+", "-", " ", "x", "#")):
                # Извлекаем IP-адрес (или имя хоста), игнорируя символы статуса
                server_info = line.split()[0].lstrip("*o+- x#")
                polled_servers.add(server_info)

        # Проверяем наличие каждого сервера из конфигурации
        missing_servers = [server for server in self.ntp_servers if server not in polled_servers]

        # Логируем отсутствующие сервера
        if missing_servers:
            for server in missing_servers:
                warning_message = f"Сервер {server} отсутствует в списке опрашиваемых серверов."
                logging.warning(f"Сервер {server} отсутствует в списке опрашиваемых серверов.")
                self.telegram_bot.send_message(warning_message)
            return False

        logging.info("Все указанные в конфигурации сервера присутствуют в списке опрашиваемых.")
        return True

    def update_drift_stat(self):
        """
            Обновляет файл NTP_DRIFT_STAT.txt, добавляя текущие дату и время, а затем содержимое ntp.drift.
        """
        drift_file = self.ntpd_drift / "ntp.drift"
        date_time = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")

        try:
            with open(self.drift_statistic_path, "a") as drift_stat, open(drift_file) as drift:
                drift_stat.write(f"{date_time} xyz\n")
                drift_stat.write(drift.read())
                logging.info(f"Файл {self.drift_statistic_path} успешно обновлен.")

        except FileNotFoundError:
            error_message = f"Файл ntp.drift не найден в {self.ntpd_drift}"
            logging.error(f"Файл ntp.drift не найден в {self.ntpd_drift}")
            self.telegram_bot.send_message(error_message)
        except Exception as e:
            error_message = f"Ошибка при обновлении {self.drift_statistic_path}: {e}"
            logging.error(f"Ошибка при обновлении {self.drift_statistic_path}: {e}")
            self.telegram_bot.send_message(error_message)

    def write_to_file(self, file_path, data, date_time, append=True):
        file_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(file_path, "a" if append else "w") as f:
                f.write(f"{date_time}\n")
                f.write(data)
            logging.info(f"Записаны данные в файл {file_path}.")

        except Exception as e:
            error_message = f"Ошибка при записи в файл {file_path}: {e}"
            logging.error(f"Ошибка при записи в файл {file_path}: {e}")
            self.telegram_bot.send_message(error_message)

    def transfer_to_final(self, source_path, destination_path, is_monthly=False):
        """
            Перемещает файл из источника в финальную директорию.
            Также удаляет старые файлы в папке назначения, кроме текущего.
        """
        source_path = Path(source_path)
        destination_path = Path(destination_path)
        try:
            if source_path.exists():
                destination_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(source_path), str(destination_path))
                logging.info(f"Перенос файла {source_path} в {destination_path} завершен.")
                final_dir = Path(
                    self.config["folders_path"]["final_data_path" if is_monthly else "final_day_data_path"])
                self.clean_final_directory(final_dir, exclude={destination_path})
            else:
                warning_message = f"Файл {source_path} не найден для переноса."
                logging.warning(f"Файл {source_path} не найден для переноса.")
                self.telegram_bot.send_message(warning_message)
        except Exception as e:
            error_message = f"Ошибка при переносе файла {source_path} в {destination_path}: {e}"
            logging.error(f"Ошибка при переносе файла {source_path} в {destination_path}: {e}")
            self.telegram_bot.send_message(error_message)

    def clean_final_directory(self, directory, exclude=None):
        """
            Удаляет все файлы в указанной директории, кроме файлов, указанных в списке exclude.
        """
        exclude = exclude or set()
        directory = Path(directory)
        for file in directory.iterdir():
            if file not in exclude and file.is_file():
                file.unlink()
                logging.info(f"Удален файл {file} из директории {directory}")

    def execute_sync(self):
        """
            Выполняет процесс синхронизации: запускает команду NTPQ, записывает данные в файлы,
            переносит данные в финальные директории по условиям (ежедневно и ежемесячно).
        """
        # Проверка состояния и возможный перезапуск службы NTP
        self.check_and_restart_ntp_service()

        # Получение данных из команды NTPQ
        ntp_data = self.run_ntpq()
        current_time = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")

        # Проверка доступности серверов
        self.verify_ntp_servers()

        # Запись данных в файлы
        for path_key in ["daily_path", "month_path", "year_path", "month_to_report_path", "day_to_report_path",
                         "short_ntpd_path"]:
            self.write_to_file(self.file_paths[path_key], ntp_data, current_time,
                               append=(path_key != "short_ntpd_path"))

        # Перенос данных в финальные директории по условиям
        now = datetime.datetime.now()

        # Ежемесячная проверка
        if now.day == 1 and now.hour == 0 and 0 <= now.minute < 10:  # Первый день нового месяца
            self.rotate_file(period="monthly")

        # Ежедневная проверка
        if now.hour == 0 and 0 <= now.minute < 10:  # Каждый день в полночь
            self.rotate_file(period="daily")

        # Обновление NTP_DRIFT_STAT.txt
        self.update_drift_stat()

        # Отправка файла ShortNtpd.log на FTP
        logging.info("Выгрузка файла ShortNtpd.log на FTP началась.")
        ftp = self.connect_to_ftp(self.local_ftp)
        if ftp:
            # Передаем файл непосредственно в функцию upload_to_ftp
            self.upload_file_to_ftp(ftp, self.file_paths["short_ntpd_path"], self.local_ftp["ftp_path"])

        logging.info("Скрипт синхронизации статистики NTPD завершён.\n" + "=" * 124)

    def rotate_file(self, period):
        """
            Ротирует файлы данных на основе периода (ежедневный или ежемесячный).
            Удаляет файлы за старые дни/месяцы и переносит текущий в финальную директорию.
        """
        previous_date, current_date, final_path, data_path = None, None, None, None

        if period == "daily":
            previous_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
            current_date = datetime.datetime.now().strftime("%Y%m%d")
            final_path = Path(self.config["folders_path"][
                                  "final_day_data_path"]) / f"{self.config['folders_path']['report_file_prefix']}{previous_date}.log"
            data_path = Path(self.config["folders_path"]["actual_day_data_path"])
        elif period == "monthly":
            previous_date = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime("%Y%m")
            current_date = datetime.datetime.now().strftime("%Y%m")
            final_path = Path(self.config["folders_path"][
                                  "final_data_path"]) / f"{self.config['folders_path']['report_file_prefix']}{previous_date}.log"
            data_path = Path(self.config["folders_path"]["actual_data_path"])

        previous_path = data_path / f"{self.config['folders_path']['report_file_prefix']}{previous_date}.log"
        current_path = data_path / f"{self.config['folders_path']['report_file_prefix']}{current_date}.log"

        # Переносим старый файл в финальную директорию
        self.transfer_to_final(previous_path, final_path, is_monthly=(period == "monthly"))
        # Удаляем старые файлы, кроме текущего
        self.clean_final_directory(data_path, exclude=[current_path])

        if period == "daily":
            logging.info(f"Создание нового файла за день: {current_path}")
            self.telegram_bot.send_message(f"Суточный файл сформирован")
        elif period == "monthly":
            logging.info(f"Создание нового файла за месяц: {current_path}")
            self.telegram_bot.send_message(f"Месячный файл сформирован")


# Запуск
sync = NTPDataSync()
sync.execute_sync()
