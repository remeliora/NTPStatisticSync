import datetime
import json
import logging
import os
import shutil
import subprocess


# Чтение конфигурации
def load_config():
    with open("config.json") as config_file:
        return json.load(config_file)


config = load_config()
log_path = config["log_path"]

# Настройка логирования
logging.basicConfig(
    filename=os.path.join(log_path, "ntp_statistic_sync.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Запуск скрипта синхронизации статистики NTP.")


# Функция для выполнения команды и получения вывода
def run_ntpq():
    try:
        result = subprocess.run(["ntpq", "-pn"], capture_output=True, text=True)
        logging.info("Команда ntpq -pn выполнена успешно.")
        return result.stdout
    except Exception as e:
        logging.error(f"Ошибка при выполнении ntpq -pn: {e}")
        return ""


# Функция для создания каталога, если его нет
def ensure_directory_exists(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
        logging.info(f"Каталог {path} создан.")


# Запись данных в файл
def write_to_file(file_path, data, append=True):
    ensure_directory_exists(os.path.dirname(file_path))
    try:
        with open(file_path, "a" if append else "w") as f:
            f.write(f"{report_date}\n")
            f.write(data)
        logging.info(f"Записаны данные в файл {file_path}.")
    except Exception as e:
        logging.error(f"Ошибка при записи в файл {file_path}: {e}")


# Перенос данных в финальные директории
def transfer_to_final(source_path, destination_path):
    ensure_directory_exists(os.path.dirname(destination_path))
    try:
        if os.path.exists(source_path):
            shutil.move(source_path, destination_path)
            logging.info(f"Перенос файла {source_path} в {destination_path} завершен.")
        else:
            logging.warning(f"Файл {source_path} не найден для переноса.")
    except Exception as e:
        logging.error(f"Ошибка при переносе файла {source_path} в {destination_path}: {e}")


# Получаем текущие дату и время
now = datetime.datetime.now()
date_id = now.strftime("%Y%m%d")
year_id = now.strftime("%Y")
month_id = now.strftime("%m")
report_date = now.strftime("%Y-%m-%d %H:%M")

# Определяем пути к файлам
file_paths = {
    "daily_path": os.path.join(log_path, year_id, month_id, f"{config['file_prefix']}{date_id}.log"),
    "month_path": os.path.join(log_path, year_id, month_id, f"{config['file_prefix']}{date_id[:6]}.log"),
    "year_path": os.path.join(log_path, year_id, f"{config['file_prefix']}{date_id[:4]}.log"),
    "month_to_report_path": os.path.join(config["actual_data_path"],
                                         f"{config['report_file_prefix']}{date_id[:6]}.log"),
    "day_to_report_path": os.path.join(config["actual_day_data_path"], f"{config['report_file_prefix']}{date_id}.log"),
    "short_ntpd_path": os.path.join(log_path, "ShortNtpd.log"),
    "final_day_path": os.path.join(config["final_day_data_path"], f"{config['report_file_prefix']}{date_id}.log"),
    "final_month_path": os.path.join(config["final_data_path"], f"{config['report_file_prefix']}{date_id[:6]}.log")
}

# Запись результатов NTP в файлы
ntp_data = run_ntpq()
# 1) В посуточный файл
write_to_file(file_paths["daily_path"], ntp_data)
# 2) В помесячный файл
write_to_file(file_paths["month_path"], ntp_data)
# 3) В годовой файл
write_to_file(file_paths["year_path"], ntp_data)
# 4) В файл для ежемесячных отчетов
write_to_file(file_paths["month_to_report_path"], ntp_data)
# 5) В оперативный короткий файл (один сеанс)
write_to_file(file_paths["day_to_report_path"], ntp_data)
# 6) В ежедневный проверочный файл
write_to_file(file_paths["short_ntpd_path"], ntp_data, append=False)

# Ежедневный и ежемесячный перенос данных
if now.time() >= datetime.time(23, 59):
    transfer_to_final(file_paths["day_to_report_path"], file_paths["final_day_path"])

if now.day == 1 and now.time() >= datetime.time(0, 0):
    transfer_to_final(file_paths["month_to_report_path"], file_paths["final_month_path"])

logging.info("Скрипт синхронизации статистики NTP завершён.")
