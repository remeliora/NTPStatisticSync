import json
import os
import datetime
import subprocess
import logging

# Чтение конфигурации
with open("config.json") as config_file:
    config = json.load(config_file)

log_path = config["log_path"]
file_prefix = config["file_prefix"]
report_path = config["report_path"]
report_file_prefix = config["report_file_prefix"]

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
        print(f"Ошибка при выполнении ntpq -pn: {e}")
        logging.error(f"Ошибка при выполнении ntpq -pn: {e}")
        return ""


# Получаем текущие дату и время
now = datetime.datetime.now()
date_id = now.strftime("%Y%m%d")
year_id = now.strftime("%Y")
month_id = now.strftime("%m")
report_date = now.strftime("%Y-%m-%d %H:%M")

# Определяем пути к файлам
file_paths = {
    "daily_path": os.path.join(log_path, year_id, month_id, f"{file_prefix}{date_id}.log"),
    "month_path": os.path.join(log_path, year_id, month_id, f"{file_prefix}{date_id[:6]}.log"),
    "year_path": os.path.join(log_path, year_id, f"{file_prefix}{date_id[:4]}.log"),
    "month_to_report_path": os.path.join(log_path, "ActualData", f"{report_file_prefix}{date_id[:6]}.log"),
    "day_to_report_path": os.path.join(log_path, "ActualDayData", f"{report_file_prefix}{date_id}.log"),
    "short_ntpd_path": os.path.join(log_path, "ShortNtpd.log")
}

# Создаем каталоги, если их нет, и логируем только создание
for path in file_paths.values():
    dir_path = os.path.dirname(path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
        logging.info(f"Каталог {dir_path} создан.")

# Запись результатов NTP в файлы
ntp_data = run_ntpq()


def write_to_file(file_path, data, append=True):
    try:
        with open(file_path, "a" if append else "w") as f:
            f.write(f"{report_date}\n")
            f.write(data)
        logging.info(f"Записаны данные в файл {file_path}.")
    except Exception as e:
        logging.error(f"Ошибка при записи в файл {file_path}: {e}")


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

logging.info("Скрипт синхронизации статистики NTP завершён.")
