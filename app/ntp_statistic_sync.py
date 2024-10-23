import subprocess
import os
from datetime import datetime
from ftplib import FTP

# Пути и FTP параметры
log_path = "/path/to/logs/"
file_prefix = "Ntpd"
ftp_host = 'localhost'
ftp_user = 'user'
ftp_passwd = 'password'
ftp_dir = '/remote/path/'

# Получение текущей даты и времени
now = datetime.now()
year_id = now.strftime("%Y")
month_id = now.strftime("%m")
day_id = now.strftime("%d")
hour_id = now.strftime("%H")
minute_id = now.strftime("%M")

# Формирование имен файлов
daily_file = os.path.join(log_path, f"{year_id}-{month_id}-{day_id}_daily.log")
monthly_file = os.path.join(log_path, f"{year_id}-{month_id}_monthly.log")
yearly_file = os.path.join(log_path, f"{year_id}_yearly.log")


# Функции
def get_ntp_data():
    try:
        result = subprocess.run(['ntpq', '-pn'], capture_output=True, text=True, check=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при выполнении ntpq -pn: {e}")
        return None


def write_to_file(file_path, data):
    try:
        with open(file_path, 'a') as f:
            f.write(data)
    except IOError as e:
        print(f"Ошибка записи в файл {file_path}: {e}")


def create_directories(path):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError as e:
            print(f"Ошибка при создании директории {path}: {e}")


def upload_to_ftp(host, user, passwd, file_path, ftp_dir):
    try:
        ftp = FTP(host)
        ftp.login(user, passwd)
        ftp.cwd(ftp_dir)

        with open(file_path, 'rb') as file:
            ftp.storbinary(f"STOR {os.path.basename(file_path)}", file)

        ftp.quit()
        print(f"Файл {file_path} успешно отправлен на FTP {host}")
    except Exception as e:
        print(f"Ошибка при отправке файла на FTP: {e}")


# Основной процесс
create_directories(log_path)
ntp_data = get_ntp_data()

if ntp_data:
    write_to_file(daily_file, ntp_data)
    write_to_file(monthly_file, ntp_data)
    write_to_file(yearly_file, ntp_data)

upload_to_ftp(ftp_host, ftp_user, ftp_passwd, daily_file, ftp_dir)
