import os
from time import sleep

from disk.api.__main__ import main

if __name__ == '__main__':
    # Ждем пока создастся и запустится контейнер бд в докере
    # (чтобы избежать лишней ошибки, т.к. без sleep бэк один раз упадет при попытке коннекта,
    # потом перезапустится и все будет ок)
    # sleep(5)

    # применяем миграции к базе данных
    # os.system('python disk/db/__main__.py upgrade head')

    # запускаем бэк
    main()
