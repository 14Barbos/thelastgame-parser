import requests
import mysql.connector
from bs4 import BeautifulSoup


def create_table(cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS game (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255),
            data VARCHAR(255),
            genre VARCHAR(255),
            image VARCHAR(255),
            description TEXT,
            developer VARCHAR(255),
            version VARCHAR(255),
            language VARCHAR(255),
            protection VARCHAR(255),
            os VARCHAR(255),
            processor VARCHAR(255),
            ram VARCHAR(255),
            video_card VARCHAR(255),
            disk_space VARCHAR(255),
            download VARCHAR(255),
            imageBack1 VARCHAR(255),
            imageBack2 VARCHAR(255),
            imageBack3 VARCHAR(255)
        )
    ''')


def save_info_to_db(cursor, game_title, data, genre, image_src, game_description, developer, version, language,
                    protection, os, processor, ram, video_card, disk_space, download_link, imageBack1, imageBack2,
                    imageBack3):
    cursor.execute('''INSERT INTO game (title, data, genre, image, description, developer, version, language, protection, 
                                        os, processor, ram, video_card, disk_space, download, imageBack1, imageBack2, imageBack3) 
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                   (game_title, data, genre, image_src,
                    game_description, developer, version,
                    language, protection, os, processor,
                    ram, video_card, disk_space, download_link,
                    imageBack1, imageBack2, imageBack3))
    conn.commit()


def scrape_page(url, cursor):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        game_blocks = soup.find_all('article')

        for game_block in game_blocks:
            game_link = game_block.find('a')['href']
            game_title_tag = game_block.find('h2', class_='post-title entry-title')
            game_title = game_title_tag.text.strip() if game_title_tag else None
            image_src = game_block.find('img').get('data-src')

            game_response = requests.get(game_link)
            if game_response.status_code == 200:
                game_soup = BeautifulSoup(game_response.text, 'html.parser')

                game_description_tag = game_soup.find('div', class_='entry themeform').find('p')
                game_description = game_description_tag.text.strip() if game_description_tag else None

                game_info_tag = game_soup.find('div', style='float: left;width:50%;')
                game_info = game_info_tag.text.strip().replace("Информация о игре", "") if game_info_tag else None

                requirements_tag = game_soup.find('div', style='float: right; border;width:45%;')
                requirements_text = requirements_tag.text.strip().replace("Минимальные системные требования",
                                                                          "") if requirements_tag else None

                image_links = [a['href'] for a in game_soup.select('#gamepics a')]
                imageBack1, imageBack2, imageBack3 = image_links[:3] if len(image_links) >= 3 else [None, None, None]

                download_link = game_soup.find('a', class_='btn_green')['href']

                game_data = {}
                for line in game_info.split('\n'):
                    parts = line.split(':')
                    if len(parts) == 2:
                        key = parts[0].strip()
                        value = parts[1].strip()
                        game_data[key] = value

                requirements_data = {}
                for line in requirements_text.split('\n'):
                    parts = line.split(':')
                    if len(parts) == 2:
                        key = parts[0].strip()
                        value = parts[1].strip()
                        requirements_data[key] = value

                save_info_to_db(
                    cursor,
                    game_title,
                    game_data.get('Год выпуска', 0),
                    game_data.get('Жанр', ''),
                    image_src,
                    game_description,
                    game_data.get('Разработчик', ''),
                    game_data.get('Версия', ''),
                    game_data.get('Язык интерфейса', ''),
                    game_data.get('Таблетка', ''),
                    requirements_data.get('Операционная система', ''),
                    requirements_data.get('Процессор', ''),
                    requirements_data.get('Оперативная память', ''),
                    requirements_data.get('Видеокарта', ''),
                    requirements_data.get('Памяти на Жестком Диске', ''),
                    download_link,
                    imageBack1,
                    imageBack2,
                    imageBack3
                )

                print(f"Информация о {game_title} сохранена в базе данных")
            else:
                print(f"Ошибка при запросе: {game_response.status_code}")
    else:
        print(f"Ошибка при запросе: {response.status_code}")


conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="content"
)

cursor = conn.cursor()
create_table(cursor)

for page_num in range(1, 792):
    page_url = f"https://thelastgame.ru/page/{page_num}/"
    print(f"Обрабатываем страницу: {page_url}")
    try:
        scrape_page(page_url, cursor)
    except Exception as e:
        print(e)

conn.close()
