import os
import asyncio
import time
from loguru import logger
from dotenv import load_dotenv
from schedule import repeat, run_pending, every
from bs4 import BeautifulSoup

from database import Database
from database.models import Lead
from amocrm.models import User
from amocrm import AmoCRMClient
from kztime import get_today_info, get_last_week_list
from googlesheet.googlesheets import GoogleSheets


COMMON_PIPE = int(os.getenv('common_pipe'))
SUCCESS_PIPE = int(os.getenv('success_pipe'))


async def start_db():
    await db.check_tables()
    await set_statuses()


async def set_statuses():
    async def set_pipline_statuses(pipeline_id: int, priority: bool):
        amo_client.start_session()
        try:
            pipeline = await amo_client.get_pipeline(pipeline_id)
            await db.insert_statuses(pipeline, priority)
        except Exception as ex:
            logger.error(f'Ошибка получения статусов воронки: {pipeline_id}. Ошибка: {ex}')
            raise Exception
        finally:
            await amo_client.close_session()
    await set_pipline_statuses(COMMON_PIPE, False)
    await set_pipline_statuses(SUCCESS_PIPE, True)


async def get_user_list():
    # получаем список пользователей
    users_response = await amo_client.get_users()
    users = users_response.get('_embedded', {}).get('users', [])
    next_page_users = users_response.get('_links', {}).get('next', {}).get('href')
    page = 2
    if next_page_users:
        while next_page_users:
            next_users_response = await amo_client.get_users(page=page)
            users.extend(next_users_response.get('_embedded', {}).get('users', []))
            next_page_users = next_users_response.get('_links', {}).get('next', {}).get('href')
            page += 1
    hunters = []
    for user_json in users:
        user = User.from_json(user_json)
        print(user.id, user.group_id, os.getenv('group_id'))
        if user.group_id == int(os.getenv('group_id')):
            hunters.append(user)
    return hunters


async def get_mop_data(today, users):
    page = await amo_client.get_managers(today, users)
    soup = BeautifulSoup(page, 'lxml')
    divs = soup.find_all('div', class_='calls_analytics_row')
    count = len(divs) - 3
    total = divs[-1].find('div', class_='calls_analytics__calls_graph__all')
    return count, total.text


async def polling_leads():
    amo_client.start_session()
    try:
        week = get_last_week_list()
    
        for i, _day in enumerate(week):
            logger.info(f'Обработка дня: {_day}')
            # Получение даты и воронок
            ts_beg, ts_end, day = get_today_info(_day)
            pipelines = [COMMON_PIPE, SUCCESS_PIPE]
            # Получение сделок из БД
            db_leads = await db.get_lead_ids(ts_beg, ts_end)
            db_leads_deleted = await db.get_lead_ids(ts_beg, ts_end, deleted=True)
            # Получение сделок из amo
            leads_response : dict = await amo_client.get_leads(ts_beg, ts_end, pipelines)
            leads = leads_response.get('_embedded', {}).get('leads', [])
            next_page = leads_response.get('_links', {}).get('next', {}).get('href')
            page = 2
            if next_page:
                while next_page:
                    next_response = await amo_client.get_leads(ts_beg, ts_end, pipelines, page=page)
                    leads.extend(next_response.get('_embedded', {}).get('leads', []))
                    next_page = next_response.get('_links', {}).get('next', {}).get('href')
                    page += 1
            #Добавление и обновление сделок
            resp_leads = set()
            statuses = await db.get_statuses()
            for lead_json in leads:
                lead = Lead.from_json(lead_json, statuses)
                resp_leads.add(lead.id)
                if lead.id not in db_leads:
                    if lead.id not in db_leads_deleted:
                        await db.add_lead(lead)
                await db.update_lead(lead)
            # "Удаляем" сделки, которые ушли в другую воронку
            diff_leads = db_leads - resp_leads
            for lead in diff_leads:
                await db.delete_lead(lead)
            # Отправка в гугл
            statistic = await db.get_statistic(ts_beg, ts_end)
            if i == 0:
                # Получаем данные о звонках
                users = await get_user_list()
                mop_data = await get_mop_data(day, users)
                google.insert_statistic(statistic, day, mop_data)
            else:
                google.insert_statistic(statistic, day)
        start_ts, _, last_day = get_today_info(week[-1])
        record_statistic, day_count = await db.get_records(start_ts)
        google.insert_records(record_statistic, day_count, last_day)
    except Exception as ex:
        logger.error(f'Не получилось получить сделки. Ошибка: {ex}')
    finally:
        await amo_client.close_session()


@repeat(every(5).minutes)
def main():
    asyncio.run(polling_leads())


if __name__ == '__main__':
    load_dotenv()
    # Получение имени текущей директории
    current_directory_name = os.path.basename(os.getcwd())
    os.makedirs(".", exist_ok=True)  # Создание папки logs, если её нет

    log_file_path = os.path.join(
        "logs", f"{current_directory_name}_{{time:YYYY-MM-DD}}.log"
    )
    logger.add( # Настройка ротации логов
        log_file_path,  # Файл лога будет называться по дате и сохраняться в поддиректории с названием текущей директории
        rotation="00:00",  # Ротация каждый день в полночь
        retention="7 days",  # Хранение логов за последние 7 дней
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",  # Формат сообщений в файле
        level="INFO",  # Минимальный уровень логирования
        compression="zip",  # Архивирование старых логов
    )

    google = GoogleSheets()

    amo_client = AmoCRMClient(
        base_url="https://teslakz.amocrm.ru",
        access_token=os.getenv("access_token"),
        client_id=os.getenv("client_id"),
        client_secret=os.getenv("client_secret"),
        permanent_access_token=True
    )

    db = Database()
    asyncio.run(start_db())
    while True:
        run_pending()
        time.sleep(1)
        # time.sleep(1)  # Uncomment if you want to add a delay between checks
