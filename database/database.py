import os
import sqlalchemy
from sqlalchemy.sql import func
from models import Base, Lead, Status
from loguru import logger
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from kztime import get_local_datetime


class Database:
    def __init__(self):
        logger.info(f"Подключение к БД: {os.getenv('db_url')}")
        self.engine = create_async_engine(os.getenv('db_url'), echo=False)
        self.async_session = async_sessionmaker(self.engine, expire_on_commit=False)
        logger.info("Подключение установлено")

    async def dispose(self):
        await self.engine.dispose()

    async def check_tables(self):
        async with self.engine.connect() as conn:
            tables = await conn.run_sync(
                lambda sync_conn: sqlalchemy.inspect(sync_conn).get_table_names()
            )
        if set(tables) != {'lead', 'status'}:
            logger.info("Таблицы не найдены, создание новых")
            await self.create_tables()
        else:
            Base.metadata.reflect
            logger.info("Таблицы cуществуют, проверка завершена") 

    async def create_tables(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Таблицы успешно созданы")
            
    async def get_lead_ids(self, from_ts: int, to_ts: int, deleted: bool = False):
        async with self.async_session() as session:
            async with session.begin():
                query = sqlalchemy.select(Lead.id).where(Lead.created_at >= from_ts, Lead.created_at <= to_ts, Lead.is_deleted == deleted)
                result = await session.execute(query)
                return set(result.scalars().fetchall())
            
    async def add_lead(self, lead: Lead):
        async with self.async_session() as session:
            async with session.begin():
                session.add(lead)
                await session.commit()

    async def update_lead(self, lead: Lead):
        statuses = await self.get_statuses()
        async with self.async_session() as session:
            async with session.begin():
                lead_db = await session.get(Lead, lead.id)
                lead_db.update_from_lead(lead, statuses)
                await session.commit()
            
    async def get_statuses(self):
        async with self.async_session() as session:
            async with session.begin():
                result = await session.execute(
                    sqlalchemy.text(
                        "SELECT * FROM status;"
                    )
                )
                statuses = result.fetchall()
                st_dict = {}
                for status in statuses:
                    _, status_id, pipeline_id, name, sort_type = status
                    if pipeline_id not in st_dict:
                        st_dict[pipeline_id] = {}
                    st_dict[pipeline_id][status_id] = sort_type
                return st_dict
    
    async def insert_statuses(self, pipeline: dict, is_high_priority: bool = False):
        pipeline_id = pipeline.get('id')
        statuses = pipeline.get('_embedded', {}).get('statuses', [])
        async with self.async_session() as session:
            async with session.begin():
                await session.execute(
                    sqlalchemy.delete(
                        Status
                    ).where(
                        Status.pipeline_id==pipeline_id
                    )
                )
                for status_json in statuses:
                    status = Status.from_json(status_json, is_high_priority)
                    if status.sort_type != -1:
                        session.add(status)
                await session.commit()

    async def delete_lead(self, lead_id):
        async with self.async_session() as session:
            async with session.begin():
                query = sqlalchemy.update(Lead).values(is_deleted=True).where(Lead.id==lead_id)
                await session.execute(query)
                await session.commit()

    async def get_statistic(self, start_ts, end_ts):
        async with self.async_session() as session:
            async with session.begin():
                total = await session.execute(
                    sqlalchemy.select(
                        func.count()
                    ).select_from(
                        Lead
                    ).where(
                        Lead.is_deleted == False,
                        Lead.created_at >= start_ts,
                        Lead.created_at <= end_ts
                    )
                )
                qual = await session.execute(
                    sqlalchemy.select(
                        func.count()
                    ).select_from(
                        Lead
                    ).where(
                        Lead.is_deleted == False,
                        Lead.is_qual == True,
                        Lead.created_at >= start_ts,
                        Lead.created_at <= end_ts
                    )
                )
                qual_back = await session.execute(
                    sqlalchemy.select(
                        func.count()
                    ).select_from(
                        Lead
                    ).join(
                        Status, Status.status_id == Lead.status
                    ).where(
                        Lead.is_deleted == False,
                        Lead.is_qual == True,
                        Lead.created_at >= start_ts,
                        Lead.created_at <= end_ts,
                        Status.sort_type < sqlalchemy.select(
                            Status.sort_type
                        ).select_from(
                            Status
                        ).where(
                            Status.name == 'Квалификация пройдена'
                        ).scalar_subquery()
                    )
                )
                record = await session.execute(
                    sqlalchemy.select(
                        func.count()
                    ).select_from(
                        Lead
                    ).where(
                        Lead.is_deleted == False,
                        Lead.is_record == True,
                        Lead.created_at >= start_ts,
                        Lead.created_at <= end_ts
                    )
                )
                record_back = await session.execute(
                    sqlalchemy.select(
                        func.count()
                    ).select_from(
                        Lead
                    ).where(
                        Lead.is_deleted == False,
                        Lead.is_record == True,
                        Lead.created_at >= start_ts,
                        Lead.created_at <= end_ts,
                        Lead.recorded_at == None
                    )
                )
                meeting = await session.execute(
                    sqlalchemy.select(
                        func.count()
                    ).select_from(
                        Lead
                    ).where(
                        Lead.is_deleted == False,
                        Lead.is_meeting == True,
                        Lead.created_at >= start_ts,
                        Lead.created_at <= end_ts
                    )
                )
                meeting_back = await session.execute(
                    sqlalchemy.select(
                        func.count()
                    ).select_from(
                        Lead
                    ).join(
                        Status, Status.status_id == Lead.status
                    ).where(
                        Lead.is_deleted == False,
                        Lead.is_meeting == True,
                        Lead.created_at >= start_ts,
                        Lead.created_at <= end_ts,
                        Status.sort_type < sqlalchemy.select(
                            Status.sort_type
                        ).select_from(
                            Status
                        ).where(
                            Status.name == 'Принимает решение'
                        ).scalar_subquery()
                    )
                )
                
                selled = await session.execute(
                    sqlalchemy.select(
                        func.count()
                    ).select_from(
                        Lead
                    ).where(
                        Lead.is_deleted == False,
                        Lead.is_selled == True,
                        Lead.created_at >= start_ts,
                        Lead.created_at <= end_ts
                    )
                )
                return (
                    total.scalars().first(),
                    qual.scalars().first(),
                    qual_back.scalars().first(),
                    record.scalars().first(),
                    record_back.scalars().first(),
                    meeting.scalars().first(),
                    meeting_back.scalars().first(),
                    selled.scalars().first()
                )
            
    async def get_records(self, start_ts: int):
        async with self.async_session() as session:
            async with session.begin():
                sql_response = await session.execute(
                    sqlalchemy.select(
                        Lead.recorded_at
                    ).select_from(
                        Lead
                    ).where(
                        Lead.recorded_at >= start_ts
                    )
                )
                records = sql_response.scalars().fetchall()  
                record_statistic = {}   
                day_count = 0      
                for record in records:
                    local_record = get_local_datetime(record)
                    if local_record.year not in record_statistic:
                        record_statistic[local_record.year] = {}
                    if local_record.month not in record_statistic[local_record.year]:
                        record_statistic[local_record.year][local_record.month] = {}
                    if local_record.day not in record_statistic[local_record.year][local_record.month]:
                        day_count += 1
                        record_statistic[local_record.year][local_record.month][local_record.day] = 0
                    record_statistic[local_record.year][local_record.month][local_record.day] += 1
                return record_statistic, day_count


# async def test():
#     db = Database()
#     s_ts, e_ts, _ = 1753038000, 1753124399, 0
#     await db.get_statistic(s_ts, e_ts)


# if __name__ == "__main__":
#     import asyncio
#     asyncio.run(test())

#     logger.info("Скрипт завершен")
