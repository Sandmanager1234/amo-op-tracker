import asyncio
import sqlalchemy
from database import Database
from database.models import Lead
from dotenv import load_dotenv
from kztime import get_last_week_list, get_today_info


load_dotenv()


async def main():
    week = get_last_week_list()
    start_ts, _, day = get_today_info(week[-1])
    db = Database()
    async with db.async_session() as session:
        async with session.begin():
            await session.execute(
                sqlalchemy.update(Lead).where(Lead.created_at >= start_ts).values(
                    {
                        'is_meeting': False,
                        'is_record': False,
                        'is_qual': False,
                        'is_selled': False
                    }
                )
            )



if __name__ == '__main__':
    asyncio.run(main())
    # Lead.is