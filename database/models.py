import os
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from loguru import logger

class Base(DeclarativeBase):
    pass


class Lead(Base):
    __tablename__ = "lead"

    id: Mapped[int] = mapped_column(primary_key=True)
    status: Mapped[str] = mapped_column()
    pipeline: Mapped[str] = mapped_column()
    recorded_at: Mapped[int] = mapped_column(nullable=True)
    is_qual: Mapped[bool] = mapped_column(default=False)
    is_record: Mapped[bool] = mapped_column(default=False)
    is_meeting: Mapped[bool] = mapped_column(default=False)
    is_selled: Mapped[bool] = mapped_column(default=False)
    is_deleted: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[int] = mapped_column()
    updated_at: Mapped[int] = mapped_column()
    
    def __get_value_from_json(self, field: dict, _all: bool = False) -> str:
        """Приватный метод для получения значения из JSON"""
        try:
            if not _all:
                value = (
                    field["values"][0]["value"]
                    if field["values"][0]["value"] is not None
                    else ""
                )
            else:
                value = ", ".join(
                    [
                        value["value"]
                        for value in field["values"]
                        if value["value"] is not None
                    ]
                )
            logger.debug(f"{field.get('field_name','Неизвестное')}: {value}")
            return value
        except (KeyError, IndexError, TypeError) as e:
            logger.warning(
                f"Ошибка при обработке поля `{field.get('field_name','Неизвестное')}`: {e}"
            )
            return "не заполнено"

    @classmethod
    def from_json(cls, json_data: dict, statuses: dict):
        self : Lead = cls()
        self.id = json_data.get('id', '')
        self.status = json_data.get('status_id', '')
        self.pipeline = json_data.get('pipeline_id', '')
        self.created_at = json_data.get('created_at', '')
        self.updated_at = json_data.get('updated_at', '')
        reject_reason = ''
        self.is_deleted = False
        self.is_record = False
        self.is_meeting = False
        self.is_selled = False
        self.is_qual = False

        fields = json_data.get('custom_fields_values', [])
        if not fields:
            fields = [] 
        for field in fields:
            match field.get("field_name", None):
                case 'Время встречи':
                    self.is_record = True
                case 'ЗНР причина':
                    reject_reason = self.__get_value_from_json(field)
                case _:
                    continue
        if (
            statuses.get(self.pipeline, {}).get(self.status, -1) > 31 and 
            statuses.get(self.pipeline, {}).get(self.status, -1) != 11000
        ) or (
            statuses.get(self.pipeline, {}).get(self.status, -1) == 11000 and
            reject_reason not in ['Не прошли квал', 'НД'] # не купили после встречи
        ):
            self.is_qual = True
        if (
            self.status == int(os.getenv('decision_status'))
        ) or (
            (
                statuses.get(self.pipeline, {}).get(self.status, -1) >= statuses.get(int(os.getenv('common_pipe')), {}).get(int(os.getenv('decision_status')), -1)
            ) and (
                reject_reason not in ['Не прошли квал', 'НД', 'Записались на встречу, но слились']
            )
        ):
            self.is_meeting = True
        if statuses.get(self.pipeline, {}).get(self.status, -1) > 100000:
            self.is_selled = True

        if self.is_selled:
            self.is_meeting = True
        if self.is_meeting:
            self.is_record = True
        if self.is_record:
            self.is_qual = True
        
        return self
    
    def update_from_lead(self, lead: 'Lead', statuses: dict):
        self.is_qual |= lead.is_qual
        self.is_meeting |= lead.is_meeting
        self.is_record |= lead.is_record
        self.is_selled |= lead.is_selled
        self.is_deleted &= lead.is_deleted
        self.recorded_at = lead.recorded_at
        if statuses.get(self.pipeline, {}).get(self.status, -1) < statuses.get(lead.pipeline, {}).get(lead.status, -1) :
            self.pipeline = lead.pipeline
            self.status = lead.status
        
    def __str__(self):
        return f'id: {self.id}; pipeline: {self.pipeline}; status: {self.status}'
    

class Status(Base):
    __tablename__ = "status"

    id: Mapped[int] = mapped_column(primary_key=True)
    status_id: Mapped[int] = mapped_column()
    pipeline_id: Mapped[int] = mapped_column()
    name : Mapped[str] = mapped_column()
    sort_type: Mapped[int] = mapped_column()

    @classmethod
    def from_json(cls, json_data: dict, is_high_priority: bool = False):
        self: Status = cls()
        self.status_id = json_data.get('id', 0)
        self.pipeline_id = json_data.get('pipeline_id', 0)
        self.name = json_data.get('name', '')
        sort_type = json_data.get('sort', -1)
        self.sort_type = sort_type if not is_high_priority or sort_type == -1 else sort_type + 100000
        return self


class Manager(Base):
    __tablename__ = "managers"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    group_id: Mapped[int] = mapped_column()
    

async def main():
    pass