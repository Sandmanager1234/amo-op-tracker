import re
import os
from loguru import logger


class Lead:
    id: int 
    status: int
    pipeline: int
    is_qual: bool = False
    is_record: bool = False
    is_meeting: bool = False
    is_selled: bool = False
    created_at: int
    updated_at: int

    qual = {
        'min': 40,
        'max': 50
    }
    meeting = 70

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
        
    # добавить created_at
    @classmethod
    def from_json(cls, data: dict) -> "Lead":
        ...