import os
import time
import gspread
import datetime
from loguru import logger
from googlesheet.template_generator import TemplateGenerator
from gspread_formatting import set_frozen, set_column_width


sg = TemplateGenerator()

class GoogleSheets:
    def __init__(self):
        try:
            logger.info("Инициализация GoogleSheets")
            self.gc = gspread.service_account(filename="credentials.json")
            self.table = self.gc.open_by_key(os.getenv("table_id"))
            logger.info("Успешное подключение к таблице")
        except Exception as e:
            logger.error(f"Ошибка при инициализации GoogleSheets: {e}")
            raise

    def get_sheet(self, today, month):
        try:
            sheet_name = f'{sg.MONTH[month]}_ОП {today.year}'
            ws = self.table.worksheet(sheet_name)
            time.sleep(0.3)
        except gspread.WorksheetNotFound as ex:
            logger.warning(f'Лист {sheet_name} не найден. Ошибка: {ex}')
            ws = self.create_worksheet(today)
        except Exception as ex:
            logger.warning(f'Ошибка получения листа: {ex}')
            time.sleep(30)
            ws = self.get_sheet(today)
        return ws

    def create_worksheet(self, today):
        try:
            shablon, month = sg.create_shablon(today)
            ws = self.table.add_worksheet(f'{sg.MONTH[month]}_ОП {today.year}', 64, 128)
            ws.insert_cols(shablon, value_input_option="USER_ENTERED")
            self.beutify_sheet(ws)
        except Exception as ex:
            logger.error(f'Ошибка создания таблицы РНП: {ex}')    
        return ws
    
    def get_sells(self, today):
        try:
            table = self.gc.open_by_key(os.getenv('sells_table'))
            sheet_name = f'{sg.MONTH[today.month].upper()} {today.year}'
            ws = table.worksheet(sheet_name)
            margin = ws.find('КОЛ-ВО ОПЛАТ')
            row_id = margin.row - 1
            col_id = 2 + today.day
            sell_amount = ws.cell(row_id, col_id).value.replace(',', '.')
            sell_count = ws.cell(row_id + 1, col_id).value.replace(',', '.')
            if not sell_amount:
                sell_amount = 0
            else:
                sell_amount = float(sell_amount.replace(' ', ''))
            if not sell_count:
                sell_count = 0
            else:
                sell_count = int(sell_count.replace(' ', ''))
            return sell_amount, sell_count
        except Exception as ex:
            logger.error(f'Не удалось получить данные о продажах. Ошибка {ex}')
            return 0, 0
        
    def insert_statistic(self, statistic: tuple, today, mop_data: tuple = None):
        total, qual, qual_back, record, record_back, meeting, meeting_back, selled = statistic
        week_num, month = sg.get_weeknum(today)
        ws : gspread.Worksheet = self.get_sheet(today, month)
        sell_amount, sell_count = self.get_sells(today)
        col_id = 5 + 2 * week_num + 7 * (week_num - 1) + today.isoweekday()
        col_data = [
                [total],
                [f'={sg.convert_num_to_letters(col_id)}{7}/{sg.convert_num_to_letters(col_id)}{5}'],
                [qual],
                [qual_back],
                [f'={sg.convert_num_to_letters(col_id)}{11}/{sg.convert_num_to_letters(col_id)}{7}'],
                [''],
                [record],
                [record_back],
                [f'={sg.convert_num_to_letters(col_id)}{14}/{sg.convert_num_to_letters(col_id)}{11}'],
                [meeting],
                [meeting_back],
                [f'={sg.convert_num_to_letters(col_id)}{17}/{sg.convert_num_to_letters(col_id)}{14}'],
                [sell_count],
                [sell_amount * 1000],
                [f'={sg.convert_num_to_letters(col_id)}{18}/{sg.convert_num_to_letters(col_id)}{17}'],
                [f'-']
        ]
        if mop_data:
            manager_count, total_calls = mop_data
            mop_row_data = [
                [''],
                [manager_count],
                [f'={sg.convert_num_to_letters(col_id)}18/{sg.convert_num_to_letters(col_id)}22'],
                [total_calls],
                [f'={sg.convert_num_to_letters(col_id)}24/{sg.convert_num_to_letters(col_id)}22']
            ]
            col_data.extend(mop_row_data)

        ws.update(
            col_data,
            f'{sg.convert_num_to_letters(col_id)}{5}:{sg.convert_num_to_letters(col_id)}{25}',
            raw=False
        )
        logger.info(f'Добавлена статистика за день: {today} в столбец {sg.convert_num_to_letters(col_id)}')
    
    def insert_records(self, record_statistic: dict, day_count, start_day):
        # i need few arrays by month which i can insert in table
        month_data = {}
        curr_week_num = -1
        prev_month = 0
        while day_count > 0:
            week_num, month = sg.get_weeknum(start_day)
            value = record_statistic.get(
                start_day.year, {}
            ).get(
                start_day.month, {}
            ).get(
                start_day.day, 0
            )
            if value != 0:
                day_count -= 1
            if month not in month_data:
                prev_month = month
                month_data[month] = {}
                month_data[month]['month'] = []
                month_data[month]['start_day'] = start_day
                month_data[month]['start_weeknum'] = week_num
            if curr_week_num == -1:
                curr_week_num = week_num
            if curr_week_num != week_num and prev_month == month:
                week_id = 7 + 9 * (week_num - 1)
                month_data[month]['month'].extend(['', f'=СУММ({sg.convert_num_to_letters(week_id + 1)}10:{sg.convert_num_to_letters(week_id + 7)}10)',])
                curr_week_num = week_num
            month_data[month]['month'].append(value)
            start_day += datetime.timedelta(days=1)
        for month in month_data:
            start_day = month_data[month]['start_day']
            week_num = month_data[month]['start_weeknum']
            ws = self.get_sheet(start_day, month)
            col_id = 5 + 2 * week_num + 7 * (week_num - 1) + start_day.isoweekday()
            ws.update(
                [month_data[month]['month']],
                f'{sg.convert_num_to_letters(col_id)}{10}:{sg.convert_num_to_letters(col_id + len(month_data[month]["month"]))}{10}',
                raw=False
            )
            logger.info(f'Добавлена статистика за месяц: {sg.MONTH[month]} в диапазон {sg.convert_num_to_letters(col_id)}{9}:{sg.convert_num_to_letters(col_id + len(month_data[month]["month"]))}{9}')
    
    def beutify_sheet(self, ws: gspread.Worksheet):
        # MERGE CELLS
        ws.merge_cells('A1:B2')
        ws.merge_cells('A3:B4')
        ws.merge_cells('A5:A25')
        ws.merge_cells('C3:C4')
        ws.merge_cells('D3:D4')
        ws.merge_cells('E3:E4')
        ws.merge_cells('F3:G3')
        ws.merge_cells('O3:P3')
        ws.merge_cells('X3:Y3')
        ws.merge_cells('AG3:AH3')
        ws.merge_cells('AP3:AQ3')
        # COLOR CELLS to rgb(0, 255, 255) CYAN
        ws.format([
            'A3:E4', 
            'F4:G4', 'H3:N4', 
            'O4:P4', 'Q3:W4', 
            'X4:Y4', 'Z3:AF4',  
            'AG4:AH4', 'AI3:AO4',  
            'AP4:AQ4', 'AR3:AX4',  
            'B6', 'B8', 'B11', 'B13', 'B19:B20', 'B23', 'B25', 'D5:D20', 'D22:D25',
            'G5:G20', 'G22:G25', 'H6:N6', 'H9:N9', 'H13:N13', 'H16:N16', 'H19:N19', 'H23:N23', 'H25:N25',
            'P5:P20', 'P22:P25','Q6:W6', 'Q9:W9', 'Q13:W13', 'Q16:W16', 'Q19:W19', 'Q23:W23', 'Q25:W25',
            'Y5:Y20', 'Y22:Y25','Z6:AF6', 'Z9:AF9', 'Z13:AF13', 'Z16:AF16', 'Z19:AF19', 'Z23:AF23', 'Z25:AF25',
            'AH5:AH20', 'AH22:AH25','AI6:AO6', 'AI9:AO9', 'AI13:AO13', 'AI16:AO16', 'AI19:AO19', 'AI23:AO23', 'AI25:AO25',
            'AQ5:AQ20', 'AQ22:AQ25','AR6:AX6', 'AR9:AX9', 'AR13:AX13', 'AR16:AX16', 'AR19:AX19', 'AR23:AX23', 'AR25:AX25'
            ], {
                "backgroundColor": {
                        "red": 0,
                        "green": 1,
                        "blue": 1
                },
            })
        # color cells to rgb(102, 102, 102) GREY
        ws.format([
                'B21:AX21'
            ], {
                "backgroundColor": {
                        "red": 0.4,
                        "green": 0.4,
                        "blue": 0.4
                },
            }
        )
        # TEXT EDIT
        ws.format(['3:4', 'A:A'], {
            'textFormat': {
                "fontSize": 10,
                "bold": True
            }
        })
        ws.format(['A1', 'A5'], {
            'textFormat': {
                "fontSize": 20,
                "bold": True
            }
        })
        ws.format(['A1:AX25'], {     
            'wrapStrategy': 'WRAP',
            'horizontalAlignment': 'CENTER',
            "verticalAlignment": 'MIDDLE'
        })
        # BORDERS 
        ws.format( #ALL
            ['A3:AX25'],
            {
               'borders': {
                    "top": {
                        'style': 'SOLID'
                    },
                    "right": {
                        'style': 'SOLID'
                    },
                    "left": {
                        'style': 'SOLID'
                    },
                    "bottom": {
                        'style': 'SOLID'
                    }
                } 
            }
        )
        ws.format(  # TOP
            [
                'A2:AX2'
            ],
            {  
                'borders': {
                    "bottom": {
                        'style': 'SOLID_THICK'
                    }
                }
            }
        )
        ws.format(  # BOTTOM
            [
                'A26:AX26'
            ],
            {  
                'borders': {
                    "top": {
                        'style': 'SOLID_THICK'
                    }
                }
            }
        )
        ws.format( # RIGHT SIDE
            [
                'B6:B25',
                'E6:E25', 'G6:G25', 
                'N6:N25', 'P6:P25',
                'W6:W25', 'Y6:Y25',
                'AF6:AF25', 'AH6:AH25',
                'AO6:AO25', 'AQ6:AQ25',
                'AX6:AX25'
            ],
            {
                'borders': {
                    "top": {
                        'style': 'SOLID'
                    },
                    "right": {
                        'style': 'SOLID_THICK'
                    },
                    "left": {
                        'style': 'SOLID'
                    },
                    "bottom": {
                        'style': 'SOLID'
                    },
                }
            }
        )
        ws.format( # RIGHT UP ANGLE SIDE
            [
                'A5', 
                'B5',
                'E5', 'G5', 'G3',
                'N5', 'P5', 'P3',
                'W5', 'Y5', 'Y3',
                'AF5', 'AH5', 'AH3',
                'AO5', 'AQ5', 'AQ3',
                'AX5', 'AX3'
            ],
            {
                'borders': {
                    "top": {
                        'style': 'SOLID_THICK'
                    },
                    "right": {
                        'style': 'SOLID_THICK'
                    },
                    "left": {
                        'style': 'SOLID'
                    },
                    "bottom": {
                        'style': 'SOLID'
                    },
                }
            }
        )
        ws.format( # RIGHT HEAD SOLID SIDE
            [
                'B3', 'E3'
            ],
            {
                'borders': {
                    "top": {
                        'style': 'SOLID_THICK'
                    },
                    "right": {
                        'style': 'SOLID_THICK'
                    },
                    "left": {
                        'style': 'SOLID'
                    },
                    "bottom": {
                        'style': 'SOLID_THICK'
                    },
                }
            }
        )
        ws.format( # RIGHT HEAD BOT SIDE
            [
                'F4', 'H4:M4',
                'O4', 'Q4:V4',
                'X4', 'Z4:AE4',
                'AG4', 'AI4:AN4',
                'AP4', 'AR4:AW4'
            ],
            {
                'borders': {
                    "top": {
                        'style': 'SOLID'
                    },
                    "right": {
                        'style': 'SOLID'
                    },
                    "left": {
                        'style': 'SOLID'
                    },
                    "bottom": {
                        'style': 'SOLID_THICK'
                    },
                }
            }
        )
        ws.format( # RIGHT HEAD SOLID SIDE
            [
                'C3', 'D3'
            ],
            {
                'borders': {
                    "top": {
                        'style': 'SOLID_THICK'
                    },
                    "right": {
                        'style': 'SOLID'
                    },
                    "left": {
                        'style': 'SOLID'
                    },
                    "bottom": {
                        'style': 'SOLID_THICK'
                    },
                }
            }
        )
        ws.format( # RIGHT HEAD SOLID SIDE
            [
                'G4', 'N4',
                'P4', 'W4',
                'Y4', 'AF4',
                'AH4', 'AO4',
                'AQ4', 'AX4'
            ],
            {
                'borders': {
                    "top": {
                        'style': 'SOLID'
                    },
                    "right": {
                        'style': 'SOLID_THICK'
                    },
                    "left": {
                        'style': 'SOLID'
                    },
                    "bottom": {
                        'style': 'SOLID_THICK'
                    },
                }
            }
        )
        # CELLS FORMAT
        ws.format([
            '6:6', '9:9', '13:13', '16:16', '20:20'
            ], {
            'numberFormat': {'type': 'PERCENT'}
        })
        # CELL SIZE
        set_column_width(ws, 'A', 225)
        set_column_width(ws, 'B', 180)
        # FREEZE
        set_frozen(ws, cols=2)

    def create_mop_sheet(self, today):
        try:
            shablon = sg.create_mop_shablon(today)
            ws = self.table.add_worksheet(f'{sg.MONTH[today.month]}_ОП {today.year}', 64, 128)
            ws.insert_cols(shablon, value_input_option="USER_ENTERED")
            self.beautify_mop_sheet(ws)
        except Exception as ex:
            logger.error(f'Ошибка создания таблицы МОПов: {ex}')    
        return ws
    
    def get_mop_sheet(self, today):
        try:
            sheet_name = f'{sg.MONTH[today.month]}_МОПы {today.year}'
            ws = self.table.worksheet(sheet_name)
            time.sleep(0.3)
        except gspread.WorksheetNotFound as ex:
            logger.warning(f'Лист {sheet_name} не найден. Ошибка: {ex}')
            ws = self.create_worksheet(today)
        except Exception as ex:
            logger.warning(f'Ошибка получения листа: {ex}')
            time.sleep(30)
            ws = self.get_sheet(today)
        return ws
    
    def beautify_mop_sheet(self, ws: gspread.Worksheet):
        # merge cells
        ws.merge_cells('B1:B2')
        # set borders
        ws.format( #ALL
            ['A3:L3'],
            {
               'borders': {
                    "top": {
                        'style': 'SOLID'
                    },
                    "right": {
                        'style': 'SOLID'
                    },
                    "left": {
                        'style': 'SOLID'
                    },
                    "bottom": {
                        'style': 'SOLID'
                    }
                } 
            }
        )
        # center text
        ws.format(['A1:L64'], {     
            'wrapStrategy': 'WRAP',
            'horizontalAlignment': 'CENTER',
            "verticalAlignment": 'MIDDLE'
        })
        # bold text
        ws.format(['3:3', 'B:B'], {
            'textFormat': {
                "fontSize": 10,
                "bold": True
            }
        })
        # color text to 
        ws.format(
            ['B1:B2'], 
            {
                "backgroundColor": {
                        "red": 0,
                        "green": 1,
                        "blue": 1
                },
            }
        )
        # color text to rgb(255, 242, 204)
        ws.format(
            ['I3:L3'], 
            {
                "backgroundColor": {
                        "red": 1.0,
                        "green": 0.9490,
                        "blue": 0.8
                },
            }
        )

