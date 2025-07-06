import os
import time
import gspread
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
            print(ex)     
        return ws
    

    def get_sells(self, today):
        try:
            table = self.gc.open_by_key(os.getenv('sells_table'))
            sheet_name = f'{sg.MONTH[today.month].upper()} {today.year}'
            ws = table.worksheet(sheet_name)
            margin = ws.find('КОЛ-ВО ОПЛАТ')
            row_id = margin.row - 1
            col_id = 3 + today.day
            sell_amount = ws.cell(row_id, col_id).value
            sell_count = ws.cell(row_id + 1, col_id).value
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
        
    
    def insert_statistic(self, statistic: tuple, today):
        total, qual, record, meeting, selled = statistic
        week_num, month = sg.get_weeknum(today)
        ws : gspread.Worksheet = self.get_sheet(today, month)
        sell_amount, sell_count = self.get_sells(today)
        col_id = 5 + 2 * week_num + 7 * (week_num - 1) + today.isoweekday()

        ws.update(
            [
                [total],
                [f'={sg.convert_num_to_letters(col_id)}{7}/{sg.convert_num_to_letters(col_id)}{5}'],
                [qual],
                [f'={sg.convert_num_to_letters(col_id)}{9}/{sg.convert_num_to_letters(col_id)}{7}'],
                [record],
                [f'={sg.convert_num_to_letters(col_id)}{11}/{sg.convert_num_to_letters(col_id)}{9}'],
                [meeting],
                [f'={sg.convert_num_to_letters(col_id)}{13}/{sg.convert_num_to_letters(col_id)}{11}'],
                [sell_count],
                [sell_amount * 1000],
                [f'={sg.convert_num_to_letters(col_id)}{14}/{sg.convert_num_to_letters(col_id)}{13}'],
                [f'-']
            ],
            f'{sg.convert_num_to_letters(col_id)}{5}:{sg.convert_num_to_letters(col_id)}{16}',
            raw=False
        )


    
    def beutify_sheet(self, ws: gspread.Worksheet):
        # MERGE CELLS
        ws.merge_cells('A1:B2')
        ws.merge_cells('A3:B4')
        ws.merge_cells('A5:A16')
        ws.merge_cells('C3:C4')
        ws.merge_cells('D3:D4')
        ws.merge_cells('E3:E4')
        ws.merge_cells('F3:G3')
        ws.merge_cells('O3:P3')
        ws.merge_cells('X3:Y3')
        ws.merge_cells('AG3:AH3')
        ws.merge_cells('AP3:AQ3')
        # COLOR CELLS to rgb(0, 255, 255)
        ws.format([
            'A3:E4', 
            'F4:G4', 'H3:N4', 
            'O4:P4', 'Q3:W4', 
            'X4:Y4', 'Z3:AF4',  
            'AG4:AH4', 'AI3:AO4',  
            'AP4:AQ4', 'AR3:AX4',  
            'B6', 'B8', 'B10', 'B12', 'B15:B16', 'D5:D16',
            'G5:G16', 'H6:N6', 'H8:N8', 'H10:N10', 'H12:N12', 'H15:N15',
            'P5:P16', 'Q6:W6', 'Q8:W8', 'Q10:W10', 'Q12:W12', 'Q15:W15',
            'Y5:Y16', 'Z6:AF6', 'Z8:AF8', 'Z10:AF10', 'Z12:AF12', 'Z15:AF15',
            'AH5:AH16', 'AI6:AO6', 'AI8:AO8', 'AI10:AO10', 'AI12:AO12', 'AI15:AO15',
            'AQ5:AQ16', 'AR6:AX6', 'AR8:AX8', 'AR10:AX10', 'AR12:AX12', 'AR15:AX15',
            ], {
                "backgroundColor": {
                        "red": 0,
                        "green": 1,
                        "blue": 1
                },
            })
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
        ws.format(['A1:AX16'], {     
            'wrapStrategy': 'WRAP',
            'horizontalAlignment': 'CENTER',
            "verticalAlignment": 'MIDDLE'
        })
        # BORDERS 
        ws.format( #ALL
            ['A3:AX16'],
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
                'A17:AX17'
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
                'B6:B16',
                'E6:E16', 'G6:G16', 
                'N6:N16', 'P6:P16',
                'W6:W16', 'Y6:Y16',
                'AF6:AF16', 'AH6:AH16',
                'AO6:AO16', 'AQ6:AQ16',
                'AX6:AX16'
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
            '6:6', '8:8', '10:10', '12:12', '16:16'
            ], {
            'numberFormat': {'type': 'PERCENT'}
        })
        # CELL SIZE
        set_column_width(ws, 'A', 225)
        set_column_width(ws, 'B', 180)
        # FREEZE
        set_frozen(ws, cols=2)