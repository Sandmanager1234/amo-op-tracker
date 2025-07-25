import calendar
import datetime


class TemplateGenerator():
    
    WEEK_NAMES = {
        1: 'Понедельник',
        2: 'Вторник',
        3: 'Среда',
        4: 'Четверг',
        5: 'Пятница',
        6: 'Суббота',
        7: 'Воскресенье'
    }

    MONTH = {
        1: 'Январь',
        2: 'Февраль',
        3: 'Март',
        4: 'Апрель',
        5: 'Май',
        6: 'Июнь',
        7: 'Июль',
        8: 'Август',
        9: 'Сентябрь',
        10: 'Октябрь',
        11: 'Ноябрь',
        12: 'Декабрь'
    }

    def get_week_month(self, today: datetime.datetime):
        weekday = today.isoweekday()
        close_monday = today - datetime.timedelta(days=weekday-1)
        return close_monday.month

    def generate_month_weeks(self, today: datetime.datetime):
        month = self.get_week_month(today)
        weeks = calendar.monthcalendar(year=today.year, month=month)
        if weeks[0][0] == 0:
            del weeks[0]
        return weeks, month
    
    def get_weeknum(self, today: datetime.datetime):
        weeks, month = self.generate_month_weeks(today)
        if month < today.month:
            weeknum = len(weeks)
        else:
            weekday = today.isoweekday()
            day_num = today.day - weekday + 7
            weeknum = day_num // 7
        return weeknum, month

    def get_formula_row(self, weeks_ids: list, row_num: int, is_avg: bool = False):
        symbs = []
        for week_id in weeks_ids:
            symbs.append(f'{self.convert_num_to_letters(week_id)}{row_num}')
        sum_vars = '+'.join(symbs)
        if is_avg:
            return f'=({sum_vars})/{len(weeks_ids)}'
        return f'={sum_vars}'


    def create_shablon(self, today: datetime.datetime):
        weeks, month = self.generate_month_weeks(today)
        weeks_num = len(weeks)
        weeks_ids = [7 + 9 * week_num for week_num in range(weeks_num)]

        cols = []
        cols.append([f'Almaty_{self.MONTH[month]}', '', 'Метрики', '', 'Отдел Клоузеров'])
        cols.append(
            [
                '',
                '', 
                '', 
                '', 
                'заявки',
                'СV_1(%) | c заявки в квал пройдено', 
                'квал пройдено', 
                'с квал назад', 
                'СV_2(%) | c квал на запись во встречу',
                'Кол-во записей на эту дату',
                'Кол-во записей на сегодня ( Активность )',
                'с записи назад',
                'СV_3(%) | c записи на встречу',
                'кол-во встреч',
                'с встреч назад',
                'СV_4(%) | cо встречи на продажу',
                'кол-во продаж',
                'Сумма Продаж',
                'средний чек',
                'СV_5(%) | c заявки в продажу',
                '',
                'Кол-во менеджеров',
                'Продажи на 1 менеджера',
                'Кол-во дозвонов',
                'Кол-во дозвона на 1 менеджера в день'
            ]
        )
        cols.append(['', '', 'План'])
        cols.append(
            [
                '', 
                '', 
                'Факт',
                '',
                self.get_formula_row(weeks_ids, 5),
                '=D7/D5',
                self.get_formula_row(weeks_ids, 7),
                self.get_formula_row(weeks_ids, 8),
                '=D11/D7',
                self.get_formula_row(weeks_ids, 10),
                self.get_formula_row(weeks_ids, 11),
                self.get_formula_row(weeks_ids, 12),
                '=D14/D11',
                self.get_formula_row(weeks_ids, 14),
                self.get_formula_row(weeks_ids, 15),
                '=D17/D14',
                self.get_formula_row(weeks_ids, 17),
                self.get_formula_row(weeks_ids, 18),
                '=D18/D17',
                '=D17/D5',
                '',
                self.get_formula_row(weeks_ids, 22, is_avg=True), 
                '=D19/D22',
                self.get_formula_row(weeks_ids, 24),
                self.get_formula_row(weeks_ids, 25, is_avg=True) 
            ]
        )
        cols.append(['', '', 'Проекция', '', '', '', '', '', '', '', '', '', '', '', '', '', '', f'=D18/(СЕГОДНЯ()-H3)*30'])
        for i, week in enumerate(weeks):
            cols.append(['', '', f'Неделя - {i + 1}', 'План'])
            week_id = weeks_ids[i]
            cols.append(
                [
                    '', 
                    '', 
                    '', 
                    'Факт',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}5:{self.convert_num_to_letters(week_id + 7)}5)',
                    f'={self.convert_num_to_letters(week_id)}7/{self.convert_num_to_letters(week_id)}5',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}7:{self.convert_num_to_letters(week_id + 7)}7)',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}8:{self.convert_num_to_letters(week_id + 7)}8)',
                    f'={self.convert_num_to_letters(week_id)}11/{self.convert_num_to_letters(week_id)}7',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}10:{self.convert_num_to_letters(week_id + 7)}10)',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}11:{self.convert_num_to_letters(week_id + 7)}11)',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}12:{self.convert_num_to_letters(week_id + 7)}12)',
                    f'={self.convert_num_to_letters(week_id)}14/{self.convert_num_to_letters(week_id)}11',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}14:{self.convert_num_to_letters(week_id + 7)}14)',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}15:{self.convert_num_to_letters(week_id + 7)}15)',
                    f'={self.convert_num_to_letters(week_id)}17/{self.convert_num_to_letters(week_id)}14',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}17:{self.convert_num_to_letters(week_id + 7)}17)',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}18:{self.convert_num_to_letters(week_id + 7)}18)',
                    f'={self.convert_num_to_letters(week_id)}18/{self.convert_num_to_letters(week_id)}17',
                    f'={self.convert_num_to_letters(week_id)}17/{self.convert_num_to_letters(week_id)}5',
                    '',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}22:{self.convert_num_to_letters(week_id + 7)}22)/7',
                    f'={self.convert_num_to_letters(week_id + 1)}19/{self.convert_num_to_letters(week_id + 1)}22',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}24:{self.convert_num_to_letters(week_id + 7)}24)',
                    f'={self.convert_num_to_letters(week_id + 1)}24/{self.convert_num_to_letters(week_id + 1)}22/6'
                ]
            )
            counter = 1
            for i, day in enumerate(week):
                diff = 0
                if day == 0:
                    day = counter
                    counter += 1
                    diff = 1
                cols.append(
                    [
                        '',
                        '',
                        f'{day}.{month + diff}',
                        f'{self.WEEK_NAMES[i + 1]}'
                    ]
                )
        return cols, month

    def convert_num_to_letters(self, last_index):
        if last_index:
            index = last_index % 26
            count = (last_index // 26)
            if not index:
                index = 26
            if last_index % 26 == 0:
                count -= 1
            if count:
                range_id = f"{self.index_to_range(count)}{self.index_to_range(index)}"
            else:
                range_id = self.index_to_range(index)
            return range_id
        else:
            return 'A'

    def index_to_range(self, index):
        return chr(64 + index)

    def create_mop_shablon(self, today):
        cols = [
            [
                '',
                '',
                '№'
            ],
            [
                f'{self.MONTH[today.month]}',
                '',
                'Менеджеры',
            ],
            [
                '',
                '',
                'лидов взято в работу'
            ],
            [
                '',
                '',
                'записи на встречу'
            ],
            [
                '',
                '',
                'дошедшие встречи'
            ],
            [
                '',
                '',
                'Купили'
            ],
            [
                '',
                '',
                'Дозвоны'
            ],
            [
                '',
                '',
                'Минуты'
            ],
            [
                '',
                '',
                'CV % с заявки в запись'
            ],
            [
                '',
                '',
                'CV % с записи в доходимость'
            ],
            [
                '',
                '',
                'CV % с доходимости в продажу'
            ],
        ]
        return cols

