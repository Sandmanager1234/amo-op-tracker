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
            weeknum = ((today - datetime.timedelta(days=weekday - 7)).day // 7)
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
                'СV_2(%) | c квал на запись во встречу',
                'кол-во записей',
                'СV_3(%) | c записи на встречу',
                'кол-во встреч на этот день',
                'кол-во встреч',
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
                '=D10/D7',
                self.get_formula_row(weeks_ids, 9),
                self.get_formula_row(weeks_ids, 10),
                '=D12/D10',
                self.get_formula_row(weeks_ids, 11),
                '=D14/D12',
                self.get_formula_row(weeks_ids, 13),
                self.get_formula_row(weeks_ids, 14),
                '=D15/D14',
                '=D14/D5',
                '',
                self.get_formula_row(weeks_ids, 19, is_avg=True), 
                '=D15/D19',
                self.get_formula_row(weeks_ids, 21),
                self.get_formula_row(weeks_ids, 22, is_avg=True) 
            ]
        )
        cols.append(['', '', 'Проекция', '', '', '', '', '', '', '', '', '', '', '', f'=D14/(СЕГОДНЯ()-H3)*30'])
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
                    f'={self.convert_num_to_letters(week_id)}9/{self.convert_num_to_letters(week_id)}7',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}9:{self.convert_num_to_letters(week_id + 7)}9)',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}10:{self.convert_num_to_letters(week_id + 7)}10)',
                    f'={self.convert_num_to_letters(week_id)}12/{self.convert_num_to_letters(week_id)}10',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}12:{self.convert_num_to_letters(week_id + 7)}12)',
                    f'={self.convert_num_to_letters(week_id)}14/{self.convert_num_to_letters(week_id)}12',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}14:{self.convert_num_to_letters(week_id + 7)}14)',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}15:{self.convert_num_to_letters(week_id + 7)}15)',
                    f'={self.convert_num_to_letters(week_id)}15/{self.convert_num_to_letters(week_id)}14',
                    f'={self.convert_num_to_letters(week_id)}14/{self.convert_num_to_letters(week_id)}5',
                    '',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}19:{self.convert_num_to_letters(week_id + 7)}19)',
                    f'={self.convert_num_to_letters(week_id + 1)}15/{self.convert_num_to_letters(week_id + 1)}19',
                    f'=СУММ({self.convert_num_to_letters(week_id + 1)}21:{self.convert_num_to_letters(week_id + 7)}21)',
                    f'={self.convert_num_to_letters(week_id + 1)}21/{self.convert_num_to_letters(week_id + 1)}19/6'
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



# if __name__ == '__main__':
#     # from kztime import get_local_datetime
#     tg = TemplateGenerator()
#     today = datetime.datetime.now() - datetime.timedelta(days=3)
#     week = tg.get_weeknum(today)
#     print(week)
#     # print(month)