import pandas as pd
from datetime import datetime
from datetime import timedelta
import random


class TestDataGenerator:

    def __init__(self, test_datas: dict, date_start: datetime = datetime(2015, 1, 1, 8)):
        self.personal_DF = pd.read_csv(test_datas['personal'], header=0, usecols=['personal_code'])
        self.personal_DF = self.personal_DF.dropna()
        self.produkt_DF = pd.read_csv(test_datas['produkt'], header=0, usecols=['produkt_code',
                                                                                'Verkaufspreis_Brutto',
                                                                                'Verkaufspreis_MwSt'])
        self.kunde_DF = pd.read_csv(test_datas['kunde'], header=0, usecols=['kunde_code'])

        self.produkt_count = self.produkt_DF.shape[0]
        self.personal_count = self.personal_DF.shape[0]
        self.kunde_count = self.kunde_DF.shape[0]
        self.opsDF_headers = ["datum", "uhrzeit"]
        self.opsDF_headers.extend(self.personal_DF.columns.values)
        self.opsDF_headers.extend(self.produkt_DF.columns.values)
        self.opsDF_headers.extend(self.kunde_DF.columns.values)
        self.opsDF_headers.append("menge")

        self.current_date = date_start

        self.delta_min = 1
        self.delta_max = 30
        self.hour_opening = 8
        self.hour_closing = 18

    def change_data(self, test_datas: dict):
        self.personal_DF = pd.read_csv(test_datas['personal'], header=0, usecols=['personal_code'])
        self.personal_DF = self.personal_DF.dropna()
        self.produkt_DF = pd.read_csv(test_datas['produkt'], header=0, usecols=['produkt_code',
                                                                                'Verkaufspreis_Brutto',
                                                                                'Verkaufspreis_MwSt'])
        self.kunde_DF = pd.read_csv(test_datas['kunde'], header=0, usecols=['kunde_code'])

        self.produkt_count = self.produkt_DF.shape[0]
        self.personal_count = self.personal_DF.shape[0]
        self.kunde_count = self.kunde_DF.shape[0]

    def get_next_datetime(self, last_datetime) -> datetime:
        current_datetime = last_datetime
        seconds_interval = random.randrange(self.delta_min, self.delta_max)
        current_datetime += timedelta(seconds=seconds_interval)
        if current_datetime.hour >= self.hour_closing:
            if current_datetime.weekday() < 5:
                current_datetime += timedelta(days=1)
            else:
                current_datetime += timedelta(days=2)

            start_hour = random.randrange(self.hour_opening, self.hour_opening + 2)
            start_minute = random.randrange(0, 59)
            current_datetime = current_datetime.replace(hour=start_hour, minute=start_minute, second=0)

        return current_datetime

    def dump_data(self, rows) -> str:
        current_date = self.current_date
        rows = []
        for i in range(rows):
            new_row = []
            current_date = self.get_next_datetime(current_date)
            new_row.append(current_date.date().isoformat())
            new_row.append(current_date.time().isoformat())

            current_personal_nr = random.randrange(0, self.personal_count)
            new_row.extend(self.personal_DF.iloc[current_personal_nr])

            current_produkt_nr = random.randrange(0, self.produkt_count)
            new_row.extend(self.produkt_DF.iloc[current_produkt_nr])

            current_kunde_nr = random.randrange(0, self.kunde_count)
            new_row.extend(self.kunde_DF.iloc[current_kunde_nr])
            rd_menge = random.gauss(1, 3)
            rd_menge = rd_menge.__round__(0).__int__()
            if rd_menge < 1: rd_menge = 1
            new_row.append(rd_menge)
            rows.append(new_row)

        opsDF = pd.DataFrame(rows, columns=self.opsDF_headers)
        opsDF_filename = "test_data/fact_data/"
        opsDF_filename += current_date.strftime("%Y%m%d%H%M%S")
        opsDF_filename += "_opsDataDump.csv"
        opsDF.to_csv(opsDF_filename)

        self.current_date = current_date
        self.set_to_next_day()
        return './' + opsDF_filename

    def set_to_next_day(self):
        self.current_date = self.current_date + timedelta(days=1)

    def skip_to_date(self, new_datetime:datetime):
        if new_datetime > self.current_date:
            self.current_date = new_datetime
            print(f'Datum geändert zu {self.current_date.isoformat()}')
        else:
            print('Das Datum kann nicht in der Vergangenheit verändert werden.')