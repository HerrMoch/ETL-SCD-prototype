from datetime import date, timedelta
from decimal import Decimal
from sqlalchemy import MetaData
from sqlalchemy import Table, Column
from sqlalchemy import Integer, String, Date, Numeric, Boolean
from sqlalchemy import ForeignKey
from sqlalchemy import create_engine, and_, func
import pandas as pd


class ETLPipeline:
    TWODIGITS = Decimal('0.01')
    LINESEPERATOR = '-' * 40

    def __init__(self, database_path="sqlite:///test.db"):
        # Festlegungen zu Spaltenbezeichnungen
        self.__scd2_cols = {'valid_as_of': 'gueltig_ab',
                            'valid_until': 'gueltig_bis',
                            'current_flag': 'aktuell'}

        self._metadata = MetaData()
        self._DT_time_table = Table("DT_Zeit", self._metadata,
                                    Column('id', Integer, primary_key=True),
                                    Column('datum', Date, nullable=False),
                                    Column('tag_der_woche', Integer),
                                    Column('tag_des_jahres', Integer),
                                    Column('tag_des_monats', Integer),
                                    Column('tag_name', String),
                                    Column('kalenderwoche', Integer),
                                    Column('monat_des_jahres', Integer),
                                    Column('monat_name', Integer),
                                    Column('jahr', Integer)
                                    )

        self._DT_product_table = Table("DT_Produkt", self._metadata,
                                       Column('id', Integer, primary_key=True),
                                       Column('produkt_code', String, nullable=False),
                                       Column('verkaufspreis_netto', Numeric(64, 2)),
                                       Column('kategorie', String),
                                       Column('name', String),
                                       Column('variante', String),
                                       Column('hersteller_code', String),
                                       Column('hersteller_artikel_code', String),
                                       Column('hersteller_name', String),
                                       Column('hersteller_plz', String),
                                       Column('hersteller_ort', String),
                                       Column('hersteller_land', String),
                                       Column(self.__scd2_cols['valid_as_of'], Date),
                                       Column(self.__scd2_cols['valid_until'], Date),
                                       Column(self.__scd2_cols['current_flag'], Boolean)
                                       )

        self._DT_employee_table = Table("DT_Personal", self._metadata,
                                        Column('id', Integer, primary_key=True),
                                        Column('personal_code', String),
                                        Column('name', String),
                                        Column('vorname', String),
                                        Column('abteilung', String)
                                        )

        self._DT_client_table = Table("DT_Kunde", self._metadata,
                                      Column('id', Integer, primary_key=True),
                                      Column('kunde_code', String),
                                      Column('name', String),
                                      Column('vorname', String),
                                      Column('firma', String),
                                      Column('plz', String),
                                      Column('ort', String),
                                      Column('land', String)
                                      )

        self._FT_sales_table = Table("FT_Umsatzanalyse", self._metadata,
                                     Column('zeit_id', Integer, ForeignKey('DT_Zeit.id'), primary_key=True),
                                     Column('personal_id', Integer, ForeignKey('DT_Personal.id'),
                                            primary_key=True),
                                     Column('produkt_id', Integer, ForeignKey('DT_Produkt.id'),
                                            primary_key=True),
                                     Column('kunde_id', Integer, ForeignKey('DT_Kunde.id')),
                                     Column('umsatz_summe_netto', Numeric(64, 2)),
                                     Column('rabatt_summe', Numeric(64, 2)),
                                     Column('menge_artikel', Integer)
                                     )

        # Alle Faktentabellen
        self.__fact_tables = [self._FT_sales_table]

        # Liste mit allen Tabellennamen
        self.table_names = [table for table in self._metadata.tables]
        # Angabe, welche Spalten die fachlichen IDs beinhalten
        self._business_id_name = {self._DT_product_table: 'produkt_code',
                                  self._DT_employee_table: 'personal_code',
                                  self._DT_time_table: 'datum',
                                  self._DT_client_table: 'kunde_code'}
        # Angabe, welche Spalten der Dimensionstabellen SCD-Typ 2 nutzen
        self.scd2 = {self._DT_product_table:
                         ['verkaufspreis_netto', 'name', 'variante'],
                     self._DT_employee_table: [],
                     self._DT_time_table: [],
                     self._DT_client_table: []}
        # Angabe, welche Spalten der Dimensionstabellen für SCD-Typ 3 zusammenhängen
        # Der erste Wert eines Wertepaares ist der jeweils aktuellere
        # Beim Ändern des ersten Wertes wird zuvor der zweite Wert mit dem ersten Überschrieben
        # anschließend wird der erste Wert mit dem neuen Wert überschrieben
        self.scd3 = {self._DT_product_table: {},
                     self._DT_employee_table: {},
                     self._DT_time_table: {},
                     self._DT_client_table: {}}

        # Prüfung der Logik
        self.__check_tables()

        self.__engine = create_engine(database_path)
        self._metadata.create_all(self.__engine)

    def __check_tables(self):
        print('Beginne Prüfung')
        for table in self._metadata.tables.values():
            if table in self.__fact_tables:
                break

            print(f'\n\nPrüfung der Tabelle "{table.name}:')
            print(self.LINESEPERATOR)

            if table in self._business_id_name.keys():
                if self._business_id_name[table] in table.columns:
                    print(f'Fachliche ID "{self._business_id_name[table]}": OK!')
                else:
                    raise NameError(f'Fachliche ID "{self._business_id_name[table]}" nicht in Tabelle vorhanden!')
            else:
                raise NameError(f'Keine Metadaten für die fachliche ID in dieser Tabelle vorhanden!')

            if table in self.scd2:
                print('Metadaten für SCD-Typ 2 vorhanden: OK')
            else:
                raise NameError('Metadaten für SCD-Typ 2 nicht vorhanden!')

            if len(self.scd2[table]) > 0:
                scd2 = True  # In dieser Dimensionstabelle existieren Spalten mit SCD-Typ 2
                str_in = ''
            else:
                scd2 = False  # In dieser Dimensionstabelle existieren keine Spalten mit SCD-Typ 2
                str_in = 'nicht '

            print(f'SCD-Typ 2 {str_in}vorhanden')
            # Prüfung ob die nötigen Spalten vorhanden sind oder fälschlicherweise vorliegen
            for col_name in self.__scd2_cols.values():
                if col_name not in table.columns:
                    if scd2:
                        raise NameError(f'Die Spalte "{col_name}" fehlt in der Dimensionstabelle "{table.name}"')
                    else:
                        print(f'Spalte "{col_name}" {str_in}vorhanden: OK!')
                else:
                    if scd2:
                        print(f'Spalte "{col_name}" vorhanden: OK!')
                    else:
                        warning_str = 'Achtung!\n'
                        warning_str += self.LINESEPERATOR
                        warning_str += f'\nDie Spalte "{col_name}" existiert in der Dimensionstabelle "{table.name}"!'
                        warning_str += '\nIn der Tabelle existieren keine SCD-Typ 2 Spalten.'
                        warning_str += f'\nDer Spaltenname "{col_name}" ist jedoch für den SCD-Typ 2 vorgesehen!\n'
                        warning_str += self.LINESEPERATOR
                        print(warning_str)
            print(f'SCD-Typ 2 OK!\n')

            if table in self.scd3:
                print('Metadaten für SCD-Typ 3 vorhanden: OK!')
            else:
                raise NameError('Metadaten für SCD-Typ 3 nicht vorhanden!')

            for col in self.scd3[table].keys():
                if col in table.columns:
                    print(f'Spalte "{col}" in Tabelle vorhanden: OK!')
                    if self.scd3[table][col] in table.columns:
                        print(f'Partnerspalte "{self.scd3[table][col]}" zu "{col}" in Tabelle vorhanden: OK!')
                    else:
                        raise NameError(
                            f'Partnerspalte "{self.scd3[table][col]}" zu "{col}" nicht in Tabelle vorhanden!')
                else:
                    raise NameError(f'Spalte "{col}" nicht in Tabelle vorhanden!')
            print('SCD-Typ 3 OK!\n')

            for col in [x.name for x in table.columns]:
                occurance = 0
                if col in self.scd2[table]:
                    print(f'Spalte "{col}": SCD-Typ 2')
                    occurance += 1
                if col in self.scd3[table].keys():
                    print(f'Spalte "{col}": SCD-Typ 3')
                    occurance += 1

                if occurance == 0:
                    print(f'Spalte "{col}": SCD-Typ 1')
                elif occurance > 1:
                    raise ValueError(f'SCD-Typ der Spalte "{col}" mehrfach deklariert!')

            print(f'\nTabelle "{table.name}": OK!')
            print(self.LINESEPERATOR)
        print('\n\nPrüfung erfolgreich abgeschlossen!')

    def __get_current_dim_id(self, DT_table, fach_ID):
        with self.__engine.connect() as conn:
            business_id_name = self._business_id_name[DT_table]
            if self.scd2[DT_table]:
                # In dieser Dimensionstabelle wird SCD-Typ 2 genutzt
                result = conn.execute(Table.select(DT_table).where(
                    and_(DT_table.c[business_id_name] == fach_ID,  # Übereinstimmung der fachlichen ID
                         DT_table.c[self.__scd2_cols['current_flag']])))  # aktuellen Datensatz finden
            else:
                # In dieser Dimensionstabelle wird kein SCD-Typ 2 genutzt
                result = conn.execute(Table.select(DT_table).where(  # Übereinstimmung der fachlichen ID
                    DT_table.c[business_id_name] == fach_ID))
            result_line = result.fetchone()
            if result_line is None:
                print(f'Der Datensatz mit der fachlichen ID "{fach_ID}" existiert nicht!')
                return None
            return result_line[0]  # gibt die unique id zurück


    def __get_table_from_name(self, table) -> Table:
        if table in self.table_names:
            return self._metadata.tables[table]
        elif table in self._metadata.tables:
            return table
        else:
            error_string = f'Table "{table}" ist unbekannt.\n'
            error_string += 'Folgende Tables existieren:\n'
            for avlbl_table_name in self.table_names:
                error_string += f'{avlbl_table_name}\n'
            raise ValueError(error_string)

    def change_dim_value(self, DT_table, dim_dict: dict, valid_as_of: date = None, scd1=False):
        if type(DT_table) == str:
            table = self.__get_table_from_name(DT_table)  # Suche die entsprechende Table zum gegebenen Namen raus
        elif DT_table in self._metadata.tables:
            table = DT_table
        else:
            raise ValueError(f'{DT_table} unbekannt!')

        fach_ID_name = self._business_id_name[table]
        if fach_ID_name in dim_dict.keys():
            fach_ID = dim_dict[fach_ID_name]
        else:
            raise ValueError("Datensatz kann nicht zugeordnet werden, da eine fachliche ID fehlt!")

        if scd1:
            print('ACHTUNG: Werte werden nun einfach überschrieben!')

        if not scd1:
            if any([k in self.scd3[table].values() for k in dim_dict.keys()]):
                raise ValueError(
                    'Um Spalten zu ändern, in denen nach SCD-Typ 3 historisierte Werte gespeichert werden, scd1 = True '
                    'setzen')
            elif any([k == self.__scd2_cols['current_flag'] for k in dim_dict.keys()]):
                raise ValueError(f'Manuelles Ändern der Spalte {self.__scd2_cols["current_flag"]} '
                                 f'ist nicht erlaubt! scd1 = True setzen!')
        DT_table_keys = [col.description for col in table.c]
        for col in table.columns:
            if col.primary_key:
                DT_table_keys.remove(col.description)
                print(f"{col.description} removed")
        if all([k in DT_table_keys for k in dim_dict.keys()]):
            tech_id = self.__get_current_dim_id(table, fach_ID)  # Suche die id (Primary Key) in der DB
            with self.__engine.begin() as conn:  # Baue eine Verbindung zur DB auf. Diese schließt automatisch
                if tech_id is None:  # Die fachliche ID existiert noch nicht
                    if not all([k in dim_dict for k in DT_table_keys]):
                        print('Nicht alle Daten vorhanden. Es werden NULL-Werte bzw. DEFAULT-Werte gespeichert.')

                    if table == self._DT_time_table:
                        dat = fach_ID
                        new_time_dict = {}
                        new_time_dict['datum'] = dat
                        new_time_dict['tag_der_woche'] = dat.isoweekday()
                        new_time_dict['tag_des_jahres'] = dat.toordinal() - date(dat.year, 1, 1).toordinal() + 1
                        new_time_dict['tag_des_monats'] = dat.day
                        new_time_dict['tag_name'] = dat.strftime('%A')
                        new_time_dict['kalenderwoche'] = dat.isocalendar()[1]
                        new_time_dict['monat_des_jahres'] = dat.month
                        new_time_dict['monat_name'] = dat.strftime('%B')
                        new_time_dict['jahr'] = dat.year
                        conn.execute(self._DT_time_table.insert(), new_time_dict)
                    else:
                        new_row_dict = dim_dict
                        if self.scd2[table]:
                            if valid_as_of is not None:
                                pass
                            else:
                                valid_as_of = conn.execute(Table.select(func.current_date())).fetchone()[0]
                            new_row_dict[self.__scd2_cols['valid_as_of']] = valid_as_of
                            new_row_dict[self.__scd2_cols['valid_until']] = date.max
                            new_row_dict[self.__scd2_cols['current_flag']] = True
                        conn.execute(table.insert(), new_row_dict)
                    return True
                else:
                    stored_values = conn.execute(Table.select(table)
                                                 .where(table.columns.id == tech_id)).fetchone()

                    if all([v == stored_values[k] for k, v in dim_dict.items()]):  # Prüfen ob Werte gleich geblieben
                        print('Übergebene Werte entsprechen gespeicherten Werten! Werte werden nicht erneut '
                              'gespeichert!')
                        return
                    else:
                        if any(x in self.scd2[table] for x in dim_dict.keys()) and not scd1:
                            # SCD-Typ 2 ausführen -> speichere eine neue Datenreihe
                            store_new_row = True
                            if valid_as_of is not None:
                                pass
                            else:
                                valid_as_of = conn.execute(Table.select(func.current_date())).fetchone()[0]

                            old_valid_as_of = conn.execute(Table.select(table.columns[self.__scd2_cols['valid_as_of']])
                                                           .where(table.columns.id == tech_id)).fetchone()[0]

                            if valid_as_of <= old_valid_as_of:
                                error_str = 'Der Gültigkeitsbeginn der geänderten Daten muss nach dem ' \
                                            'Gültigkeitsbeginn ' \
                                            'der zuletzt gültigen Datenreihe liegen. '
                                error_str += f'\nGültigkeitsbeginn der geänderten Daten: {valid_as_of}'
                                error_str += f'\nGültigkeitsbeginn der zuletzt gültigen: {old_valid_as_of}'
                                raise ValueError(error_str)
                        else:
                            store_new_row = False

                        old_row = conn.execute(Table.select(table).where(table.columns.id == tech_id)).fetchone()
                        new_row = dict(old_row)
                        new_row.pop('id')
                        for k, v in dim_dict.items():
                            new_row[k] = v

                        if not scd1:
                            for k in dim_dict.keys():
                                if k in self.scd3[table].keys():
                                    # SCD-Typ 3 ausführen
                                    old_value = conn.execute(Table.select(
                                        table.columns[k]).where(
                                        table.columns.id == tech_id)).fetchone()[0]
                                    scd3_column = self.scd3[table][k]
                                    new_row[k] = dim_dict[k]  # Neuer Wert
                                    new_row[scd3_column] = old_value  # Alter Wert

                        if store_new_row:
                            new_row[self.__scd2_cols['valid_as_of']] = valid_as_of
                            new_row[self.__scd2_cols['valid_until']] = date.max
                            conn.execute(table.insert(), new_row)
                            last_old_validity_day = valid_as_of - timedelta(days=1)
                            conn.execute(table.update().where(
                                table.columns.id == tech_id).values(
                                {self.__scd2_cols['current_flag']: False,
                                 self.__scd2_cols['valid_until']: last_old_validity_day}))
                        else:
                            conn.execute(table.update().where(
                                table.columns.id == tech_id).values(
                                dim_dict))
        else:
            raise ValueError(f'Nicht alle Spalten aus dem dim_dict: dict existieren in {table.name}')

    def __save_date(self, datum: date):
        self.change_dim_value('DT_Zeit', {'datum', datum})

    def __etl(self, dumpDF: pd.DataFrame):

        aggregation_date = date.fromisoformat(dumpDF.iloc[0]['datum'])  # Das erste Datum, über das aggregiert wird
        aggregation_complete = False  # Flag, das den Abschluss einer Aggregation (über einen Tag) signalisiert
        self.__save_date(aggregation_date)  # Speichere das Datum ab, damit dazu eine technische id existiert
        new_FT_rows = []
        for dump_index, dump_row in dumpDF.iterrows():  # iteriere über alle Datenreihen des eingelesenen DataFrames
            cur_prod = dump_row["produkt_ID"]  # die Produkt-ID in der aktuellen Datenreihe
            cur_pers = dump_row["verkaeufer_ID"]  # die Personal-ID in der aktuellen Datenreihe
            cur_date = date.fromisoformat(dump_row["datum"])  # das Datum in der aktuellen Datenreihe

            """
            Die Schleife aggregiert die passenden Daten (gleicher Verkäufer + gleiches Produkt + gleicher Kunde)
            über einen Tag. Liegt in der aktuellen Datenreihe ein neues Datum vor, wird die Aggregation abgeschlossen.
            Die aggregierten Daten werden gespeichert und eine neue Aggregation wird begonnen.
            """
            if cur_date != aggregation_date:
                aggregation_complete = True
                with self.engine.begin() as conn:
                    conn.execute(self.FT_Umsatzanalyse_table.insert(), new_FT_rows)
                    new_FT_rows = []
                    print(f"{aggregation_date} stored")
                    aggregation_date = cur_date


            new_FT_row = {}
            with self.engine.begin() as conn:
                if cur_prod in self.all_saved_prods:
                    pass
                else:
                    self.__store_dim_row(dump_row, conn, cur_prod, 'Produkt')

                result = conn.execute(Table.select(self.DT_produkt_table.c.id).where(self.DT_produkt_table.c.produkt_ID == cur_prod))
                new_FT_row["produkt_id"] = result.fetchone()[0]

                if cur_pers in self.all_saved_verk:
                    pass
                else:
                    self.__store_dim_row(dump_row, conn, cur_pers, 'Verkaeufer')

                result = conn.execute(Table.select(self.DT_verkaeufer_table.c.id).where(self.DT_verkaeufer_table.c.verkaeufer_ID == cur_pers))
                new_FT_row["verkaeufer_id"] = result.fetchone()[0]

                if cur_date in self.all_saved_dates:
                    pass
                else:
                    self.__store_dim_row(dump_row, conn, cur_date, 'Zeit')

                result = conn.execute(Table.select(self.DT_zeit_table.c.id).where(self.DT_zeit_table.c.datum == cur_date))
                new_FT_row["zeit_id"] = result.fetchone()[0]

                """
                TRANSFORMATION
                """
                vk_preis = Decimal.from_float(dump_row['verkaufspreis_netto']).quantize(self.TWODIGITS)
                menge = dump_row['menge']
                new_FT_row['verkaufspreis_summe_netto'] = menge * vk_preis
                new_FT_row['menge'] = menge

                """ 
                AGGREGATION
                """
                already_existed = False
                for row in new_FT_rows:
                    if (row['produkt_id'] == new_FT_row['produkt_id'] and
                                    row['verkaeufer_id'] == new_FT_row['verkaeufer_id']):
                        row['menge'] += new_FT_row['menge']
                        row['verkaufspreis_summe_netto'] += new_FT_row['verkaufspreis_summe_netto']
                        already_existed = True

                if not already_existed:
                    new_FT_rows.append(new_FT_row)

                """
                LADEN DER FAKTEN
                """
                if dump_index == (dumpDF.shape[0] - 1):
                    conn.execute(self.FT_Umsatzanalyse_table.insert(), new_FT_rows)
                    print(f"{cur_date} stored")
                    print("Complete!")

    def etl_csv(self, csv_file):
        dumpDF = self.__read_csv(csv_file)
        self.__etl(dumpDF)