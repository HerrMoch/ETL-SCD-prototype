from datetime import date, timedelta
from decimal import Decimal
from sqlalchemy import Table, Column
from sqlalchemy import create_engine, and_, func
from sqlalchemy.engine.base import Connection
import pandas as pd
from DatabaseMeta import DatabaseMeta


class ETLPipeline:
    # TWODIGITS = Decimal('0.01')


    def __init__(self, database_metadata: DatabaseMeta):
        self.dbm = database_metadata

        self.__engine = create_engine(self.dbm.database_path)
        self.dbm.metadata.create_all(self.__engine)

    def change_database_metadata(self, database_metadata: DatabaseMeta):
        self.__init__(database_metadata)

    def __get_current_dim_id(self, DT_table, fach_ID, datum: date = None):
        with self.__engine.connect() as conn:
            business_id_name = self.dbm.business_id_name[DT_table]
            if self.dbm.scd2[DT_table]:
                # In dieser Dimensionstabelle wird SCD-Typ 2 genutzt
                if datum is None:
                    # aktuell gültige Datenreihe
                    result = conn.execute(Table.select(DT_table).where(
                        and_(DT_table.c[business_id_name] == fach_ID,  # Übereinstimmung der fachlichen ID
                             DT_table.c[self.dbm.scd2_cols['current_flag']])))  # aktuellen Datensatz finden
                else:
                    # zum Zeitpunkt 'date' gültige Datenreihe
                    sql_str = DT_table.select(DT_table.columns.id).where(
                        and_(DT_table.columns[business_id_name] == fach_ID,
                             datum >= DT_table.columns[self.dbm.scd2_cols['valid_as_of']],
                             datum <= DT_table.columns[self.dbm.scd2_cols['valid_until']])
                    )
                    result = conn.execute(sql_str)
            else:
                # In dieser Dimensionstabelle wird kein SCD-Typ 2 genutzt
                result = conn.execute(Table.select(DT_table).where(  # Übereinstimmung der fachlichen ID
                    DT_table.c[business_id_name] == fach_ID))
            result_line = result.fetchone()
            if result_line is None:
                return False
            else:
                return result_line['id']  # gibt die unique id zurück

    def __get_table_from_name(self, table) -> Table:
        if table in self.dbm.table_names:
            return self.dbm.metadata.tables[table]
        elif table in self.dbm.metadata.tables:
            return table
        else:
            error_string = f'Table "{table}" ist unbekannt.\n'
            error_string += 'Folgende Tables existieren:\n'
            for avlbl_table_name in self.dbm.table_names:
                error_string += f'{avlbl_table_name}\n'
            raise ValueError(error_string)

    def change_dim_value(self, DT_table, dim_dict: dict, valid_as_of: date = None, scd1=False):
        if type(DT_table) == str:
            table = self.__get_table_from_name(DT_table)  # Suche die entsprechende Table zum gegebenen Namen raus
        elif DT_table.name in self.dbm.metadata.tables:
            table = DT_table
        else:
            raise ValueError(f'{DT_table} unbekannt!')

        fach_ID_name = self.dbm.business_id_name[table]
        if fach_ID_name in dim_dict.keys():
            fach_ID = dim_dict[fach_ID_name]
        else:
            raise ValueError("Datensatz kann nicht zugeordnet werden, da eine fachliche ID fehlt!")

        if scd1:
            print('ACHTUNG: Werte werden nun einfach überschrieben!')

        if not scd1:
            if any([k in self.dbm.scd3[table].values() for k in dim_dict.keys()]):
                raise ValueError(
                    'Um Spalten zu ändern, in denen nach SCD-Typ 3 historisierte Werte gespeichert werden, scd1 = True '
                    'setzen')
            elif any([k == self.dbm.scd2_cols['current_flag'] for k in dim_dict.keys()]):
                raise ValueError(f'Manuelles Ändern der Spalte {self.dbm.scd2_cols["current_flag"]} '
                                 f'ist nicht erlaubt! scd1 = True setzen!')
        DT_table_keys = [col.description for col in table.c]
        for col in table.columns:
            if col.primary_key:
                DT_table_keys.remove(col.description)
        if all([k in DT_table_keys for k in dim_dict.keys()]):
            tech_id = self.__get_current_dim_id(table, fach_ID)  # Suche die id (Primary Key) in der DB
            with self.__engine.begin() as conn:  # Baue eine Verbindung zur DB auf. Diese schließt automatisch
                if tech_id is False:  # Die fachliche ID existiert noch nicht

                    if table == self.dbm.DT_time_table:
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
                        result = conn.execute(self.dbm.DT_time_table.insert(), new_time_dict)
                    else:
                        new_row_dict = dim_dict
                        if self.dbm.scd2[table]:
                            if valid_as_of is not None:
                                pass
                            else:
                                valid_as_of = conn.execute(Table.select(func.current_date())).fetchone()[0]
                            new_row_dict[self.dbm.scd2_cols['valid_as_of']] = valid_as_of
                            new_row_dict[self.dbm.scd2_cols['valid_until']] = date.max
                            new_row_dict[self.dbm.scd2_cols['current_flag']] = True
                        result = conn.execute(table.insert(), new_row_dict)
                    return result.inserted_primary_key[0]
                else:
                    stored_values = conn.execute(Table.select(table)
                                                 .where(table.columns.id == tech_id)).fetchone()

                    if all([v == stored_values[k] for k, v in dim_dict.items()]):  # Prüfen ob Werte gleich geblieben
                        print('Übergebene Werte entsprechen gespeicherten Werten! Werte werden nicht erneut '
                              'gespeichert!')
                        return False
                    else:
                        if any(x in self.dbm.scd2[table] for x in dim_dict.keys()) and not scd1:
                            # SCD-Typ 2 ausführen -> speichere eine neue Datenreihe
                            store_new_row = True
                            if valid_as_of is not None:
                                pass
                            else:
                                valid_as_of = conn.execute(Table.select(func.current_date())).fetchone()[0]

                            old_valid_as_of = \
                                conn.execute(Table.select(table.columns[self.dbm.scd2_cols['valid_as_of']])
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
                                if k in self.dbm.scd3[table].keys():
                                    # SCD-Typ 3 ausführen

                                    old_value = conn.execute(Table.select(
                                        table.columns[k]).where(
                                        table.columns.id == tech_id)).fetchone()[0]
                                    scd3_column = self.dbm.scd3[table][k]
                                    print(f'{k} in {table} zu {scd3_column}')
                                    new_row[k] = dim_dict[k]  # Neuer Wert
                                    new_row[scd3_column] = old_value  # Alter Wert

                        if store_new_row:
                            new_row[self.dbm.scd2_cols['valid_as_of']] = valid_as_of
                            new_row[self.dbm.scd2_cols['valid_until']] = date.max
                            result = conn.execute(table.insert(), new_row)
                            stored_id = result.inserted_primary_key
                            last_old_validity_day = valid_as_of - timedelta(days=1)
                            conn.execute(table.update().where(
                                table.columns.id == tech_id).values(
                                {self.dbm.scd2_cols['current_flag']: False,
                                 self.dbm.scd2_cols['valid_until']: last_old_validity_day}))
                        else:
                            stored_id = stored_values['id']
                            conn.execute(table.update().where(
                                table.columns.id == tech_id).values(
                                new_row))

                        if self.dbm.inner_depend[table] is not None and len(self.dbm.inner_depend[table].keys()) > 0:
                            for master_col, slave_cols in self.dbm.inner_depend[table].items():
                                if any(col in slave_cols for col in dim_dict.keys()):
                                    if master_col in dim_dict.keys():
                                        inner_dep_id = dim_dict[master_col]
                                    else:
                                        inner_dep_id = stored_values[master_col]

                                    to_store_items = {}
                                    for slave_col in slave_cols:
                                        if slave_col in dim_dict.keys():
                                            to_store_items[slave_col] = dim_dict[slave_col]
                                        else:
                                            to_store_items[slave_col] = stored_values[slave_col]

                                    conn.execute(table.update().where(
                                        table.columns[master_col] == inner_dep_id).values(
                                        to_store_items))

                    return stored_id
        else:
            raise ValueError(f'Nicht alle Spalten aus dem dim_dict: dict existieren in {table.name}')

    def __store_retrieve_date_id(self, datum: date):
        result = self.__get_current_dim_id(self.dbm.DT_time_table, datum)
        if result is False:
            return self.change_dim_value(self.dbm.DT_time_table, {'datum': datum})
        else:
            return result

    def etl_fact_csv(self, csv_file):
        """
        Führt den ETL-Prozess für ein csv-file mit Faktendaten (Transaktionsdaten) durch.
        Die Daten werden aus der Datei extrahiert, transformiert, aggregiert (tageweise) und in die
        Datenbank geladen.

        :param csv_file: Die csv-Datei der neuen Fakten. Folgende Spalten werden benötigt:
            'datum': Datum der Transaktion, 'personal_code': Fachliche ID des Personals,
            'produkt_code': Fachliche ID des Produkts, 'kunde_code': Fachliche ID des Kunden,
            'Verkaufspreis_Brutto': Brutto-Verkaufspreis eines Artikels,
            'Verkaufspreis_MwSt': Abgeführte Mehrwertsteuer eines Artikels,
            'menge': Verkaufte Menge des Produkts
        :return: Gibt True zurück, wenn der ETL-Prozess abgeschlossen ist.
        """

        """
        EXTRAKTION
        Liest die csv-Datei in ein Pandas-DataFrame ein
        """
        dumpDF = pd.read_csv(csv_file)

        # Erzeuge ein leeres Pandas-Dataframe mit vordefinierten Spaltennamen
        # Das Dataframe nimmt die zu speichernden Fakten-Datenreihen auf
        col_headings = [co.description for co in self.dbm.FT_sales_table.columns]
        rows = pd.DataFrame(columns=col_headings)

        for dump_index, dump_row in dumpDF.iterrows():
            # Iteriere über alle Reihen des eingelesenen DataFrames 'dumpDF'
            cur_prod = dump_row['produkt_code']  # der Produkt-Code in der aktuellen Datenreihe
            cur_empl = dump_row['personal_code']  # die Personal-Code in der aktuellen Datenreihe
            cur_clie = dump_row['kunde_code']  # der Kunde-Code in der aktuellen Datenreihe
            cur_date = date.fromisoformat(dump_row['datum'])  # das Datum in der aktuellen Datenreihe

            # Erzeuge ein leeres 'dict', dass zur Speicherung der neuen Fakten-Datenreihe dient
            new_FT_row = {}

            # Ableiten der technischen IDs zu den fachlichen Codes
            cur_prod_id = self.__get_current_dim_id(self.dbm.DT_product_table, cur_prod, cur_date)
            cur_empl_id = self.__get_current_dim_id(self.dbm.DT_employee_table, cur_empl, cur_date)
            cur_clie_id = self.__get_current_dim_id(self.dbm.DT_client_table, cur_clie, cur_date)
            cur_date_id = self.__store_retrieve_date_id(cur_date)

            # Abspeichern der technischen IDs im 'dict'
            new_FT_row['produkt_id'] = cur_prod_id
            new_FT_row['personal_id'] = cur_empl_id
            new_FT_row['kunde_id'] = cur_clie_id
            new_FT_row['zeit_id'] = cur_date_id

            """
            TRANSFORMATION
            Brutto-Preis und MwSt-Anteil werden zum Netto-Preis verrechnet.
            Mit der Menge verrechnet ergibt sich die Netto-Umsatzsumme.
            Allein die Netto-Umsatzsumme wird anschließend gespeichert.
            """
            # Berechnen des Netto-Preises
            vk_preis_brutto = dump_row['Verkaufspreis_Brutto']
            vk_preis_mwst = dump_row['Verkaufspreis_MwSt']
            vk_preis = vk_preis_brutto - vk_preis_mwst

            # Berechnen der Netto-Umsatzsumme
            menge = dump_row['menge']
            new_FT_row['umsatz_summe_netto'] = menge * vk_preis
            new_FT_row['menge_artikel'] = menge

            """ 
            AGGREGATION
            Existiert im DataFrame bereits eine Datenreihe mit identischen technischen IDs,
            wird die Menge und die Netto-Umsatzsumme hinzuaddiert.
            Existiert noch keine solche Datenreihe, wird die aktuelle Datenreihe im DataFrame gespeichert.
            """

            # Condition für die Übereinstimmung der technischen IDs
            bo = (rows['produkt_id'] == cur_prod_id) & (rows['personal_id'] == cur_empl_id) \
                 & (rows['kunde_id'] == cur_clie_id) & (rows['zeit_id'] == cur_date_id)

            # Suche im DataFrame die Datenreihe,
            # bei der alle technischen IDs mit denen der aktuellen Datenreihe übereinstimmen
            select_row = rows.loc[bo]

            if select_row.shape[0] == 0:
                # Es wurde keine übereinstimmende Datenreihe gefunden.
                # Die aktuelle Datenreihe wird im DataFrame gespeichert
                rows = rows.append(new_FT_row, ignore_index=True)
            elif select_row.shape[0] == 1:
                # Es wurde eine übereinstimmende Datenreihe gefunden.
                # Menge und Netto-Umsatzsumme werden hinzuaddiert.
                select_row['umsatz_summe_netto'] += new_FT_row['umsatz_summe_netto']
                select_row['menge_artikel'] += new_FT_row['menge_artikel']
                rows.update(select_row)

        """
        LADEN
        Die im DataFrame gespeicherten Datenreihen werden in die Datenbank geladen
        """
        with self.__engine.begin() as conn:
            # Aufgrund der internen Verrechnung können mehr als 2 Nachkommastellen auftreten.
            # Vor dem Speichern in der Datenbank wird daher auf 2 Nachkommastellen gerundet.
            rows = rows.round({'umsatz_summe_netto': 2})
            rows.to_sql(self.dbm.FT_sales_table.name, conn, if_exists='append', index=False)
            return True

    def etl_dim_csv(self, csv_file):
        df = pd.read_csv(csv_file, dtype=str)
        if 'produkt_code' in df.columns:
            table = self.dbm.DT_product_table
            df['Verkaufspreis_Brutto'] = df['Verkaufspreis_Brutto'].apply(lambda x: float(x))
            df['Verkaufspreis_MwSt'] = df['Verkaufspreis_MwSt'].apply(lambda x: float(x))
            df['verkaufspreis_netto'] = df['Verkaufspreis_Brutto'] - df['Verkaufspreis_MwSt']
            df = df.round({'verkaufspreis_netto': 2})
            df = df.drop(['Verkaufspreis_Brutto', 'Verkaufspreis_MwSt'], axis=1)
        elif 'kunde_code' in df.columns:
            table = self.dbm.DT_client_table
        elif 'personal_code' in df.columns:
            table = self.dbm.DT_employee_table
        else:
            print('Konnte die Datei keiner Dimension zuordnen')
            return False

        if 'uhrzeit' in df.columns:
            df = df.drop('uhrzeit', axis=1)

        df['datum'] = df['datum'].apply(lambda x: date(int(x[-2:]) + 2000, int(x[-5:-3]), int(x[:2])))
        df = df.sort_values('datum')
        for i in range(df.shape[0]):
            row = df.iloc[i]
            datum = row['datum']
            row = row.drop('datum')
            self.change_dim_value(table, row.to_dict(), datum)

    def dump_dim_data_to_csv(self, DT_table, output:str):
        if type(DT_table) == str:
            table = self.__get_table_from_name(DT_table)  # Suche die entsprechende Table zum gegebenen Namen raus
        elif DT_table.name in self.dbm.metadata.tables:
            table = DT_table
        else:
            raise ValueError(f'{DT_table} unbekannt!')

        with open(output, 'w') as file:
            with self.__engine.connect() as conn:
                result = conn.execute(table.select())

