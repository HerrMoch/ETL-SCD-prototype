from sqlalchemy import MetaData
from sqlalchemy import Table, Column
from sqlalchemy import Integer, String, Date, Numeric, Boolean
from sqlalchemy import ForeignKey


class DatabaseMeta:
    LINESEPERATOR = '-' * 40

    def __init__(self, database_path="sqlite:///test.db", verbose_mode=True):
        self.database_path = database_path
        # Festlegungen zu Spaltenbezeichnungen
        self.scd2_cols = {'valid_as_of': 'gueltig_ab',
                          'valid_until': 'gueltig_bis',
                          'current_flag': 'aktuell'}

        self.metadata = MetaData()
        self.DT_time_table = Table("DT_Zeit", self.metadata,
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

        self.DT_product_table = Table("DT_Produkt", self.metadata,
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
                                      Column(self.scd2_cols['valid_as_of'], Date),
                                      Column(self.scd2_cols['valid_until'], Date),
                                      Column(self.scd2_cols['current_flag'], Boolean)
                                      )

        self.DT_employee_table = Table("DT_Personal", self.metadata,
                                       Column('id', Integer, primary_key=True),
                                       Column('personal_code', String),
                                       Column('name', String),
                                       Column('vorname', String),
                                       Column('abteilung', String),
                                       Column('abteilung_vorher', String)
                                       )

        self.DT_client_table = Table("DT_Kunde", self.metadata,
                                     Column('id', Integer, primary_key=True),
                                     Column('kunde_code', String),
                                     Column('name', String),
                                     Column('vorname', String),
                                     Column('firma', String),
                                     Column('plz', String),
                                     Column('ort', String),
                                     Column('land', String)
                                     )

        self.FT_sales_table = Table("FT_Umsatzanalyse", self.metadata,
                                    Column('zeit_id', Integer, ForeignKey('DT_Zeit.id'), primary_key=True),
                                    Column('personal_id', Integer, ForeignKey('DT_Personal.id'),
                                           primary_key=True),
                                    Column('produkt_id', Integer, ForeignKey('DT_Produkt.id'),
                                           primary_key=True),
                                    Column('kunde_id', Integer, ForeignKey('DT_Kunde.id'), primary_key=True),
                                    Column('umsatz_summe_netto', Numeric(64, 2)),
                                    Column('menge_artikel', Integer)
                                    )

        # Alle Faktentabellen
        self.fact_tables = [self.FT_sales_table]

        # Liste mit allen Tabellennamen
        self.table_names = [table for table in self.metadata.tables]
        # Angabe, welche Spalten die fachlichen IDs beinhalten
        self.business_id_name = {self.DT_product_table: 'produkt_code',
                                 self.DT_employee_table: 'personal_code',
                                 self.DT_time_table: 'datum',
                                 self.DT_client_table: 'kunde_code'}
        # Angabe über Abhängigkeiten zwischen Spalten in einer Tabelle
        self.inner_depend = {self.DT_product_table: {
            'hersteller_code': ['hersteller_name', 'hersteller_plz', 'hersteller_ort', 'hersteller_land']},
                             self.DT_employee_table: {}, self.DT_client_table: {}, self.DT_time_table: {}}
        # Angabe, welche Spalten der Dimensionstabellen SCD-Typ 2 nutzen
        self.scd2 = {self.DT_product_table:
                         ['verkaufspreis_netto', 'name', 'variante'],
                     self.DT_employee_table: [],
                     self.DT_time_table: [],
                     self.DT_client_table: []}
        # Angabe, welche Spalten der Dimensionstabellen für SCD-Typ 3 zusammenhängen
        # Der erste Wert eines Wertepaares ist der jeweils aktuellere
        # Beim Ändern des ersten Wertes wird zuvor der zweite Wert mit dem ersten Überschrieben
        # anschließend wird der erste Wert mit dem neuen Wert überschrieben
        self.scd3 = {self.DT_product_table: {},
                     self.DT_employee_table: {
                         'abteilung': 'abteilung_vorher'},
                     self.DT_time_table: {},
                     self.DT_client_table: {}}

        # Prüfung der Logik
        self.__check_tables(verbose_mode)

    def __check_tables(self, verbose):
        if verbose: print('Beginne Prüfung')
        for table in self.metadata.tables.values():
            if table in self.fact_tables:
                break

            if verbose: print(f'\n\nPrüfung der Tabelle "{table.name}:')
            if verbose: print(self.LINESEPERATOR)

            if table in self.business_id_name.keys():
                if self.business_id_name[table] in table.columns:
                    if verbose: print(f'Fachliche ID "{self.business_id_name[table]}": OK!')
                else:
                    raise NameError(f'Fachliche ID "{self.business_id_name[table]}" nicht in Tabelle vorhanden!')
            else:
                raise NameError(f'Keine Metadaten für die fachliche ID in dieser Tabelle vorhanden!')

            if table in self.scd2:
                if verbose: print('Metadaten für SCD-Typ 2 vorhanden: OK')
            else:
                raise NameError('Metadaten für SCD-Typ 2 nicht vorhanden!')

            if len(self.scd2[table]) > 0:
                scd2 = True  # In dieser Dimensionstabelle existieren Spalten mit SCD-Typ 2
                str_in = ''
            else:
                scd2 = False  # In dieser Dimensionstabelle existieren keine Spalten mit SCD-Typ 2
                str_in = 'nicht '

            if verbose: print(f'SCD-Typ 2 {str_in}vorhanden')
            # Prüfung ob die nötigen Spalten vorhanden sind oder fälschlicherweise vorliegen
            for col_name in self.scd2_cols.values():
                if col_name not in table.columns:
                    if scd2:
                        raise NameError(f'Die Spalte "{col_name}" fehlt in der Dimensionstabelle "{table.name}"')
                    else:
                        if verbose: print(f'Spalte "{col_name}" {str_in}vorhanden: OK!')
                else:
                    if scd2:
                        if verbose: print(f'Spalte "{col_name}" vorhanden: OK!')
                    else:
                        warning_str = 'Achtung!\n'
                        warning_str += self.LINESEPERATOR
                        warning_str += f'\nDie Spalte "{col_name}" existiert in der Dimensionstabelle "{table.name}"!'
                        warning_str += '\nIn der Tabelle existieren keine SCD-Typ 2 Spalten.'
                        warning_str += f'\nDer Spaltenname "{col_name}" ist jedoch für den SCD-Typ 2 vorgesehen!\n'
                        warning_str += self.LINESEPERATOR
                        print(warning_str)
            if verbose: print(f'SCD-Typ 2 OK!\n')

            if table in self.scd3:
                if verbose: print('Metadaten für SCD-Typ 3 vorhanden: OK!')
            else:
                raise NameError('Metadaten für SCD-Typ 3 nicht vorhanden!')

            for col in self.scd3[table].keys():
                if col in table.columns:
                    if verbose: print(f'Spalte "{col}" in Tabelle vorhanden: OK!')
                    if self.scd3[table][col] in table.columns:
                        if verbose: print(
                            f'Partnerspalte "{self.scd3[table][col]}" zu "{col}" in Tabelle vorhanden: OK!')
                    else:
                        raise NameError(
                            f'Partnerspalte "{self.scd3[table][col]}" zu "{col}" nicht in Tabelle vorhanden!')
                else:
                    raise NameError(f'Spalte "{col}" nicht in Tabelle vorhanden!')
            if verbose: print('SCD-Typ 3 OK!\n')

            for col in [x.name for x in table.columns]:
                occurance = 0
                if col in self.scd2[table]:
                    if verbose: print(f'Spalte "{col}": SCD-Typ 2')
                    occurance += 1
                if col in self.scd3[table].keys():
                    if verbose: print(f'Spalte "{col}": SCD-Typ 3')
                    occurance += 1

                if occurance == 0:
                    if verbose: print(f'Spalte "{col}": SCD-Typ 1')
                elif occurance > 1:
                    raise ValueError(f'SCD-Typ der Spalte "{col}" mehrfach deklariert!')

            if verbose: print(f'\nTabelle "{table.name}": OK!')
            if verbose: print(self.LINESEPERATOR)
        print('\n\nPrüfung erfolgreich abgeschlossen!')
