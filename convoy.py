import json
import pandas as pd
import sqlite3


class Convoy:
    def __init__(self):
        self.data, self.file_name, self.extension = self.input_file()
        self.size = self.data.shape[0]

    @staticmethod
    def input_file():
        while input_str := input('Input file name\n'):
            if input_str.endswith('xlsx'):
                return pd.read_excel(input_str, sheet_name='Vehicles', dtype=str), input_str[:-5], 'xlsx'
            elif input_str.endswith('csv'):
                extension = True if '[CHECKED]' in input_str else 'csv'
                return pd.read_csv(input_str), input_str.replace('[CHECKED]', '')[:-4], extension
            elif input_str.endswith('s3db'):
                return pd.read_sql('SELECT * FROM convoy', sqlite3.connect(input_str)), input_str[:-5], 's3db'

    def export_to_csv(self):
        self.data.to_csv(f'{self.file_name}.csv', index=None)
        print(f"{self.size} line{'s were' if self.size > 1 else ' was'} added to {self.file_name}.csv")

    def export_to_sql(self):
        self.vehicles_score()
        with sqlite3.connect(f"{self.file_name}.s3db") as conn:
            types = {c: 'INTEGER PRIMARY KEY' if c == 'vehicle_id' else 'INTEGER NOT NULL' for c in self.data.columns}
            self.data.to_sql('convoy', conn, index=None, dtype=types, if_exists='replace')
        print(f"{self.size} record{'s were' if self.size > 1 else ' was'} inserted into {self.file_name}.s3db")

    def export_to_json(self):
        fit = self.data.loc[self.data['score'] > 3]
        fit.drop(columns='score', inplace=True)
        with open(f'{self.file_name}.json', 'w') as file:
            file.write(json.dumps({'convoy': fit.to_dict(orient='records')}, indent=4))
        print(f"{fit.shape[0]} vehicle{'s were' if fit.shape[0] > 1 else ' was'} saved into {self.file_name}.json")

    def export_to_xml(self):
        def body(row):
            return '\t<vehicle>\n{}\n\t</vehicle>'.format(
                '\n'.join('\t\t<{0}>{1}</{0}>'.format(i, row[i]) for i in row.index))
        fit = self.data.loc[self.data['score'] <= 3]
        fit.drop(columns='score', inplace=True)
        with open(f'{self.file_name}.xml', 'w') as file:
            print('<convoy>', '\n'.join(fit.apply(body, axis=1)), '</convoy>', sep='\n', file=file)
        print(f"{fit.shape[0]} vehicle{'s were' if fit.shape[0] != 1 else ' was'} saved into {self.file_name}.xml")

    def fix_data(self):
        fixed = (self.data.replace(r'\d', '', regex=True) != '').sum().sum()
        self.data = self.data.replace(r'\D', '', regex=True).astype('int32')
        self.data.to_csv(f'{self.file_name}[CHECKED].csv', index=None)
        print(f"{fixed} cell{'s were' if fixed > 1 else ' was'} corrected in {f'{self.file_name}[CHECKED].csv'}")

    def process_data(self):
        self.export_to_csv() if self.extension == 'xlsx' else None
        self.fix_data() if self.extension in ('xlsx', 'csv') else None
        self.export_to_sql() if self.extension != 's3db' else None
        self.export_to_json()
        self.export_to_xml()

    def vehicles_score(self):
        self.data['score'] = (self.data['fuel_consumption'] * 4.5) / self.data['engine_capacity']
        self.data['score'] = self.data['score'].apply(lambda x: 0 if x > 2 else 1 if x >= 1 else 2)
        self.data['score'] += self.data['fuel_consumption'].apply(lambda x: 2 if x <= 230 / 4.5 else 1)
        self.data['score'] += self.data['maximum_load'].apply(lambda x: 2 if x >= 20 else 0)


if __name__ == '__main__':
    Convoy().process_data()
