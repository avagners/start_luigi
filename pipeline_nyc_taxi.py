import requests
import luigi
import os
import datetime
import pandas as pd
from typing import List
from luigi.contrib.sqla import CopyToTable
from luigi.util import requires
from sqlalchemy import Numeric, Date
from dateutil.relativedelta import relativedelta


def download_dataset(filename: str) -> requests.Response:
    url = f'https://s3.amazonaws.com/nyc-tlc/trip+data/{filename}'
    response = requests.get(url, stream=True)
    response.raise_for_status()
    return response


def get_filename(year: int, month: int) -> str:
    return f'yellow_tripdata_{year}-{month:02}.csv'


class DownloadTaxiTripTask(luigi.Task):
    date = luigi.MonthParameter()

    @property
    def filename(self):
        return get_filename(self.date.year, self.date.month)

    def run(self):

        self.output().makedirs()  # in case path does not exist
        response = download_dataset(self.filename)

        with self.output().open(mode='w') as f:
            for chunk in response.iter_lines():
                f.write('{}\n'.format(chunk.decode('utf-8')))

    def output(self):
        return luigi.LocalTarget(os.path.join('yellow-taxi-data', self.filename))


def group_by_pickup_date(
    file_object, group_by='pickup_date', metrics: List[str] = None
) -> pd.DataFrame:
    if metrics is None:
        metrics = ['tip_amount', 'total_amount']

    df = pd.read_csv(file_object)
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['pickup_date'] = df['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d')
    df = df.groupby(group_by)[metrics].sum().reset_index()
    return df


@requires(DownloadTaxiTripTask)
class AggregateTaxiTripTask(luigi.Task):
    date = luigi.MonthParameter()

    def run(self):
        with self.input().open() as input, self.output().open('w') as output:
            self.output().makedirs()
            df = group_by_pickup_date(input)
            df.to_csv(output.name, index=False)

    def output(self):
        filename = get_filename(self.date.year, self.date.month)[:-4]
        return luigi.LocalTarget(
            os.path.join('yellow-taxi-data', f'{filename}-agg.csv')
        )


@requires(AggregateTaxiTripTask)
class CopyTaxiTripData2SQLite(CopyToTable):

    table = 'nyc_trip_agg_data'
    connection_string = 'sqlite:///sqlite.db'

    columns = [
        (['pickup_date', Date()], {}),
        (['tip_amount', Numeric(2)], {}),
        (['total_amount', Numeric(2)], {}),
    ]

    def rows(self):
        with self.input().open() as csv_file:
            # use pandas not to deal with type conversions
            df = pd.read_csv(csv_file, parse_dates=['pickup_date'])
            rows = df.to_dict(orient='split')['data']
            return rows


class YellowTaxiDateRangeTask(luigi.WrapperTask):
    start = luigi.MonthParameter()
    stop = luigi.MonthParameter()

    def requires(self):
        current_month = self.start
        while current_month <= self.stop:
            yield CopyTaxiTripData2SQLite(date=current_month)
            current_month += relativedelta(months=1)


if __name__ == '__main__':
    luigi.build([YellowTaxiDateRangeTask(start=datetime.date(2020, 6, 1), stop=datetime.date(2020, 12, 1))], workers=3)
