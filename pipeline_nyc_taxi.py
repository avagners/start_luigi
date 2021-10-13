import requests
import luigi
import os
import pandas as pd
from typing import List


def download_dataset(filename: str) -> requests.Response:
    url = f'https://s3.amazonaws.com/nyc-tlc/trip+data/{filename}'
    response = requests.get(url, stream=True)
    response.raise_for_status()
    return response


def get_filename(year: int, month: int) -> str:
    return f'yellow_tripdata_{year}-{month:02}.csv'


class DownloadTaxiTripTask(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()

    @property
    def filename(self):
        return get_filename(self.year, self.month)

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


class AggregateTaxiTripTask(luigi.Task):
    year = luigi.IntParameter()
    month = luigi.IntParameter()

    def requires(self):
        return DownloadTaxiTripTask(year=self.year, month=self.month)

    def run(self):
        with self.input().open() as input, self.output().open('w') as output:
            self.output().makedirs()
            df = group_by_pickup_date(input)
            df.to_csv(output.name, index=False)

    def output(self):
        filename = get_filename(self.year, self.month)[:-4]
        return luigi.LocalTarget(
            os.path.join('yellow-taxi-data', f'{filename}-agg.csv')
        )


if __name__ == '__main__':
    luigi.build([AggregateTaxiTripTask(year=2020, month=11)], local_scheduler=True)
