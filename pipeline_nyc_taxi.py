import requests
import luigi
import os


def download_dataset(filename: str) -> requests.Response:
    url = f'https://nyc-tlc.s3.amazonaws.com/trip+data/{filename}'
    response = requests.get(url, stream=True)
    response.raise_for_status()
    return response


def get_filename(year: int, month: int) -> str:
    return f'fhv_tripdata_{year}-{month:02}.csv'


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


if __name__ == '__main__':
    luigi.build([DownloadTaxiTripTask(year=2021, month=7)], local_scheduler=True)
