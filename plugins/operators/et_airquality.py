from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import s3fs
import os
import hashlib


class ETAirQualityOperator(BaseOperator):
    """Class used to extract and transform air quality data"""
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, aws_key_id, aws_secret_key, s3_in_bucket, s3_in_prefix,
                 s3_out_bucket, s3_out_prefix, *args, **kwargs):
        super(ETAirQualityOperator, self).__init__(*args, **kwargs)

        self.aws_key_id = aws_key_id
        self.aws_secret_key = aws_secret_key
        self.s3_in_bucket = s3_in_bucket
        self.s3_in_prefix = s3_in_prefix
        self.s3_out_bucket = s3_out_bucket
        self.s3_out_prefix = s3_out_prefix

    def execute(self, context):
        # Set aws credentials as OS environment variables (used by s3fs)
        os.environ['AWS_ACCESS_KEY_ID'] = self.aws_key_id
        os.environ['AWS_SECRET_ACCESS_KEY'] = self.aws_secret_key
        # Extract data
        date = context['ds']
        df = self.read_data(date)
        self.log.info(f'Data extraction completed! ({df.shape[0]})')
        # Transform data
        df = self.transform_data(df)
        self.log.info(f'Data transformation completed!')
        # Write data to s3
        self.write_data(df, date)
        self.log.info(f'Data write to s3 completed!')

    def read_data(self, date):
        """Extract data from s3"""

        def get_files_s3(s3_bucket, prefix, suffix):
            """Get path to all files in given s3 bucket with given prefix and
             suffix"""
            fs = s3fs.S3FileSystem(anon=True)
            content = fs.ls(f'{s3_bucket}/{prefix}')
            return [f's3a://{f}' for f in content if f[-len(suffix):] == suffix]

        # Get list of all data files for a given day
        files = get_files_s3(self.s3_in_bucket,
                             f'{self.s3_in_prefix}/{date}/', '.ndjson')
        # Load data from all data files into a single dataframe
        df = pd.concat((pd.read_json(f, lines=True) for f in files),
                       ignore_index=True, sort=True)
        return df

    def transform_data(self, df):
        """Transform data"""

        def hash_string(string):
            """Hash string using md5 and convert to bigint"""
            res = hashlib.md5(string.encode(encoding='UTF-8'))
            bigint_max = 2**64//2
            return int(res.hexdigest(), 16) % bigint_max

        # Convert ppm values
        molecular_weight = {
            'o3': 48,
            'so2': 64.066,
            'co': 28.01,
            'no2': 46.0055
        }

        def convert_ppm(param, unit):
            if unit == 'ppm':
                return 1000 * 0.0409 * molecular_weight.get(param, -1)
            else:
                return 1

        conversion_factor = df['parameter'].combine(df['unit'], convert_ppm)
        df['value'] = conversion_factor * df['value']
        df['value'] = df['value'].round(1)
        df.drop(columns=['unit'], inplace=True)
        self.log.info('ppm conversion completed.')

        # Drop redundant columns
        df.drop(columns=['averagingPeriod', 'mobile'], inplace=True)
        # Remove entries with missing values in the following fields:
        # value/date/coordinates/parameter
        df.dropna(subset=['value', 'coordinates', 'parameter', 'date'],
                  inplace=True)
        # Remove entries with negative values
        df.drop(df[df.value < 0].index, inplace=True)
        self.log.info('Data cleaning completed.')

        # Unpack and enrich geodata
        df['latitude'] = df['coordinates'].map(
            lambda x: round(x['latitude'], 1), na_action='ignore')
        df['longitude'] = df['coordinates'].map(
            lambda x: round(x['longitude'], 1), na_action='ignore')
        df['zone_id'] = df['latitude'].combine(
            df['longitude'], lambda x1, x2: f'lat{x1:.1f}long{x2:.1f}')
        df.drop(columns=['coordinates'], inplace=True)
        self.log.info('Transformation of geodata completed.')

        # Unpack time data
        df['ts'] = df['date'].map(
            lambda x: pd.Timestamp(x['local']).floor(freq='H'),
            na_action='ignore')
        df['timestamp'] = df['ts'].map(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        df.drop(columns=['date'], inplace=True)
        self.log.info('Transformation of timestamp data completed.')

        # Aggregate data to hourly buckets
        groupby_cols = ['ts', 'parameter', 'location', 'zone_id']
        agg_dict = {col: 'first' for col in list(df.columns) if col not in (
                groupby_cols + ['value'])}
        agg_dict['value'] = 'mean'
        df = df.groupby(groupby_cols, as_index=False).agg(agg_dict)
        self.log.info('Data aggregated to hourly buckets.')

        # Create indexes for location, source and city tables
        df['city_id'] = df['city'].combine(
            df['zone_id'], lambda x1, x2: hash_string(x1 + x2))
        df['location_id'] = df['location'].combine(
            df['zone_id'], lambda x1, x2: hash_string(x1 + x2))
        df['source_id'] = df['sourceName'].combine(
            df['sourceType'], lambda x1, x2: hash_string(x1 + x2))
        self.log.info('Indexes for location, source and city tables created.')

        # Unpack attribution data
        df['attr_id'] = df['attribution'].map(
            lambda x: hash_string(str(x)), na_action='ignore')
        df['attr_name1'] = df['attribution'].map(
            lambda x: x[0].get('name', None), na_action='ignore')
        df['attr_name2'] = df['attribution'].map(
            lambda x: x[1].get('name', None) if len(x) == 2 else None,
            na_action='ignore')
        df['attr_url1'] = df['attribution'].map(
            lambda x: x[0].get('url', None), na_action='ignore')
        df['attr_url2'] = df['attribution'].map(
            lambda x: x[1].get('url', None) if len(x) == 2 else None,
            na_action='ignore')
        df.drop(columns=['attribution'], inplace=True)
        self.log.info('Transformation of attribution data completed.')

        # Create index for air quality table
        df['id'] = df['timestamp'] + df['parameter'] + df['location_id'].apply(str)
        df['id'] = df['id'].map(lambda x: hash_string(x))
        df.set_index('id', inplace=True)
        self.log.info('Index for air quality table created.')

        return df

    def write_data(self, df, date):
        """Write processed data to s3 as parquet files"""

        def make_sub_df(src_df, index_col, cols):
            """Create sub-DataFrame from DataFrame

            Args:
                src_df(obj): pandas DataFrame
                index_col(str): index column of the sub-DataFrame
                cols(list of str): list of columns of the sub-DataFrame
            """
            cols.append(index_col)
            return src_df[cols].drop_duplicates(index_col).set_index(index_col)

        def make_time_df(src_df):
            time_df = make_sub_df(src_df, 'timestamp', ['ts'])
            time_df['hour'] = time_df['ts'].map(lambda x: x.hour)
            time_df['day'] = time_df['ts'].map(lambda x: x.day)
            time_df['week'] = time_df['ts'].map(lambda x: x.week)
            time_df['month'] = time_df['ts'].map(lambda x: x.month)
            time_df['year'] = time_df['ts'].map(lambda x: x.year)
            time_df['weekday'] = time_df['ts'].map(lambda x: x.dayofweek)
            return time_df.drop(columns=['ts'])

        dfs_dict = {
            'attribution': make_sub_df(df, 'attr_id',
                                       ['attr_name1', 'attr_url1',
                                        'attr_name2', 'attr_url2']),
            'city': make_sub_df(df, 'city_id', ['city', 'country', 'zone_id']),
            'location': make_sub_df(df, 'location_id', ['location', 'zone_id']),

            'zone': make_sub_df(df, 'zone_id', ['latitude', 'longitude']),
            'source': make_sub_df(df, 'source_id', ['sourceName', 'sourceType']),
            'time': make_time_df(df),
            'air_quality': df[['parameter', 'value', 'timestamp', 'zone_id',
                               'city_id', 'attr_id', 'source_id',
                               'location_id']]
        }

        s3_path_common = f's3a://{self.s3_out_bucket}/{self.s3_out_prefix}'

        for key, val in dfs_dict.items():
            s3_path = f'{s3_path_common}/{date}/{key}.csv'
            val.to_csv(s3_path)
            self.log.info(f'{key} data saved to s3 ({val.shape[0]})')
