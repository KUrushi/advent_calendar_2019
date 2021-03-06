import tensorflow_data_validation as tfdv

from pathlib import Path

from tfx_bsl.arrow import table_util
from google.cloud import bigquery, bigquery_storage_v1beta1
from metaflow import FlowSpec, step, Parameter, current, IncludeFile
import pyarrow as pa
from tensorflow_data_validation.statistics import stats_impl
from tensorflow_data_validation.utils import stats_util


class ValidationFlow(FlowSpec):
    PROJECT_ID = Parameter(
        'PROJECT_ID',
        help='GCPのプロジェクトID',
        default='',
        required=True
    )
    ARTIFACT_PATH = Parameter(
        'ARTIFACT_PATH',
        help='tfdvの成果物を保存するディレクトリ',
        default=Path('./artifact')
    )

    training_query = IncludeFile('TRAINING_DATA_PATH',
                                     default=Path('./sql/training_example.sql'))

    validate_query = IncludeFile('VALIDATE_DATA_PATH',
                                     default=Path('./sql/validate_example.sql'))

    @step
    def start(self):
        if not self.ARTIFACT_PATH.exists():
            self.ARTIFACT_PATH.mkdir()

        self.save_dir = self.ARTIFACT_PATH / Path(current.run_id)
        self.save_dir.mkdir()
        self.next(self.get_example, self.get_validate)

    @step
    def get_example(self):
        bqclient = bigquery.Client(project=self.PROJECT_ID)
        bqstorage_client = bigquery_storage_v1beta1.BigQueryStorageClient()
        result = bqclient.query(self.training_query)
        self.train_data = result.to_arrow(bqstorage_client=bqstorage_client)
        writer = pa.RecordBatchFileWriter((Path(self.save_dir) / Path('training_examples.arrow')).as_posix(),
                                          self.train_data.schema)
        writer.write_table(self.train_data)

        self.next(self.generate_stats)

    @step
    def get_validate(self):
        bqclient = bigquery.Client(project=self.PROJECT_ID)
        bqstorage_client = bigquery_storage_v1beta1.BigQueryStorageClient()
        result = bqclient.query(self.validate_query)
        self.valid_data = result.to_arrow(bqstorage_client=bqstorage_client)

        writer = pa.RecordBatchFileWriter(
            (Path(self.save_dir) / Path('validate_examples.arrow')).as_posix(),
             self.valid_data.schema)
        writer.write_table(self.valid_data)
        self.next(self.generate_validate_stats)

    @step
    def generate_stats(self):
        self.train_stats = stats_impl.generate_statistics_in_memory(table_util.MergeTables([self.train_data]))
        stats_util.write_stats_text(self.train_stats, (Path(self.save_dir) / Path('train_stats.pbtxt')).as_posix())
        self.next(self.infer_schema)

    @step
    def generate_validate_stats(self):
        self.valid_stats = stats_impl.generate_statistics_in_memory(table_util.MergeTables([self.valid_data]))
        stats_util.write_stats_text(self.valid_stats, (Path(self.save_dir) / Path('valid_stats.pbtxt')).as_posix())
        self.next(self.valid_anomalies)

    @step
    def infer_schema(self):
        self.schema = tfdv.infer_schema(self.train_stats)
        tfdv.write_schema_text(self.schema, (Path(self.save_dir) / Path('schema.pbtxt')).as_posix())
        self.save_dir = self.save_dir
        self.next(self.valid_anomalies)

    @step
    def valid_anomalies(self, inputs):
        self.merge_artifacts(inputs)
        self.anomalies = tfdv.validate_statistics(statistics=inputs.generate_validate_stats.valid_stats,
                                                  schema=inputs.infer_schema.schema)
        tfdv.write_anomalies_text(self.anomalies,
                                  (Path(inputs.infer_schema.save_dir) / Path('anomalies.pbtxt')).as_posix())
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == '__main__':
    ValidationFlow()
