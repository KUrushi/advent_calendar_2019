from tempfile import NamedTemporaryFile
from pathlib import Path

from google.cloud import bigquery, bigquery_storage_v1beta1
from metaflow import FlowSpec, step, Parameter, current, IncludeFile
import pyarrow as pa


class GetExampleFlow(FlowSpec):
    PROJECT_ID = Parameter(
        name='PROJECT_ID',
        help='GCPのプロジェクトID',
        default='',
        required=True
    )
    ARTIFACT_PATH = Parameter(
        name='ARTIFACT_PATH',
        help='tfdvの成果物を保存するディレクトリ',
        default=Path('./artifact')
    )

    training_data = IncludeFile('TRAINING_DATA_PATH',
                                is_text=True,
                                default=Path('./sql/training_example.sql'))

    validate_data = IncludeFile('VALIDATE_DATA_PATH',
                                is_text=True,
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
        result = bqclient.query(self.training_data)
        self.data = result.to_arrow(bqstorage_client=bqstorage_client)
        with NamedTemporaryFile(dir=self.save_dir) as f:
            writer = pa.RecordBatchFileWriter((Path(self.save_dir) / Path('training_examples.arrow')).as_posix(),
                                              self.data.schema)
            writer.write_table(self.data)

        self.next(self.join)

    @step
    def get_validate(self):
        bqclient = bigquery.Client(project=self.PROJECT_ID)
        bqstorage_client = bigquery_storage_v1beta1.BigQueryStorageClient()
        result = bqclient.query(self.validate_data)
        self.data = result.to_arrow(bqstorage_client=bqstorage_client)

        with NamedTemporaryFile(dir=self.save_dir) as f:
            writer = pa.RecordBatchFileWriter(
                (Path(self.save_dir) / Path('validate_examples.arrow')).as_posix(),
                self.data.schema)
            writer.write_table(self.data)
        self.next(self.join)

    @step
    def join(self, inputs):
        self.next(self.end)
    @step
    def end(self):
        pass


if __name__ == '__main__':
    GetExampleFlow()

