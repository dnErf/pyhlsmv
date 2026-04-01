import pytest
import shutil
import tempfile
from pathlib import Path
from pyhlsmv.storage import HStorage, TableSchema
from pyhlsmv.sst import SSTable


@pytest.fixture
def temp_directory():
    temp_d = tempfile.mkdtemp()
    yield temp_d
    shutil.rmtree(temp_d)


@pytest.fixture
def storage(temp_directory):
    return HStorage(base_path = temp_directory)


class TestCreation:
    def test_create_schema_basic(self, storage, temp_directory):
        schema_name = "test_schema"
        storage.create_schema(schema_name)

        assert schema_name in storage.schemas
        metadata = storage.schemas[schema_name]
        assert metadata.name == schema_name
        assert isinstance(metadata.created_at, str)
        assert metadata.tables == []
        assert metadata.integrity_hash == ""

        schema_path = Path(temp_directory) / schema_name
        assert schema_path.exists()
        assert schema_path.is_dir()

        metadata_file = schema_path / "schema.arrow"
        assert metadata_file.exists()

    def test_create_schema_multiple(self, storage, temp_directory):
        schema_name = "test_schema"
        storage.create_schema(schema_name)

        initial_metadata = storage.schemas[schema_name]
        initial_create_at = initial_metadata.created_at

        storage.create_schema(schema_name)

        assert storage.schemas[schema_name].created_at == initial_create_at
        assert storage.schemas[schema_name].tables == []


if __name__ == "__main__":
    pytest.main([__file__])
