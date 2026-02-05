import pyarrow as pa
import pyarrow.dataset as ds
import pandas as pd

def createPartitions(
    df,
    base_dir,
    partition_schema,
    compression="snappy",
    flavor="hive",
    basename_template="part-{i}.parquet",
    existing_data_behavior="delete_matching",
):  
    # pandas -> arrow table
    table = pa.Table.from_pandas(df, preserve_index=False)

    # definição das partições
    partitioning = ds.partitioning(
        pa.schema(partition_schema),
        flavor=flavor
    )

    # formato parquet
    fmt = ds.ParquetFileFormat()
    opts = fmt.make_write_options(compression=compression)

    # escrita do dataset
    ds.write_dataset(
        data=table,
        base_dir=base_dir,
        format=fmt,
        file_options=opts,
        partitioning=partitioning,
        basename_template=basename_template,
        existing_data_behavior=existing_data_behavior,
    )

# dataframe de exemplo
my_dataframe = pd.DataFrame({
    "Year": [2020, 2021, 2022],
    "Month": [1, 2, 3],
    "Value": [100, 200, 300]
})

# diretório de exportação
file_export = "data/partitions"

# esquema de partição
partition_schema = [
    pa.field("Year", pa.int16()),
    pa.field("Month", pa.int8()),
]

# criação do dataset
createPartitions(
    df=my_dataframe,
    base_dir=file_export,
    partition_schema=partition_schema
)