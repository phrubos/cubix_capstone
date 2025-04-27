from cubix_capstone.utils.datalake import read_file_from_datalake, write_file_to_datalake


def bronze_ingest(
    source_path: str,
    bronze_path: str,
    file_name: str,
    container_name: str,
    format: str,
    mode: str,
    partition_by: list[str],
):
    df = read_file_from_datalake(container_name=container_name, file_path=f"{source_path}/{file_name}", format=format)
    return write_file_to_datalake(
        df=df,
        container_name=container_name,
        file_path=f"{bronze_path}/{file_name}",
        format=format,
        mode=mode,
        partition_by=partition_by
        )