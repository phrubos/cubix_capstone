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

    """"
        Ingests a file from the source path in the datalake and writes it to the bronze path.
        Args:
            source_path (str): The path to the source file in the datalake.
            bronze_path (str): The path to the bronze file in the datalake.
            
            file_name (str): The name of the file to ingest.
            container_name (str): The name of the container in the datalake.
            format (str): The format of the file to ingest.
            mode (str): The mode to write the file to the datalake.
            partition_by (list[str]): The partitioning columns of the file to ingest.
        Returns:
            None
    """
    df = read_file_from_datalake(container_name=container_name, file_path=f"{source_path}/{file_name}", format=format)
    return write_file_to_datalake(
        df=df,
        container_name=container_name,
        file_path=f"{bronze_path}/{file_name}",
        format=format,
        mode=mode,
        partition_by=partition_by
        )
