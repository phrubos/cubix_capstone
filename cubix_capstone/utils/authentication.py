import os
from pyspark.sql import SparkSession



def _create_authentication_config(tenant_id: str, client_id: str, client_secret: str) -> dict[str, str]:
    """
    Function to create the authentication configuration for the datalake
    Args:

    tenant_id: str: The tenant ID of the Azure AD application
    client_id: str: The client ID of the Azure AD application
    client_secret: str: The client secret of the Azure AD application

    returns:
    config: dict[str, str]: The authentication configuration for the datalake
    """
    return {
        "fs.azure.account.auth.type.dlakecubixdataengineer.dfs.core.windows.net": "OAuth",
        "fs.azure.account.oauth.provider.type.dlakecubixdataengineer.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id.dlakecubixdataengineer.dfs.core.windows.net": client_id,
        "fs.azure.account.oauth2.client.secret.dlakecubixdataengineer.dfs.core.windows.net": client_secret,
        "fs.azure.account.oauth2.client.endpoint.dlakecubixdataengineer.dfs.core.windows.net": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }

def authenticate_user() -> None:
    """
    Function to authenticate the user with the datalake using the Azure AD application
    Args:

    returns:
    None
    """

    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")

    if not tenant_id or not client_id or not client_secret:
        raise ValueError("AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET must be set")


    spark = SparkSession.getActiveSession()

    config = _create_authentication_config(tenant_id, client_id, client_secret)

    for key, value in config.items():
        spark.conf.set(key, value)
