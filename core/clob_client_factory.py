"""
core/clob_client_factory.py

สร้าง ClobClient จาก py-clob-client พร้อม API credentials
"""
from py_clob_client.client import ClobClient
from config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def create_clob_client() -> ClobClient:
    """
    สร้าง ClobClient พร้อม credentials
    Pattern จาก reference: create_or_derive_api_creds() แล้ว set_api_creds()
    """
    client = ClobClient(
        host=settings.CLOB_URL,
        key=settings.PRIVATE_KEY,
        chain_id=settings.CHAIN_ID,
        signature_type=settings.SIGNATURE_TYPE,
        funder=settings.FUNDER if settings.FUNDER else settings.WALLET_ADDRESS,
    )
    try:
        creds = client.create_or_derive_api_creds()
        client.set_api_creds(creds)
        logger.info("ClobClient initialised with API credentials")
    except Exception as exc:
        logger.warning(f"Could not derive API creds (may work without): {exc}")
    return client
