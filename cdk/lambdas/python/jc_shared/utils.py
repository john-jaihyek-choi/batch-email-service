import json
import logging
import csv
import io
import os
from typing import Dict, List, Any

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


def generate_csv(headers: List[str], contents: List[Dict[str, Any]]) -> str:
    # Generate CSV content in memory
    try:
        output = io.StringIO()
        csv_writer = csv.DictWriter(output, fieldnames=headers)
        csv_writer.writeheader()

        for content in contents:
            csv_writer.writerow(content)

        csv_content = output.getvalue()
        output.close()

        return csv_content
    except Exception as e:
        logger.error(f"Error generating csv: {e}")


def generate_handler_response(
    status_code: int, message: str, body: Dict[Any, Any] = None
):
    try:
        response = {
            "StatusCode": status_code,
            "Message": message,
            "Header": {"Content-Type": "application/json"},
            "Body": json.dumps(body),
        }
        return response
    except TypeError as e:
        logger.exception(f"Type error at generate_handler_response: {e}")
