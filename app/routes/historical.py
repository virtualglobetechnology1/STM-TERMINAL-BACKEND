# app/routes/historical.py

from fastapi import APIRouter
from app.schemas.historical_schema import HistoricalRequest
from app.services.s3_service import get_csv_from_s3
from app.utils.response import success_response, error_response

router = APIRouter()


@router.post("/historical-data")
async def get_historical_data(request: HistoricalRequest):
    try:
        if not request.ticker:
            return error_response("ticker is required")

        if not request.start_date or not request.end_date:
            return error_response("start_date and end_date are required")

        ticker = request.ticker.upper()
        bucket = "dd-historical-data"
        key    = f"stocks-data-2013-2025/{ticker}.csv"

        # Async S3 fetch
        try:
            csv_data = await get_csv_from_s3(bucket, key)
        except Exception as e:
            return error_response("Symbol not found in S3", e)

        # Async CSV process
        try:
            from app.services.csv_service import process_csv_from_s3

            filtered_data = await process_csv_from_s3(
                bucket,
                key,
                request.start_date,
                request.end_date
            )
        except Exception as e:
            return error_response("Error processing CSV", e)

        return success_response(
            message="Historical data fetched successfully",
            data=filtered_data
        )

    except Exception as e:
        return error_response("Internal server error", e)
