def success_response(message, data=None):
    return {
        "success": True,
        "message": message,
        "totalCount": len(data) if data else 0,
        "data": data or [],
        "error": None
    }


def error_response(message, error_detail=None):
    return {
        "success": False,
        "message": message,
        "totalCount": 0,
        "data": [],
        "error": str(error_detail) if error_detail else None
    }