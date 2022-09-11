from http import HTTPStatus

from aiohttp.web_response import Response


def ok_response(body: dict = {}):
    return Response(body=body, status=HTTPStatus.OK)


def bad_response(description: str = ''):
    return Response(
        body={"message": "Validation Failed"} | ({'description': description} if description else {}),
        status=HTTPStatus.BAD_REQUEST
    )


def not_found_response():
    return Response(body={"message": "Item not found"}, status=HTTPStatus.NOT_FOUND)
