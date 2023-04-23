import copy
import datetime


def _changeRequestDates(params, start=None, end=None, isPullingData=False):
    requestParams = None
    if params:
        requestParams = copy.copy(params)
        if isPullingData:
            print(start)
            if "start" in requestParams:
                requestParams.update({"start": str(start)})
            if "end" in requestParams:
                requestParams.update({"end": str(end)})
        else:
            requestParams.update({"start" : None})
            requestParams.update({"end" : None})

    return requestParams


def _append_url(domain: str, endpoint: str) -> str:
    if domain.endswith("/"):
        if endpoint.startswith("/"):
            return domain + endpoint[1:]

        return domain + endpoint

    if endpoint.startswith("/"):
        return domain + endpoint

    return domain + "/" + endpoint
