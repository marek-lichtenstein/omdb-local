import json
import os
import concurrent.futures
import requests
from movies.conf import DATA_MAP, SITE


class Credentials:
    def __init__(self, creds=None, key=None):
        if creds:
            self.creds = self._load_creds(creds)
        elif key:
            self.creds = self._check_response({"apikey": key})
        else:
            self.creds = None

        if not self.creds:
            raise ValueError(
                "Failed to create credentials. No apikey or filepath provided."
            )

    def _load_creds(self, creds):
        if not os.path.isfile(creds):
            raise ValueError(f"Provided filepath: {creds} is invalid.")
        try:
            with open(creds) as jsn:
                return self._check_creds(json.load(jsn))
        except ValueError as err:
            raise ValueError(f"Error during loading credentials file: {err}")

    def _check_creds(self, creds):
        check = isinstance(creds, dict) and creds.get("apikey")
        if not check:
            raise ValueError("Provided credentials file is invalid.")
        return self._check_response(creds)

    def _check_response(self, creds):
        error = None
        try:
            response_code = requests.get(SITE, params=creds).status_code
            if response_code == 200:
                return creds
            raise ValueError(
                f"Invalid response code: {response_code}, while checking your creds. Provide valid credentials."
            )
        except Exception as err:
            error = err
        finally:
            if error:
                raise ValueError(
                    f"While trying to check your credentials an error happened: {error}"
                )

    def apikey(self):
        return self.creds["apikey"]


class Requester:
    def __init__(self, credentials):
        self.site = SITE
        self.key = credentials

    def request_many(self, titles, messages=False):
        if messages:
            print("\nDownloading data, please wait...")
        data = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            promises = {
                executor.submit(self._get_request, title): title for title in titles
            }
            for promise in concurrent.futures.as_completed(promises):
                result = promise.result()
                data.append(result)
        if messages:
            print("Data downloaded succesfully.")
        return data

    def request(self, title):
        return self._get_request(title)

    def _get_request(self, title):
        response = self._request(title)
        if response.get("Response") == "False":
            response_info = response.get("Error", "unknown")
            msg = f"Download of {title} failed to: {response_info}"
            raise ValueError(msg)
        return response

    def _request(self, title):
        error = None
        try:
            return requests.get(self.site, params=self._params(title)).json()
        except Exception as err:
            error = err
        finally:
            if error:
                raise ValueError(f"Connection to a server failed due to {error}")

    def _params(self, title):
        return {"t": title, "apikey": self.key}


class Downloader:
    def __init__(self, credentials):
        self.req = Requester(Credentials(credentials).apikey())

    def download_one(self, title, process=False, rotated=False):
        data = self.req.request(title)
        if process:
            if rotated:
                return rotated_row(data)
            return row(data)
        return data

    def download_many(self, titles, process=False, rotated=False):
        data = self.req.request_many(titles)
        if process:
            if rotated:
                return rotated_rows(data)
            return rows(data)
        return data


def row(data):
    return tuple(data.get(col, "N/A") for col in DATA_MAP)


def rows(data_pack):
    return [row(data) for data in data_pack]


def rotated_row(data):
    return rotate([data.get(col, "N/A") for col in DATA_MAP])


def rotated_rows(data_pack):
    return [rotated_row(data) for data in data_pack]


def rotate(data):
    data.append(data.pop(0))
    return tuple(data)
