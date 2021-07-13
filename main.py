import datetime
import json
import sys
import time
import pandas
import pytz
import requests


class Vtiger:
    def __init__(self, api_username, api_access_key, api_host):
        self.api_username = api_username
        self.api_access_key = api_access_key
        self.api_host = api_host
        self.user_id, self.full_name, self.email, self.time_zone = self.get_user_info()
        print(
            f"Connection Details:\n# User id: {self.user_id}\n# Name: {self.full_name}\n# Email: {self.email}\n# UTC: {self.time_zone}")

    def beginning_of_week(self):
        return (datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(
            days=-datetime.datetime.now().replace(hour=0, minute=0, second=0,
                                                  microsecond=0).weekday())) - datetime.timedelta(hours=self.time_zone)

    def beginning_of_month(self):
        return datetime.datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(
            hours=self.time_zone)

    def get_new_and_updated_cases(self, last_update):
        unix_stamp = time.mktime(last_update.timetuple())
        print(f"Current unix time stamp {unix_stamp}")
        data = self.call_api(f"{self.api_host}/sync?modifiedTime={unix_stamp}&elementType=Cases&syncType=application")[
            'result']
        print(len(data['updated']))
        return data

    def get_case_overview(self):
        monday = self.beginning_of_week()
        month_start = self.beginning_of_month()
        total_cases = int(
            self.call_api(f"{self.api_host}/query?query=SELECT COUNT(*) FROM Cases;")['result'][0]['count'])
        total_open_cases = int(
            self.call_api(f"{self.api_host}/query?query=SELECT COUNT(*) FROM Cases WHERE casestatus != "
                          f"'closed' AND casestatus != 'resolved';")['result'][0]['count'])
        week_cases = int(self.call_api(
            f"{self.api_host}/query?query=SELECT COUNT(*) FROM Cases WHERE createdtime >= '{monday}';")['result'][0][
                             'count'])
        week_open_cases = int(
            self.call_api(f"{self.api_host}/query?query=SELECT COUNT(*) FROM Cases WHERE casestatus != "
                          f"'closed' AND casestatus != 'resolved' AND createdtime >= '{monday}';")[
                'result'][0]['count'])
        month_cases = int(self.call_api(
            f"{self.api_host}/query?query=SELECT COUNT(*) FROM Cases WHERE createdtime >= '{month_start}';")['result'][
                              0][
                              'count'])
        month_open_cases = int(
            self.call_api(f"{self.api_host}/query?query=SELECT COUNT(*) FROM Cases WHERE casestatus != "
                          f"'closed' AND casestatus != 'resolved' AND createdtime >= '{month_start}';")[
                'result'][0]['count'])

        if not total_cases == 0:
            total_solve_rate = int((1 - total_open_cases / total_cases) * 100)
        else:
            total_solve_rate = 100
        if not week_cases == 0:
            week_solve_rate = int((1 - week_open_cases / week_cases) * 100)
        else:
            week_solve_rate = 100
        if not month_cases == 0:
            month_solve_rate = int((1 - month_open_cases / month_cases) * 100)
        else:
            month_solve_rate = 100

        data = [["Week", week_cases, week_open_cases, week_solve_rate],
                ["Month", month_cases, month_open_cases, month_solve_rate],
                ["All time", total_cases, total_open_cases, total_solve_rate]]

        data_form = pandas.DataFrame(data, [0, 1, 2], ["Time Frame", "Created Cases", "Open Cases", "Solve Rate"])
        return data_form

    def get_case_details(self):
        data = self.call_api(f"{self.api_host}/describe?elementType=Cases")['result']['fields']

    def get_user_info(self):
        # Gets account details of current connection
        data = self.call_api(f"{self.api_host}/me")
        account_id = data['result']['id']
        full_name = f"{data['result']['first_name']} {data['result']['last_name']}"
        email = data['result']['email1']

        timezone = data['result']['time_zone']
        current_time = datetime.datetime.now().astimezone(pytz.timezone(timezone))
        utc = current_time.utcoffset().total_seconds() / 60 / 60

        return account_id, full_name, email, utc

    def call_api(self, url):
        r = requests.get(url, auth=(self.api_username, self.api_access_key))
        header = r.headers
        if not r.ok:
            print(f"API call failed.\nReason:\n{r.reason}")
            time.sleep(60)
            sys.exit(0)
        sys.stdout.flush()
        # Limit on API calls per minute
        # This stops API calls if we come close to hitting this limit.
        # Limit has been set to 5 to allow for a little leeway
        if int(header['X-FloodControl-Remaining']) <= 5:
            print("Flood control triggered.")
            wait_seconds = abs(int(header['X-FloodControl-Reset']) - int(time.time()))
            print(f"Waiting {wait_seconds} seconds before continuing api call.")
            time.sleep(wait_seconds)
        return json.loads(r.text)


class SqlConnector:
    def __init__(self, sql_username, sql_password, sql_host, sql_database):
        self.username = sql_username
        self.password = sql_password
        self.host = sql_host
        self.database = sql_database
        # self.connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'f'SERVER={sql_host};DATABASE={sql_database};UID={sql_username};PWD={sql_password}')

    def set_last_sync(self):
        print("DELETE FROM metaData WHERE 1 = 1")
        print(f"INSERT INT metaData VALUES {datetime.datetime.now()}")

    def get_last_sync(self):
        print("SELECT lastUpdate FROM metaData")
        return datetime.datetime(2021, 7, 13, 10, 0)

    def process_cases(self, cases):
        for entry in cases['updated']:
            print(f"SELECT COUNT(*) FROM caseDetails WHERE caseDetails.id = {entry['id']}")
            result = 0

            if result == "1":
                print(f"Updating case {entry['id']}")
                print(
                    f"UPDATE `caseDetails` SET `title` = '{entry['title']}',`caseStatus` = '{entry['casestatus']}',`casePriority` = '{entry['casepriority']}', `impact_type` = '{entry['impact_type']}',`impact_area` = '{entry['impact_area']}',`assigned_user_id` = '{entry['assigned_user_id']}', `age` = '{entry['age']}', `resolution_type = '{entry['resolution_type']}'`, `created_time` = '{entry['createdtime']}', `case_no` = '{entry['case_no']}', `created_uer_id` = '{entry['created_user_id']}', `modifiedtime` = '{entry['modifiedtime']}', `reopen_count` = '{entry['reopen_count']}', `resolution_time` = '{entry['resolution_time']}' WHERE `id` = {entry['id']}")
            else:
                print(f"Creating new case {entry['id']}")
                print(
                    f"INSERT INTO `caseDetails`.(`id`, `title`,`caseStatus`,`casePriority`,`impact_type`,`impact_area`,`assigned_user_id`, `age`, `resolution_type`, `created_time`, `case_no`, `created_uer_id`, `modifiedtime`, `reopen_count`, `resolution_time`) VALUES ('{entry['id']}', '{entry['title']}', '{entry['casestatus']}', '{entry['casepriority']}', '{entry['impact_type']}', '{entry['impact_area']}', '{entry['assigned_user_id']}', '{entry['age']}', '{entry['resolution_type']}', '{entry['createdtime']}', '{entry['case_no']}', '{entry['created_user_id']}', '{entry['modifiedtime']}', '{entry['reopen_count']}', '{entry['resolution_time']};")


if __name__ == "__main__":
    # api_username is related email
    # Access Key can be found inside Account, My preferences
    api_username = "tobias.heatlie@sbs.co.uk"
    api_access_key = "4HE1LgQZP6JY64AB"
    api_host = "https://sbshelpdesk.od2.vtiger.com/restapi/v1/vtiger/default"
    sql_username = "test"
    sql_password = "1234"
    sql_host = "127.0.0.1,1434"
    sql_database = "test"
    vtiger = Vtiger(api_username, api_access_key, api_host)
    sql_con = SqlConnector(sql_username, sql_password, sql_host, sql_database)
    case_overview = vtiger.get_case_overview()
    print(case_overview)
    cases_to_process = vtiger.get_new_and_updated_cases(sql_con.get_last_sync())
    if cases_to_process is not None:
        sql_con.process_cases(cases_to_process)
    vtiger.get_case_details()
