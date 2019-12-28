import json
import os
import boto3
import psycopg2
from time import perf_counter as pc


class Config:
    """Lambda function runtime configuration"""

    TIMEOUT = 'TIMEOUT'
    REPORT_AS_CW_METRICS = 'REPORT_AS_CW_METRICS'
    CW_METRICS_NAMESPACE = 'CW_METRICS_NAMESPACE'
    RDS_DATABASE = 'RDS_DATABASE'
    RDS_HOSTNAME = 'RDS_HOSTNAME'
    RDS_PORT = 'RDS_PORT'
    RDS_USERNAME = 'RDS_USERNAME'
    RDS_PASSWORD = 'RDS_PASSWORD'

    def __init__(self, event):
        self.event = event
        self.defaults = {
            self.TIMEOUT: 120,
            self.REPORT_AS_CW_METRICS: '1',
            self.CW_METRICS_NAMESPACE: 'TcpPortCheck',
            self.RDS_DATABASE: 'leases16',
            self.RDS_HOSTNAME: 'localhost',
            self.RDS_PORT: '5432',
            self.RDS_USERNAME: 'leases16',
            self.RDS_PASSWORD: 'leases16password'
        }

    def __get_property(self, property_name):
        if property_name in self.event:
            return self.event[property_name]
        if property_name in os.environ:
            return os.environ[property_name]
        if property_name in self.defaults:
            return self.defaults[property_name]
        return None

    @property
    def timeout(self):
        return self.__get_property(self.TIMEOUT)

    @property
    def reportbody(self):
        return self.__get_property(self.REPORT_RESPONSE_BODY)

    @property
    def cwoptions(self):
        return {
            'enabled': self.__get_property(self.REPORT_AS_CW_METRICS),
            'namespace': self.__get_property(self.CW_METRICS_NAMESPACE),
        }

    @property
    def rds_database(self):
        return self.__get_property(self.RDS_DATABASE)

    @property
    def rds_hostname(self):
        return self.__get_property(self.RDS_HOSTNAME)

    @property
    def rds_port(self):
        return self.__get_property(self.RDS_PORT)

    @property
    def rds_username(self):
        return self.__get_property(self.RDS_USERNAME)

    @property
    def rds_password(self):
        return self.__get_property(self.RDS_PASSWORD)


class PostgresCheck:
    """Execution of Postgres request"""

    def __init__(self, config):
        self.config = config

    def execute(self):
        try:
            # start the stopwatch
            t0 = pc()

            db = psycopg2.connect(
                database=self.config.rds_database,
                user=self.config.rds_username,
                password=self.config.rds_password,
                host=self.config.rds_hostname,
                port=self.config.rds_port,
                connect_timeout=self.config.timeout
            )

            available = '1'

            # stop the stopwatch
            t1 = pc()

            result = {
                'TimeTaken': int((t1 - t0) * 1000),
                'Available': available
            }
            print(f"Socket connect result: {db}")
            # return structure with data
            return result
        except Exception as e:
            print(f"Failed to connect to RDS {self.config.rds_database} {self.config.rds_hostname}:{self.config.rds_port}\n{e}")
            return {'Available': 0, 'Reason': str(e)}


class ResultReporter:
    """Reporting results to CloudWatch"""

    def __init__(self, config):
        self.config = config
        self.options = config.cwoptions

    def report(self, result):
        if self.options['enabled'] == '1':
            try:
                endpoint = f"{self.config.rds_hostname}:{self.config.rds_port}"
                cloudwatch = boto3.client('cloudwatch')
                metric_data = [{
                    'MetricName': 'Available',
                    'Dimensions': [
                        {'Name': 'Endpoint', 'Value': endpoint}
                    ],
                    'Unit': 'None',
                    'Value': int(result['Available'])
                }]
                if result['Available'] == '1':
                    metric_data.append({
                        'MetricName': 'TimeTaken',
                        'Dimensions': [
                            {'Name': 'Endpoint', 'Value': endpoint}
                        ],
                        'Unit': 'Milliseconds',
                        'Value': int(result['TimeTaken'])
                    })

                result = cloudwatch.put_metric_data(
                    MetricData=metric_data,
                    Namespace=self.config.cwoptions['namespace']
                )

                print(f"Sent data to CloudWatch requestId=:{result['ResponseMetadata']['RequestId']}")
            except Exception as e:
                print(f"Failed to publish metrics to CloudWatch:{e}")


def port_check(event, context):
    """Lambda function handler"""

    config = Config(event)

    postgres_check = PostgresCheck(config)
    result = postgres_check.execute()

    # report results
    ResultReporter(config).report(result)

    result_json = json.dumps(result, indent=4)
    # log results
    print(f"Result of checking {config.rds_database} {config.rds_hostname}:{config.rds_port}\n{result_json}")

    # return to caller
    return result
