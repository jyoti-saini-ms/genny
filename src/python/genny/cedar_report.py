import datetime
import json
import os
import subprocess
import sys

from collections import namedtuple

import requests

from genny import cedar

CedarBucketConfig = namedtuple('CedarBucketConfig', [
    'api_key',
    'api_secret',
    'api_token',
    'region',
    'name',
    'prefix'
])

CedarTestArtifact = namedtuple('CedarTestArtifact', [
    'bucket',
    'path',
    'tags',  # [str]
    'local_path',
    'created_at',
    'is_uncompressed',
])

CedarTestInfo = namedtuple('CedarTestInfo', [
    'test_name',
    'trial',
    'tags',  # [str]
    'args'  # {str: str}
])

CedarTest = namedtuple('CedarTest', [
    'info',  # CedarTestInfo
    'created_at',
    'completed_at',
    'artifacts',  # [CedarTestArtifact]
    'metrics',  # unused
    'sub_tests'  # unused
])

CedarReport = namedtuple('CedarReport', [
    'project',
    'version',
    'variant',
    'task_name',
    'task_id',
    'execution_number',
    'mainline',
    'tests',  # [CedarTest]
    'bucket'  # BucketConfig
])

REPORT_FILE = 'cedar_report.json'


class _Config(object):
    """
    OO representation of environment variables used by this file.
    """

    def __init__(self, env, metrics_file_names, test_run_time):
        # EVG related.
        self.project = env['EVG_project']
        self.version = env['EVG_version']
        self.variant = env['EVG_variant']
        self.task_name = env['EVG_task_name']
        self.task_id = env['EVG_task_id']
        self.execution_number = env['EVG_execution_number']
        # This env var is either the string "true" or unset.
        self.mainline = not (env['EVG_is_patch'] == 'true')

        # We set these for convenience.
        self.test_name = env['test_name']
        self.metrics_file_names = metrics_file_names
        self.test_run_time = test_run_time
        self.now = datetime.datetime.utcnow()

        # AWS related.
        self.api_key = env['aws_key']
        self.api_secret = env['aws_secret']
        self.cloud_region = 'us-east-1'  # N. Virginia.
        self.cloud_bucket = 'dsi-genny-metrics'

    @property
    def created_at(self):
        return self.now - self.test_run_time


def build_report(config):
    artifacts = []

    for path in config.metrics_file_names:
        base_name = os.path.basename(path)
        a = CedarTestArtifact(
            bucket=config.cloud_bucket,
            path=base_name,
            tags=[],
            local_path=path,
            created_at=config.created_at,
            is_uncompressed=True
        )
        artifacts.append(a._asdict())

    bucket_prefix = '{}_{}'.format(config.task_id, config.execution_number)

    bucket_config = CedarBucketConfig(
        api_key=config.api_key,
        api_secret=config.api_secret,
        api_token=None,
        region=config.cloud_region,
        name=config.cloud_bucket,
        prefix=bucket_prefix
    )

    test_info = CedarTestInfo(
        test_name=config.test_name,
        trial=0,
        tags=[],
        args={}
    )

    test = CedarTest(
        info=test_info._asdict(),
        created_at=config.created_at,
        completed_at=config.now,
        artifacts=bucket_config._asdict(),
        metrics=None,
        sub_tests=None
    )

    report = CedarReport(
        project=config.project,
        version=config.version,
        variant=config.variant,
        task_name=config.task_name,
        task_id=config.task_id,
        execution_number=config.execution_number,
        mainline=config.mainline,
        tests=[test._asdict()],
        bucket=bucket_config
    )

    return report._asdict()


class CertRetriever(object):
    """Retrieves client certificate and key from the cedar API using Jira username and password."""

    def __init__(self, username, password):
        self.auth = json.dumps({
            'username': username,
            'password': password
        })

    @staticmethod
    def _fetch(url, output, **kwargs):
        if os.path.exists(output):
            return output
        resp = requests.get(url, **kwargs)
        resp.raise_for_status()
        with open(output, 'w') as pem:
            pem.write(resp.text)
        return output

    def root_ca(self):
        """
        :return: the root cert authority pem file from cedar
        """
        return self._fetch('https://cedar.mongodb.com/rest/v1/admin/ca', 'cedar.ca.pem')

    def user_cert(self):
        """
        :return: the user-level pem
        """
        return self._fetch(
            'https://cedar.mongodb.com/rest/v1/admin/users/certificate',
            'cedar.user.crt',
            data=self.auth)

    def user_key(self):
        """
        :return: the user-level key
        """
        return self._fetch(
            'https://cedar.mongodb.com/rest/v1/admin/users/certificate/key',
            'cedar.user.key',
            data=self.auth)


class ShellCuratorRunner(object):
    """Runs curator"""

    def __init__(self, retriever=None):
        """
        :param retriever: CertRetriever to use. Will construct one from given config if None
        """
        self.retriever = retriever

    def get_command(self):
        """
        Do your magic.
        :return: output from host.run_command(the-generated-command)
        """

        command = [
            'curator',
            'poplar',
            'send',
            '--service',
            'cedar.mongodb.com:7070',
            '--cert',
            self.retriever.user_cert(),
            '--key',
            self.retriever.user_key(),
            '--ca',
            self.retriever.root_ca(),
            '--path',
            REPORT_FILE,
        ]
        return command

    @staticmethod
    def run(cmd):
        """
        Run curator in a subprocess.

        :raises: CalledProcessError if return code is non-zero.
        """
        res = subprocess.run(cmd)
        res.check_returncode()


def build_parser():
    parser = cedar.build_parser()
    parser.description += " and create a cedar report"
    parser.add_argument('--report-file', default=REPORT_FILE, help='path to generated report file')
    parser.add_argument('--test-name', help='human friendly name for this test, defaults to the '
                                            'EVG_task_name environment variable')
    return parser


class ISODateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()

        return super().default(self, o)


def main__cedar_report(argv=sys.argv[1:], env=None, cert_retriever_cls=CertRetriever):
    """
    Generate a cedar report and upload it using poplar

    :param argv: command line argument
    :param env: shell environment; defaults to os.environ
    :param cert_retriever_cls: class for cert retriever, can be overridden if no certificates are required.
    :return:
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    if not env:
        env = os.environ.copy()

    if args.test_name:
        env['test_name'] = args.test_name
    else:
        env['test_name'] = env['EVG_task_name']

    metrics_file_names, test_run_time = cedar.run(args)
    config = _Config(env, metrics_file_names, test_run_time)

    report_dict = build_report(config)

    with open(os.path.join(args.output_dir, args.report_file), 'w') as f:
        json.dump(report_dict, f, cls=ISODateTimeEncoder)

    jira_user = env['perf_jira_user']
    jira_pwd = env['perf_jira_pw']

    cr = cert_retriever_cls(jira_user, jira_pwd)
    runner = ShellCuratorRunner(cr)
    runner.run(runner.get_command())