import json

import requests
import simplejson
import urllib3

from domain.deploy import Cluster
from domain.deploy import Deploy
from environment import Environment
from helper_functions import *
from redstack.blueprints import BlueprintBuilder
from redstack.exceptions import AmbariException

logger = logging.getLogger("root_logger")


class Ambari:
    def __init__(self, deploy):
        # type: (Deploy) -> None
        """
        Constructor for Ambari
        :param deploy: The current deploy
        """
        self.deploy = deploy

        self.ambari_ip = deploy.cluster.master_node.floating_ip

        self.api_root = 'https://{0}:8443/api/v1/'.format(self.ambari_ip)

        self.auth = ('admin', 'admin')
        self.headers = {'X-Requested-By': 'ambari'}

        self.retries = 5
        self.short_sleep = 5

        self.retry_exceptions = (requests.exceptions.ConnectionError, requests.exceptions.Timeout, KeyError,
                                 ValueError, simplejson.scanner.JSONDecodeError)

    def install(self):
        # type: () -> None
        """
        installs HDP with ambari
        :return: 
        """
        logger.info('Starting preparation of Ambari to install Hadoop on {0}'.format(self.ambari_ip))

        if self.deploy.define_custom_repos:
            self._put_stack()
            logger.info('Stack definition posted to cluster')
            time.sleep(self.short_sleep)

            self._put_utils()
            logger.info('Utils definition posted to cluster')
            time.sleep(self.short_sleep)

        self._post_blueprint()
        logger.info('Blueprints posted to cluster')
        time.sleep(self.short_sleep)

        self._start_install()
        time.sleep(self.short_sleep)

        self._change_admin_password()
        set_root_mysql_password(self.deploy.cluster.master_node, self.deploy.cluster.ssh_user,
                                self.deploy.cluster.private_key, self.deploy.mysql_root_password)

    def _put_stack(self):
        # type: () -> None
        """
        Puts the stack definition to ambari server for installing an exact version from a hosted location
        :return: 
        """
        endpoint = 'stacks/HDP/versions/{0}/operating_systems/redhat7/repositories/HDP-{0}'.format(
            self.deploy.hdp_major_version)
        payload = json.dumps(self.deploy.stack_definition)

        self._put(endpoint, payload)

    def _put_utils(self):
        # type: () -> None
        """
        Puts the stack utils definition to ambari server for installing an exact version from a hosted location
        :return: 
        """
        endpoint = 'stacks/HDP/versions/{0}/operating_systems/redhat7/repositories/HDP-{0}'.format(
            self.deploy.hdp_major_version, self.deploy.hdp_utils_version)
        payload = json.dumps(self.deploy.utils_definition)

        self._put(endpoint, payload)

    def _post_blueprint(self):
        # type: () -> None
        """
        Posts the blueprint to ambari
        :return: 
        """

        endpoint = 'blueprints/{0}'.format(self.deploy.stack_name)
        payload = json.dumps(self.deploy.blueprint)

        retry(self._post, 40, self.retry_exceptions, endpoint, payload)

    def _start_install(self):
        # type: () -> None
        """
        Adds the hostmapping to the cluster and start the ambari installation
        :return: 
        """
        endpoint = 'clusters/{0}'.format(self.deploy.stack_name)
        payload = json.dumps(self.deploy.host_mapping)

        logger.info('Hostmapping applied to cluster, starting installation of hadoop')

        response_dict = retry(self._post, self.retries, self.retry_exceptions, endpoint, payload)
        time.sleep(20)

        pending = True
        while pending:
            pending = retry(self._monitor_request, self.retries, self.retry_exceptions, response_dict['href'])

    def _change_admin_password(self):
        # type: () -> None
        """
        Change the admin password for ambari on the cluster
        :return: 
        """

        payload_dict = {
            "Users":
                {
                    "user_name": "admin",
                    "old_password": "admin",
                    "password": self.deploy.ambari_password
                }
        }
        payload = json.dumps(payload_dict)
        endpoint = 'users/{0}'.format('admin')

        retry(self._put, self.retries, self.retry_exceptions, endpoint, payload)
        logger.info('Changed ambari password for admin')

    def _monitor_request(self, request_url):
        # type: (str) -> bool
        """
        Polls against the given URL for percentage of completion, and outputs as a status with time and
        percent completed
        :param request_url: Ambari url
        :return: None
        """

        time.sleep(self.short_sleep)

        # get the response
        response_dict = retry(self._get, self.retries, KeyError, full_url=request_url)

        # extract meaningful response variables
        percent = response_dict['Requests']['progress_percent']
        status = response_dict['Requests']['request_status']

        if status == 'TIMEDOUT':
            pass
        elif status == 'COMPLETED':
            return False
        elif status == 'PENDING' or status == 'IN_PROGRESS':
            logger.info("Ambari install for {0} in progress... {1}%".format(
                self.deploy.name, str(percent).split('.')[0]))
        else:
            if percent >= 95:
                logger.warning("Build failed, but continuing because it was beyond 95%")
            else:
                raise AmbariException('{0}'.format(response_dict))

        return True

    def _post(self, endpoint=None, payload=None, full_url=None):
        # type: (str, dict) -> {}
        """
        performs a post to the cluster, with headers and auth
        :param endpoint: The endpoint after the api root 
        :param payload: The json object to send
        :return: the response dict if it exists else none
        """
        if full_url:
            response = requests.post(full_url, payload, auth=self.auth, headers=self.headers, verify=False)
        else:
            response = requests.post(self.api_root + endpoint, payload, auth=self.auth, headers=self.headers,
                                     verify=False)

        if response.status_code >= 400:
            logger.error(self.api_root + endpoint)
            logger.error(payload)
            raise AmbariException('post failed to {0} with code {1} - {2}'.format(endpoint, response.status_code,
                                                                                  response.reason))
        try:
            response_dict = response.json()
            return response_dict
        except simplejson.scanner.JSONDecodeError:
            return

    def _put(self, endpoint=None, payload=None, full_url=None):
        # type: (str, dict) -> {}
        """
        performs a put to the cluster, with headers and auth
        :param endpoint: The endpoint after the api root 
        :param payload: The json object to send
        :return: the response dict if it exists else none
        """
        if full_url:
            response = requests.put(full_url, payload, auth=self.auth, headers=self.headers, verify=False)
        else:
            response = requests.put(self.api_root + endpoint, payload, auth=self.auth, headers=self.headers,
                                    verify=False)

        if response.status_code >= 400:
            logger.error(self.api_root + endpoint)
            logger.error(payload)
            raise AmbariException('post failed to {0} with code {1} - {2}'.format(endpoint, response.status_code,
                                                                                  response.reason))
        try:
            response_dict = response.json()
            return response_dict
        except simplejson.scanner.JSONDecodeError:
            return

    def _get(self, endpoint=None, full_url=None):
        # type: (str) -> {}
        """
        performs a get to the cluster, with headers and auth
        :param endpoint: The endpoint after the api root 
        :return: the response dict if it exists else none
        """
        if full_url:
            response = requests.get(full_url, auth=self.auth, headers=self.headers, verify=False)
        else:
            response = requests.get(self.api_root + endpoint, auth=self.auth, headers=self.headers, verify=False)

        if response.status_code >= 400:
            logger.error(self.api_root + endpoint)
            raise AmbariException('get failed to {0} with code {1} - {2}'.format(endpoint, response.status_code,
                                                                                 response.reason))
        try:
            response_dict = response.json()
            return response_dict
        except simplejson.scanner.JSONDecodeError:
            return


if __name__ == '__main__':
    setup_logger()
    args = parse_args()

    cluster = Cluster(json_file=args.cluster)
    deploy = Deploy(config_file=args.config, cluster=cluster)

    blueprints = BlueprintBuilder(deploy)
    blueprints.create_all()

    environment = Environment(deploy)
    environment.create()

    ambari = Ambari(deploy)
    ambari.install()
