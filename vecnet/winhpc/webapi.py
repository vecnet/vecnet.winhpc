#!/usr/bin/env python
# This file is part of the vecnet.winhpc package.
# For copyright and licensing information about this package, see the
# NOTICE.txt and LICENSE.txt files in its top-level directory; they are
# available at https://github.com/vecnet/winhpc
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License (MPL), version 2.0.  If a copy of the MPL was not distributed
# with this file, You can obtain one at http://mozilla.org/MPL/2.0/.
from StringIO import StringIO
from xml.etree import ElementTree
from xml.dom import minidom
import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import ConnectionError, HTTPError


HPC_Pack_2012 = "2012-11-01.4.0"
HPC_Pack_2008_R2_SP4 = "2012-03-31.3.4"
HPC_Pack_2008_R2_SP3 = "2011-11-01"


class WebAPI:
    def __init__(self,
                 host,
                 username,
                 password,
                 port=443,
                 hpc_cluster_name=None,
                 api_version=HPC_Pack_2008_R2_SP3):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.api_version = api_version
        self.response = None
        # HTTP Request headers
        self.headers = {"Content-type": "application/xml"}
        if self.api_version is not None:
            self.headers["api-version"] = self.api_version
        # Additional arguments that will be passes to requests library for each get and post request to the REST API
        self.requests_kwargs = {
            "auth": HTTPBasicAuth(self.username, self.password),
            "verify": False
        }

        if hpc_cluster_name is not None:
            self.hpc_cluster_name = hpc_cluster_name
        else:
            clusters = self.get_clusters()
            if clusters:
                self.hpc_cluster_name = clusters[0]
            else:
                self.hpc_cluster_name = None

        self.base_url = "https://{host}:{port}/WindowsHPC/{HPC_cluster_name}/".format(
            host=self.host,
            port=self.port,
            HPC_cluster_name=self.hpc_cluster_name,
        )

    # ---------------------------------------------------------------------------------------------- #
    # Helper functions
    # ---------------------------------------------------------------------------------------------- #
    @staticmethod
    def _xml_from_properties(**properties):
        xml = "<ArrayOfProperty xmlns=\"http://schemas.microsoft.com/HPCS2008R2/common\">"
        for property_name in properties:
            xml += "<Property><Name>%s</Name><Value>%s</Value></Property>" % (property_name, properties[property_name])
        xml += "</ArrayOfProperty>"
        return xml

    def _get_string_from_response(self, xml=None):
        if xml is None:
            xml = self.response
        tree = ElementTree.parse(StringIO(xml))
        root = tree.getroot()
        return root.text

    def _requested_properties_to_string(self, requested_properties):
        assert isinstance(requested_properties, list)
        url = ""
        for property_name in requested_properties:
            url += property_name + ","
            # Remove trailing comma
            url = url[:-1]
        return url

    def _get_properties_from_xml(self, xml=None):
        """ List of properties in xml response from WebAPI
        Expected xml format:
        <ArrayOfProperty xmlns="http://schemas.microsoft.com/HPCS2008R2/common"
                         xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
        <Property>
            <Name>job_property1_name</Name>
            <Value>job_property1_value</Value>
        </Property>
        <Property>
            <Name>job_property2_name</Name>
            <Value>job_property2_value</Value>
        </Property>
        ...
        <ArrayOfProperty>

        :param xml: (optional) xml to be parse. If None, self.response is used
        :raises: AttributeError if xml document is malformed
        :return: Dictionary of properties
        """
        if xml is None:
            xml = self.response
        properties = {}
        # tree = ElementTree.parse(StringIO(xml))
        # root = tree.getroot()
        parsed_xml = minidom.parse(StringIO(xml))

        for property_tag in parsed_xml.getElementsByTagName('Property'):
            # Get content of <name> and <value> tags in <Property>
            name = property_tag.getElementsByTagName('Name')[0].firstChild.data
            try:
                value = property_tag.getElementsByTagName('Value')[0].firstChild.data
            except AttributeError:
                # <value/> tag found, settings attribute value is None
                value = None
            properties[name] = value
        return properties

    def request(self, method, url, data=None, headers=None):
        if headers is None:
            headers = self.headers
        else:
            headers = dict(self.headers.items() + headers.items())

        if method == "post":
            http = requests.post
        elif method == "get":
            http = requests.get
        elif method == "put":
            http = requests.put
        else:
            raise RuntimeError("HTTP method %s is not supported" % method)

        try:
            r = http(url,
                     data=data,
                     headers=headers,
                     **self.requests_kwargs)
            self.response = r.text
            if r.status_code != 200:
                return False
        except ConnectionError as e:
            self.response = "%s" % e
            return False
        return True

    def post(self, url, data, headers=None):
        """ Send HTTP Post request to WinHPC WebAPI server using correct headers, api-version and credentials
        :param url: URL to post
        :param data: Data to be posted
        :param headers: (optional) Additional headers if necessary
        :return: True if request successfully completed
        :return: False if error happened
        """
        return self.request("post", url, data, headers=headers)

    def get(self, url):
        """ Send HTTP Get request to WinHPC WebAPI server using correct headers, api-version and credentials
        :param url: URL of the resource
        :return: True if request successfully completed
        :return: False if error happened
        """
        return self.request("get", url)

    def put(self, url, data):
        """ Send HTTP PUT request to WinHPC WebAPI server using correct headers, api-version and credentials
        :param url: URL of the resources
        :param data: Data to be send in HTTP request body
        :return: True if request completed successfully
        :return: False if error occurs
        """
        return self.request("put", url, data)

    # ------------------------------------------------------------------------------------------- #
    # HPC Web Service API functions
    # Please refer to HPC Web Service API Reference for additional details
    # http://msdn.microsoft.com/en-us/library/hh560258(v=vs.85).aspx
    # ------------------------------------------------------------------------------------------- #
    def add_task(self, job_id, **properties):
        # Adds a task to a job.
        # http://msdn.microsoft.com/en-us/library/hh560262(v=vs.85).aspx
        url = self.base_url + "/Job/%s/Tasks" % job_id
        xml = self._xml_from_properties(**properties)
        if not self.post(url, xml):
            return None
        # Extract task_id from xml response
        return self._get_string_from_response()

    def cancel_job(self, job_id,
                   forced=False,
                   message=""):
        # Cancel the specified job
        # http://msdn.microsoft.com/en-us/library/hh560253(v=vs.85).aspx
        url = self.base_url + "/Job/%s/Cancel" % job_id
        xml = "<string xmlns=\"http://schemas.microsoft.com/2003/10/Serialization/\">%s</string>" % message
        headers = {"forced": forced}
        return self.post(url, xml, headers=headers)

    def cancel_task(self, job_id, task_id, forced=False, message=""):
        # Cancels the specified task.
        # http://msdn.microsoft.com/en-us/library/hh560264(v=vs.85).aspx
        url = self.base_url + "Job/%s/Task/%s/Cancel" % (job_id, task_id)
        xml = "<string xmlns=\"http://schemas.microsoft.com/2003/10/Serialization/\">%s</string>" % message
        if forced:
            url += "?Forced=True"
        return self.post(url, xml)

    def create_job(self, **properties):
        # Creates a new job on the HPC cluster, for which the specified properties have the specified values.
        # http://msdn.microsoft.com/en-us/library/hh560265(v=vs.85).aspx
        url = self.base_url + "/Jobs"

        xml = self._xml_from_properties(**properties)
        if not self.post(url, xml):
            return None

        # Extract job_id from xml response
        return self._get_string_from_response()

    def create_job_from_xml(self, xml):
        # Creates a new job on the HPC cluster by using the information in the specified job XML string.
        # http://msdn.microsoft.com/en-us/library/hh560266(v=vs.85).aspx
        url = self.base_url + "Jobs/JobFile"
        if not self.post(url, xml):
            return None
        # Extract job_id from xml response
        return self._get_string_from_response()

    def get_active_head_node(self):
        # Gets the name of the active head node of the HPC cluster.
        # http://msdn.microsoft.com/en-us/library/dn275935(v=vs.85).aspx
        if self.api_version < HPC_Pack_2008_R2_SP4:
            raise NotImplementedError("Minimum api-version supported is %s" % HPC_Pack_2008_R2_SP4)
        url = self.base_url + "ActiveHeadnode"
        if not self.get(url):
            return None
        # Extract headnode name from xml response
        # <string xmlns="http://schemas.microsoft.com/2003/10/Serialization/">active_head_node_name</string>
        return self._get_string_from_response()

    def get_clusters(self):
        # Gets the name of the cluster that hosts the instance of the REST web service.
        # http://msdn.microsoft.com/en-us/library/hh770490(v=vs.85).aspx
        url = "https://%s:%s/WindowsHPC/Clusters" % (self.host, self.port)

        if not self.get(url):
            return None

        # Parse response and return list of clusters
        clusters = []
        tree = ElementTree.parse(StringIO(self.response))
        root = tree.getroot()
        if True:
            for clusters_in_xml in root[0][0]:
                clusters.append(clusters_in_xml[1].text)
        return clusters

    def get_job(self, job_id, requested_properties=None):
        # Get information about the specified job
        # http://msdn.microsoft.com/en-us/library/hh529653(v=vs.85).aspx
        url = self.base_url + "Job/%s" % job_id
        if requested_properties is not None:
            # Convert a list of properties requested by user into a string in GET request
            url += "?properties=" + self._requested_properties_to_string(requested_properties)
        if not self.get(url):
            return None

        # Parse response and return list of job properties
        return self._get_properties_from_xml()

    def get_job_as_xml(self, job_id):
        # Gets information about the specified job.
        # http://msdn.microsoft.com/en-us/library/hh529653(v=vs.85).aspx
        # This is a get_job call with render = HpcJobXml
        # Returns XML formatted as found in the Create Job From XML operation.
        url = self.base_url + "Job/%s" % job_id + "?Render=HpcJobXml"
        if not self.get(url):
            return None
        return self.response

    def get_job_custom_properties(self, job_id, requested_properties=None):
        # Gets the values of the specified custom properties for the job,
        # or the values of all of the properties if none are specified.
        # http://msdn.microsoft.com/en-us/library/hh560267(v=vs.85).aspx
        url = self.base_url + "Job/%s/CustomProperties" % job_id
        if requested_properties is not None:
            url += "?Names=" + self._requested_properties_to_string(requested_properties)
        if not self.get(url):
            return None
        return self._get_properties_from_xml()

    def get_job_property(self, job_id, property_name):
        # Get single property of the job (State, Name)
        # Just a convenience wrapper for get_job
        properties = self.get_job(job_id, [property_name])
        if len(properties) == 0:
            return None
        return properties[property_name]

    def get_job_environment_variables(self, job_id, requested_variables=None):
        # Gets the values of the specified environment variables for the job,
        # or the values of all of the environment variables if none are specified.
        # http://msdn.microsoft.com/en-us/library/hh560268(v=vs.85).aspx
        url = self.base_url + "Job/%s/EnvVariables" % job_id
        if requested_variables is not None:
            url += "?properties=" + self._requested_properties_to_string(requested_variables)
        if not self.get(url):
            return None

        # Parse list of env variables in self.response
        return self._get_properties_from_xml()

    def get_subtask(self, job_id, task_id, subtask_id, requested_properties=None):
        # Gets the values of the specified properties for the specified subtask,
        # or the values of all of the properties if no properties are specified.
        # http://msdn.microsoft.com/en-us/library/hh529655(v=vs.85).aspx
        url = self.base_url + "Job/%s/Task/%s/SubTask/%s" % (job_id, task_id, subtask_id)
        if requested_properties is not None:
            url += "?Properties=" + self._requested_properties_to_string(requested_properties)
        if not self.get(url):
            return None
        return self._get_properties_from_xml()

    def get_subtask_as_xml(self, job_id, task_id, subtask_id):
        # Gets the values of the specified properties for the specified subtask,
        # or the values of all of the properties if no properties are specified.
        # http://msdn.microsoft.com/en-us/library/hh529655(v=vs.85).aspx
        # This is a get_subtask call with render = HpcJobXml
        # Returns Subtask in XML format
        url = self.base_url + "Job/%s/Task/%s/SubTask/%s?Render=HpcJobXml" % (job_id, task_id, subtask_id)
        if not self.get(url):
            return None
        return self.response

    def get_task(self, job_id, task_id, requested_properties=None):
        # Gets the values of the specified properties for the specified task,
        # or the values of all of the properties if no properties are specified.
        # http://msdn.microsoft.com/en-us/library/hh529656(v=vs.85).aspx
        url = self.base_url + "Job/%s/Task/%s" % (job_id, task_id)
        if requested_properties is not None:
            url += "?Properties=" + self._requested_properties_to_string(requested_properties)
        if not self.get(url):
            return None
        return self._get_properties_from_xml()

    def get_task_environment_variables(self, job_id, task_id, requested_env_variables=None):
        # Gets the values of the specified environment variables for the task,
        # or the values of all of the environment variables if none are specified.
        # http://msdn.microsoft.com/en-us/library/hh529657(v=vs.85).aspx
        url = self.base_url + "Job/%s/Task/%s/EnvVariables" % (job_id, task_id)
        if requested_env_variables is not None:
            url += "?Names=" + self._requested_properties_to_string(requested_env_variables)
        if not self.get(url):
            return None
        return self._get_properties_from_xml()

    def get_version(self):
        # Gets the version of Microsoft HPC Pack that is installed on the HPC cluster that hosts the web service.
        # http://msdn.microsoft.com/en-us/library/hh560257(v=vs.85).aspx
        url = self.base_url + "Version"
        if not self.get(url):
            return None
        # Extract version from xml response
        return self._get_string_from_response()

    def requeue_job(self, job_id):
        # Resubmits the specified job to the queue.
        # http://msdn.microsoft.com/en-us/library/hh529659(v=vs.85).aspx
        # Only jobs that are in the Canceled or Failed state can be requeued.
        # To create a new job that is based on a Finished or Running job, save the job as an XML file
        # using get_job_as_xml() and create a new job using create_job_from_xml()
        url = self.base_url + "Job/%s/Requeue" % job_id
        return self.post(url, '<ArrayOfProperty xmlns="http://schemas.microsoft.com/HPCS2008R2/common" />')

    def set_job_environment_variables(self, job_id, **variables):
        # Sets the value of one or more environment variables for a job.
        # http://msdn.microsoft.com/en-us/library/hh529663(v=vs.85).aspx
        url = self.base_url + "Job/%s/EnvVariables" % job_id
        xml = self._xml_from_properties(**variables)
        return self.post(url, xml)

    def set_job_properties(self, job_id, **properties):
        # Sets the values for the properties of the specified job.
        # http://msdn.microsoft.com/en-us/library/hh529664(v=vs.85).aspx
        if self.api_version is None:
            raise RuntimeError("Minimum supported api-version: %s" % HPC_Pack_2008_R2_SP3)
        url = self.base_url + "Job/%s" % job_id
        xml = self._xml_from_properties(**properties)
        return self.put(url, xml)

    def set_job_custom_properties(self, job_id, **properties):
        # Sets the values of custom properties for a job.
        # http://msdn.microsoft.com/en-us/library/hh529662(v=vs.85).aspx
        url = self.base_url + "Job/%s/Custom" % job_id
        xml = self._xml_from_properties(**properties)
        return self.post(url, xml)

    def set_task_environment_variables(self, job_id, task_id, **env_variables):
        # Sets the value of one or more environment variables for a task.
        # http://msdn.microsoft.com/en-us/library/hh529665(v=vs.85).aspx
        url = self.base_url + "/Job/%s/Task/%s/EnvVariables" % (job_id, task_id)
        xml = self._xml_from_properties(**env_variables)
        return self.post(url, xml)

    def set_task_properties(self, job_id, task_id, **properties):
        # Sets the values of properties for a task in a job.
        # http://msdn.microsoft.com/en-us/library/hh529667(v=vs.85).aspx
        url = self.base_url + "Job/%s/Task/%s" % (job_id, task_id)
        xml = self._xml_from_properties(**properties)
        return self.put(url, xml)

    def submit_job(self, job_id, **properties):
        # Creates a new job on the HPC cluster, for which the specified properties have the specified values.
        # http://msdn.microsoft.com/en-us/library/hh560265(v=vs.85).aspx
        url = self.base_url + "/Job/%s/Submit" % job_id
        xml = self._xml_from_properties(**properties)
        return self.post(url, xml)
