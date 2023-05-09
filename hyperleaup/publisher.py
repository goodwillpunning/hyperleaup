import os
import logging
import tableauserverclient as TSC
from tableauserverclient import DatasourceItem


def datasource_to_string(datasource: DatasourceItem) -> str:
    """Returns a multi-line string containing a DatasourceItem's attributes"""
    return f"""
      name: {datasource.name}
      id: {datasource.id}
      content_url: {datasource.content_url}
      created_at: {datasource.created_at}
      certified: {datasource.certified}
      certification_note: {datasource.certification_note}
      datasource_type: {datasource.datasource_type}
      owner_id: {datasource.owner_id}
      project_id: {datasource.project_id}
      project_name: {datasource.project_name}
      tags: {datasource.tags}
      updated_at: {datasource.updated_at}
      """


class Publisher:
    """Publishes a Hyper file to a Tableau Server
    
    
     - asJob: 	A Boolean value that is used to publish data sources asynchronously. If you set this value to false (the default), the publishing process runs as a synchronous process. If a data source is very large, the process might time out before it finishes. If you set this value to true, the process runs asynchronously, and a job will start to perform the publishing process and return the job ID"""

    def __init__(self, tableau_server_url: str,
                 username: str, password: str,
                 token_name: str,
                 token_value: str,
                 site_id: str,
                 project_name: str,
                 datasource_name: str,
                 hyper_file_path: str,
                 as_job: bool):
        self.tableau_server_url = tableau_server_url
        self.username = username
        self.password = password
        self.token_name = token_name
        self.token_value = token_value
        self.site_id = site_id
        self.project_name = project_name
        self.project_id = None
        self.datasource_name = datasource_name
        self.datasource_luid = None
        self.hyper_file_path = hyper_file_path
        self.as_job = as_job

    def publish(self, creation_mode = 'Overwrite'):
        """Publishes a Hyper File to a Tableau Server"""

        # Ensure that the Hyper File exists
        if not os.path.isfile(self.hyper_file_path):
            error = "{0}: Hyper File not found".format(self.hyper_file_path)
            raise IOError(error)

        # Check the Hyper File size
        hyper_file_size = os.path.getsize(self.hyper_file_path)
        logging.info(f"The Hyper File size is (in bytes): {hyper_file_size}")

        # Build authorization with Tableau Server
        if self.username is not None and self.password is not None:
            tableau_auth = TSC.TableauAuth(username=self.username, password=self.password, site_id=self.site_id)
        elif self.token_name is not None and self.token_value is not None:
            tableau_auth = TSC.PersonalAccessTokenAuth(token_name=self.token_name, personal_access_token=self.token_value, site_id=self.site_id)
        else:
            raise ValueError(f'Invalid credentials. Cannot create authorization to connect with Tableau Server.')

        server = TSC.Server(self.tableau_server_url)
        server.use_server_version()

        # Sign-in to Tableau Server using auth object
        with server.auth.sign_in(tableau_auth):

            # Search for project on the Tableau server, filtering by project name
            req_options = TSC.RequestOptions()
            req_options.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                              TSC.RequestOptions.Operator.Equals,
                                              self.project_name))
            projects, pagination = server.projects.get(req_options=req_options)
            for project in projects:
                if project.name == self.project_name:
                    logging.info(f'Found project on Tableau server. Project ID: {project.id}')
                    self.project_id = project.id

            # If project was not found on the Tableau Server, raise an error
            if self.project_id is None:
                raise ValueError(f'Invalid project name. Could not find project named "{self.project_name}" '
                                 f'on the Tableau server.')

            # Next, check if the datasource already exists and needs to be overwritten
            create_mode = TSC.Server.PublishMode.Overwrite
            if creation_mode.upper() == 'OVERWRITE':

                # Search for the datasource under project name
                req_options = TSC.RequestOptions()
                req_options.filter.add(TSC.Filter(TSC.RequestOptions.Field.ProjectName,
                                                  TSC.RequestOptions.Operator.Equals,
                                                  self.project_name))
                req_options.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                                  TSC.RequestOptions.Operator.Equals,
                                                  self.datasource_name))
                datasources, pagination = server.datasources.get(req_options=req_options)
               
            elif creation_mode.upper() == 'APPEND':
                create_mode = TSC.Server.PublishMode.Append
                
                
            else:
                raise ValueError(f'Invalid "creation_mode" : {creation_mode}')

            # Finally, publish the Hyper File to the Tableau server
            logging.info(f'Publishing Hyper File located at: "{self.hyper_file_path}"')
            logging.info(f'Create mode: {create_mode}')
            datasource_item_id = TSC.DatasourceItem(project_id=self.project_id, name=self.datasource_name)
            logging.info(f'Publishing datasource: \n{datasource_to_string(datasource_item_id)}')
            datasource_item = server.datasources.publish(datasource_item_id, self.hyper_file_path, create_mode, as_job=self.as_job)
            self.datasource_luid = datasource_item.id
            logging.info(f'Published datasource to Tableau server. Datasource LUID : {self.datasource_luid}')

        logging.info("Done.")

        return self.datasource_luid
