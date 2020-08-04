import os
import tableauserverclient as TSC


class Publisher:
    """Publishes a Hyper file to a Tableau Server"""

    def __init__(self, tableau_server_url: str,
                 username: str, password: str,
                 site_id: str,
                 project_name: str,
                 datasource_name: str,
                 hyper_file_path: str):
        self.tableau_server_url = tableau_server_url
        self.username = username
        self.password = password
        self.site_id = site_id
        self.project_name = project_name
        self.project_id = None
        self.datasource_name = datasource_name
        self.datasource_luid = None
        self.hyper_file_path = hyper_file_path

    def publish(self):
        """Publishes a Hyper File to a Tableau Server"""

        # Ensure that the Hyper File exists
        if not os.path.isfile(self.hyper_file_path):
            error = "{0}: Hyper File not found".format(self.hyper_file_path)
            raise IOError(error)

        # Check the Hyper File size
        hyper_file_size = os.path.getsize(self.hyper_file_path)
        print(f"The Hyper File size is (in bytes): {hyper_file_size}")

        try:
            username_pw_auth = TSC.TableauAuth(self.username, self.password)
            server = TSC.Server(self.tableau_server_url)
            with server.auth.sign_in(username_pw_auth):

                # Search for project on the Tableau server
                projects, pagination = server.projects.get()
                for project in projects:
                    if project.name == self.project_name:
                        print(f'Found project on Tableau server. Project ID: {project.id}')
                        self.project_id = project.id
                if self.project_id is None:
                    raise ValueError(f'Invalid project name. Could not find project named "{self.project_name}" '
                                     f'on the Tableau server.')

                # Next, check if the datasource already exists and needs to be overwritten
                create_mode = 'CreateNew'
                datasources, pagination = server.datasources.get()
                for datasource in datasources:
                    if datasource.name == self.datasource_name:
                        print(f'Overwriting existing datasource named "{self.datasource_name}".')
                        create_mode = 'Overwrite'
                        break

                # Finally, publish the Hyper File to the Tableau server
                print(f'Publishing Hyper File located at: "{self.hyper_file_path}"')
                datasource_item = TSC.DatasourceItem(project_id=self.project_id, name=self.project_name)
                datasource_item = server.datasources.publish(datasource_item=datasource_item,
                                                             file_path=self.hyper_file_path,
                                                             mode=create_mode)
                self.datasource_luid = datasource_item.id
                print(f'Published datasource to Tableau server. Datasource LUID : {self.datasource_luid}')

        except Exception as e:
            print(e)

        print("Done.")

        return self.datasource_luid
