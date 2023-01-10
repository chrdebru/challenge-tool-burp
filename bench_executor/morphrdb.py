#!/usr/bin/env python3

import os
import psutil
import configparser
from io import StringIO
from container import Container
from logger import Logger

VERSION = '3.12.5'
TIMEOUT = 6 * 3600 # 6 hours


class MorphRDB(Container):
    def __init__(self, data_path: str, config_path: str, directory: str,
                 verbose: bool):
        self._data_path = os.path.abspath(data_path)
        self._config_path = os.path.abspath(config_path)
        self._logger = Logger(__name__, directory, verbose)

        os.umask(0)
        os.makedirs(os.path.join(self._data_path, 'morphrdb'), exist_ok=True)
        super().__init__(f'blindreviewing/morph-rdb:v{VERSION}', 'Morph-RDB',
                         self._logger,
                         volumes=[f'{self._data_path}/shared:/data/shared',
                                  f'{self._data_path}/morphrdb:/data'])

    @property
    def root_mount_directory(self) -> str:
        return __name__.lower()

    def _execute_with_timeout(self, arguments) -> bool:
        # Set Java heap to 1/2 of available memory instead of the default 1/4
        max_heap = int(psutil.virtual_memory().total * (1/2))

        # Execute command
        cmd = f'java -Xmx{max_heap} -Xms{max_heap} ' + \
              f'-cp .:morph-rdb-dist-3.12.6.jar:dependency/* ' + \
              f'es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunner ' + \
              f'/data config.properties'
        success =  self.run_and_wait_for_exit(cmd)

        return success

    def execute(self, arguments: list) -> bool:
        try:
            return self._execute_with_timeout(arguments)
        except TimeoutError:
            msg = f'Timeout ({TIMEOUT}s) reached for Morph-RDB'
            self._logger.warning(msg)

        return False

    def execute_mapping(self, mapping_file: str, output_file: str,
                        serialization: str, rdb_username: str = None,
                        rdb_password: str = None, rdb_host: str = None,
                        rdb_port: str = None, rdb_name: str = None,
                        rdb_type: str = None) -> bool:

        if serialization == 'nquads':
            serialization = 'N-QUADS'
        elif serialization == 'ntriples':
            serialization = 'N-TRIPLE'
        else:
            raise NotImplementedError('Unsupported serialization: '
                                      f'"{serialization}"')

        # Generate INI configuration file since no CLI is available
        config = configparser.ConfigParser()
        mapping_file = os.path.join('shared', os.path.basename(mapping_file))
        output_file = os.path.join('shared', os.path.basename(output_file))
        config['root']= {
            'mappingdocument.file.path': mapping_file,
            'output.file.path': output_file,
            'output.rdflanguage': serialization,
        }

        if rdb_username is not None and rdb_password is not None \
            and rdb_host is not None and rdb_port is not None \
            and rdb_name is not None and rdb_type is not None:
            config['root']['database.name[0]'] = rdb_name
            if rdb_type == 'MySQL':
                config['root']['database.driver[0]'] = 'com.mysql.jdbc.Driver'
                config['root']['database.type[0]'] = 'mysql'
                dsn = f'jdbc:mysql://{rdb_host}:{rdb_port}/{rdb_name}' + \
                      f'?allowPublicKeyRetrieval=true&useSSL=false'
                config['root']['database.url[0]'] = dsn
            elif rdb_type == 'PostgreSQL':
                config['root']['database.driver[0]'] = 'org.postgresql.Driver'
                config['root']['database.type[0]'] = 'postgresql'
                dsn = f'jdbc:postgresql://{rdb_host}:{rdb_port}/{rdb_name}'
                config['root']['database.url[0]'] = dsn
            else:
                raise ValueError(f'Unknown RDB type: "{rdb_type}"')
            config['root']['database.user[0]'] = rdb_username
            config['root']['database.pwd[0]'] = rdb_password
            config['root']['no_of_database'] = '1'

        path = os.path.join(self._data_path, 'morphrdb')
        os.umask(0)
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, 'config.properties'), 'w') as f:
            config.write(f, space_around_delimiters=False)

        # .properties files are like .ini files but without a [HEADER]
        # Use a [root] header and remove it after writing
        with open(os.path.join(path, 'config.properties'), 'r') as f:
            data = f.read()

        with open(os.path.join(path, 'config.properties'), 'w') as f:
            f.write(data.replace('[root]\n', ''))

        return self.execute([])
