#!/usr/bin/env python3

"""
The Basic and Unassuming RML Processor (BURP)

**Repository**: https://github.com/kg-construct/BURP
"""

import os
import psutil
from typing import Optional
from timeout_decorator import timeout, TimeoutError  # type: ignore
from bench_executor.logger import Logger
from bench_executor.container import Container

VERSION = '1.0.0'
TIMEOUT = 6 * 3600  # 6 hours


class BURP(Container):
    """The Basic and Unassuming RML Processor (BURP)."""

    def __init__(self, data_path: str, config_path: str, directory: str,
                 verbose: bool, expect_failure: bool = False):
        """Creates an instance of the BURP class.

        Parameters
        ----------
        data_path : str
            Path to the data directory of the case.
        config_path : str
            Path to the config directory of the case.
        directory : str
            Path to the directory to store logs.
        verbose : bool
            Enable verbose logs.
        expect_failure : bool
            If a failure is expected, default False.
        """
        self._data_path = os.path.abspath(data_path)
        self._config_path = os.path.abspath(config_path)
        self._logger = Logger(__name__, directory, verbose)
        self._verbose = verbose

        os.makedirs(os.path.join(self._data_path, 'burp'), exist_ok=True)
        super().__init__(f'kgconstruct/burp:v{VERSION}', 'BURP',
                         self._logger, expect_failure=expect_failure,
                         volumes=[f'{self._data_path}/burp:/data',
                                  f'{self._data_path}/shared:/data/shared'])

    @property
    def root_mount_directory(self) -> str:
        """Subdirectory in the root directory of the case for BURP.

        Returns
        -------
        subdirectory : str
            Subdirectory of the root directory for BURP.

        """
        return __name__.lower()

    @timeout(TIMEOUT)
    def _execute_with_timeout(self, arguments: list) -> bool:
        """Execute a mapping with a provided timeout.

        Returns
        -------
        success : bool
            Whether the execution was successfull or not.
        """
        # Set Java heap to 1/2 of available memory instead of the default 1/4
        max_heap = int(psutil.virtual_memory().total * (1/2))

        # Execute command
        cmd = f'java -Xmx{max_heap} -Xms{max_heap}' + \
              ' -jar burp/burp.jar'
        if self._verbose:
            cmd += ' -vvvvvvvvvvvvv'
        cmd += f' {" ".join(arguments)}'

        self._logger.debug(f'Executing BURP with arguments '
                           f'{" ".join(arguments)}')

        return self.run_and_wait_for_exit(cmd)

    def execute(self, arguments: list) -> bool:
        """Execute BURP with given arguments.

        Parameters
        ----------
        arguments : list
            Arguments to supply to BURP.

        Returns
        -------
        success : bool
            Whether the execution succeeded or not.
        """
        try:
            return self._execute_with_timeout(arguments)
        except TimeoutError:
            msg = f'Timeout ({TIMEOUT}s) reached for BURP'
            self._logger.warning(msg)

        return False

    def execute_mapping(self,
                        mapping_file: str,
                        serialization: str,
                        output_file: Optional[str] = None,
                        base_iri: Optional[int] = None) -> bool:
        """Execute a mapping file with BURP.

        N-Quads is currently supported as serialization format for BURP.

        Parameters
        ----------
        Usage: burp [-h] [-b=<baseIRI>] -m=<mappingFile> [-o=<outputFile>]
        -b, --baseIRI=<baseIRI>             Used in resolving relative IRIs produced by the RML mapping
        -h, --help                          Display a help message
        -m, --mappingFile=<mappingFile>     The RML mapping file
        -o, --outputFile=<outputFile>       The output file

        Returns
        -------
        success : bool
            Whether the execution was successfull or not.
        """
        arguments = ['-m', os.path.join('/data/shared/', mapping_file) ]

        if output_file is not None:
            arguments.append('-o')
            arguments.append(os.path.join('/data/shared/', output_file))

        if base_iri is not None:
            arguments.append('-b')
            arguments.append(base_iri)

        return self.execute(arguments)
