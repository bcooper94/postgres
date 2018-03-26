#! /usr/bin/env python

import os
from sys import argv
from os.path import isfile, join
import subprocess

class Postgres:
    def __init__(self, port, sqlQueriesDir, expectedOutputDir):
        self.port = port
        self.process = None
        self.sqlQueriesDir = sqlQueriesDir
        self.expectedOutputDir = expectedOutputDir

    def run(self):
        self._changeToTopLevelDir()
        fileNames = self._listFileNamesIn(self.sqlQueriesDir)
        process = subprocess.run(['pgsql/bin/pg_ctl', 'start', '-p' + self.port,
            '-D pg_data'])
        if process and process.returncode != 0:
            raise Error('Failed to start Postgres')
    
    def test(self, database):
        for file in self.lis
        expectedOutput = self._getExpectedOutputFrom(self
        process = subprocess.run(['pgsql/bin/psql/', '-d', database, '-f',
            self.sqlQueriesDir])
    
    def _getExpectedOutputFrom(self, file):
        expectedOutput = None
        with open(file, 'r') as output:
            expectedOutput = output.read().split('\n')
        return expectedOutput

    def restart(self):
        self._changeToTopLevelDir()
        process = subprocess.run()

    def _listFileNamesIn(self, path):
        return [file for file in os.listdir(path) if isfile(join(path, file))
                if file.endswith('.out')]

def main():
   print(listFileNamesIn('expected'))
   port = argv[1]
   print('port={}'.format(port))
   psql = runPostgres(port)

