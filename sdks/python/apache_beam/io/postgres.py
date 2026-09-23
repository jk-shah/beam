#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""PostgreSQL I/O transforms backed by the native Go postgresio connector.

This module exposes Python PTransforms for reading, bulk writing, and
continuous Change Data Capture (CDC) streaming with PostgreSQL databases:

  * :class:`ReadFromPostgres`: Reads rows from PostgreSQL tables or queries.
  * :class:`WriteToPostgres`: High-performance bulk sink using COPY and UNNEST upserts.
  * :class:`ReadFromPostgresCDC`: Streams change data capture events via logical replication.
"""

from apache_beam.io.postgres_cdc import POSTGRES_CDC_READ_URN
from apache_beam.io.postgres_cdc import POSTGRES_READ_URN
from apache_beam.io.postgres_cdc import POSTGRES_WRITE_URN
from apache_beam.io.postgres_cdc import PostgreSqlWriteResult
from apache_beam.io.postgres_cdc import ReadFromPostgres
from apache_beam.io.postgres_cdc import ReadFromPostgresCDC
from apache_beam.io.postgres_cdc import ReadFromPostgresCdc
from apache_beam.io.postgres_cdc import ReadFromPostgresIO
from apache_beam.io.postgres_cdc import WriteToPostgres
from apache_beam.io.postgres_cdc import WriteToPostgresBulk
from apache_beam.io.postgres_cdc import WriteToPostgresIO
from apache_beam.io.postgres_cdc import default_expansion_service

__all__ = [
    'POSTGRES_CDC_READ_URN',
    'POSTGRES_READ_URN',
    'POSTGRES_WRITE_URN',
    'PostgreSqlWriteResult',
    'ReadFromPostgres',
    'ReadFromPostgresIO',
    'ReadFromPostgresCDC',
    'ReadFromPostgresCdc',
    'WriteToPostgres',
    'WriteToPostgresIO',
    'WriteToPostgresBulk',
    'default_expansion_service',
]
