#!/bin/bash
# Live view of DB2 EMPLOYEES table — refreshes every second.

watch -n 1 "docker exec db2-luw su - db2inst1 -c \
  'db2 connect to testdb > /dev/null && \
   db2 \"SELECT ID, NAME, DEPARTMENT, SALARY, UPDATED_AT \
         FROM DB2INST1.EMPLOYEES ORDER BY UPDATED_AT DESC\"' \
  2>&1 | grep -v 'Database Connection\|Database server\|SQL auth\|Local database\|^$'"
