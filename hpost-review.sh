#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

usage ()
{
    echo "Usage: hpost-review.sh rev-list JIRA# ...";
    echo "To get the local commits, use:"
    echo "./hpost-review.sh HEAD^..HEAD 1234"
    exit 1;
}

if [ $# -lt 2 ] ; then
    usage
fi;

REVLIST=$1;
JIRA=$2
shift 2;

# Run the rat plugin
echo 'Checking source for license headers'
mvn -Prat -DskipTests > /dev/null
RAT_STATUS=$?

if [[ $RAT_STATUS -ne 0 ]] ; then
	echo "Maven rat plugin failed. Add license headers and try again."
	exit 1;
fi;
echo 'Checking source for license headers: PASSED'

# Check if the commit is prefixed with [HELIX-NNN]
echo 'Checking commit message format'
BUG_NAME=HELIX-$JIRA
COMMIT_PREFIX=\[$BUG_NAME\]
DESCRIPTION=$(git log --pretty=format:%s $REVLIST)

if [[ "$DESCRIPTION" != "$COMMIT_PREFIX"* ]] ; then
    echo "Commit message must start with $COMMIT_PREFIX"
    usage
fi;
echo 'Checking commit message format: PASSED'

# Check if HELIX-NNN is a valid bug
echo 'Checking JIRA existence'
JIRA_URL=https://issues.apache.org/jira/rest/api/latest/issue/$BUG_NAME
JIRA_STATUS=$(curl -o /dev/null --silent --head --write-out '%{http_code}\n' $JIRA_URL)

if [[ $JIRA_STATUS -eq 404 ]]; then
    echo "$BUG_NAME does not exist in JIRA"
    usage
fi;
echo 'Checking JIRA existence: PASSED'

post-review --server="https://reviews.apache.org" --target-groups=helix --summary="$(git log --pretty=format:%s $REVLIST)" --description="$(git whatchanged $REVLIST)" --diff-filename=<(git diff --no-prefix $REVLIST) --repository-url=git://git.apache.org/helix.git -o --bugs-closed=$BUG_NAME $*

