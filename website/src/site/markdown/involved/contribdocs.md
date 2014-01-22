<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

Improve this Website
--------------------

Learn something new about Helix that you think would benefit others? The source for this website is available for you to look at and edit!

### Get the Code
```
git clone https://git-wip-us.apache.org/repos/asf/helix.git
cd helix/website
```

### Make Changes

The website is structured as follows:

```
website/ -- the root directory for the Helix website
  src/site -- source files for all the top-level webpages
  	site.xml -- metadata for the top-level website
  version-docs/ -- the root directory for the documentation for a given Helix version
    src/site -- source files for documentation specific to the version
      site.xml -- metadata for the release documentation
    pom.xml -- Maven pom for the release documentation package
  pom.xml -- parent Maven pom for the website
  deploySite.sh -- script to deploy the website publicly (for committers)
```

### Build Locally

To build the website, do the following:

```
mvn site
mvn site:stage
```

Then, the entire website will be placed in `target/staging`. If you have Python installed, this command will start a local web server on port 8000:

```
pushd target/staging; python -m SimpleHTTPServer; popd
```

### Submitting Changes

Once satisfied with any new changes to the website, the standard [code contribution](https://cwiki.apache.org/confluence/display/HELIX/Contributor+Workflow) guidelines apply, including code review and submitting patches.

If you\'re a Helix committer, you can run `deploySite.sh` to go live.
