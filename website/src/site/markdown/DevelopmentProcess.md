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

---

# Development Process

## Project Based  Development

### Project Proposal & Review

#### Overall Steps

The developer for Apache Helix should use following steps for project based development:
* Write a Wiki to describe your proposal for the project.
* Send the proposal to [dev@helix.apache.org](dev@helix.apache.org) for reviewing
* Addressing all the comments and concerns.
* Get **AT LEAST ONE** approval from Apache Helix PMCs.
* Create your project branch and start the implementation.
* Make sure all tests passed before merging to the main branch.

#### Proposal Guidance

Before the implementation, a project proposal must required for following items documented:
* Problem Statement
* Proposed Solutions
  * Architecture / Structure Change 
  * Potential Impact
  * Behavior Change
* Alternative Solutions
* Roll Out Guidance for Users

For all the list components must be covered in the proposal documentation. This proposal must be submitted in the [github wiki](https://github.com/apache/helix/wiki) under **Document Ready for Review** and mark title prefix with different statuses:
* **\[Draft\]**: document is working in progress 
* **\[In Review\]**: document is ready to be reviewed and people are reviewing it.
* **\[Done\]**: document is reviewed and signed off by Apache Helix PMC members.

#### Definition of Project 

To define a project, you need to understand the complexity of the feature you implement, improve or fixing bugs.
Here're the minimum requirements for a project based development. Any one of the item qualified, you must start a project based development:
 * The work need to be done is a new feature that either :
   * May cause multiple modules touched .
   * Over 200 lines code change need to be break down into small PRs.
   * New modules to be added.
 * The work need to be done is improvement / bug fixing either:
   * Over 200 lines code change.
   * Impact user behavior
   * Impact Helix client components, such as HelixManager, Participant, Spectator and so on.
   * Backward incompatible.
   * Require major refactoring code structure.

Feelfree to add more items and give suggestions to [dev mailing list](dev@helix.apache.org).

## Regular Bug Fixing

### Bug Fixing Steps

For all developers who are willing to contributing to Apache Helix for fixing bugs, please following the steps we decribed below.
* If there is a github issue, assign that ticket to you if you are committer. For contributors, you can leave a comment to let other committers to assign that issue to you. If there is no issue in github, create an issue for that.
* Working on the bug fixing in your forked repo.
* Submit PR based on our [template](https://github.com/apache/helix/wiki/Pull-Request-Description-Template)
* Address all review comments and follow [merge steps](https://github.com/apache/helix/wiki/Pull-Request-Merge-Steps)

### Relevant Work

When you work on bug fixing and you found other things need to be done, feel free to create issues and leave **TODO** comments in the code to avoid make the PR larger and larger. 
