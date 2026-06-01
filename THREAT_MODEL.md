# Apache Helix — Threat Model (v1 draft)

**Status:** v1 draft, authored by the ASF Security Team for the Helix PMC to
review. Drafted against `apache/helix` (default branch `master`), 2026-06-02.

**Provenance legend:** *(documented)* — from the project's README / repo
structure / docs; *(maintainer)* — ratified by a Helix PMC member; *(inferred)*
— reasoned from architecture / domain norms, **not yet confirmed** (each has a
matching §14 item).

**Draft confidence:** documented ~10 · maintainer 0 · inferred ~22. A
react-to-me draft — the Helix PMC (Junkai Xue, Jiajun Wang) confirms/corrects,
especially the §14 items.

**Revision triggers:** a change to the helix-rest auth model; a change to how
Helix authenticates/authorizes against the metadata store (ZooKeeper); a new
network-facing component (gateway, agent); a change to what znode data is
trusted.

---

## §1 Header

- **Project:** Apache Helix (`apache/helix`) — a **generic cluster-management
  framework** for partitioned, replicated, distributed resources: it assigns
  resource partitions to nodes, drives state transitions against a declarative
  state model, and reacts to node join/leave/failure. *(documented — README)*
- **Scope of this model:** the `apache/helix` repository only — the single
  OSSF-active repo under the Helix PMC. *(documented — PMC, 2026-06-01)*
- **What Helix is:** a coordination layer. The **metadata store (ZooKeeper)**
  is the control-plane source of truth; Helix components read/write it to
  coordinate. Helix is embedded by an operator's application (the participant
  hosts the operator's resources). It is not a standalone secure-by-default
  service. *(inferred — Q1)*

## §2 Scope and intended use

Helix is run by an **operator** who stands up a ZooKeeper ensemble, runs a
Controller (often as a dedicated process or embedded), embeds the Participant
in the application nodes that host resources, and optionally runs **helix-rest**
for cluster administration. *(documented — README; `helix-core`, `helix-rest`
dirs)*

### Component families (distinct threat profiles)

| Family | Path | Role / trust notes |
|---|---|---|
| Cluster-management core | `helix-core` | Controller / Participant / Spectator roles; computes + applies state transitions via the metadata store. *(documented)* |
| **Admin REST service** | `helix-rest` | Network-facing HTTP API to create/modify clusters, resources, instances, configs. The **primary network trust boundary** — who may call it, with what auth. *(inferred — Q5)* |
| Metadata-store access | `zookeeper-api`, `meta-client`, `metadata-store-directory-common` | The ZooKeeper-backed control plane Helix reads/writes. Whoever can write these znodes controls the cluster. *(documented — dirs)* |
| Gateway / Agent | `helix-gateway`, `helix-agent` | Additional participant-integration surfaces (e.g. gRPC gateway, external-process agent). Network/IPC profile pending confirmation. *(inferred — Q8)* |
| Distributed lock | `helix-lock` | ZK-backed locks; correctness/liveness, not a direct attack surface. *(inferred)* |
| Recipes / website / tests | `recipes`, `website`, `*/test` | Examples + docs, not the shipped runtime. *(inferred — Q3)* |

**In scope:** the core + helix-rest + metadata-store access + gateway/agent.
**Intended caller trust:** the operator and the application embedding the
Participant/Spectator are **trusted**; the **helix-rest caller** and **anyone
who can reach the ZooKeeper ensemble** are where untrusted actors appear.
*(inferred — Q2)*

## §3 Out of scope (explicit non-goals)

- `recipes/`, `website/`, `*/test`, build files — not the shipped runtime.
  *(inferred — Q3)*
- **ZooKeeper's own security** (its authN/authZ, ACL enforcement, transport) —
  Helix relies on a correctly-secured ensemble; hardening ZK is the operator's.
  *(inferred — Q6)*
- **The security of the resources Helix manages** — the databases/services/
  partitions hosted by Participants are the operator's applications, not Helix.
  *(inferred — Q1)*
- Build / release / supply-chain hygiene. Per rubric §1. *(documented — rubric)*

## §4 Trust boundaries and data flow

1. **Client → helix-rest.** A network client issues cluster-admin operations
   (create cluster, add/drop resource, change config, enable/disable instance).
   The high-value boundary: auth/authz on management actions + transport
   security. *(inferred — Q5,Q7)*
2. **Any actor → ZooKeeper metadata store.** The real control plane. An actor
   who can write the cluster's znodes can redirect partitions, disable nodes,
   or rewrite the state model — bypassing helix-rest entirely. Helix relies on
   ZK ACLs / network isolation here. *(inferred — Q6)*
3. **Controller ↔ Participant ↔ Spectator (via the store).** Components
   coordinate by reading/writing znodes; they trust the store's contents.
   Malformed/forged znode data is a conditional surface (Q9). *(inferred — Q9)*
4. **Operator/application → embedded Participant/Spectator.** In-process,
   trusted — the app already controls what it hosts. *(inferred — Q2)*

## §5 Assumptions about the environment

JVM; a reachable ZooKeeper ensemble (the control plane); the helix-rest listen
interface; network reachability between Controller, Participants, and the
ensemble; system trust store if TLS is used. *(inferred — Q11)*

## §6 Assumptions about inputs — per-parameter trust

| Input | Source | Trusted? | Notes |
|---|---|---|---|
| helix-rest requests (cluster/resource/instance/config ops) | Network client | **Untrusted-capable** | Auth/authz is the boundary — can an unauthenticated client administer a cluster? (Q5) |
| ZooKeeper znode data (ideal state, external view, configs, messages) | The ensemble | Trusted-by-reliance | Forged/corrupt znodes if an attacker can write ZK (Q6,Q9). |
| Cluster / resource / instance configuration | Operator (via REST or API) | Trusted | Operator-supplied control input. *(inferred)* |
| State-model definitions | Operator/app | Trusted | Defines legal transitions; operator's. *(inferred)* |
| The managed resources' own data/traffic | Operator's app | Pass-through | Helix coordinates placement; it doesn't handle the resource payloads. *(inferred)* |

## §7 Adversary model

- **Out of scope:** the operator and the application embedding Helix — they
  already control the cluster and what it hosts. *(inferred — Q2)*
- **In scope:** a **remote actor against helix-rest** (administering a cluster
  they shouldn't), an actor who can **reach/write the ZooKeeper ensemble**
  (full control-plane compromise), and a **network MITM** between components /
  on the REST channel. *(inferred — Q5,Q6,Q7)*
- **Conditional:** an actor who can inject **malformed znode data** that a
  Controller/Participant/Spectator parses. *(inferred — Q9)*

## §8 Security properties the project provides *(all pending PMC confirmation)*

- **helix-rest access control:** the admin API is expected to enforce
  authentication/authorization on cluster-mutating operations (default posture
  is the key question). *(inferred — Q5)* — violation symptom: an
  unauthenticated client mutates cluster state on a default deployment;
  severity: critical.
- **Metadata-store integrity reliance:** Helix behaves correctly given a
  trustworthy ZK ensemble; it relies on ZK ACLs for control-plane integrity.
  *(inferred — Q6)*
- **Transport security:** TLS available for helix-rest and for the ZK
  connection where supported. *(inferred — Q7)*
- **Robustness to malformed znode data:** parsing store contents should fail
  safe rather than crash/corrupt. *(inferred — Q9)*

## §9 Security properties the project does *not* provide

- **Not a sandbox / not an isolation boundary** for the resources it manages —
  Helix places and transitions them; their security is the operator's app.
  *(inferred — Q1)*
- **No defense if ZooKeeper is open/unauthenticated.** If an attacker can write
  the ensemble, Helix's coordination is fully subvertible — that is a ZK
  hardening responsibility, not a Helix defect. *(inferred — Q6)*
- **No first-party authorization model beyond what helix-rest / ZK ACLs
  enforce** — Helix does not add per-tenant RBAC on top. *(inferred — Q5,Q10)*
- **No guarantee against a hostile Controller/Participant** already inside the
  trust domain (they hold ZK write access by design). *(inferred — Q2)*

## §10 Downstream (operator) responsibilities

- **Secure ZooKeeper** — ACLs, authentication, and network isolation of the
  ensemble; it is the control plane. *(inferred — Q6)*
- **Lock down helix-rest** — authenticate it, don't expose the admin API to an
  untrusted network, restrict who can mutate clusters. *(inferred — Q5)*
- Network-isolate Controller / Participants / ensemble; use TLS on sensitive
  channels. *(inferred — Q7)*
- Secure the resources Helix manages (their own auth/encryption). *(inferred — Q1)*

## §11 Known misuse patterns

- An **open / unauthenticated ZooKeeper ensemble** reachable from an untrusted
  network → full control-plane takeover. *(inferred — Q6)*
- **helix-rest exposed with weak/no auth** to an untrusted network → anyone can
  administer clusters. *(inferred — Q5)*
- Treating Helix as a security boundary for the hosted resources. *(inferred — Q1)*

### §11a Known non-findings (recurring false positives) *(seed — PMC to expand, Q18a)*

- "Helix reads and writes ZooKeeper znodes / a Participant takes itself
  offline / the Controller drives state transitions" — **by design**; that is
  the coordination function, not a vuln. *(inferred — Q1)*
- "Helix trusts the data in ZooKeeper" — relies on a secured ensemble (§10);
  ZK hardening is downstream, not a Helix code defect. *(inferred — Q6)*
- "Code in `recipes/` does X" — examples, not the shipped runtime. *(inferred — Q3)*

## §12 Conditions that would change this model

A change to the helix-rest auth/authz model; a change to how Helix
authenticates against the metadata store; a new network-facing component
(gateway/agent); a change to what znode data is trusted or how it's parsed.
*(inferred)*

## §13 Triage dispositions

| Disposition | When | Section |
|---|---|---|
| **VALID** | Violates a §8 property under the §7 adversary (esp. unauth helix-rest admin, crash on malformed znode data) in in-scope code | §7, §8 |
| **OUT-OF-MODEL** | Adversary is the trusted operator/app or an in-domain component; or code in `recipes`/`website`/tests | §3, §7 |
| **DOWNSTREAM-RESPONSIBILITY** | ZK hardening, helix-rest exposure, network isolation, the managed resources' own security | §10 |
| **DISCLAIMED** | A §9 non-guarantee (not a sandbox; no defense if ZK is open; no first-party RBAC) | §9 |
| **MODEL-GAP** | Plausible, not covered → escalate to PMC | §12 |

## §14 Open questions for the maintainers

1. **(Q1)** Confirm framing: Helix coordinates placement/state of operator-owned resources; it is not a security/isolation boundary for them.
2. **(Q2)** Confirm the operator + embedding application + in-domain components (Controller/Participant/Spectator) are trusted (out of the adversary model).
3. **(Q3)** Confirm out-of-scope: `recipes/`, `website/`, tests, build.
4. **(Q5)** **helix-rest is the crux:** what is its *default* auth posture? Can a default deployment accept unauthenticated cluster-mutating requests, or is auth required / is it bound to an internal interface? Any built-in authz on who can administer which clusters?
5. **(Q6)** ZooKeeper trust: is "secure the ensemble (ACLs/auth/isolation)" an operator responsibility, and does Helix rely on ZK ACLs for control-plane integrity? Does Helix set ACLs on the znodes it creates?
6. **(Q7)** TLS: available/default for helix-rest and for the ZK client connection?
7. **(Q9)** Robustness: should a Controller/Participant/Spectator tolerate malformed/forged znode data without crashing, or is store integrity fully assumed?
8. **(Q8)** What are the trust/network profiles of `helix-gateway` and `helix-agent` (new-ish surfaces)? Are they network-facing, and authenticated?
9. **(Q10)** Is any first-party authorization (per-tenant/per-cluster RBAC) provided, or is that entirely helix-rest-auth + ZK ACLs?
10. **(Q11)** Confirm the environment assumptions (JVM, ZK ensemble, REST interface) worth stating.
11. **(Q18a)** What do scanners most often report against Helix that you consider non-findings? (Expand §11a.)
12. **(meta)** OK to land this as `THREAT_MODEL.md` with the `AGENTS.md → SECURITY.md → THREAT_MODEL.md` chain (this PR creates all three)?

## §15 Machine-readable companion

Not generated for v1.
