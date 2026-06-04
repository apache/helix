# Apache Helix — Threat Model (v1)

**Status:** v1, authored by the ASF Security Team and ratified by the Helix PMC.
Drafted against `apache/helix` (default branch `master`), 2026-06-02; PMC review
folded 2026-06-04 (Junkai Xue).

**Provenance legend:** *(documented)* — from the project's README / repo
structure / docs; *(maintainer)* — ratified by a Helix PMC member; *(inferred)*
— reasoned from architecture / domain norms, **not yet confirmed** (each has a
matching §14 item).

**Draft confidence:** documented ~10 · maintainer ~24 · inferred ~3. The Helix
PMC (Junkai Xue) confirmed the §14 items; the few remaining *(inferred)* tags
mark genuinely-open details (see §14).

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
  service. The control components run independent of user data. *(maintainer —
  Q1)*

## §2 Scope and intended use

Helix is run by an **operator** who stands up a ZooKeeper ensemble, runs a
Controller (often as a dedicated process or embedded), embeds the Participant
in the application nodes that host resources, and optionally runs **helix-rest**
for cluster administration. *(documented — README; `helix-core`, `helix-rest`
dirs)*

### Component families (distinct threat profiles)

| Family | Path | Role / trust notes |
|---|---|---|
| Cluster-management core | `helix-core` | Controller / Participant / Spectator roles; computes + applies state transitions via the metadata store. They run independent of user data. *(maintainer — Q1,Q2)* |
| **Admin REST service** | `helix-rest` | Network-facing HTTP API to create/modify clusters, resources, instances, configs. Shipped as a **skeleton**: the operator implements the authentication mechanism, and the **default is no auth**. The primary network trust boundary. *(maintainer — Q5)* |
| Metadata-store access | `zookeeper-api`, `meta-client`, `metadata-store-directory-common` | The ZooKeeper-backed control plane Helix reads/writes. Whoever can write these znodes controls the cluster. *(documented — dirs)* |
| Gateway / Agent | `helix-gateway`, `helix-agent` | Additional participant-integration surfaces. A **pluggable framework** — the user supplies the implementation. *(maintainer — Q8)* |
| Distributed lock | `helix-lock` | ZK-backed locks; correctness/liveness, not a direct attack surface. *(inferred)* |
| Recipes / website / tests | `recipes`, `website`, `*/test` | Examples + docs, not the shipped runtime. *(maintainer — Q3)* |

**In scope:** the core + helix-rest + metadata-store access + gateway/agent.
**Intended caller trust:** the operator and the application embedding the
Participant/Spectator are **trusted**; the **helix-rest caller** and **anyone
who can reach the ZooKeeper ensemble** are where untrusted actors appear.
*(maintainer — Q2)*

## §3 Out of scope (explicit non-goals)

- `recipes/`, `website/`, `*/test`, build files — not the shipped runtime.
  *(maintainer — Q3)*
- **ZooKeeper's own security** (its authN/authZ, ACL enforcement, transport) —
  ZooKeeper runs as a **separate service**; maintaining/securing it is **not
  Helix's responsibility**, and Helix sets **no ACLs** on the znodes it creates.
  *(maintainer — Q6)*
- **The security of the resources Helix manages** — the databases/services/
  partitions hosted by Participants are the operator's applications, not Helix.
  *(maintainer — Q1)*
- Build / release / supply-chain hygiene. Per rubric §1. *(documented — rubric)*

## §4 Trust boundaries and data flow

1. **Client → helix-rest.** A network client issues cluster-admin operations
   (create cluster, add/drop resource, change config, enable/disable instance).
   helix-rest ships as a skeleton with **no default auth**; the operator
   implements authentication. This is the high-value boundary. *(maintainer —
   Q5)*
2. **Any actor → ZooKeeper metadata store.** The real control plane. An actor
   who can write the cluster's znodes can redirect partitions, disable nodes,
   or rewrite the state model — bypassing helix-rest entirely. ZooKeeper access
   goes through the **ZooKeeper client's own authz** mechanism; Helix does not
   set ACLs on the znodes it creates. *(maintainer — Q6,Q7)*
3. **Controller ↔ Participant ↔ Spectator (via the store).** Components
   coordinate by reading/writing znodes; they trust the store's contents.
   They run independent of user data. Malformed/forged znode data is a
   conditional surface (§7, Q9). *(maintainer — Q2,Q9)*
4. **Operator/application → embedded Participant/Spectator.** In-process,
   trusted — the app already controls what it hosts. *(maintainer — Q2)*

## §5 Assumptions about the environment

JVM; a reachable ZooKeeper ensemble (the control plane); the helix-rest listen
interface; network reachability between Controller, Participants, and the
ensemble; system trust store if TLS is used. *(maintainer — Q11)*

## §6 Assumptions about inputs — per-parameter trust

| Input | Source | Trusted? | Notes |
|---|---|---|---|
| helix-rest requests (cluster/resource/instance/config ops) | Network client | **Untrusted-capable** | helix-rest is a skeleton; operator implements auth; **default is no auth** (Q5). |
| ZooKeeper znode data (ideal state, external view, configs, messages) | The ensemble | Trusted-by-reliance | ZK access is gated by the ZK client's own authz; forged/corrupt znodes if an attacker can write ZK (Q6,Q9). *(maintainer — Q6,Q7)* |
| Cluster / resource / instance configuration | Operator (via REST or API) | Trusted | Operator-supplied control input. *(maintainer — Q2)* |
| State-model definitions | Operator/app | Trusted | Defines legal transitions; operator's. *(maintainer — Q2)* |
| The managed resources' own data/traffic | Operator's app | Pass-through | Helix coordinates placement; it doesn't handle the resource payloads. *(maintainer — Q1)* |

## §7 Adversary model

- **Out of scope:** the operator and the application embedding Helix — they
  already control the cluster and what it hosts. In-domain components
  (Controller/Participant/Spectator) run independent of user data and are
  trusted. *(maintainer — Q2)*
- **In scope:** a **remote actor against helix-rest** (the API ships with no
  default auth, so on an unprotected deployment an unauthenticated caller can
  administer a cluster), an actor who can **reach/write the ZooKeeper ensemble**
  (full control-plane compromise), and a **network MITM** between components /
  on the REST channel. *(maintainer — Q5,Q6)*
- **Conditional:** an actor who can inject **malformed znode data** that a
  Controller/Participant/Spectator parses. *(maintainer — Q9)*

## §8 Security properties the project provides

- **helix-rest access control:** helix-rest is a **skeleton** — it does **not**
  provide authentication by default; the operator implements the auth mechanism.
  A default deployment will accept unauthenticated cluster-mutating requests.
  *(maintainer — Q5)* — violation symptom for a deployment that *has*
  implemented auth: an unauthenticated client mutates cluster state despite the
  operator's auth layer; severity: critical. (An out-of-the-box unauthenticated
  deployment is the documented default, not a defect — see §9, §11.)
- **Metadata-store integrity reliance:** Helix behaves correctly given a
  trustworthy ZK ensemble; ZooKeeper is a separate service and access is gated
  by the ZK client's own authz. Helix sets no ACLs on the znodes it creates.
  *(maintainer — Q6,Q7)*
- **Robustness to malformed znode data:** parsing store contents should fail
  safe rather than crash/corrupt. *(maintainer — Q9)*

## §9 Security properties the project does *not* provide

- **helix-rest provides no built-in authentication.** It is a skeleton; auth is
  the operator's to implement, and the default is no auth. *(maintainer — Q5)*
- **Not a sandbox / not an isolation boundary** for the resources it manages —
  Helix places and transitions them; their security is the operator's app.
  *(maintainer — Q1)*
- **No defense if ZooKeeper is open/unauthenticated.** ZooKeeper is a separate
  service that is not Helix's responsibility to secure; Helix sets no ACLs on
  the znodes it creates. If an attacker can write the ensemble, Helix's
  coordination is fully subvertible. *(maintainer — Q6)*
- **No first-party authorization model** — Helix does not add per-tenant RBAC.
  Authorization is **helix-rest-auth (operator-implemented) + ZK ACLs**.
  *(maintainer — Q5,Q10)*
- **No guarantee against a hostile Controller/Participant** already inside the
  trust domain (they hold ZK write access by design). *(maintainer — Q2)*

## §10 Downstream (operator) responsibilities

- **Secure ZooKeeper** — it runs as a separate service that Helix does not
  manage; the operator handles its authz (via the ZK client), authentication,
  and network isolation. Helix sets no ACLs on the znodes it creates, so any
  ZNode-level access control is the operator's. *(maintainer — Q6,Q7)*
- **Implement helix-rest auth** — helix-rest ships as a skeleton with no default
  auth; the operator implements the authentication mechanism, must not expose
  the admin API to an untrusted network, and restricts who can mutate clusters.
  *(maintainer — Q5)*
- **Supply gateway/agent implementations** — these are a pluggable framework;
  the operator provides the implementation and its trust/network controls.
  *(maintainer — Q8)*
- Network-isolate Controller / Participants / ensemble; use TLS on sensitive
  channels. *(inferred — Q7a)*
- Secure the resources Helix manages (their own auth/encryption). *(maintainer
  — Q1)*

## §11 Known misuse patterns

- An **open / unauthenticated ZooKeeper ensemble** reachable from an untrusted
  network → full control-plane takeover. ZK is a separate service the operator
  secures. *(maintainer — Q6)*
- **helix-rest exposed without an operator-supplied auth layer** to an untrusted
  network → anyone can administer clusters (no auth is the default). *(maintainer
  — Q5)*
- Treating Helix as a security boundary for the hosted resources. *(maintainer
  — Q1)*

### §11a Known non-findings (recurring false positives)

- "Helix reads and writes ZooKeeper znodes / a Participant takes itself
  offline / the Controller drives state transitions" — **by design**; that is
  the coordination function, not a vuln. *(maintainer — Q1)*
- "Helix trusts the data in ZooKeeper" — relies on a secured, separate ZK
  service (§10); ZK hardening is downstream, not a Helix code defect.
  *(maintainer — Q6)*
- "helix-rest has no authentication" — by design; it is a skeleton and auth is
  operator-implemented, default none. *(maintainer — Q5)*
- "Code in `recipes/` does X" — examples, not the shipped runtime. *(maintainer
  — Q3)*
- **Findings reported from the Helix UI** — the Helix UI is **already
  deprecated**; reports against it are not actionable. *(maintainer — Q18a)*

## §12 Conditions that would change this model

A change to the helix-rest auth/authz model; a change to how Helix
authenticates against the metadata store; a new network-facing component
(gateway/agent); a change to what znode data is trusted or how it's parsed.
*(maintainer)*

## §13 Triage dispositions

| Disposition | When | Section |
|---|---|---|
| **VALID** | Violates a §8 property under the §7 adversary (e.g. auth bypass against an operator's helix-rest auth layer, crash on malformed znode data) in in-scope code | §7, §8 |
| **OUT-OF-MODEL** | Adversary is the trusted operator/app or an in-domain component; helix-rest's no-default-auth posture itself; reports from the deprecated Helix UI; or code in `recipes`/`website`/tests | §3, §7, §11a |
| **DOWNSTREAM-RESPONSIBILITY** | ZK hardening, helix-rest auth implementation/exposure, gateway/agent implementation, network isolation, the managed resources' own security | §10 |
| **DISCLAIMED** | A §9 non-guarantee (no built-in helix-rest auth; not a sandbox; no defense if ZK is open; no first-party RBAC) | §9 |
| **MODEL-GAP** | Plausible, not covered → escalate to PMC | §12 |

## §14 Open questions for the maintainers

Q1, Q2, Q3, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q18a, and the meta question were
confirmed by the Helix PMC (Junkai Xue, 2026-06-04) and folded above as
*(maintainer)*. The remaining open items are narrower:

1. **(Q7a)** TLS posture: is TLS available/default for the helix-rest listen
   interface, and for the ZooKeeper client connection? (The PMC confirmed ZK
   access goes through the ZK client's own authz, but the transport-encryption
   default for each channel is not yet stated.) *(inferred)*
2. **(Q-lock)** Confirm `helix-lock` (ZK-backed distributed locks) carries no
   direct external attack surface beyond the ZK-control-plane exposure already
   captured in §4.2 — i.e. correctness/liveness only. *(inferred)*

## §15 Machine-readable companion

Not generated for v1.
