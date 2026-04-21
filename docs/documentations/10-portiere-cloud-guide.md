# Portiere Cloud (Coming Soon)

Portiere Cloud is currently on our **development roadmap**. The open-source SDK operates in local mode only.

---

## Planned Features

The following features are planned for the Portiere Cloud platform:

### Team Collaboration
- **Web dashboard** for reviewing schema and concept mappings as a team
- **Role-based access control (RBAC)** with organization and project-level permissions
- **Task assignment** and progress tracking for clinical SME review workflows
- **Comments and discussion** threads on individual mappings

### Cloud Sync
- **`push()` / `pull()`** to synchronize local mapping artifacts with the cloud
- **Hybrid mode** — work locally, sync to cloud when ready for team review
- **Version history** and audit trails for all mapping changes

### Managed Infrastructure
- **Hosted inference** — use Portiere-managed LLM and embedding models without BYO keys
- **Hosted vector stores** — managed knowledge layer backends with automatic scaling
- **API access** — REST API for programmatic project management and mapping operations

### Enterprise Features
- **SSO (SAML/OIDC)** integration for enterprise identity providers
- **HIPAA BAA** and compliance certifications
- **On-premise deployment** option for regulated environments
- **Custom SLAs** and dedicated support engineering

---

## Current Status

In the open-source SDK, cloud-related methods raise `NotImplementedError`:

```python
project.push()       # NotImplementedError
project.pull()       # NotImplementedError
Client(api_key=...)  # NotImplementedError
```

All mapping, search, and AI inference features work fully in local mode.

---

## Stay Updated

For updates on Portiere Cloud availability, contact us at [sales@cuspal.co](mailto:sales@cuspal.co).
