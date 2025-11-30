# Release Confidence & Quality Assurance

## Critical User Journeys (CUJs)

The following critical paths MUST be manually verified before a release:

- [ ] Connection to qBittorrent (Auth & Connectivity)
- [ ] Successful Move (End-to-End Transfer)
- [ ] Dry Run Verification (Non-destructive simulation)
- [ ] Permission Handling (Remote chmod checks)

## Dogfooding Protocol

Maintainers MUST use the preview release for >1 week before promotion to stable.
