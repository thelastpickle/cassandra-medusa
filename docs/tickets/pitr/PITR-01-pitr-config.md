# PITR-01: Add `[pitr]` config section

- **Type:** Task
- **Size:** XS (<2 ideal days)
- **Estimated Ideal Days:** 1 day (Est. Calendar Time: 2-3 days)
- **Parallel Track:** Track A - Config
- **Blocked By / Dependencies:** None
- **Blocks:** PITR-04, PITR-06, PITR-10, PITR-11

---

## Purpose
> WHY are we doing this work? What problem does it solve and what value does it deliver?

PITR needs settings: on/off switch, archive interval, spool dir, staging dir, restore grace period. Every other PITR ticket reads these settings. This ticket adds them in one place, with safe defaults.

The default must keep PITR off (`enabled = false`). Operators who do not use PITR must see no change (design doc goal G3).

## Requirements
> WHAT needs to be true for this work to be successful? (Bulleted list)

- Add a `PitrConfig` namedtuple to `medusa/config.py` with these fields (design doc section 4.3):
  - `enabled`
  - `commitlog_archive_interval_seconds`
  - `commitlog_spool_dir`
  - `commitlog_staging_dir`
  - `commitlog_restore_grace_period_seconds`
- Add a `pitr` field to `MedusaConfig`.
- Register the section in `CONFIG_SECTIONS`.
- Add these defaults in `_build_default_config()`:
  - `enabled = false`
  - `commitlog_archive_interval_seconds = 60`
  - `commitlog_spool_dir = /var/lib/cassandra/commitlog_spool_dir`
  - `commitlog_staging_dir = /var/lib/cassandra/medusa_commitlog_staging`
  - `commitlog_restore_grace_period_seconds = 3600`
- `load_config` must fill `MedusaConfig.pitr` from the `[pitr]` section.
- A `medusa.ini` file with no `[pitr]` section must load. `pitr.enabled` must be false.
- Do NOT add `archive_command` as a config field. Code derives it at runtime from `commitlog_spool_dir` (design doc section 4.3).
- Do NOT change the behaviour of any other config section.
- A generic CLI override key such as `enabled` must not turn on PITR. Today `_override_config_with_args` applies one `enabled` key to every section that has an `enabled` field.
- Existing code and tests that build `MedusaConfig(...)` must still work.
- Document the `[pitr]` section in `medusa-example.ini`. Comment out each setting. Show the defaults. Explain that `commitlog_spool_dir` must be on the same filesystem as the Cassandra commitlog directory.
- Add a header comment: "[pitr] is disabled unless enabled = true."
- Required unit tests in `tests/config_test.py`:
  - Defaults when the ini file has no `[pitr]` section.
  - Custom values for all five fields from an ini file, read through `load_config`.
  - `pitr.enabled` stays false when the CLI args contain the generic `enabled` key.

## Definition of Done (Acceptance Criteria)
> How will we know we did the work right?

- [ ] `load_config` on a file with no `[pitr]` section returns the five default values. `pitr.enabled` is false.
- [ ] `load_config` on a file with a custom `[pitr]` section returns the custom values.
- [ ] `medusa-example.ini` documents the `[pitr]` section and all five settings.
- [ ] Unit test coverage implemented (>= 80% coverage for new logic).
- [ ] The full existing unit test suite passes. No test needs a behaviour change.
- [ ] Documentation / comments updated.
- [ ] Linters (`flake8`) and CI pass cleanly.

## Proposed Implementation (Optional / Starting Point)
> Suggested technical approach as a non-prescriptive starting point.

This is a suggestion. You own the final design.

- [`medusa/config.py`](../../../medusa/config.py:81): add `PitrConfig` next to `KubernetesConfig`.
- [`medusa/config.py`](../../../medusa/config.py:63): add `'pitr'` to the `MedusaConfig` fields.
- [`medusa/config.py`](../../../medusa/config.py:86): add `'pitr': PitrConfig` to `CONFIG_SECTIONS`. `_override_config_with_args` does `CONFIG_SECTIONS[config_section]` for every section in the parser. A missing entry raises `KeyError`.
- [`medusa/config.py`](../../../medusa/config.py:181): add `config['pitr'] = {...}` after the `kubernetes` block in `_build_default_config()`.
- [`medusa/config.py`](../../../medusa/config.py:347): pass `pitr=_namedtuple_from_dict(PitrConfig, config['pitr'])` in `load_config`.
- Generic `enabled` key: `_override_config_with_args` ([line 225](../../../medusa/config.py:225)) copies a CLI `enabled` value into every section with an `enabled` field. See the `FIXME collision` notes in [`tests/config_test.py`](../../../tests/config_test.py:145). Skip the `pitr` section in that loop, or use a different CLI key for it.
- Existing test code builds `MedusaConfig(...)` by keyword in many files (for example `tests/purge_test.py`, `tests/storage_test.py`, `tests/cassandra_utils_test.py`, `tests/service/grpc/server_test.py`). Two options:
  - Add `pitr=None` to each call.
  - Give the new field a default (`namedtuple(..., defaults=[None])`). This is a smaller change.
- Values are strings after parsing, like the other sections. Callers convert them with `int()` and `evaluate_boolean()` from `medusa.utils`.
- Tests: follow the style of `test_use_sudo_kubernetes_enabled_without_config_file` in [`tests/config_test.py`](../../../tests/config_test.py:190). Use `tests/resources/config/medusa.ini` for the no-section case. Add a new ini file under `tests/resources/config/` for the custom-values case.
- Docs: add a `[pitr]` block after `[kubernetes]` in [`medusa-example.ini`](../../../medusa-example.ini:213).
