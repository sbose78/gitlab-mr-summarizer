# AI Review Bot Configuration

This file configures custom review criteria for the AI review bot. The bot will look for the `## Review Criteria` section and include those checks in its review process.

## Review Criteria

- **Business Logic Validation:**  
  Check if the changes align with our business rules documented in `docs/business_rules.md`.

- **Naming Conventions:**  
  - Models should follow pattern: `[mart/stage/int]_[domain]_[entity]_[verb]`
  - CTEs should use snake_case and be descriptive
  - Columns should follow source naming unless explicitly transformed

- **Performance Standards:**  
  - Incremental models must have a valid incremental_strategy
  - Fact tables should use clustering keys on frequently filtered columns
  - Lookups should use refs to dimension tables, not source tables

- **Documentation Requirements:**  
  - Each model must have a description and grain defined
  - Business owners must be tagged using `@mention` in descriptions
  - Changes to KPI calculations need sign-off comment in the MR

## Other Sections

You can include other sections in this file for other purposes. The bot will only use the content under `## Review Criteria` for MR reviews. 