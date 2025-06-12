# Reviewer Bots for Dataverse

This repository contains two reviewer bots for Dataverse, organized into the following sub-folders:

## `mr-summarizer`
A webhook handler that summarizes merge requests (MRs) in Dataverse Data Product Config repository. It analyzes the MR diff, generating concise summaries to help reviewers quickly understand proposed changes.
.

## `dbt-repo-analyzer`
A webhook handler that determines if an MR to promote a data product to a higher environment has been raised, and if so, analyze the associated dbt repo for maturity.
