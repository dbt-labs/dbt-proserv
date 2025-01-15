## Release Notes
<!-- 
* Use this section to notate what features/updates/fixes will be merged to
* production. You can find this by viewing the merged pull requests to the
* middle branch since the last promotion to production.
-->

## To-do before or after merge
<!---
* Include any notes about things that need to happen before or after this PR is 
* merged to production, e.g.:
- [ ] [BEFORE] Ensure PR !56 is merged, which fixes ...
- [ ] [AFTER] Update the production dbt Cloud job to remove the command ...
-->


## Screenshots:
<!--- 
* If needed, include screenshot(s) that prove all objects being promoted
* are fully functional (can execute and don't fail on tests).
* You may choose to remove this section if you have a CI job that fires on 
* release PRs.
-->


## Checklist:
<!-- This list should always be checked, but not all may apply. -->

- [ ] I have ensured the target branch of this pull request is `main`.
    <!--- 
    * You can see this at the top of your pull request. Edit the pull request
    * and change the target branch if it's not correct.
    -->
- [ ] I have titled this pull request following our guidelines.
    <!---
    * Please follow the convention `Release: [date]`
    * Example:
        + Release: 2024-06-17
    -->
- [ ] A recent job was triggered in dbt Cloud which encompasses everything
      being promoted, and I have either attached link(s) to the successful
      run(s) or the pipeline status on the PR reflects the status of these
      changes.
