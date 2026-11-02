ALTER TABLE job_resolutions
    ADD COLUMN command_id uuid;

CREATE UNIQUE INDEX job_resolutions_command
    ON job_resolutions (job_id, command_id)
    WHERE command_id IS NOT NULL;
