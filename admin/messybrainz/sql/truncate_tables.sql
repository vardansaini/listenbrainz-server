BEGIN;

TRUNCATE artist_credit                CASCADE;
TRUNCATE recording                    CASCADE;
TRUNCATE recording_json               CASCADE;
TRUNCATE release                      CASCADE;

COMMIT;
