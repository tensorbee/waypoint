DROP VIEW user_summary;
CREATE OR REPLACE VIEW user_summary AS
SELECT
    id,
    username,
    email
FROM users;
