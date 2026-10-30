pub const SCHEMA: &[&str] = &[
    "CREATE TABLE aggregate_groups (id BIGINT PRIMARY KEY)",
    "CREATE TABLE aggregate_items (id BIGINT PRIMARY KEY, group_id BIGINT, amount BIGINT)",
    "CREATE TABLE aggregate_saved (amount BIGINT)",
    "INSERT INTO aggregate_groups VALUES (1)",
];

pub const GLOBAL: &str = "SELECT sum(amount) FROM aggregate_items WHERE group_id = 1";
pub const GROUPED: &str =
    "SELECT group_id, sum(amount) FROM aggregate_items WHERE group_id = 1 GROUP BY group_id";
pub const JOINED: &str = "SELECT sum(i.amount) FROM aggregate_items i JOIN aggregate_groups g ON i.group_id = g.id WHERE i.group_id = 1";
pub const JOINED_GROUPED: &str = "SELECT i.group_id AS group_id, sum(i.amount) FROM aggregate_items i JOIN aggregate_groups g ON i.group_id = g.id WHERE i.group_id = 1 GROUP BY i.group_id";
pub const WINDOW: &str =
    "SELECT sum(amount) OVER (ORDER BY id) FROM aggregate_items WHERE group_id = 1";

// PostgreSQL widens SUM(bigint) to NUMERIC. The explicit reference casts test
// OmenDB's existing bounded BIGINT result domain, not PostgreSQL's widening.
pub const CASES: &[(&str, &str, &str)] = &[
    (
        "global",
        GLOBAL,
        "SELECT CAST(sum(amount) AS BIGINT) AS sum FROM aggregate_items WHERE group_id = 1",
    ),
    (
        "grouped",
        GROUPED,
        "SELECT group_id, CAST(sum(amount) AS BIGINT) AS sum FROM aggregate_items WHERE group_id = 1 GROUP BY group_id",
    ),
    (
        "joined",
        JOINED,
        "SELECT CAST(sum(i.amount) AS BIGINT) AS sum FROM aggregate_items i JOIN aggregate_groups g ON i.group_id = g.id WHERE i.group_id = 1",
    ),
    (
        "joined grouped",
        JOINED_GROUPED,
        "SELECT i.group_id AS group_id, CAST(sum(i.amount) AS BIGINT) AS sum FROM aggregate_items i JOIN aggregate_groups g ON i.group_id = g.id WHERE i.group_id = 1 GROUP BY i.group_id",
    ),
    (
        "window",
        WINDOW,
        "SELECT CAST(sum(amount) OVER (ORDER BY id) AS BIGINT) AS sum FROM aggregate_items WHERE group_id = 1",
    ),
];
