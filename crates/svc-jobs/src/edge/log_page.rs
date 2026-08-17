use br_util_graphql::{Connection, Edge, PageInfo};

use super::types::log::{GqlRunLog, log_of};
use crate::db::logs::{LogPage, cursor_of};

pub fn log_connection(page: LogPage) -> Connection<GqlRunLog> {
    let edges: Vec<Edge<GqlRunLog>> = page
        .lines
        .iter()
        .map(|(job_id, line)| Edge::new(log_of(*job_id, line), cursor_of(line)))
        .collect();
    let start_cursor = edges.first().map(|edge| edge.cursor.clone());
    let end_cursor = edges.last().map(|edge| edge.cursor.clone());
    Connection::new(
        edges,
        PageInfo {
            has_next_page: page.has_next_page,
            has_previous_page: page.has_previous_page,
            start_cursor,
            end_cursor,
        },
    )
}
