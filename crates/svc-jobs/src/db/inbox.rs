use async_trait::async_trait;
use bc_jobs::domain::mail::views::{AttachmentMeta, UnreadItem, UnreadMail};
use bc_jobs::domain::runner::work::WorkNamed;
use bc_jobs::ports::inbox::Inbox;
use std::collections::{HashMap, HashSet};

use bc_jobs::domain::ids::{AgentId, ItemId, MailId, RunId};
use bc_jobs::domain::mail::access::{guard_address_is_of_the_run, guard_marks_only_own_items};
use bc_jobs::domain::mail::inbox::ItemKind;
use bc_jobs::domain::runner::refusal::RunnerRefusal;
use bc_jobs::domain::runner::work::MarkReadRefusal;
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::PgStore;
use super::acts_for::acts_for_in;
use super::hydrate::unavailable;
use super::work::{addresses_of_run, require_run, require_work};

const UNREAD_SQL: &str = "\
    SELECT i.item_id::uuid AS item_id, i.seq, i.kind, i.address, i.run_id::uuid AS run_id, \
        i.accepted_at, i.notice, i.mail_id, m.sender_address, m.author, \
        m.source_conversation_id, m.subject, m.body \
    FROM inbox_items i \
    LEFT JOIN mails m ON m.mail_id = i.mail_id \
    WHERE i.read_at IS NULL \
      AND (i.address = ANY($1) OR ($3 AND i.run_id = ANY($2))) \
    ORDER BY i.seq";

#[async_trait]
impl Inbox for PgStore {
    async fn unread(
        &self,
        work: WorkNamed<'_>,
        address: Option<AgentId>,
    ) -> Result<Result<Vec<UnreadItem>, RunnerRefusal>, PortError> {
        let mut tx = self.begin().await?;
        let context = match require_work(&mut tx, work, false).await? {
            Ok(context) => context,
            Err(refusal) => return Ok(Err(RunnerRefusal::Work(refusal))),
        };
        let addresses = addresses_of_run(&mut tx, context.run_id).await?;
        let (watched, with_run_notices) = match address {
            Some(one) => {
                if let Err(refusal) = guard_address_is_of_the_run(one, &addresses) {
                    return Ok(Err(refusal.into()));
                }
                (vec![one.as_uuid()], false)
            }
            None => (
                addresses.iter().map(|address| address.as_uuid()).collect(),
                true,
            ),
        };
        let acts_for = acts_for_in(&mut tx, context.run_id).await?;
        let rows = sqlx::query(UNREAD_SQL)
            .bind(&watched)
            .bind(runs_reachable(context.run_id, &acts_for))
            .bind(with_run_notices)
            .fetch_all(&mut *tx)
            .await
            .map_err(unavailable)?;
        let mail_ids: Vec<Uuid> = rows
            .iter()
            .filter(|row| row.get::<String, _>("kind") == "MAIL")
            .filter_map(|row| row.get::<Option<Uuid>, _>("mail_id"))
            .collect();
        let mut attachments = attachments_of(&mut tx, &mail_ids).await?;
        let items = rows
            .iter()
            .map(|row| unread_item(row, &mut attachments))
            .collect::<Result<Vec<_>, PortError>>()?;
        Ok(Ok(items))
    }

    async fn mark_read(
        &self,
        run_id: Uuid,
        instance_key: &str,
        runner_type: &str,
        item_ids: &[Uuid],
        at: DateTime<Utc>,
    ) -> Result<Result<(), MarkReadRefusal>, PortError> {
        let mut tx = self.begin().await?;
        let (run_id, _) =
            match require_run(&mut tx, run_id, instance_key, Some(runner_type)).await? {
                Ok(found) => found,
                Err(refusal) => return Ok(Err(refusal.into())),
            };
        let addresses: Vec<Uuid> = addresses_of_run(&mut tx, run_id)
            .await?
            .into_iter()
            .map(|address| address.as_uuid())
            .collect();
        let acts_for = acts_for_in(&mut tx, run_id).await?;
        let reachable: Vec<Uuid> = sqlx::query_scalar(
            "SELECT item_id::uuid FROM inbox_items \
             WHERE item_id = ANY($1) AND (address = ANY($2) OR run_id = ANY($3))",
        )
        .bind(item_ids)
        .bind(&addresses)
        .bind(runs_reachable(run_id, &acts_for))
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
        let reachable: HashSet<ItemId> = reachable
            .into_iter()
            .filter_map(|item| ItemId::new(item).ok())
            .collect();
        let requested: Option<Vec<ItemId>> = item_ids
            .iter()
            .map(|item| ItemId::new(*item).ok())
            .collect();
        let Some(requested) = requested else {
            return Ok(Err(MarkReadRefusal::ItemNotYours));
        };
        if guard_marks_only_own_items(&requested, &reachable).is_err() {
            return Ok(Err(MarkReadRefusal::ItemNotYours));
        }
        sqlx::query(
            "UPDATE inbox_items SET read_at = $2 WHERE item_id = ANY($1) AND read_at IS NULL",
        )
        .bind(item_ids)
        .bind(at)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(Ok(()))
    }
}

async fn attachments_of(
    tx: &mut PgConnection,
    mail_ids: &[Uuid],
) -> Result<HashMap<Uuid, Vec<AttachmentMeta>>, PortError> {
    let rows = sqlx::query(
        "SELECT mail_id, attachment_id, name, content_type, size FROM mail_attachments \
         WHERE mail_id = ANY($1) AND listed ORDER BY mail_id, attachment_id",
    )
    .bind(mail_ids)
    .fetch_all(&mut *tx)
    .await
    .map_err(unavailable)?;
    let mut grouped: HashMap<Uuid, Vec<AttachmentMeta>> = HashMap::new();
    for row in &rows {
        grouped
            .entry(row.get("mail_id"))
            .or_default()
            .push(AttachmentMeta {
                attachment_id: bc_jobs::domain::ids::AttachmentId::new(row.get("attachment_id")),
                name: row.get("name"),
                content_type: row.get("content_type"),
                size: row.get("size"),
            });
    }
    Ok(grouped)
}

fn unread_item(
    row: &sqlx::postgres::PgRow,
    attachments: &mut HashMap<Uuid, Vec<AttachmentMeta>>,
) -> Result<UnreadItem, PortError> {
    let kind = ItemKind::from_db_str(&row.get::<String, _>("kind"))?;
    let mail_id: Option<Uuid> = row.get("mail_id");
    let mail = match (kind, mail_id) {
        (ItemKind::Mail, Some(mail_id)) => Some(UnreadMail {
            mail_id: MailId::new(mail_id),
            from: AgentId::new(row.get("sender_address")),
            author: row.get("author"),
            source_conversation: row.get("source_conversation_id"),
            subject: row.get("subject"),
            body: row.get("body"),
            attachments: attachments.remove(&mail_id).unwrap_or_default(),
        }),
        _ => None,
    };
    let notice = match row.get::<Option<serde_json::Value>, _>("notice") {
        Some(stored) => {
            Some(
                serde_json::from_value(stored).map_err(|error| PortError::Unavailable {
                    detail: error.to_string(),
                })?,
            )
        }
        None => None,
    };
    Ok(UnreadItem {
        item_id: ItemId::new(row.get("item_id"))?,
        seq: row.get("seq"),
        kind,
        address: row.get("address"),
        run_id: row.get("run_id"),
        accepted_at: row.get("accepted_at"),
        mail_id: mail_id.map(MailId::new),
        subject: row.get("subject"),
        mail,
        notice,
    })
}

fn runs_reachable(run_id: RunId, acts_for: &bc_jobs::domain::requester::ActsFor) -> Vec<Uuid> {
    let mut runs = vec![run_id.as_uuid()];
    runs.extend(acts_for.runs().map(|run| run.as_uuid()));
    runs
}
