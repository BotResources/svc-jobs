use bc_jobs::domain::mail::refused::{MailNoticeKey, mail_notice};
use bc_jobs::ports::PortError;
use bc_jobs::ports::mail::{Minting, RefusedMail};

use super::checks::guard_sender;
use crate::db::PgStore;
use crate::db::call_notices::{Delivering, NoticeFor, insert_notice};
use crate::db::hydrate::unavailable;
use crate::db::work::require_work;

pub(super) async fn note_refusal(
    store: &PgStore,
    request: RefusedMail<'_>,
    minting: Minting<'_>,
) -> Result<(), PortError> {
    let Some(notice) = mail_notice(request.mail_id, request.refusal) else {
        return Ok(());
    };
    let mut tx = store.begin().await?;
    let Ok(context) = require_work(&mut tx, request.work, true).await? else {
        return Ok(());
    };
    if guard_sender(&mut tx, request.from, context.run_id)
        .await?
        .is_err()
    {
        return Ok(());
    }
    let target = NoticeFor::mail_notice(
        context.run_id,
        request.from,
        stored_key(&request.refusal.notice_key(request.mail_id)),
    );
    insert_notice(
        &mut tx,
        target,
        &notice,
        Delivering {
            ids: minting.ids,
            metadata: minting.metadata,
            at: minting.at,
        },
    )
    .await?;
    tx.commit().await.map_err(unavailable)
}

fn stored_key(key: &MailNoticeKey) -> String {
    let named: Vec<String> = key
        .bad_addresses
        .iter()
        .map(|(address, reason)| format!("{}:{reason}", address.as_uuid()))
        .collect();
    format!(
        "{}/{}/{}",
        key.mail_id.as_uuid(),
        key.refusal,
        named.join(",")
    )
}
