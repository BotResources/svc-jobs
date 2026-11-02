use async_trait::async_trait;
use bc_jobs::domain::ids::ItemId;
use bc_jobs::domain::mail::content::{MailContent, SendAdmission};
use bc_jobs::domain::mail::delivery::{Deliveries, RecipientKind, decide_deliveries};
use bc_jobs::domain::mail::inbox::ItemKind;
use bc_jobs::domain::mail::refusal::MailRefusal;
use bc_jobs::domain::runner::refusal::RunnerRefusal;
use bc_jobs::ports::PortError;
use bc_jobs::ports::mail::{MailSender, Minting, RefusedMail, SendMail, Sent};
use uuid::Uuid;

use super::checks::{guard_attachments, guard_sender, recipients_of};
use super::live::live_run_of;
use super::load::load_mail;
use super::locks::lock_mail_attachments;
use super::refused::note_refusal;
use super::rows::{insert_item, insert_mail};
use crate::db::PgStore;
use crate::db::doorbell::{Arrival, Ring, ring};
use crate::db::hydrate::unavailable;
use crate::db::restart::wake_addresses;
use crate::db::work::require_work;

#[async_trait]
impl MailSender for PgStore {
    async fn send_mail(
        &self,
        request: SendMail<'_>,
        minting: Minting<'_>,
    ) -> Result<Result<Sent, RunnerRefusal>, PortError> {
        let mut tx = self.begin().await?;
        let context = match require_work(&mut tx, request.work, true).await? {
            Ok(context) => context,
            Err(refusal) => return Ok(Err(RunnerRefusal::Work(refusal))),
        };
        let content = request.content;
        if let Err(refusal) = guard_sender(&mut tx, content.from, context.run_id).await? {
            return Ok(Err(refusal.into()));
        }
        let known = load_mail(&mut tx, content.mail_id).await?;
        match MailContent::admit(known.as_ref().map(|view| &view.content), &content) {
            Ok(SendAdmission::Absorbed) => {
                return Ok(Ok(Sent {
                    refused_restarts: vec![],
                    wakes_restart: false,
                }));
            }
            Ok(SendAdmission::New) => {}
            Err(refusal) => return Ok(Err(refusal.into())),
        }
        if !content.attachments.is_empty() {
            lock_mail_attachments(&mut tx, content.mail_id.as_uuid()).await?;
        }
        if let Err(refusal) = guard_attachments(&mut tx, &content).await? {
            return Ok(Err(refusal.into()));
        }
        let recipients = recipients_of(&mut tx, &content).await?;
        let Deliveries(plan) = match decide_deliveries(context.initial_job_id, &recipients) {
            Ok(plan) => plan,
            Err(refusal) => return Ok(Err(refusal.into())),
        };
        let inserted = insert_mail(
            &mut tx,
            &content,
            context.run_id,
            context.initial_job_id,
            minting.at,
        )
        .await?;
        if !inserted {
            return Ok(Err(MailRefusal::MailIdReused.into()));
        }
        for delivery in &plan {
            let item_id = ItemId::new(minting.ids.next())?;
            let kind = match delivery.kind {
                RecipientKind::Direct => ItemKind::Mail,
                RecipientKind::Cc => ItemKind::CcNotice,
            };
            insert_item(
                &mut tx,
                item_id,
                kind,
                delivery.address,
                content.mail_id.as_uuid(),
                minting.at,
            )
            .await?;
            if (delivery.rings_live_run || delivery.starts_restart)
                && let Some(run_id) = live_run_of(&mut tx, delivery.address).await?
            {
                ring(
                    &mut tx,
                    Arrival {
                        run_id,
                        address: Some(delivery.address.as_uuid()),
                        item_id,
                        kind,
                    },
                    Ring {
                        ids: minting.ids,
                        metadata: minting.metadata,
                        at: minting.at,
                    },
                )
                .await?;
            }
        }
        let waking: Vec<Uuid> = plan
            .iter()
            .filter(|delivery| delivery.starts_restart)
            .map(|delivery| delivery.address.as_uuid())
            .collect();
        wake_addresses(&mut tx, &waking, minting.at).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(Ok(Sent {
            wakes_restart: !waking.is_empty(),
            refused_restarts: plan
                .iter()
                .filter_map(|delivery| delivery.refused_restart)
                .collect(),
        }))
    }
    async fn note_refused_mail(
        &self,
        request: RefusedMail<'_>,
        minting: Minting<'_>,
    ) -> Result<(), PortError> {
        note_refusal(self, request, minting).await
    }
}
