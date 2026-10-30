use async_trait::async_trait;
use bc_jobs::domain::ids::{AgentId, MailId};
use bc_jobs::domain::mail::access::guard_reads_mail;
use bc_jobs::domain::mail::delivery::RecipientKind;
use bc_jobs::domain::mail::refusal::MailRefusal;
use bc_jobs::domain::mail::views::MailRead;
use bc_jobs::domain::runner::refusal::RunnerRefusal;
use bc_jobs::domain::runner::work::WorkNamed;
use bc_jobs::ports::PortError;
use bc_jobs::ports::mail::MailReader;
use sqlx::Row;

use super::attachments::listed_attachments;
use super::load::load_mail;
use crate::db::PgStore;
use crate::db::hydrate::unavailable;
use crate::db::work::{addresses_of_run, require_work};

#[async_trait]
impl MailReader for PgStore {
    async fn read_mail(
        &self,
        work: WorkNamed<'_>,
        mail_id: MailId,
        address: AgentId,
    ) -> Result<Result<MailRead, RunnerRefusal>, PortError> {
        let mut tx = self.begin().await?;
        let context = match require_work(&mut tx, work, false).await? {
            Ok(context) => context,
            Err(refusal) => return Ok(Err(RunnerRefusal::Work(refusal))),
        };
        let addresses = addresses_of_run(&mut tx, context.run_id).await?;
        let kind =
            sqlx::query("SELECT kind FROM mail_recipients WHERE mail_id = $1 AND address = $2")
                .bind(mail_id.as_uuid())
                .bind(address.as_uuid())
                .fetch_optional(&mut *tx)
                .await
                .map_err(unavailable)?
                .map(|row| match row.get::<String, _>("kind").as_str() {
                    "DIRECT" => RecipientKind::Direct,
                    _ => RecipientKind::Cc,
                });
        let read_as = match guard_reads_mail(address, &addresses, kind) {
            Ok(read_as) => read_as,
            Err(refusal) => return Ok(Err(refusal.into())),
        };
        let Some(view) = load_mail(&mut tx, mail_id).await? else {
            return Ok(Err(MailRefusal::MailNotYours.into()));
        };
        let attachments = listed_attachments(&mut tx, mail_id).await?;
        Ok(Ok(MailRead {
            view,
            attachments,
            read_as,
        }))
    }
}
