mod support;

use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail};
use support::tree::correspondents;
use uuid::Uuid;

const ROUNDS: usize = 30;

fn outcome(response: &serde_json::Value, name: &str) -> Result<(), String> {
    let result = gesture::field(response, name);
    if result["success"] == true {
        Ok(())
    } else {
        Err(result["reason"].as_str().unwrap_or_default().to_owned())
    }
}

#[tokio::test]
async fn two_trees_uploading_the_same_attachment_id_at_once_keep_one_content_and_one_uploader() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let gql = fixture.gql();
    let runner = runner_access();
    let left = correspondents(&fixture, "collision-attach-left").await;
    let right = correspondents(&fixture, "collision-attach-right").await;

    for round in 0..ROUNDS {
        let left_mail = Mail::new(left.sender, &[left.receiver]);
        let mut right_mail = Mail::new(right.sender, &[right.receiver]);
        right_mail.mail_id = left_mail.mail_id;
        let attachment_id = Uuid::now_v7();
        let (left_content, right_content) = (format!("left {round}"), format!("right {round}"));

        let (from_left, from_right) = tokio::join!(
            gesture::upload_attachment(
                &gql,
                &runner,
                &left.parent_work,
                &left_mail,
                attachment_id,
                "note.txt",
                &left_content,
            ),
            gesture::upload_attachment(
                &gql,
                &runner,
                &right.parent_work,
                &right_mail,
                attachment_id,
                "note.txt",
                &right_content,
            ),
        );
        let left_outcome = outcome(&from_left, "jobsUploadAttachment");
        let right_outcome = outcome(&from_right, "jobsUploadAttachment");
        let (winner, loser, winner_from, winner_content) = match (&left_outcome, &right_outcome) {
            (Ok(()), Err(reason)) => ("left", reason, left.sender, left_content),
            (Err(reason), Ok(())) => ("right", reason, right.sender, right_content),
            both => panic!("exactly one uploader wins the id in round {round}: {both:?}"),
        };
        assert_eq!(loser, "mail_id_reused", "{winner} won round {round}");
        let (uploader, content): (Uuid, String) = sqlx::query_as(
            "SELECT uploader_address, content FROM mail_attachments \
             WHERE mail_id = $1 AND attachment_id = $2",
        )
        .bind(left_mail.mail_id)
        .bind(attachment_id)
        .fetch_one(durable.pool())
        .await
        .expect("the attachment row reads");
        assert_eq!(uploader, winner_from, "round {round}");
        assert_eq!(content, winner_content, "round {round}");
    }
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn two_trees_sending_the_same_mail_id_at_once_get_one_mail_and_one_mail_id_reused() {
    let fixture = JobsFixture::start().await;
    let gql = fixture.gql();
    let runner = runner_access();
    let left = correspondents(&fixture, "collision-send-left").await;
    let right = correspondents(&fixture, "collision-send-right").await;

    for round in 0..ROUNDS {
        let left_mail = Mail::new(left.sender, &[left.receiver]).with_body("left");
        let mut right_mail = Mail::new(right.sender, &[right.receiver]).with_body("right");
        right_mail.mail_id = left_mail.mail_id;

        let (from_left, from_right) = tokio::join!(
            gesture::send_mail(&gql, &runner, &left.parent_work, &left_mail),
            gesture::send_mail(&gql, &runner, &right.parent_work, &right_mail),
        );
        let results = [
            outcome(&from_left, "jobsSendMail"),
            outcome(&from_right, "jobsSendMail"),
        ];
        let accepted = results.iter().filter(|result| result.is_ok()).count();
        assert_eq!(
            accepted, 1,
            "one mail is accepted in round {round}: {results:?}"
        );
        assert!(
            results
                .iter()
                .any(|result| result == &Err("mail_id_reused".to_owned())),
            "the other is refused as mail_id_reused in round {round}: {results:?}"
        );
    }
    fixture.shutdown().await;
}
