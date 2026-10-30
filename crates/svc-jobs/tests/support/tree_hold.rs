use sqlx::{Connection, PgConnection};
use uuid::Uuid;

pub struct TreeHold {
    connection: PgConnection,
}

impl TreeHold {
    pub async fn take(app_url: &str, initial_job_id: Uuid) -> Self {
        let mut connection = PgConnection::connect(app_url)
            .await
            .expect("the holder connects through the application role");
        sqlx::query("BEGIN")
            .execute(&mut connection)
            .await
            .expect("the holder opens a transaction");
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("directory:{initial_job_id}"))
            .execute(&mut connection)
            .await
            .expect("the holder takes the Tree lock");
        Self { connection }
    }

    pub async fn release(mut self) {
        sqlx::query("ROLLBACK")
            .execute(&mut self.connection)
            .await
            .expect("the holder lets the Tree go");
        self.connection
            .close()
            .await
            .expect("the holder closes its connection");
    }
}
