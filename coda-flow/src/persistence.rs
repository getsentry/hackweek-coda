use anyhow::Error;
use futures::future;
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};
use tracing::{event, Level};

pub struct Database {
    client: Client,
}

impl Database {
    pub async fn connect(
        host: &str,
        port: &str,
        user: &str,
        password: &str,
    ) -> Result<Self, Error> {
        let (client, connection) = tokio_postgres::connect(
            format!(
                "host={} port={} user={} password={}",
                host, port, user, password
            )
            .as_str(),
            NoTls,
        )
        .await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                event!(
                    Level::ERROR,
                    "error while connecting to the database: {}",
                    e
                );
            }
        });

        Ok(Database { client })
    }
}

const CREATE_WORKFLOWS_TABLE: &str = "CREATE TABLE IF NOT EXISTS coda_workflows (
id SERIAL PRIMARY KEY,
workflow_name VARCHAR(255),
workflow_run_id UUID,
params_id UUID,
workflow_state VARCHAR(255),
date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);";

const CREATE_WORKFLOWS_ITQ_TABLE: &str = "CREATE TABLE coda_workflows_itq (
id SERIAL PRIMARY KEY,
workflow_id INT REFERENCES coda_workflows(id),
date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);";

pub struct History {
    db: Arc<Database>,
}

impl History {
    pub async fn init(db: Arc<Database>) -> Self {
        History { db }
    }

    pub async fn prepare_tables(&self) -> Result<(), Error> {
        future::try_join(
            self.db.client.execute(CREATE_WORKFLOWS_TABLE, &[]),
            self.db.client.execute(CREATE_WORKFLOWS_ITQ_TABLE, &[]),
        )
        .await?;
        Ok(())
    }
}
