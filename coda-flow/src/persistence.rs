use crate::entities::{Workflow, WorkflowState};
use anyhow::Error;
use futures::future;
use std::fmt;
use std::sync::{Arc, Mutex};
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
id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
workflow_name VARCHAR(255),
workflow_run_id UUID,
workflow_state VARCHAR(255),
workflow_params_id UUID NULL,
date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);";

const CREATE_WORKFLOWS_ITQ_TABLE: &str = "CREATE TABLE IF NOT EXISTS coda_workflows_itq (
id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
workflow_id INT REFERENCES coda_workflows(id),
date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);";

const SELECT_WORKFLOW_BY_NAME_AND_RUN_ID: &str = "SELECT *
FROM coda_workflows
WHERE workflow_name = $1 AND workflow_run_id = $2
FOR UPDATE
LIMIT 1
";

const INSERT_WORKFLOW_INTO_WORKFLOWS: &str = "INSERT INTO coda_workflows
(workflow_name, workflow_run_id, workflow_state, workflow_params_id)
VALUES ($1, $2, $3, $4)
RETURNING id
";

const INSERT_WORKFLOW_INTO_WORKFLOWS_ITQ: &str = "INSERT INTO coda_workflows_itq
(workflow_id)
VALUES ($1)
";

pub struct FlowHistory {
    db: Arc<Mutex<Database>>,
}

impl FlowHistory {
    pub async fn init(db: Arc<Mutex<Database>>) -> Result<Self, Error> {
        let history = FlowHistory { db };
        history.prepare_tables().await?;

        Ok(history)
    }

    pub async fn prepare_tables(&self) -> Result<(), Error> {
        let mut db = self.db.lock().unwrap();
        future::try_join(
            db.client.execute(CREATE_WORKFLOWS_TABLE, &[]),
            db.client.execute(CREATE_WORKFLOWS_ITQ_TABLE, &[]),
        )
        .await?;
        Ok(())
    }

    pub async fn enqueue_workflow(&self, workflow: Workflow) -> Result<(), Error> {
        let mut db = self.db.lock().unwrap();
        let transaction = db.client.transaction().await?;

        let workflows = transaction
            .query(
                SELECT_WORKFLOW_BY_NAME_AND_RUN_ID,
                &[&workflow.workflow_name, &workflow.workflow_run_id],
            )
            .await?;

        let workflow_id: i32 = if workflows.is_empty() {
            transaction
                .query(
                    INSERT_WORKFLOW_INTO_WORKFLOWS,
                    &[
                        &workflow.workflow_name,
                        &workflow.workflow_run_id,
                        &WorkflowState::Created.to_string(),
                        &workflow.workflow_params_id,
                    ],
                )
                .await?[0]
                .get("id")
        } else {
            workflows[0].get("id")
        };

        transaction
            .execute(INSERT_WORKFLOW_INTO_WORKFLOWS_ITQ, &[&workflow_id])
            .await?;

        transaction.commit().await?;
        Ok(())
    }

    pub async fn dequeue_workflow(&self) -> Result<(), Error> {
        Ok(())
    }

    pub async fn update_workflow_state(&self) -> Result<(), Error> {
        Ok(())
    }
}
