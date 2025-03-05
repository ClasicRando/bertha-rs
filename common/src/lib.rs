use derive_builder::Builder;
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::json;
use sqlx::Row;
use sqlx::postgres::PgRow;
use std::fmt::Formatter;
use time::{Duration, PrimitiveDateTime};

macro_rules! uuid_v7_type {
    ($typ:ident) => {
        #[derive(::serde::Serialize, ::serde::Deserialize, ::core::clone::Clone)]
        pub struct $typ(::uuid::Uuid);

        impl ::core::default::Default for $typ {
            fn default() -> Self {
                Self(::uuid::Uuid::now_v7())
            }
        }

        impl ::std::fmt::Display for $typ {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl ::core::ops::Deref for $typ {
            type Target = ::uuid::Uuid;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl ::core::convert::From<::uuid::Uuid> for $typ {
            fn from(value: ::uuid::Uuid) -> Self {
                Self(value)
            }
        }

        impl<'q> ::sqlx::Encode<'q, ::sqlx::postgres::Postgres> for $typ {
            fn encode_by_ref(
                &self,
                buf: &mut <::sqlx::postgres::Postgres as sqlx::Database>::ArgumentBuffer<'q>,
            ) -> ::core::result::Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                self.0.encode_by_ref(buf)
            }
        }

        impl<'r> ::sqlx::Decode<'r, ::sqlx::postgres::Postgres> for $typ {
            fn decode(
                value: <::sqlx::postgres::Postgres as sqlx::Database>::ValueRef<'r>,
            ) -> ::core::result::Result<Self, ::sqlx::error::BoxDynError> {
                ::uuid::Uuid::decode(value).map(|uuid| $typ(uuid))
            }
        }

        impl ::sqlx::types::Type<::sqlx::postgres::Postgres> for $typ {
            fn type_info() -> <::sqlx::Postgres as sqlx::Database>::TypeInfo {
                <::uuid::Uuid as ::sqlx::types::Type<::sqlx::postgres::Postgres>>::type_info()
            }
        }
    };
}

uuid_v7_type!(WorkflowId);

#[derive(Serialize, Deserialize, Builder, sqlx::FromRow)]
#[builder(pattern = "owned")]
pub struct Workflow {
    #[builder(default)]
    workflow_id: WorkflowId,
    name: String,
    description: String,
    #[builder(default = "1")]
    version: u32,
    #[builder(default)]
    failure_workflow: Option<WorkflowId>,
    #[builder(default)]
    input_variables: Vec<String>,
    #[builder(default)]
    output_variables: Vec<String>,
    #[builder(default = "true")]
    allow_restart: bool,
    #[builder(default = "Duration::seconds(0)")]
    timeout: Duration,
    maintainer_email: String,
    #[builder(default)]
    #[sqlx(default)]
    tasks: Vec<Task>,
}

impl Workflow {
    #[inline]
    pub fn workflow_id(&self) -> &WorkflowId {
        &self.workflow_id
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn description(&self) -> &str {
        &self.description
    }

    #[inline]
    pub fn version(&self) -> u32 {
        self.version
    }

    #[inline]
    pub fn failure_workflow(&self) -> Option<&WorkflowId> {
        self.failure_workflow.as_ref()
    }

    #[inline]
    pub fn input_variables(&self) -> &[String] {
        &self.input_variables
    }

    #[inline]
    pub fn output_variables(&self) -> &[String] {
        &self.output_variables
    }

    #[inline]
    pub fn allow_restart(&self) -> bool {
        self.allow_restart
    }

    #[inline]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    #[inline]
    pub fn maintainer_email(&self) -> &str {
        &self.maintainer_email
    }

    #[inline]
    pub fn tasks(&self) -> &[Task] {
        &self.tasks
    }

    #[inline]
    pub fn with_tasks(self, tasks: Vec<Task>) -> Self {
        Self {
            workflow_id: self.workflow_id,
            name: self.name,
            description: self.description,
            version: self.version,
            failure_workflow: self.failure_workflow,
            input_variables: self.input_variables,
            output_variables: self.output_variables,
            allow_restart: self.allow_restart,
            timeout: self.timeout,
            maintainer_email: self.maintainer_email,
            tasks,
        }
    }
}

uuid_v7_type!(TaskId);

#[derive(Serialize, Deserialize, Builder)]
#[builder(pattern = "owned")]
pub struct Task {
    #[builder(default)]
    task_id: TaskId,
    name: String,
    description: String,
    #[builder(default = "0")]
    retry_count: u32,
    #[builder(default)]
    retry_strategy: RetryStrategy,
    #[builder(default)]
    timeout_resolve: TimeoutResolve,
    #[builder(default = "Duration::seconds(0)")]
    timeout: Duration,
    #[builder(default = "Duration::seconds(60)")]
    worker_heartbeat_timeout: Duration,
    #[builder(default = "Duration::seconds(0)")]
    worker_reserve_timeout: Duration,
    #[builder(default)]
    input_variables: Vec<String>,
    #[builder(default)]
    output_variables: Vec<String>,
    #[builder(default = "0")]
    max_running_instances: u32,
    maintainer_email: String,
}

impl Task {
    #[inline]
    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn description(&self) -> &str {
        &self.description
    }

    #[inline]
    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }

    #[inline]
    pub fn retry_strategy(&self) -> &RetryStrategy {
        &self.retry_strategy
    }

    #[inline]
    pub fn timeout_resolve(&self) -> &TimeoutResolve {
        &self.timeout_resolve
    }

    #[inline]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    #[inline]
    pub fn worker_heartbeat_timeout(&self) -> Duration {
        self.worker_heartbeat_timeout
    }

    #[inline]
    pub fn worker_reserve_timeout(&self) -> Duration {
        self.worker_reserve_timeout
    }

    #[inline]
    pub fn input_variables(&self) -> &[String] {
        &self.input_variables
    }

    #[inline]
    pub fn output_variables(&self) -> &[String] {
        &self.output_variables
    }

    #[inline]
    pub fn max_running_instances(&self) -> u32 {
        self.max_running_instances
    }

    #[inline]
    pub fn maintainer_email(&self) -> &str {
        &self.maintainer_email
    }
}

impl<'r> sqlx::FromRow<'r, PgRow> for Task {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let task_id = row.try_get("task_id")?;
        let retry_strategy: &'r str = row.try_get("retry_strategy")?;
        let retry_delay = row.try_get::<i32, _>("retry_delay")?.unsigned_abs();
        let backoff_rate: f32 = row.try_get("retry_backoff_rate")?;
        let retry_strategy = match retry_strategy {
            "manual" => RetryStrategy::Manual,
            "fixed" => RetryStrategy::Fixed { delay: retry_delay },
            "linear" => RetryStrategy::Linear {
                delay: retry_delay,
                backoff_rate,
            },
            "exponential" => RetryStrategy::Exponential { delay: retry_delay },
            _ => {
                return Err(sqlx::Error::ColumnDecode {
                    index: "retry_strategy".to_string(),
                    source: format!(
                        "Could not decode retry_strategy ({retry_strategy}) in task = {task_id}"
                    )
                    .into(),
                });
            },
        };
        let timeout: i32 = row.try_get("timeout")?;
        let worker_heartbeat_timeout: i32 = row.try_get("worker_heartbeat_timeout")?;
        let worker_reserve_timeout: i32 = row.try_get("worker_reserve_timeout")?;
        Ok(Self {
            task_id,
            name: row.try_get("name")?,
            description: row.try_get("description")?,
            retry_count: row.try_get::<i32, _>("retry_count")?.unsigned_abs(),
            retry_strategy,
            timeout_resolve: row.try_get("task_id")?,
            timeout: Duration::seconds(timeout as i64),
            worker_heartbeat_timeout: Duration::seconds(worker_heartbeat_timeout as i64),
            worker_reserve_timeout: Duration::seconds(worker_reserve_timeout as i64),
            input_variables: row.try_get("input_variables")?,
            output_variables: row.try_get("output_variables")?,
            max_running_instances: row
                .try_get::<i32, _>("max_running_instances")?
                .unsigned_abs(),
            maintainer_email: row.try_get("maintainer_email")?,
        })
    }
}

#[derive(Default, Serialize, Deserialize, Clone)]
pub enum RetryStrategy {
    #[default]
    Manual,
    Fixed {
        delay: u32,
    },
    Linear {
        delay: u32,
        backoff_rate: f32,
    },
    Exponential {
        delay: u32,
    },
}

#[derive(sqlx::Type, Default, Serialize, Deserialize, Clone)]
#[sqlx(rename_all = "lowercase")]
pub enum TimeoutResolve {
    #[default]
    Cascade,
    Retry,
}

#[derive(sqlx::Type, Default, Serialize, Deserialize, Clone)]
#[repr(i16)]
pub enum WorkflowRunStatus {
    Canceled = -3,
    TimedOut = -2,
    Failed = -1,
    #[default]
    Running = 1,
    Completed = 2,
    Paused = 3,
}

fn deserialize_input_data<'de, D>(deserializer: D) -> Result<serde_json::Value, D::Error>
where
    D: Deserializer<'de>,
{
    let Some(value) = Option::<serde_json::Value>::deserialize(deserializer)? else {
        return Ok(json!({}));
    };

    if matches!(value, serde_json::Value::Object(_)) {
        return Ok(value);
    }

    Err(Error::custom(format!(
        "input_data must be an object. Found {value}"
    )))
}

fn default_input_data() -> serde_json::Value {
    json!({})
}

#[derive(Serialize, Deserialize)]
pub struct WorkflowRunRequest {
    workflow_id: WorkflowId,
    #[serde(deserialize_with = "deserialize_input_data")]
    #[serde(default = "default_input_data")]
    input_data: serde_json::Value,
}

impl WorkflowRunRequest {
    pub fn new(workflow_id: WorkflowId, input_data: serde_json::Value) -> Self {
        Self {
            workflow_id,
            input_data,
        }
    }

    #[inline]
    pub fn workflow_id(&self) -> &WorkflowId {
        &self.workflow_id
    }

    #[inline]
    pub fn input_data(&self) -> &serde_json::Value {
        &self.input_data
    }
}

uuid_v7_type!(WorkflowRunId);

#[derive(Serialize, Deserialize, sqlx::FromRow)]
pub struct WorkflowRun {
    run_id: WorkflowRunId,
    workflow_id: WorkflowId,
    run_status: WorkflowRunStatus,
    start_time: PrimitiveDateTime,
    end_time: Option<PrimitiveDateTime>,
    input_data: Option<serde_json::Value>,
    output_data: Option<serde_json::Value>,
    #[sqlx(default)]
    tasks: Vec<WorkflowRunTask>,
}

impl WorkflowRun {
    #[inline]
    pub fn run_id(&self) -> &WorkflowRunId {
        &self.run_id
    }

    #[inline]
    pub fn workflow_id(&self) -> &WorkflowId {
        &self.workflow_id
    }

    #[inline]
    pub fn run_status(&self) -> &WorkflowRunStatus {
        &self.run_status
    }

    #[inline]
    pub fn start_time(&self) -> PrimitiveDateTime {
        self.start_time
    }

    #[inline]
    pub fn end_time(&self) -> Option<PrimitiveDateTime> {
        self.end_time
    }

    #[inline]
    pub fn input_data(&self) -> Option<&serde_json::Value> {
        self.input_data.as_ref()
    }

    #[inline]
    pub fn output_data(&self) -> Option<&serde_json::Value> {
        self.output_data.as_ref()
    }

    #[inline]
    pub fn tasks(&self) -> &[WorkflowRunTask] {
        &self.tasks
    }

    #[inline]
    pub fn with_tasks(self, tasks: Vec<WorkflowRunTask>) -> Self {
        Self {
            run_id: self.run_id,
            workflow_id: self.workflow_id,
            run_status: self.run_status,
            start_time: self.start_time,
            end_time: self.end_time,
            input_data: self.input_data,
            output_data: self.output_data,
            tasks,
        }
    }
}

#[derive(Serialize, Deserialize, sqlx::FromRow)]
pub struct WorkflowRunTask {
    run_id: WorkflowRunId,
    #[sqlx(try_from = "i32")]
    task_order: u32,
    input_data: Option<serde_json::Value>,
    output_data: Option<serde_json::Value>,
    task_status: WorkflowRunTaskStatus,
}

impl WorkflowRunTask {
    #[inline]
    pub fn run_id(&self) -> &WorkflowRunId {
        &self.run_id
    }

    #[inline]
    pub fn task_order(&self) -> u32 {
        self.task_order
    }

    #[inline]
    pub fn input_data(&self) -> Option<&serde_json::Value> {
        self.input_data.as_ref()
    }

    #[inline]
    pub fn output_data(&self) -> Option<&serde_json::Value> {
        self.output_data.as_ref()
    }

    #[inline]
    pub fn task_status(&self) -> WorkflowRunTaskStatus {
        self.task_status
    }
}

#[derive(sqlx::Type, Serialize, Deserialize, Clone, PartialEq, Copy)]
#[repr(i16)]
pub enum WorkflowRunTaskStatus {
    Canceled = -3,
    TimedOut = -2,
    Failed = -1,
    Pending = 0,
    Scheduled = 1,
    Running = 2,
    Completed = 3,
}

uuid_v7_type!(WorkerId);

pub struct TaskResult {
    run_id: WorkflowRunId,
    task_order: u32,
    status: TaskResultStatus,
}

impl TaskResult {
    fn new(workflow_run_task: &WorkflowRunTask, status: TaskResultStatus) -> Self {
        Self {
            run_id: workflow_run_task.run_id.clone(),
            task_order: workflow_run_task.task_order,
            status,
        }
    }

    /// Create a new [TaskResult] with a [TaskResultStatus::InProgress] status. This includes no
    /// progress update to the dispatcher node.
    pub fn in_progress(workflow_run_task: &WorkflowRunTask, worker_id: WorkerId) -> Self {
        Self::new(
            workflow_run_task,
            TaskResultStatus::InProgress {
                worker_id,
                percent_completed: None,
            },
        )
    }

    /// Create a new [TaskResult] with a [TaskResultStatus::InProgress] status. This requires a
    /// progress update to the dispatcher node.
    pub fn progress_update(
        workflow_run_task: &WorkflowRunTask,
        worker_id: WorkerId,
        percent_completed: u8,
    ) -> Self {
        Self::new(
            workflow_run_task,
            TaskResultStatus::InProgress {
                worker_id,
                percent_completed: Some(percent_completed),
            },
        )
    }

    /// Create a new [TaskResult] with a [TaskResultStatus::Failed] status. Provides an error
    /// message to describe why the task failed.
    pub fn failed(workflow_run_task: WorkflowRunTask, error_message: String) -> Self {
        Self::new(
            &workflow_run_task,
            TaskResultStatus::Failed { error_message },
        )
    }
    
    /// Create a new [TaskResult] with a [TaskResultStatus::Failed] status. Provides a
    /// [std::error::Error] type as the failure cause. This will be converted to a string.
    pub fn error<E: std::error::Error>(workflow_run_task: WorkflowRunTask, error: E) -> Self {
        Self::new(
            &workflow_run_task,
            TaskResultStatus::Failed {
                error_message: error.to_string(),
            },
        )
    }

    /// Create a new [TaskResult] with a [TaskResultStatus::Completed] status. General constructor
    /// where both output fields are optional.
    pub fn completed(
        workflow_run_task: WorkflowRunTask,
        output_message: Option<String>,
        output_data: Option<serde_json::Value>,
    ) -> Self {
        Self::new(
            &workflow_run_task,
            TaskResultStatus::Completed {
                output_message,
                output_data,
            },
        )
    }

    /// Create a new [TaskResult] with a [TaskResultStatus::Completed] status. Specialized
    /// constructor for results that only contain an output message and no output date.
    pub fn completed_with_message(
        workflow_run_task: WorkflowRunTask,
        output_message: String,
    ) -> Self {
        Self::completed(workflow_run_task, Some(output_message), None)
    }

    /// Create a new [TaskResult] with a [TaskResultStatus::Completed] status. Specialized
    /// constructor for results that only contain output data and no output message.
    pub fn completed_with_data(
        workflow_run_task: WorkflowRunTask,
        output_data: serde_json::Value,
    ) -> Self {
        Self::completed(workflow_run_task, None, Some(output_data))
    }

    pub fn run_id(&self) -> &WorkflowRunId {
        &self.run_id
    }

    pub fn task_order(&self) -> u32 {
        self.task_order
    }

    pub fn status(&self) -> &TaskResultStatus {
        &self.status
    }
}

pub enum TaskResultStatus {
    InProgress {
        worker_id: WorkerId,
        percent_completed: Option<u8>,
    },
    Failed {
        error_message: String,
    },
    Completed {
        output_message: Option<String>,
        output_data: Option<serde_json::Value>,
    },
}
